use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing_test::traced_test;

use d_engine_server::{EmbeddedEngine, RocksDBStateMachine, RocksDBStorageEngine};

use crate::common::{create_node_config, get_available_ports, node_config, reset};

const TEST_DIR: &str = "embedded/failover";
const DB_ROOT_DIR: &str = "./db/embedded/failover";
const LOG_DIR: &str = "./logs/embedded/failover";

/// Test 3-node cluster leader failover with EmbeddedEngine API
///
/// Scenario:
/// 1. Start 3-node cluster
/// 2. Kill leader node
/// 3. Verify re-election and data consistency
/// 4. Restart killed node and verify rejoin
#[tokio::test]
#[traced_test]
#[cfg(feature = "rocksdb")]
async fn test_embedded_leader_failover() -> Result<(), Box<dyn std::error::Error>> {
    reset(TEST_DIR).await?;

    let ports = get_available_ports(3).await;

    info!("Starting 3-node cluster");

    let mut engines = Vec::new();
    let mut configs = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let config_str = create_node_config(node_id, ports[i], &ports, DB_ROOT_DIR, LOG_DIR).await;
        let config = node_config(&config_str);

        let storage_path = config.cluster.db_root_dir.join("storage");
        let sm_path = config.cluster.db_root_dir.join("state_machine");

        tokio::fs::create_dir_all(&storage_path).await?;
        tokio::fs::create_dir_all(&sm_path).await?;

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

        let config_path = format!("/tmp/d-engine-test-failover-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        configs.push((config_str, config_path));

        let engine = EmbeddedEngine::start(Some(&configs[i].1), storage, state_machine).await?;
        engines.push(engine);
    }

    // Wait for cluster initialization
    for engine in &engines {
        engine.ready().await;
    }

    info!("All nodes initialized, waiting for leader election");

    let initial_leader = engines[0].wait_leader(Duration::from_secs(10)).await?;
    info!(
        "Initial leader elected: {} (term {})",
        initial_leader.leader_id, initial_leader.term
    );

    // Write test data before failover
    engines[0]
        .client()
        .put(b"before-failover".to_vec(), b"initial-value".to_vec())
        .await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    let val = engines[0].client().get(b"before-failover".to_vec()).await?;
    assert_eq!(val, Some(b"initial-value".to_vec()));

    info!("Initial data written successfully");

    // Subscribe to leader changes on node 2
    let mut leader_rx = engines[1].leader_notifier();

    // Kill node 1 (likely the initial leader)
    info!("Killing node 1");
    let killed_engine = engines.remove(0);
    let killed_config = configs.remove(0);
    killed_engine.stop().await?;

    // Wait for re-election event
    info!("Waiting for re-election");
    tokio::time::timeout(Duration::from_secs(5), leader_rx.changed())
        .await
        .expect("Should receive leader change notification")?;

    let new_leader = leader_rx.borrow().clone();
    assert!(new_leader.is_some(), "New leader should be elected");

    let new_leader_info = new_leader.unwrap();
    assert_ne!(
        new_leader_info.leader_id, 1,
        "New leader should not be node 1"
    );
    info!(
        "New leader elected: {} (term {})",
        new_leader_info.leader_id, new_leader_info.term
    );

    // Cluster should still be operational with 2/3 nodes
    engines[0]
        .client()
        .put(b"after-failover".to_vec(), b"still-works".to_vec())
        .await?;

    // Verify old data still readable
    let old_val = engines[0].client().get(b"before-failover".to_vec()).await?;
    assert_eq!(
        old_val,
        Some(b"initial-value".to_vec()),
        "Old data should be preserved"
    );

    // Verify new data written successfully
    let new_val = engines[0].client().get(b"after-failover".to_vec()).await?;
    assert_eq!(
        new_val,
        Some(b"still-works".to_vec()),
        "New data should be written"
    );

    info!("Cluster operational with 2/3 nodes");

    // Restart node 1 and verify it rejoins
    info!("Restarting node 1");
    {
        let config = node_config(&killed_config.0);
        let storage_path = config.cluster.db_root_dir.join("storage");
        let sm_path = config.cluster.db_root_dir.join("state_machine");

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

        let restarted_engine =
            EmbeddedEngine::start(Some(&killed_config.1), storage, state_machine).await?;

        restarted_engine.ready().await;

        // Wait for sync
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify restarted node synced data from cluster
        let synced_val = restarted_engine.client().get(b"after-failover".to_vec()).await?;
        assert_eq!(
            synced_val,
            Some(b"still-works".to_vec()),
            "Restarted node should sync cluster data"
        );

        info!("Node 1 rejoined and synced successfully");

        engines.insert(0, restarted_engine);
    }

    // Cleanup
    for engine in engines {
        engine.stop().await?;
    }

    Ok(())
}

/// Test minority failure (2/3 nodes down) causes cluster unavailability
#[tokio::test]
#[traced_test]
#[cfg(feature = "rocksdb")]
async fn test_minority_failure_blocks_writes() -> Result<(), Box<dyn std::error::Error>> {
    reset(&format!("{TEST_DIR}_minority")).await?;

    let ports = get_available_ports(3).await;
    let db_root = format!("{DB_ROOT_DIR}_minority");
    let log_dir = format!("{LOG_DIR}_minority");

    info!("Starting 3-node cluster for minority failure test");

    let mut engines = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let config_str = create_node_config(node_id, ports[i], &ports, &db_root, &log_dir).await;
        let config = node_config(&config_str);

        let storage_path = config.cluster.db_root_dir.join("storage");
        let sm_path = config.cluster.db_root_dir.join("state_machine");

        tokio::fs::create_dir_all(&storage_path).await?;
        tokio::fs::create_dir_all(&sm_path).await?;

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

        let config_path = format!("/tmp/d-engine-test-minority-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        let engine = EmbeddedEngine::start(Some(&config_path), storage, state_machine).await?;
        engines.push(engine);
    }

    for engine in &engines {
        engine.ready().await;
    }

    engines[0].wait_leader(Duration::from_secs(10)).await?;

    // Write initial data
    engines[0].client().put(b"test-key".to_vec(), b"test-value".to_vec()).await?;

    info!("Killing 2 nodes to lose majority");

    // Kill nodes 1 and 2 (lose majority)
    let engine1 = engines.remove(0);
    let engine2 = engines.remove(0);

    engine1.stop().await?;
    engine2.stop().await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    info!("2 nodes killed, verifying cluster cannot serve writes");

    // Remaining single node should reject writes (no majority)
    let write_result = tokio::time::timeout(
        Duration::from_secs(3),
        engines[0].client().put(b"should-fail".to_vec(), b"no-majority".to_vec()),
    )
    .await;

    // Expect timeout or error
    assert!(
        write_result.is_err() || write_result.unwrap().is_err(),
        "Write should fail without majority"
    );

    info!("Minority failure test passed - cluster correctly refused writes");

    // Cleanup
    engines[0].stop().await?;

    Ok(())
}
