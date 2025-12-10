use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing_test::traced_test;

use d_engine_server::{EmbeddedEngine, RocksDBStateMachine, RocksDBStorageEngine};

use crate::common::{create_node_config, get_available_ports, node_config, reset};

const TEST_DIR: &str = "embedded/scale_to_cluster";
const DB_ROOT_DIR: &str = "./db/embedded/scale_to_cluster";
const LOG_DIR: &str = "./logs/embedded/scale_to_cluster";

/// Test scaling from single-node to 3-node cluster
///
/// Scenario:
/// 1. Start single node, write data
/// 2. Stop single node
/// 3. Restart as 3-node cluster with same data directory
/// 4. Verify cluster healthy and data preserved
#[tokio::test]
#[traced_test]
#[cfg(feature = "rocksdb")]
async fn test_scale_single_to_cluster() -> Result<(), Box<dyn std::error::Error>> {
    reset(TEST_DIR).await?;

    let _port_guard = get_available_ports(3).await;
    let ports = _port_guard.as_slice();
    let node1_data_dir = format!("{DB_ROOT_DIR}/node1");

    // Phase 1: Single-node development environment
    info!("Phase 1: Starting single-node mode");
    {
        // Create config file with longer timeout
        let config_content = r#"
[raft]
general_raft_timeout_duration_in_ms = 5000
"#
        .to_string();
        let config_path = "/tmp/scale_to_cluster_test_phase1.toml";
        tokio::fs::write(config_path, config_content).await?;

        let engine = EmbeddedEngine::with_rocksdb(&node1_data_dir, Some(config_path)).await?;
        engine.ready().await;

        let leader = engine.wait_leader(Duration::from_secs(2)).await?;
        info!(
            "Single-node leader elected: {} (term {})",
            leader.leader_id, leader.term
        );

        // Write development data
        engine.client().put(b"dev-key".to_vec(), b"dev-value".to_vec()).await?;
        engine.client().put(b"app-version".to_vec(), b"1.0".to_vec()).await?;

        // Wait for commit to propagate
        tokio::time::sleep(Duration::from_millis(100)).await;

        let val = engine.client().get_linearizable(b"dev-key".to_vec()).await?;
        assert_eq!(val.as_deref(), Some(b"dev-value".as_ref()));

        info!("Single-node data written successfully");
        engine.stop().await?;
    }

    // Phase 2: Scale to 3-node cluster
    info!("Phase 2: Scaling to 3-node cluster");

    let mut engines = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let mut config_str =
            create_node_config(node_id, ports[i], ports, DB_ROOT_DIR, LOG_DIR).await;

        // Add timeout config
        config_str.push_str("\n[raft]\ngeneral_raft_timeout_duration_in_ms = 5000\n");

        let config = node_config(&config_str);

        // Each node needs its own storage directory to avoid RocksDB lock conflicts
        let node_db_root = config.cluster.db_root_dir.join(format!("node{node_id}"));
        let storage_path = node_db_root.join("storage");
        let sm_path = node_db_root.join("state_machine");

        tokio::fs::create_dir_all(&storage_path).await?;
        tokio::fs::create_dir_all(&sm_path).await?;

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

        // Write config to temp file for this test
        let config_path = format!("/tmp/d-engine-test-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        let engine = EmbeddedEngine::start(Some(&config_path), storage, state_machine).await?;
        engines.push(engine);
    }

    // Wait for all nodes to initialize
    for engine in &engines {
        engine.ready().await;
    }

    info!("All 3 nodes initialized, waiting for leader election");

    // Wait for leader election in cluster mode
    let leader = engines[0].wait_leader(Duration::from_secs(10)).await?;
    info!(
        "Cluster leader elected: {} (term {})",
        leader.leader_id, leader.term
    );

    // Phase 3: Verify cluster operational
    info!("Phase 3: Verifying cluster health");

    // Old data should still be readable (from single-node phase)
    // Note: This assumes node 1 retained its data directory
    let old_val = engines[0].client().get_eventual(b"dev-key".to_vec()).await?;
    assert_eq!(
        old_val.as_deref(),
        Some(b"dev-value".as_ref()),
        "Single-node data should be preserved"
    );

    // Write new data in cluster mode
    engines[0]
        .client()
        .put(b"cluster-key".to_vec(), b"cluster-value".to_vec())
        .await?;

    // Allow replication time
    tokio::time::sleep(Duration::from_millis(500)).await;

    // All nodes should be able to read cluster data
    for (i, engine) in engines.iter().enumerate() {
        let val = engine.client().get_eventual(b"cluster-key".to_vec()).await?;
        assert_eq!(
            val.as_deref(),
            Some(b"cluster-value".as_ref()),
            "Node {} should read cluster data",
            i + 1
        );
    }

    info!("Cluster operational, all nodes can read/write");

    // Cleanup
    for engine in engines {
        engine.stop().await?;
    }

    Ok(())
}

/// Test that 3-node cluster can continue after 1 node failure
#[tokio::test]
#[traced_test]
#[cfg(feature = "rocksdb")]
async fn test_cluster_survives_single_failure() -> Result<(), Box<dyn std::error::Error>> {
    reset(&format!("{TEST_DIR}_failover")).await?;

    let _port_guard = get_available_ports(3).await;
    let ports = _port_guard.as_slice();
    let db_root = format!("{DB_ROOT_DIR}_failover");
    let log_dir = format!("{LOG_DIR}_failover");

    info!("Starting 3-node cluster");

    let mut engines = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let mut config_str =
            create_node_config(node_id, ports[i], ports, &db_root, &log_dir).await;

        // Add timeout config
        config_str.push_str("\n[raft]\ngeneral_raft_timeout_duration_in_ms = 5000\n");

        let config = node_config(&config_str);

        // Each node needs its own storage directory to avoid RocksDB lock conflicts
        let node_db_root = config.cluster.db_root_dir.join(format!("node{node_id}"));
        let storage_path = node_db_root.join("storage");
        let sm_path = node_db_root.join("state_machine");

        tokio::fs::create_dir_all(&storage_path).await?;
        tokio::fs::create_dir_all(&sm_path).await?;

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

        let config_path = format!("/tmp/d-engine-test-failover-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        let engine = EmbeddedEngine::start(Some(&config_path), storage, state_machine).await?;
        engines.push(engine);
    }

    for engine in &engines {
        engine.ready().await;
    }

    let leader = engines[0].wait_leader(Duration::from_secs(10)).await?;
    info!("Initial leader: {}", leader.leader_id);

    // Write data before failure - use leader engine
    let leader_idx = (leader.leader_id - 1) as usize;
    engines[leader_idx]
        .client()
        .put(b"before-fail".to_vec(), b"value1".to_vec())
        .await?;

    // Stop node 1 (might be leader)
    info!("Stopping node 1");
    let stopped_engine = engines.remove(0);
    stopped_engine.stop().await?;

    // Wait for re-election
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Remaining nodes should elect new leader
    let new_leader = engines[0].wait_leader(Duration::from_secs(5)).await?;
    info!("New leader after failover: {}", new_leader.leader_id);

    // Cluster should still accept writes (2/3 majority)
    // After removing engines[0], new leader might be at different index
    let new_leader_idx = if new_leader.leader_id == 1 {
        panic!("Node 1 was stopped, cannot be leader");
    } else if new_leader.leader_id == 2 {
        0 // Node 2 is now at index 0
    } else {
        1 // Node 3 is at index 1
    };

    engines[new_leader_idx]
        .client()
        .put(b"after-fail".to_vec(), b"value2".to_vec())
        .await?;

    // Wait for commit to propagate
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify both old and new data readable (eventual consistency)
    let old_val = engines[new_leader_idx].client().get_eventual(b"before-fail".to_vec()).await?;
    assert_eq!(old_val.as_deref(), Some(b"value1".as_ref()));

    let new_val = engines[new_leader_idx].client().get_eventual(b"after-fail".to_vec()).await?;
    assert_eq!(new_val.as_deref(), Some(b"value2".as_ref()));

    info!("Cluster operational with 2/3 nodes");

    // Cleanup
    for engine in engines {
        engine.stop().await?;
    }

    Ok(())
}
