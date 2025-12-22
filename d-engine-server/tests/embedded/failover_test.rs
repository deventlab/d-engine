use d_engine_server::api::EmbeddedEngine;
#[cfg(feature = "rocksdb")]
use d_engine_server::{RocksDBStateMachine, RocksDBStorageEngine};
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing_test::traced_test;

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
/// 4. Verify cluster operational with 2/3 nodes
#[tokio::test]
#[traced_test]
#[serial]
#[cfg(feature = "rocksdb")]
async fn test_embedded_leader_failover() -> Result<(), Box<dyn std::error::Error>> {
    reset(TEST_DIR).await?;

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!("Starting 3-node cluster");

    let mut engines = Vec::new();
    let mut configs = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let config_str = create_node_config(node_id, ports[i], ports, DB_ROOT_DIR, LOG_DIR).await;
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

        configs.push((config_str, config_path));

        let engine =
            EmbeddedEngine::start_custom(Some(&configs[i].1), storage, state_machine).await?;
        engines.push(engine);
    }

    // Wait for initial leader
    let initial_leader = engines[0]
        .wait_ready(Duration::from_secs(10))
        .await
        .expect("Failed to elect initial leader");
    info!(
        "Initial leader elected: {} (term {})",
        initial_leader.leader_id, initial_leader.term
    );

    let leader_idx = (initial_leader.leader_id - 1) as usize;

    // Write some data to initial leader
    engines[leader_idx]
        .client()
        .put(b"before-failover".to_vec(), b"initial-value".to_vec())
        .await?;

    // Wait for replication to all nodes
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify data replicated to all nodes (eventual consistency)
    for engine in &engines {
        let val = engine.client().get_eventual(b"before-failover".to_vec()).await?;
        assert_eq!(val.as_deref(), Some(b"initial-value".as_slice()));
    }
    info!("Initial data written successfully");

    // Subscribe to leader changes on a non-leader node
    let leader_idx = (initial_leader.leader_id - 1) as usize;
    let watcher_idx = if leader_idx == 0 { 1 } else { 0 };
    let watcher_id = watcher_idx + 1;

    info!(
        "Initial leader: {}, Watcher node: {}",
        initial_leader.leader_id, watcher_id
    );
    let mut leader_rx = engines[watcher_idx].leader_change_notifier();

    // Kill the actual leader node
    info!("Killing leader node {}", initial_leader.leader_id);
    let killed_engine = engines.remove(leader_idx);
    let _killed_config = configs.remove(leader_idx);
    killed_engine.stop().await?;

    // Wait for re-election event
    info!("Waiting for re-election detected by node {}", watcher_id);
    let new_leader_info = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            // Check current state
            let current = *leader_rx.borrow();

            if let Some(leader) = current {
                if leader.leader_id != initial_leader.leader_id {
                    return leader;
                }
            }
            // Wait for change
            if leader_rx.changed().await.is_err() {
                // If the channel closes, we can't wait anymore
                panic!("Leader watch channel closed unexpectedly");
            }
        }
    })
    .await
    .expect("Timeout waiting for new leader election");

    assert_ne!(
        new_leader_info.leader_id, initial_leader.leader_id,
        "New leader should not be the killed node"
    );
    info!(
        "New leader elected: {} (term {})",
        new_leader_info.leader_id, new_leader_info.term
    );

    // Find new leader engine to write
    let mut leader_client = None;
    for engine in &engines {
        if engine.node_id() == new_leader_info.leader_id {
            leader_client = Some(engine.client());
            break;
        }
    }
    let leader_client = leader_client.expect("New leader not found in engines");

    // Cluster should still be operational with 2/3 nodes
    leader_client.put(b"after-failover".to_vec(), b"still-works".to_vec()).await?;

    // Allow time for state machine application
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify old data still readable (from surviving follower)
    let old_val = engines[0].client().get_eventual(b"before-failover".to_vec()).await?;
    assert_eq!(
        old_val.as_deref(),
        Some(b"initial-value".as_slice()),
        "Old data should be preserved"
    );

    // Verify new data written successfully (read from Leader with strong consistency)
    let new_val = leader_client.get_linearizable(b"after-failover".to_vec()).await?;
    assert_eq!(
        new_val.as_deref(),
        Some(b"still-works".as_slice()),
        "New data should be written"
    );

    info!("Cluster operational with 2/3 nodes");

    // Cleanup
    for engine in engines {
        engine.stop().await?;
    }

    Ok(())
}

/// Test node rejoin after temporary failure
///
/// Scenario:
/// 1. Start 3-node cluster
/// 2. Kill a follower node
/// 3. Verify cluster still operational (2/3 quorum)
/// 4. Restart killed follower
/// 5. Verify it rejoins and syncs data
#[tokio::test]
#[traced_test]
#[serial]
#[cfg(feature = "rocksdb")]
async fn test_embedded_node_rejoin() -> Result<(), Box<dyn std::error::Error>> {
    reset(&format!("{TEST_DIR}_rejoin")).await?;

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();
    let db_root = format!("{DB_ROOT_DIR}_rejoin");
    let log_dir = format!("{LOG_DIR}_rejoin");

    info!("Starting 3-node cluster for rejoin test");

    let mut engines = Vec::new();
    let mut configs = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let config_str = create_node_config(node_id, ports[i], ports, &db_root, &log_dir).await;
        let config = node_config(&config_str);

        let node_db_root = config.cluster.db_root_dir.join(format!("node{node_id}"));
        let storage_path = node_db_root.join("storage");
        let sm_path = node_db_root.join("state_machine");

        tokio::fs::create_dir_all(&storage_path).await?;
        tokio::fs::create_dir_all(&sm_path).await?;

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

        let config_path = format!("/tmp/d-engine-test-rejoin-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        configs.push((config_str, config_path));

        let engine =
            EmbeddedEngine::start_custom(Some(&configs[i].1), storage, state_machine).await?;
        engines.push(engine);
    }

    let leader_info = engines[0]
        .wait_ready(Duration::from_secs(10))
        .await
        .expect("Failed to elect leader");
    info!(
        "Leader elected: {} (term {})",
        leader_info.leader_id, leader_info.term
    );

    let leader_idx = (leader_info.leader_id - 1) as usize;

    // Write initial data
    engines[leader_idx]
        .client()
        .put(b"before-kill".to_vec(), b"initial".to_vec())
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Find a follower to kill (not the leader)
    let follower_idx = if leader_idx == 0 { 1 } else { 0 };
    let follower_id = (follower_idx + 1) as u64;

    info!("Killing follower node {}", follower_id);
    let killed_engine = engines.remove(follower_idx);
    let killed_config = configs.remove(follower_idx);
    killed_engine.stop().await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Cluster should still work with 2/3 nodes
    let remaining_leader_idx =
        engines.iter().position(|e| e.node_id() == leader_info.leader_id).unwrap();
    engines[remaining_leader_idx]
        .client()
        .put(b"after-kill".to_vec(), b"still-works".to_vec())
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    info!(
        "Cluster operational with 2/3 nodes, restarting follower {}",
        follower_id
    );

    // Restart the killed follower
    let config = node_config(&killed_config.0);
    let node_db_root = config.cluster.db_root_dir.join(format!("node{follower_id}"));
    let storage_path = node_db_root.join("storage");
    let sm_path = node_db_root.join("state_machine");

    let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
    let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

    let restarted_engine =
        EmbeddedEngine::start_custom(Some(&killed_config.1), storage, state_machine).await?;
    restarted_engine.wait_ready(Duration::from_secs(30)).await?;

    // Wait for sync
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify restarted follower synced all data
    let val1 = restarted_engine.client().get_eventual(b"before-kill".to_vec()).await?;
    assert_eq!(
        val1.as_deref(),
        Some(b"initial".as_slice()),
        "Should sync old data"
    );

    let val2 = restarted_engine.client().get_eventual(b"after-kill".to_vec()).await?;
    assert_eq!(
        val2.as_deref(),
        Some(b"still-works".as_slice()),
        "Should sync new data written while offline"
    );

    info!("Follower {} rejoined and synced successfully", follower_id);

    // Cleanup
    engines.push(restarted_engine);
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

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();
    let db_root = format!("{DB_ROOT_DIR}_minority");
    let log_dir = format!("{LOG_DIR}_minority");

    info!("Starting 3-node cluster for minority failure test");

    let mut engines = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let config_str = create_node_config(node_id, ports[i], ports, &db_root, &log_dir).await;
        let config = node_config(&config_str);

        let node_db_root = config.cluster.db_root_dir.join(format!("node{node_id}"));
        let storage_path = node_db_root.join("storage");
        let sm_path = node_db_root.join("state_machine");

        tokio::fs::create_dir_all(&storage_path).await?;
        tokio::fs::create_dir_all(&sm_path).await?;

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

        let config_path = format!("/tmp/d-engine-test-minority-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        let engine =
            EmbeddedEngine::start_custom(Some(&config_path), storage, state_machine).await?;
        engines.push(engine);
    }

    let leader_info = engines[0].wait_ready(Duration::from_secs(10)).await?;
    info!(
        "Leader elected successfully: node {}",
        leader_info.leader_id
    );

    // Write initial data to the actual leader
    info!(
        "Writing initial test data to leader (node {})",
        leader_info.leader_id
    );
    let leader_idx = (leader_info.leader_id - 1) as usize;
    engines[leader_idx]
        .client()
        .put(b"test-key".to_vec(), b"test-value".to_vec())
        .await?;
    info!("Initial data written successfully");

    info!("Killing 2 nodes to lose majority (keeping leader alive but unable to get quorum)");

    // Kill 2 non-leader nodes, leaving the leader isolated without majority
    // Indices: 0, 1, 2 -> nodes: 1, 2, 3
    let mut indices_to_kill = vec![0, 1, 2];
    indices_to_kill.remove(leader_idx); // Remove leader index
    indices_to_kill.truncate(2); // Take first 2 non-leader indices

    info!(
        "Killing nodes at engine indices: {:?} (leader is at index {})",
        indices_to_kill, leader_idx
    );

    // Remove in reverse order to avoid index shifting issues
    let mut killed_engines = Vec::new();
    for &idx in indices_to_kill.iter().rev() {
        let engine = engines.remove(idx);
        killed_engines.push(engine);
    }

    // Stop killed engines
    for engine in killed_engines {
        let _ = engine.stop().await;
    }

    info!("Sleeping 2 seconds for cluster to stabilize");
    tokio::time::sleep(Duration::from_secs(2)).await;

    info!("2 nodes killed, verifying leader cannot serve writes without majority");
    info!("Remaining engine count: {}", engines.len());

    // The leader (now alone) should reject writes since it can't reach quorum
    info!("Attempting write on isolated leader (should fail due to no majority)");
    let write_result = tokio::time::timeout(
        Duration::from_secs(3),
        engines[0].client().put(b"should-fail".to_vec(), b"no-majority".to_vec()),
    )
    .await;

    info!("Write result: {:?}", write_result);

    // Expect timeout or error
    match &write_result {
        Ok(Ok(_)) => {
            panic!("Write should not succeed without majority!");
        }
        Ok(Err(e)) => {
            info!("Write correctly rejected with error: {:?}", e);
        }
        Err(_) => {
            info!("Write correctly timed out");
        }
    }

    info!("Minority failure test passed - cluster correctly refused writes");

    // Cleanup
    let remaining_engine = engines.remove(0);
    let _ = remaining_engine.stop().await;

    Ok(())
}
