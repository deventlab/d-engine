//! Integration tests for EmbeddedEngine cluster state APIs (Ticket #234)
//!
//! These tests verify is_leader() and leader_info() behavior in multi-node scenarios:
//! - Multi-node leader election
//! - Leader failover
//! - Cluster view consistency

use crate::common::get_available_ports;
use crate::common::node_config;
use d_engine_server::EmbeddedEngine;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing_test::traced_test;

use d_engine_server::{RocksDBStateMachine, RocksDBStorageEngine};

/// Test: 3-node cluster should elect exactly one leader
///
/// Setup:
/// - Start 3 EmbeddedEngine instances as a cluster
/// - Wait for leader election
///
/// Verification:
/// - Exactly 1 node has is_leader() == true
/// - 2 nodes have is_leader() == false
/// - All nodes agree on the same leader_id via leader_info()
///
/// Business scenario: Basic 3-node HA deployment
#[tokio::test]
#[traced_test]
async fn test_multi_node_single_leader() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!("Starting 3-node cluster on ports: {:?}", ports);

    // Create configs for 3-node cluster
    let cluster_config = format!(
        r#"
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 2, status = 2 }}
]
db_root_dir = '{}'
log_dir = '{}'

[raft]
general_raft_timeout_duration_in_ms = 3000
"#,
        ports[0],
        ports[1],
        ports[2],
        db_root_dir.display(),
        log_dir.display()
    );

    // Node 1
    let node1_config = format!(
        r#"
[cluster]
node_id = 1
listen_address = '127.0.0.1:{}'
{}
"#,
        ports[0], cluster_config
    );
    let node1_config_path = "/tmp/cluster_state_test_n1.toml";
    tokio::fs::write(node1_config_path, &node1_config).await?;

    // Node 2
    let node2_config = format!(
        r#"
[cluster]
node_id = 2
listen_address = '127.0.0.1:{}'
{}
"#,
        ports[1], cluster_config
    );
    let node2_config_path = "/tmp/cluster_state_test_n2.toml";
    tokio::fs::write(node2_config_path, &node2_config).await?;

    // Node 3
    let node3_config = format!(
        r#"
[cluster]
node_id = 3
listen_address = '127.0.0.1:{}'
{}
"#,
        ports[2], cluster_config
    );
    let node3_config_path = "/tmp/cluster_state_test_n3.toml";
    tokio::fs::write(node3_config_path, &node3_config).await?;

    // Start engines
    let config1 = node_config(&node1_config);
    let storage_path1 = config1.cluster.db_root_dir.join("node1/storage");
    let sm_path1 = config1.cluster.db_root_dir.join("node1/state_machine");
    tokio::fs::create_dir_all(&storage_path1).await?;
    tokio::fs::create_dir_all(&sm_path1).await?;
    let storage1 = Arc::new(RocksDBStorageEngine::new(storage_path1)?);
    let sm1 = Arc::new(RocksDBStateMachine::new(sm_path1)?);
    let engine1 = EmbeddedEngine::start_custom(storage1, sm1, Some(node1_config_path)).await?;

    let config2 = node_config(&node2_config);
    let storage_path2 = config2.cluster.db_root_dir.join("node2/storage");
    let sm_path2 = config2.cluster.db_root_dir.join("node2/state_machine");
    tokio::fs::create_dir_all(&storage_path2).await?;
    tokio::fs::create_dir_all(&sm_path2).await?;
    let storage2 = Arc::new(RocksDBStorageEngine::new(storage_path2)?);
    let sm2 = Arc::new(RocksDBStateMachine::new(sm_path2)?);
    let engine2 = EmbeddedEngine::start_custom(storage2, sm2, Some(node2_config_path)).await?;

    let config3 = node_config(&node3_config);
    let storage_path3 = config3.cluster.db_root_dir.join("node3/storage");
    let sm_path3 = config3.cluster.db_root_dir.join("node3/state_machine");
    tokio::fs::create_dir_all(&storage_path3).await?;
    tokio::fs::create_dir_all(&sm_path3).await?;
    let storage3 = Arc::new(RocksDBStorageEngine::new(storage_path3)?);
    let sm3 = Arc::new(RocksDBStateMachine::new(sm_path3)?);
    let engine3 = EmbeddedEngine::start_custom(storage3, sm3, Some(node3_config_path)).await?;

    // Wait for leader election
    info!("Waiting for leader election...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check leadership status
    let is_leader_1 = engine1.is_leader();
    let is_leader_2 = engine2.is_leader();
    let is_leader_3 = engine3.is_leader();

    info!(
        "Leadership status: n1={}, n2={}, n3={}",
        is_leader_1, is_leader_2, is_leader_3
    );

    // Exactly one leader
    let leader_count = [is_leader_1, is_leader_2, is_leader_3].iter().filter(|&&x| x).count();
    assert_eq!(leader_count, 1, "Exactly one node should be leader");

    // All nodes agree on leader_id
    let info1 = engine1.leader_info().expect("Node 1 should have leader info");
    let info2 = engine2.leader_info().expect("Node 2 should have leader info");
    let info3 = engine3.leader_info().expect("Node 3 should have leader info");

    assert_eq!(
        info1.leader_id, info2.leader_id,
        "All nodes should agree on leader"
    );
    assert_eq!(
        info2.leader_id, info3.leader_id,
        "All nodes should agree on leader"
    );

    info!(
        "Cluster elected leader: {} (term {})",
        info1.leader_id, info1.term
    );

    // Cleanup
    engine1.stop().await?;
    engine2.stop().await?;
    engine3.stop().await?;

    Ok(())
}

/// Test: All nodes in cluster return consistent leader_info()
///
/// Setup:
/// - Start 3-node cluster
/// - Wait for leader election
/// - Each node calls leader_info() multiple times
///
/// Verification:
/// - All nodes agree on the same leader_id
/// - All nodes report the same term
/// - Results are consistent across multiple calls on each node
///
/// Business scenario: Monitoring dashboard polling cluster state from all nodes
#[tokio::test]
#[traced_test]
async fn test_leader_info_consistency_across_cluster() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!("Starting 3-node cluster for consistency test");

    // Create 3-node cluster configuration
    let cluster_config = format!(
        r#"
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 2, status = 2 }}
]
db_root_dir = '{}'
log_dir = '{}'

[raft]
general_raft_timeout_duration_in_ms = 3000
"#,
        ports[0],
        ports[1],
        ports[2],
        db_root_dir.display(),
        log_dir.display()
    );

    let node1_config = format!(
        "[cluster]\nnode_id = 1\nlisten_address = '127.0.0.1:{}'\n{}",
        ports[0], cluster_config
    );
    let node2_config = format!(
        "[cluster]\nnode_id = 2\nlisten_address = '127.0.0.1:{}'\n{}",
        ports[1], cluster_config
    );
    let node3_config = format!(
        "[cluster]\nnode_id = 3\nlisten_address = '127.0.0.1:{}'\n{}",
        ports[2], cluster_config
    );

    let node1_config_path = "/tmp/consistency_test_n1.toml";
    let node2_config_path = "/tmp/consistency_test_n2.toml";
    let node3_config_path = "/tmp/consistency_test_n3.toml";

    tokio::fs::write(node1_config_path, &node1_config).await?;
    tokio::fs::write(node2_config_path, &node2_config).await?;
    tokio::fs::write(node3_config_path, &node3_config).await?;

    // Start all nodes
    let config1 = node_config(&node1_config);
    let storage_path1 = config1.cluster.db_root_dir.join("node1/storage");
    let sm_path1 = config1.cluster.db_root_dir.join("node1/state_machine");
    tokio::fs::create_dir_all(&storage_path1).await?;
    tokio::fs::create_dir_all(&sm_path1).await?;
    let engine1 = EmbeddedEngine::start_custom(
        Arc::new(RocksDBStorageEngine::new(storage_path1)?),
        Arc::new(RocksDBStateMachine::new(sm_path1)?),
        Some(node1_config_path),
    )
    .await?;

    let config2 = node_config(&node2_config);
    let storage_path2 = config2.cluster.db_root_dir.join("node2/storage");
    let sm_path2 = config2.cluster.db_root_dir.join("node2/state_machine");
    tokio::fs::create_dir_all(&storage_path2).await?;
    tokio::fs::create_dir_all(&sm_path2).await?;
    let engine2 = EmbeddedEngine::start_custom(
        Arc::new(RocksDBStorageEngine::new(storage_path2)?),
        Arc::new(RocksDBStateMachine::new(sm_path2)?),
        Some(node2_config_path),
    )
    .await?;

    let config3 = node_config(&node3_config);
    let storage_path3 = config3.cluster.db_root_dir.join("node3/storage");
    let sm_path3 = config3.cluster.db_root_dir.join("node3/state_machine");
    tokio::fs::create_dir_all(&storage_path3).await?;
    tokio::fs::create_dir_all(&sm_path3).await?;
    let engine3 = EmbeddedEngine::start_custom(
        Arc::new(RocksDBStorageEngine::new(storage_path3)?),
        Arc::new(RocksDBStateMachine::new(sm_path3)?),
        Some(node3_config_path),
    )
    .await?;

    // Wait for leader election
    info!("Waiting for leader election...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Call leader_info() multiple times on each node
    let mut info1_results = vec![];
    let mut info2_results = vec![];
    let mut info3_results = vec![];

    for _ in 0..10 {
        info1_results.push(engine1.leader_info());
        info2_results.push(engine2.leader_info());
        info3_results.push(engine3.leader_info());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Verify consistency within each node
    let first_info1 = info1_results[0].expect("Node 1 should have leader info");
    for info in &info1_results {
        assert_eq!(
            *info,
            Some(first_info1),
            "Node 1 should return consistent leader_info"
        );
    }

    let first_info2 = info2_results[0].expect("Node 2 should have leader info");
    for info in &info2_results {
        assert_eq!(
            *info,
            Some(first_info2),
            "Node 2 should return consistent leader_info"
        );
    }

    let first_info3 = info3_results[0].expect("Node 3 should have leader info");
    for info in &info3_results {
        assert_eq!(
            *info,
            Some(first_info3),
            "Node 3 should return consistent leader_info"
        );
    }

    // Verify consistency across all nodes
    assert_eq!(
        first_info1.leader_id, first_info2.leader_id,
        "All nodes should agree on leader_id"
    );
    assert_eq!(
        first_info2.leader_id, first_info3.leader_id,
        "All nodes should agree on leader_id"
    );

    assert_eq!(
        first_info1.term, first_info2.term,
        "All nodes should agree on term"
    );
    assert_eq!(
        first_info2.term, first_info3.term,
        "All nodes should agree on term"
    );

    info!(
        "Cluster consensus: leader={}, term={}",
        first_info1.leader_id, first_info1.term
    );

    // Cleanup
    engine1.stop().await?;
    engine2.stop().await?;
    engine3.stop().await?;

    Ok(())
}

/// Test: Leader failover updates is_leader() status on follower
///
/// Setup:
/// - Start 3-node cluster
/// - Identify leader and followers
/// - Stop leader node
/// - Wait for re-election
///
/// Verification:
/// - One of the old followers becomes new leader (is_leader() changes from false -> true)
/// - New leader_info() reflects new leader_id
/// - Term increases
///
/// Business scenario: Leader node crashes, cluster recovers automatically
#[tokio::test]
#[traced_test]
async fn test_leader_failover_state_change() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!("Starting 3-node cluster for failover test");

    // Create 3-node cluster (similar setup as above)
    let cluster_config = format!(
        r#"
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 2, status = 2 }}
]
db_root_dir = '{}'
log_dir = '{}'

[raft]
general_raft_timeout_duration_in_ms = 3000
"#,
        ports[0],
        ports[1],
        ports[2],
        db_root_dir.display(),
        log_dir.display()
    );

    let node1_config = format!(
        "[cluster]\nnode_id = 1\nlisten_address = '127.0.0.1:{}'\n{}",
        ports[0], cluster_config
    );
    let node2_config = format!(
        "[cluster]\nnode_id = 2\nlisten_address = '127.0.0.1:{}'\n{}",
        ports[1], cluster_config
    );
    let node3_config = format!(
        "[cluster]\nnode_id = 3\nlisten_address = '127.0.0.1:{}'\n{}",
        ports[2], cluster_config
    );

    let node1_config_path = "/tmp/failover_test_n1.toml";
    let node2_config_path = "/tmp/failover_test_n2.toml";
    let node3_config_path = "/tmp/failover_test_n3.toml";

    tokio::fs::write(node1_config_path, &node1_config).await?;
    tokio::fs::write(node2_config_path, &node2_config).await?;
    tokio::fs::write(node3_config_path, &node3_config).await?;

    // Start all nodes
    let config1 = node_config(&node1_config);
    let storage_path1 = config1.cluster.db_root_dir.join("node1/storage");
    let sm_path1 = config1.cluster.db_root_dir.join("node1/state_machine");
    tokio::fs::create_dir_all(&storage_path1).await?;
    tokio::fs::create_dir_all(&sm_path1).await?;
    let engine1 = EmbeddedEngine::start_custom(
        Arc::new(RocksDBStorageEngine::new(storage_path1)?),
        Arc::new(RocksDBStateMachine::new(sm_path1)?),
        Some(node1_config_path),
    )
    .await?;

    let config2 = node_config(&node2_config);
    let storage_path2 = config2.cluster.db_root_dir.join("node2/storage");
    let sm_path2 = config2.cluster.db_root_dir.join("node2/state_machine");
    tokio::fs::create_dir_all(&storage_path2).await?;
    tokio::fs::create_dir_all(&sm_path2).await?;
    let engine2 = EmbeddedEngine::start_custom(
        Arc::new(RocksDBStorageEngine::new(storage_path2)?),
        Arc::new(RocksDBStateMachine::new(sm_path2)?),
        Some(node2_config_path),
    )
    .await?;

    let config3 = node_config(&node3_config);
    let storage_path3 = config3.cluster.db_root_dir.join("node3/storage");
    let sm_path3 = config3.cluster.db_root_dir.join("node3/state_machine");
    tokio::fs::create_dir_all(&storage_path3).await?;
    tokio::fs::create_dir_all(&sm_path3).await?;
    let engine3 = EmbeddedEngine::start_custom(
        Arc::new(RocksDBStorageEngine::new(storage_path3)?),
        Arc::new(RocksDBStateMachine::new(sm_path3)?),
        Some(node3_config_path),
    )
    .await?;

    // Wait for initial leader election
    tokio::time::sleep(Duration::from_secs(5)).await;

    let initial_info = engine1.leader_info().expect("Should have leader");
    let initial_term = initial_info.term;
    let initial_leader_id = initial_info.leader_id;

    info!(
        "Initial leader: {} (term {})",
        initial_leader_id, initial_term
    );

    // Stop the current leader
    match initial_leader_id {
        1 => {
            info!("Stopping node 1 (leader)");
            engine1.stop().await?;

            // Wait for re-election
            tokio::time::sleep(Duration::from_secs(5)).await;

            // One of node2/node3 should become new leader
            let new_leader_found = engine2.is_leader() || engine3.is_leader();
            assert!(new_leader_found, "A new leader should be elected");

            // Verify term increased
            if engine2.is_leader() {
                let new_info = engine2.leader_info().unwrap();
                assert!(
                    new_info.term > initial_term,
                    "Term should increase after re-election"
                );
                assert_eq!(new_info.leader_id, 2, "Node 2 should be new leader");
            } else {
                let new_info = engine3.leader_info().unwrap();
                assert!(
                    new_info.term > initial_term,
                    "Term should increase after re-election"
                );
                assert_eq!(new_info.leader_id, 3, "Node 3 should be new leader");
            }

            engine2.stop().await?;
            engine3.stop().await?;
        }
        2 => {
            info!("Stopping node 2 (leader)");
            engine2.stop().await?;
            tokio::time::sleep(Duration::from_secs(5)).await;

            let new_leader_found = engine1.is_leader() || engine3.is_leader();
            assert!(new_leader_found, "A new leader should be elected");

            engine1.stop().await?;
            engine3.stop().await?;
        }
        3 => {
            info!("Stopping node 3 (leader)");
            engine3.stop().await?;
            tokio::time::sleep(Duration::from_secs(5)).await;

            let new_leader_found = engine1.is_leader() || engine2.is_leader();
            assert!(new_leader_found, "A new leader should be elected");

            engine1.stop().await?;
            engine2.stop().await?;
        }
        _ => panic!("Invalid leader_id"),
    }

    Ok(())
}
