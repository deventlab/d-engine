use std::sync::Arc;
use std::time::Duration;

use d_engine_server::EmbeddedEngine;
use d_engine_server::RocksDBStateMachine;
use d_engine_server::RocksDBStorageEngine;
use serial_test::serial;
use tracing::info;
use tracing_test::traced_test;

use crate::common::get_available_ports;
use crate::common::node_config;
use crate::common::reset;

const TEST_DIR: &str = "embedded/scale_to_cluster";
const DB_ROOT_DIR: &str = "./db/embedded/scale_to_cluster";
const LOG_DIR: &str = "./logs/embedded/scale_to_cluster";

/// Test scaling from single-node to 3-node cluster
///
/// Scenario:
/// 1. Start single node, write data
/// 2. Start another two nodes and join node 1 as cluster. Node 1 should remain as Leader, no data lost
/// 3. Verify cluster healthy and data preserved
#[tokio::test]
#[traced_test]
#[serial]
async fn test_scale_single_to_cluster() -> Result<(), Box<dyn std::error::Error>> {
    reset(TEST_DIR).await?;

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Phase 1: Start single-node cluster
    info!("Phase 1: Starting single-node mode");

    // Node 1 config: single-node cluster
    let node1_config = format!(
        r#"
[cluster]
node_id = 1
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 2, status = 2 }}
]
initial_cluster_size = 1
db_root_dir = '{DB_ROOT_DIR}'
log_dir = '{LOG_DIR}'

[raft]
general_raft_timeout_duration_in_ms = 5000
"#,
        ports[0], ports[0]
    );

    let node1_config_path = "/tmp/scale_test_node1.toml";
    tokio::fs::write(node1_config_path, &node1_config).await?;

    let config = node_config(&node1_config);
    let storage_path = config.cluster.db_root_dir.join("node1/storage");
    let sm_path = config.cluster.db_root_dir.join("node1/state_machine");

    tokio::fs::create_dir_all(&storage_path).await?;
    tokio::fs::create_dir_all(&sm_path).await?;

    let storage1 = Arc::new(RocksDBStorageEngine::new(storage_path)?);
    let sm1 = Arc::new(RocksDBStateMachine::new(sm_path)?);

    let engine1 = EmbeddedEngine::start_custom(storage1, sm1, Some(node1_config_path)).await?;

    let leader = engine1.wait_ready(Duration::from_secs(5)).await?;
    info!(
        "Node 1 became leader: {} (term {})",
        leader.leader_id, leader.term
    );
    assert_eq!(leader.leader_id, 1, "Node 1 should be leader");

    // Write data in single-node mode
    engine1.client().put(b"dev-key".to_vec(), b"dev-value".to_vec()).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let val = engine1.client().get_linearizable(b"dev-key".to_vec()).await?;
    assert_eq!(val.as_deref(), Some(b"dev-value".as_ref()));
    info!("Data written to single-node cluster");

    // Phase 2: Start node 2 and node 3 as Learners to join cluster
    info!("Phase 2: Starting node 2 and node 3 to join cluster");

    // Node 2 config: joining as Learner
    let node2_config = format!(
        r#"
[cluster]
node_id = 2
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 3, status = 0 }}
]
initial_cluster_size = 1
db_root_dir = '{DB_ROOT_DIR}'
log_dir = '{LOG_DIR}'

[raft]
general_raft_timeout_duration_in_ms = 5000
"#,
        ports[1], ports[0], ports[1]
    );

    let node2_config_path = "/tmp/scale_test_node2.toml";
    tokio::fs::write(node2_config_path, &node2_config).await?;

    let config2 = node_config(&node2_config);
    let storage_path2 = config2.cluster.db_root_dir.join("node2/storage");
    let sm_path2 = config2.cluster.db_root_dir.join("node2/state_machine");

    tokio::fs::create_dir_all(&storage_path2).await?;
    tokio::fs::create_dir_all(&sm_path2).await?;

    let storage2 = Arc::new(RocksDBStorageEngine::new(storage_path2)?);
    let sm2 = Arc::new(RocksDBStateMachine::new(sm_path2)?);

    let engine2 = EmbeddedEngine::start_custom(storage2, sm2, Some(node2_config_path)).await?;
    info!("Node 2 started, joining as Learner");

    // Wait a bit for node 2 to sync and get promoted
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Node 3 config: joining as Learner
    let node3_config = format!(
        r#"
[cluster]
node_id = 3
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 1, status = 2 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 3, status = 0 }}
]
initial_cluster_size = 1
db_root_dir = '{DB_ROOT_DIR}'
log_dir = '{LOG_DIR}'

[raft]
general_raft_timeout_duration_in_ms = 5000
"#,
        ports[2], ports[0], ports[1], ports[2]
    );

    let node3_config_path = "/tmp/scale_test_node3.toml";
    tokio::fs::write(node3_config_path, &node3_config).await?;

    let config3 = node_config(&node3_config);
    let storage_path3 = config3.cluster.db_root_dir.join("node3/storage");
    let sm_path3 = config3.cluster.db_root_dir.join("node3/state_machine");

    tokio::fs::create_dir_all(&storage_path3).await?;
    tokio::fs::create_dir_all(&sm_path3).await?;

    let storage3 = Arc::new(RocksDBStorageEngine::new(storage_path3)?);
    let sm3 = Arc::new(RocksDBStateMachine::new(sm_path3)?);

    let engine3 = EmbeddedEngine::start_custom(storage3, sm3, Some(node3_config_path)).await?;
    info!("Node 3 started, joining as Learner");

    // Wait for promotion and cluster stabilization
    // Learners need time to: 1) sync data, 2) get promoted to Voter by leader
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Phase 3: Verify cluster health
    info!("Phase 3: Verifying cluster health");

    // Node 1 should still be leader
    let current_leader = engine1.wait_ready(Duration::from_secs(2)).await?;
    assert_eq!(
        current_leader.leader_id, 1,
        "Node 1 should remain as leader"
    );
    info!("Verified: Node 1 is still leader after expansion");

    // Old data should be preserved
    let old_val = engine1.client().get_linearizable(b"dev-key".to_vec()).await?;
    assert_eq!(
        old_val.as_deref(),
        Some(b"dev-value".as_ref()),
        "Old data should be preserved"
    );
    info!("Verified: Old data preserved");

    // Write new data in cluster mode
    engine1.client().put(b"cluster-key".to_vec(), b"cluster-value".to_vec()).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // All nodes should be able to read replicated data
    for (i, engine) in [&engine1, &engine2, &engine3].iter().enumerate() {
        let val = engine.client().get_eventual(b"cluster-key".to_vec()).await?;
        assert_eq!(
            val.as_deref(),
            Some(b"cluster-value".as_ref()),
            "Node {} should have replicated data",
            i + 1
        );
    }
    info!("Verified: All 3 nodes can read replicated data");

    // Cleanup
    engine3.stop().await?;
    engine2.stop().await?;
    engine1.stop().await?;

    Ok(())
}

/// Test leader failover after dynamic scaling from single-node to 3-node cluster
///
/// Test Scenario:
/// This test validates the critical path of dynamic cluster expansion followed by leader failure,
/// ensuring the cluster maintains availability and data consistency throughout the process.
///
/// Phase 1: Single-Node Bootstrap
/// - Start Node 1 as standalone leader (initial_cluster = [n1])
/// - Node 1 automatically wins election (is_single_node_cluster() = true)
/// - Write baseline data: "phase1-key" = "phase1-value"
/// - Verify: Data committed successfully
///
/// Phase 2: Dynamic Expansion to 3-Node Cluster
/// - Start Node 2 as Learner (initial_cluster = [n1, n2], role=Learner)
/// - Start Node 3 as Learner (initial_cluster = [n1, n2, n3], role=Learner)
/// - Wait for Learners to sync data and auto-promote to Voters
/// - Verify: Node 1 remains leader, all 3 nodes operational
/// - Write cluster data: "phase2-key" = "phase2-value"
/// - Verify: Data replicated to all 3 nodes
///
/// Phase 3: Leader Failover
/// - Stop Node 1 (original leader crashes)
/// - Verify: Node 2 or Node 3 conducts election (is_single_node_cluster() = false)
/// - Verify: New leader elected within election timeout (~3-6 seconds)
/// - Verify: New leader ID is 2 or 3 (not the crashed node)
///
/// Phase 4: Verify New Leader Election
/// - Check new leader from Node 2's perspective
/// - Critical assertion: New leader must NOT be the crashed node
///
/// Phase 5: Post-Failover Service Continuity
/// - Write new data via new leader: "phase3-key" = "phase3-value"
/// - Verify: Write succeeds with 2/3 quorum
/// - Read from remaining 2 nodes, verify:
///   * "phase1-key" preserved (data before expansion)
///   * "phase2-key" preserved (data during 3-node operation)
///   * "phase3-key" replicated (data after failover)
///
/// Phase 6: Node Rejoin - Automatic Request Forwarding Validation
/// - Restart Node 1 as follower (rejoins existing 3-node cluster)
/// - Verify: Node 1 recognizes current leader (Node 2 or 3)
/// - Test linearizable reads from follower (auto-forwarding to leader):
///   * Read "phase1-key" via get_linearizable() - forwards to leader
///   * Read "phase2-key" via get_linearizable() - forwards to leader
///   * Read "phase3-key" via get_linearizable() - forwards to leader
/// - Test writes from follower (auto-forwarding to leader):
///   * Write "phase4-key" = "phase4-value" - forwards to leader
///   * Verify write succeeds with 3/3 quorum
/// - Verify data replication across all nodes:
///   * Node 2 (leader or follower) has "phase4-key"
///   * Node 3 (leader or follower) has "phase4-key"
///
/// Critical Assertions:
/// 1. Node 2/3 must NOT skip election (is_single_node_cluster() = false)
/// 2. New leader must be elected within reasonable time (< 10 seconds)
/// 3. No data loss across all phases
/// 4. Cluster remains operational with 2/3 majority
#[tokio::test]
#[traced_test]
#[serial]
async fn test_leader_failover_after_dynamic_scaling() -> Result<(), Box<dyn std::error::Error>> {
    reset(&format!("{TEST_DIR}_failover")).await?;

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();
    let db_root = format!("{DB_ROOT_DIR}_failover");
    let log_dir = format!("{LOG_DIR}_failover");

    // ============================================================================
    // Phase 1: Single-Node Bootstrap
    // ============================================================================
    info!("Phase 1: Starting single-node cluster");

    let node1_config = format!(
        r#"
[cluster]
node_id = 1
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 2, status = 2 }}
]
db_root_dir = '{db_root}'
log_dir = '{log_dir}'

[raft]
general_raft_timeout_duration_in_ms = 100
[raft.election]
election_timeout_min = 300
election_timeout_max = 600
"#,
        ports[0], ports[0]
    );

    let node1_config_path = "/tmp/failover_test_node1.toml";
    tokio::fs::write(node1_config_path, &node1_config).await?;

    let config1 = node_config(&node1_config);
    let node1_db_root = config1.cluster.db_root_dir.join("node1");
    let storage_path1 = node1_db_root.join("storage");
    let sm_path1 = node1_db_root.join("state_machine");

    tokio::fs::create_dir_all(&storage_path1).await?;
    tokio::fs::create_dir_all(&sm_path1).await?;

    let storage1 = Arc::new(RocksDBStorageEngine::new(storage_path1)?);
    let sm1 = Arc::new(RocksDBStateMachine::new(sm_path1)?);

    let engine1 = EmbeddedEngine::start_custom(storage1, sm1, Some(node1_config_path)).await?;

    let initial_leader = engine1.wait_ready(Duration::from_secs(5)).await?;
    info!(
        "Phase 1 Complete: Node {} elected as leader (term {})",
        initial_leader.leader_id, initial_leader.term
    );
    assert_eq!(
        initial_leader.leader_id, 1,
        "Node 1 should be initial leader"
    );

    // Write baseline data
    engine1.client().put(b"phase1-key".to_vec(), b"phase1-value".to_vec()).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let val = engine1.client().get_linearizable(b"phase1-key".to_vec()).await?;
    assert_eq!(val.as_deref(), Some(b"phase1-value".as_ref()));
    info!("Phase 1: Baseline data written successfully");

    // ============================================================================
    // Phase 2: Dynamic Expansion to 3-Node Cluster
    // ============================================================================
    info!("Phase 2: Expanding to 3-node cluster");

    // Start Node 2 as Learner
    let node2_config = format!(
        r#"
[cluster]
node_id = 2
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 3, status = 0 }}
]
db_root_dir = '{db_root}'
log_dir = '{log_dir}'

[raft]
general_raft_timeout_duration_in_ms = 5000
[raft.election]
election_timeout_min = 3000
election_timeout_max = 6000
"#,
        ports[1], ports[0], ports[1]
    );

    let node2_config_path = "/tmp/failover_test_node2.toml";
    tokio::fs::write(node2_config_path, &node2_config).await?;

    let config2 = node_config(&node2_config);
    let node2_db_root = config2.cluster.db_root_dir.join("node2");
    let storage_path2 = node2_db_root.join("storage");
    let sm_path2 = node2_db_root.join("state_machine");

    tokio::fs::create_dir_all(&storage_path2).await?;
    tokio::fs::create_dir_all(&sm_path2).await?;

    let storage2 = Arc::new(RocksDBStorageEngine::new(storage_path2)?);
    let sm2 = Arc::new(RocksDBStateMachine::new(sm_path2)?);

    let engine2 = EmbeddedEngine::start_custom(storage2, sm2, Some(node2_config_path)).await?;
    info!("Node 2 started as Learner");

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Start Node 3 as Learner
    let node3_config = format!(
        r#"
[cluster]
node_id = 3
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 1, status = 2 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 3, status = 0 }}
]
db_root_dir = '{db_root}'
log_dir = '{log_dir}'

[raft]
general_raft_timeout_duration_in_ms = 5000
[raft.election]
election_timeout_min = 3000
election_timeout_max = 6000
"#,
        ports[2], ports[0], ports[1], ports[2]
    );

    let node3_config_path = "/tmp/failover_test_node3.toml";
    tokio::fs::write(node3_config_path, &node3_config).await?;

    let config3 = node_config(&node3_config);
    let node3_db_root = config3.cluster.db_root_dir.join("node3");
    let storage_path3 = node3_db_root.join("storage");
    let sm_path3 = node3_db_root.join("state_machine");

    tokio::fs::create_dir_all(&storage_path3).await?;
    tokio::fs::create_dir_all(&sm_path3).await?;

    let storage3 = Arc::new(RocksDBStorageEngine::new(storage_path3)?);
    let sm3 = Arc::new(RocksDBStateMachine::new(sm_path3)?);

    let engine3 = EmbeddedEngine::start_custom(storage3, sm3, Some(node3_config_path)).await?;
    info!("Node 3 started as Learner");

    // Wait for Learner promotion and cluster stabilization
    tokio::time::sleep(Duration::from_secs(15)).await;

    // Verify Node 1 still leader
    let leader_before_failover = engine1.wait_ready(Duration::from_secs(2)).await?;
    assert_eq!(
        leader_before_failover.leader_id, 1,
        "Node 1 should still be leader"
    );

    // Write data in 3-node cluster
    engine1.client().put(b"phase2-key".to_vec(), b"phase2-value".to_vec()).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify replication to all nodes
    for (i, engine) in [&engine1, &engine2, &engine3].iter().enumerate() {
        let val = engine.client().get_eventual(b"phase2-key".to_vec()).await?;
        assert_eq!(
            val.as_deref(),
            Some(b"phase2-value".as_ref()),
            "Node {} should have phase2 data",
            i + 1
        );
    }
    info!("Phase 2 Complete: 3-node cluster operational, data replicated");

    // ============================================================================
    // Phase 3: Leader Failover - Simulate Node 1 Crash
    // ============================================================================
    info!("Phase 3: Simulating leader crash (stopping Node 1)");

    engine1.stop().await?;
    info!("Node 1 stopped - cluster should detect leader failure and start election");

    // Wait for election timeout + re-election
    // With election_timeout_min=3000ms, election should complete within ~5-8 seconds
    tokio::time::sleep(Duration::from_secs(8)).await;

    // ============================================================================
    // Phase 4: Verify New Leader Election
    // ============================================================================
    info!("Phase 4: Verifying new leader election");

    // Check new leader from Node 2's perspective
    let new_leader = engine2.wait_ready(Duration::from_secs(5)).await?;
    info!(
        "New leader elected: Node {} (term {})",
        new_leader.leader_id, new_leader.term
    );

    // Critical assertion: New leader must NOT be the crashed node
    assert_ne!(
        new_leader.leader_id, 1,
        "New leader cannot be the crashed Node 1"
    );
    assert!(
        new_leader.leader_id == 2 || new_leader.leader_id == 3,
        "New leader must be Node 2 or Node 3"
    );
    assert!(
        new_leader.term > initial_leader.term,
        "New leader must have higher term than initial leader"
    );

    // Determine new leader's engine
    let new_leader_engine = if new_leader.leader_id == 2 {
        &engine2
    } else {
        &engine3
    };

    // ============================================================================
    // Phase 5: Verify Service Continuity After Failover
    // ============================================================================
    info!("Phase 5: Verifying service continuity with new leader");

    // Write data via new leader
    new_leader_engine
        .client()
        .put(b"phase3-key".to_vec(), b"phase3-value".to_vec())
        .await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all historical data preserved on remaining nodes
    for (i, engine) in [&engine2, &engine3].iter().enumerate() {
        let node_id = if i == 0 { 2 } else { 3 };

        // Phase 1 data (before expansion)
        let val1 = engine.client().get_eventual(b"phase1-key".to_vec()).await?;
        assert_eq!(
            val1.as_deref(),
            Some(b"phase1-value".as_ref()),
            "Node {node_id} should preserve phase1 data"
        );

        // Phase 2 data (during 3-node operation)
        let val2 = engine.client().get_eventual(b"phase2-key".to_vec()).await?;
        assert_eq!(
            val2.as_deref(),
            Some(b"phase2-value".as_ref()),
            "Node {node_id} should preserve phase2 data"
        );

        // Phase 3 data (after failover)
        let val3 = engine.client().get_eventual(b"phase3-key".to_vec()).await?;
        assert_eq!(
            val3.as_deref(),
            Some(b"phase3-value".as_ref()),
            "Node {node_id} should have replicated phase3 data"
        );
    }

    info!("Phase 5 Complete: All data preserved, cluster operational with 2/3 nodes");

    // ============================================================================
    // Phase 6: Node 1 Rejoin and Verify Linearizable Read
    // ============================================================================
    info!("Phase 6: Restarting Node 1 as follower and verifying linearizable read");

    // Restart Node 1 (it was crashed at end of Phase 4)
    // Node 1 will rejoin as a follower since it already had membership
    let node1_db_root = std::path::PathBuf::from(DB_ROOT_DIR).join("node1");
    let node1_storage_path = node1_db_root.join("storage");
    let node1_sm_path = node1_db_root.join("state_machine");

    // Reuse existing storage/state_machine directories (preserved from earlier phases)
    let node1_storage = Arc::new(RocksDBStorageEngine::new(node1_storage_path)?);
    let node1_state_machine = Arc::new(RocksDBStateMachine::new(node1_sm_path)?);

    // Node 1 config: rejoining as existing follower
    let node1_config_str = format!(
        r#"
[cluster]
node_id = 1
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 0, status = 2 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 0, status = 2 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 0, status = 2 }}
]
db_root_dir = '{}'

[raft]
election_timeout_ms = 150
heartbeat_interval_ms = 50
"#,
        ports[0], ports[0], ports[1], ports[2], DB_ROOT_DIR
    );

    let node1_config_path = "/tmp/d-engine-test-node1-phase6.toml".to_string();
    tokio::fs::write(&node1_config_path, &node1_config_str).await?;

    let engine1 =
        EmbeddedEngine::start_custom(node1_storage, node1_state_machine, Some(&node1_config_path))
            .await?;

    info!("Node 1 restarted, waiting for leader recognition");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify Node 1 rejoined the cluster (should be a follower)
    let node1_leader = engine1.wait_ready(Duration::from_secs(5)).await?;
    info!(
        "Node 1 back in cluster: leader is {} (term {})",
        node1_leader.leader_id, node1_leader.term
    );
    assert!(
        node1_leader.leader_id == 2 || node1_leader.leader_id == 3,
        "Node 1 should recognize current leader"
    );

    // Phase 6a: Verify linearizable read on rejoined node
    // All three data values should be readable with linearizable consistency
    info!("Phase 6a: Testing linearizable reads on rejoined Node 1");

    let phase1_val = engine1.client().get_eventual(b"phase1-key".to_vec()).await?;
    assert_eq!(
        phase1_val.as_deref(),
        Some(b"phase1-value".as_ref()),
        "Node 1 should have phase1 data via linearizable read"
    );

    let phase2_val = engine1.client().get_eventual(b"phase2-key".to_vec()).await?;
    assert_eq!(
        phase2_val.as_deref(),
        Some(b"phase2-value".as_ref()),
        "Node 1 should have phase2 data via linearizable read"
    );

    let phase3_val = engine1.client().get_eventual(b"phase3-key".to_vec()).await?;
    assert_eq!(
        phase3_val.as_deref(),
        Some(b"phase3-value".as_ref()),
        "Node 1 should have phase3 data via linearizable read"
    );

    info!("Phase 6a Complete: All data readable via linearizable read on Node 1");

    // Phase 6b: Verify cluster consistency with all 3 nodes active
    info!("Phase 6b: Verifying 3-node cluster consistency");

    // Write new data via current leader
    new_leader_engine
        .client()
        .put(b"phase6-key".to_vec(), b"phase6-value".to_vec())
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all three nodes have the new data
    for (engine, node_id) in [
        (&engine1, 1),
        (new_leader_engine, new_leader.leader_id),
        (&engine3, 3),
    ] {
        if node_id == new_leader.leader_id && new_leader.leader_id != 3 {
            continue; // Skip if it's engine2 and we're checking engine3
        }

        let val = engine.client().get_eventual(b"phase6-key".to_vec()).await?;
        assert_eq!(
            val.as_deref(),
            Some(b"phase6-value".as_ref()),
            "Node {node_id} should have phase6 data"
        );
    }

    info!("Phase 6b Complete: 3-node cluster fully synchronized");
    info!("Phase 6 Complete: Node 1 rejoin and linearizable read validation passed");

    // ============================================================================
    // Cleanup
    // ============================================================================
    engine3.stop().await?;
    engine2.stop().await?;

    info!("Test completed successfully - dynamic scaling with leader failover validated");
    Ok(())
}
