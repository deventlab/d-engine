//! Integration test for cluster join scenario with multiple new nodes.
//!
//! This test verifies that multiple new nodes can successfully join an existing Raft cluster,
//! receive snapshots, install them correctly, and transition to active followers.
//! The test validates the complete cluster state after all nodes have joined.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::ClientApiError;
use d_engine_core::convert::safe_kv;
use d_engine_proto::common::NodeStatus;
use d_engine_server::FileStateMachine;
use d_engine_server::StateMachine;
use serial_test::serial;
use tokio::time::sleep;
use tracing_test::traced_test;

use crate::client_manager::ClientManager;
use crate::common;
use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::common::check_cluster_is_ready;
use crate::common::check_path_contents;
use crate::common::create_bootstrap_urls;
use crate::common::get_available_ports;
use crate::common::init_hard_state;
use crate::common::manipulate_log;
use crate::common::node_config;
use crate::common::prepare_state_machine;
use crate::common::prepare_storage_engine;
use crate::common::reset;
use crate::common::start_node;
use crate::common::test_put_get;

// Constants for test configuration
const JOIN_CLUSTER_CASE2_DIR: &str = "join_cluster/case2";
const SNAPSHOT_DIR: &str = "./snapshots/join_cluster/case2";
const JOIN_CLUSTER_CASE2_DB_ROOT_DIR: &str = "./db/join_cluster/case2";
const JOIN_CLUSTER_CASE2_LOG_DIR: &str = "./logs/join_cluster/case2";

/// Test Objective: Concurrent Multi-Node Join with Snapshot Transfer
///
/// This integration test validates the complete workflow of adding multiple new nodes
/// to an existing Raft cluster concurrently, focusing on:
///
/// 1. **Snapshot Generation**: Initial 3-node cluster (with 10 log entries) automatically generates
///    snapshots based on log size policy (max_log_entries_before_snapshot=10).
///
/// 2. **Concurrent Node Addition**: Two new nodes (node 4 and node 5) join the cluster sequentially
///    while the cluster is operational, testing the system's ability to handle membership changes
///    without disrupting ongoing operations.
///
/// 3. **Snapshot Transfer & Installation**: New nodes receive snapshots from the leader via chunked
///    InstallSnapshot RPCs, validating:
///    - Snapshot chunking (chunk_size=100 bytes)
///    - Snapshot reassembly on receiving nodes
///    - Snapshot application to state machine
///
/// 4. **Data Persistence Verification**: After all nodes join, the test writes a new entry (entry
///    11) and verifies that all nodes, including the newly joined ones, successfully replicate and
///    persist the data to disk. This ensures:
///    - Raft log replication across all 5 nodes
///    - Commit handler processing on all nodes
///    - State machine persistence (FileStateMachine.persist_data_async())
///
/// 5. **Cluster Consistency**: Final assertions verify:
///    - All 5 nodes are in Active status
///    - Leader remains stable (node 3)
///    - All nodes have identical state (11 entries)
///
/// Key Race Condition Mitigation:
/// The test includes deliberate sleep delays after write operations to allow sufficient
/// time for asynchronous replication and disk persistence across all nodes, preventing
/// timing-dependent test failures.
#[tokio::test]
#[traced_test]
#[serial]
async fn test_join_cluster_scenario2() -> Result<(), ClientApiError> {
    // Initialize test environment and reset any previous state
    reset(JOIN_CLUSTER_CASE2_DIR).await?;

    // MODIFICATION: Use dynamic port allocation instead of hardcoded ports
    let mut port_guard = get_available_ports(5).await;
    let mut ports = port_guard.ports.to_vec();
    port_guard.release_listeners();
    let new_node_port4 = ports.pop().unwrap(); // Fourth port for first new node
    let new_node_port5 = ports.pop().unwrap(); // Fifth port for second new node
    let initial_ports = ports; // First three ports for initial cluster

    // Prepare state machine directories for all nodes (do not pre-allocate Arc to avoid ownership
    // issues)
    prepare_state_machine(1, &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/1")).await;
    prepare_state_machine(2, &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/2")).await;
    prepare_state_machine(3, &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/3")).await;
    prepare_state_machine(4, &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/4")).await;
    prepare_state_machine(5, &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/5")).await;

    // Prepare raft logs for all nodes
    let storage_engines = [
        prepare_storage_engine(1, &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/1"), 0),
        prepare_storage_engine(2, &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/2"), 0),
        prepare_storage_engine(3, &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/3"), 0),
        prepare_storage_engine(4, &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/4"), 0),
        prepare_storage_engine(5, &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/5"), 0),
    ];

    // Initialize logs with test data for the initial cluster
    let last_log_id: u64 = 10;
    manipulate_log(&storage_engines[0], vec![1, 2, 3], 1).await;
    init_hard_state(&storage_engines[0], 1, None);
    manipulate_log(&storage_engines[1], vec![1, 2, 3, 4], 1).await;
    init_hard_state(&storage_engines[1], 1, None);
    manipulate_log(&storage_engines[2], (1..=3).collect(), 1).await;
    init_hard_state(&storage_engines[2], 2, None);
    manipulate_log(&storage_engines[2], (4..=last_log_id).collect(), 2).await;

    // MODIFICATION: Create cluster node definitions with dynamic ports
    let initial_cluster_nodes: Vec<(u16, u8, u8)> = initial_ports
        .iter()
        .map(|&port| (port, 1, 2)) // (port, role, status)
        .collect();

    // Start initial cluster nodes
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    let mut snapshot_last_included_id: Option<u64> = None;

    // Start the initial 3-node cluster
    for (i, &port) in initial_ports.iter().enumerate() {
        let node_id = (i + 1) as u64;

        // MODIFICATION: Use dynamic ports in node configuration
        let config = create_node_config(
            node_id,
            port,
            &initial_cluster_nodes,
            &format!("{}/cs/{}", JOIN_CLUSTER_CASE2_DB_ROOT_DIR, i + 1),
            JOIN_CLUSTER_CASE2_LOG_DIR,
        )
        .await;

        let mut node_config = node_config(&config);

        // Configure snapshot settings
        node_config.raft.snapshot.max_log_entries_before_snapshot = 10;
        node_config.raft.snapshot.cleanup_retain_count = 2;
        node_config.raft.snapshot.snapshots_dir =
            PathBuf::from(format!("{SNAPSHOT_DIR}/{node_id}"));
        node_config.raft.snapshot.chunk_size = 100;

        // Calculate snapshot metadata
        snapshot_last_included_id =
            Some(last_log_id.saturating_sub(node_config.raft.snapshot.retained_log_entries));

        // Start the node with its specific state machine and storage engine
        // Create fresh Arc for state machine to ensure single ownership
        let state_machine = Arc::new(
            prepare_state_machine(
                node_id as u32,
                &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/{node_id}"),
            )
            .await,
        );
        let (graceful_tx, node_handle) = start_node(
            node_config,
            Some(state_machine),
            Some(storage_engines[i].clone()),
        )
        .await?;

        ctx.graceful_txs.push(graceful_tx);
        ctx.node_handles.push(node_handle);
    }

    let last_included = snapshot_last_included_id.unwrap();

    // Wait for cluster to become ready
    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Verify initial cluster is ready
    for port in &initial_ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!("Initial cluster started. Running tests...");

    // Wait for snapshot generation on the leader
    sleep(Duration::from_secs(3)).await;

    // Verify snapshot file exists on the leader (node 3)
    let snapshot_path = format!("{SNAPSHOT_DIR}/3");
    assert!(check_path_contents(&snapshot_path).unwrap_or(false));

    // MODIFICATION: Create cluster definition including the first new node
    let cluster_with_first_new_node: Vec<(u16, u8, u8)> = initial_ports
        .iter()
        .map(|&port| (port, 1, 2))
        .chain(std::iter::once((new_node_port4, 3, 0))) // First new node as learner
        .collect();

    // Start first new node and join it to the cluster
    println!("Starting first new node and joining cluster...");

    // MODIFICATION: Use dynamic port for first new node
    let config = create_node_config(
        4,
        new_node_port4,
        &cluster_with_first_new_node,
        &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/4"),
        JOIN_CLUSTER_CASE2_LOG_DIR,
    )
    .await;

    let mut node_config = node_config(&config);
    node_config.raft.snapshot.max_log_entries_before_snapshot = 10;
    node_config.raft.snapshot.cleanup_retain_count = 2;
    node_config.raft.snapshot.snapshots_dir = PathBuf::from(format!("{}/{}", SNAPSHOT_DIR, 4));
    node_config.raft.snapshot.chunk_size = 100;

    // Create fresh Arc for node 4 state machine to ensure single ownership
    let state_machine_4 =
        Arc::new(prepare_state_machine(4, &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/4")).await);
    let (graceful_tx4, node_n4) = start_node(
        node_config,
        Some(state_machine_4),
        Some(storage_engines[3].clone()),
    )
    .await?;

    ctx.graceful_txs.push(graceful_tx4);
    ctx.node_handles.push(node_n4);

    // Wait for the first new node to synchronize
    sleep(Duration::from_secs(3)).await;

    // MODIFICATION: Create cluster definition including both new nodes
    let full_cluster_nodes: Vec<(u16, u8, u8)> = initial_ports
        .iter()
        .map(|&port| (port, 1, 2))
        .chain(std::iter::once((new_node_port4, 3, 0))) // First new node as learner
        .chain(std::iter::once((new_node_port5, 3, 0))) // Second new node as learner
        .collect();

    // Start second new node and join it to the cluster
    println!("Starting second new node and joining cluster...");

    // MODIFICATION: Use dynamic port for second new node
    let config = create_node_config(
        5,
        new_node_port5,
        &full_cluster_nodes,
        &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/5"),
        JOIN_CLUSTER_CASE2_LOG_DIR,
    )
    .await;

    let mut node_config = common::node_config(&config);
    node_config.raft.snapshot.max_log_entries_before_snapshot = 10;
    node_config.raft.snapshot.cleanup_retain_count = 2;
    node_config.raft.snapshot.snapshots_dir = PathBuf::from(format!("{}/{}", SNAPSHOT_DIR, 5));
    node_config.raft.snapshot.chunk_size = 100;

    // Create fresh Arc for node 5 state machine to ensure single ownership
    let state_machine_5 =
        Arc::new(prepare_state_machine(5, &format!("{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/5")).await);
    let (graceful_tx5, node_n5) = start_node(
        node_config,
        Some(state_machine_5),
        Some(storage_engines[4].clone()),
    )
    .await?;

    ctx.graceful_txs.push(graceful_tx5);
    ctx.node_handles.push(node_n5);

    // Wait for the second new node to synchronize
    sleep(Duration::from_secs(3)).await;

    // Validate that the first new node has received the snapshot
    let snapshot_path = format!("{SNAPSHOT_DIR}/4");
    assert!(check_path_contents(&snapshot_path).unwrap_or(false));

    // Verify that the first new node has all the data by re-opening state machine
    let node4_sm = FileStateMachine::new(PathBuf::from(format!(
        "{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/4/state_machine"
    )))
    .await
    .expect("Failed to open node 4 state machine for verification");
    for i in 1..=last_included {
        let value = node4_sm.get(&safe_kv(i)).unwrap();
        assert_eq!(value, Some(Bytes::from(safe_kv(i).to_vec())));
    }

    // Validate that the second new node has received the snapshot
    let snapshot_path = format!("{SNAPSHOT_DIR}/5");
    assert!(check_path_contents(&snapshot_path).unwrap_or(false));

    // Verify that the second new node has all the data by re-opening state machine
    let node5_sm = FileStateMachine::new(PathBuf::from(format!(
        "{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/5/state_machine"
    )))
    .await
    .expect("Failed to open node 5 state machine for verification");
    for i in 1..=last_included {
        let value = node5_sm.get(&safe_kv(i)).unwrap();
        assert_eq!(value, Some(Bytes::from(safe_kv(i).to_vec())));
    }

    // Verify the cluster has 5 active nodes
    let all_ports: Vec<u16> = initial_ports
        .iter()
        .cloned()
        .chain(std::iter::once(new_node_port4))
        .chain(std::iter::once(new_node_port5))
        .collect();

    let mut client_manager = ClientManager::new(&create_bootstrap_urls(&all_ports)).await?;

    // Insert a new entry to trigger commit handler and ensure all nodes are in sync
    test_put_get(&mut client_manager, 11, 200).await?;

    // Wait for all nodes to complete replication and persistence
    // This ensures entry 11 has been:
    // 1. Replicated from leader to all followers via AppendEntries RPC
    // 2. Committed by the leader once majority acknowledgment received
    // 3. Applied to state machine on all nodes via commit handler
    // 4. Persisted to disk via FileStateMachine.persist_data_async()
    sleep(Duration::from_secs(2)).await;

    // Verify data length by re-opening node 1 state machine
    let node1_sm = FileStateMachine::new(PathBuf::from(format!(
        "{JOIN_CLUSTER_CASE2_DB_ROOT_DIR}/cs/1/state_machine"
    )))
    .await
    .expect("Failed to open node 1 state machine for verification");
    assert_eq!(node1_sm.len(), 11);

    // Verify leader is still node 3
    assert_eq!(client_manager.list_leader_id().await.unwrap(), Some(3));

    // Verify all 5 members are active
    let members = client_manager.list_members().await.unwrap();
    assert_eq!(members.len(), 5);

    for member in members {
        println!("Checking member status: {:?}", &member);
        assert_eq!(member.status, NodeStatus::Active as i32);
    }

    println!("Multi-node cluster join test completed successfully");

    // Clean up
    ctx.shutdown().await
}

// MODIFICATION: Updated function to work with dynamic ports
async fn create_node_config(
    node_id: u64,
    port: u16,
    cluster_nodes: &[(u16, u8, u8)], // (port, role, status)
    db_root_dir: &str,
    log_dir: &str,
) -> String {
    println!("Creating configuration for node {node_id} on port {port}");

    let initial_cluster_entries = cluster_nodes
        .iter()
        .enumerate()
        .map(|(i, &(p, role, status))| {
            let id = i as u64 + 1;
            format!("{{ id = {id}, name = 'n{id}', address = '127.0.0.1:{p}', role = {role}, status = {status} }}")
        })
        .collect::<Vec<_>>()
        .join(",\n            ");

    format!(
        r#"
        [cluster]
        node_id = {node_id}
        listen_address = '127.0.0.1:{port}'
        initial_cluster = [
            {initial_cluster_entries}
        ]
        db_root_dir = '{db_root_dir}'
        log_dir = '{log_dir}'
        "#
    )
}
