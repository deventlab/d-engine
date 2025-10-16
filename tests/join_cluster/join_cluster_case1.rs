//! Integration test for cluster join scenario.
//!
//! This test verifies that a new node can successfully join an existing Raft cluster,
//! synchronize its state via snapshot, and become a fully functional member.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine::client::ClientApiError;
use d_engine::convert::safe_kv;
use d_engine::storage::StateMachine;
use tokio::time::sleep;
use tracing::debug;
use tracing_test::traced_test;

use crate::common::check_cluster_is_ready;
use crate::common::check_path_contents;
use crate::common::get_available_ports;
use crate::common::init_hard_state;
use crate::common::manipulate_log;
use crate::common::node_config;
use crate::common::prepare_state_machine;
use crate::common::prepare_storage_engine;
use crate::common::reset;
use crate::common::start_node;
use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;

// Constants for test configuration
const JOIN_CLUSTER_CASE1_DIR: &str = "join_cluster/case1";
const SNAPSHOT_DIR: &str = "./snapshots/join_cluster/case1";
const JOIN_CLUSTER_CASE1_DB_ROOT_DIR: &str = "./db/join_cluster/case1";
const JOIN_CLUSTER_CASE1_LOG_DIR: &str = "./logs/join_cluster/case1";

#[tokio::test]
#[traced_test]
async fn test_join_cluster_scenario1() -> Result<(), ClientApiError> {
    debug!("Starting cluster join scenario test...");
    reset(JOIN_CLUSTER_CASE1_DIR).await?;

    let mut ports = get_available_ports(4).await;
    let new_node_port = ports.pop().unwrap(); // Last port for the new node
    let initial_ports = ports.clone(); // First three ports for initial cluster

    // Prepare state machines for all nodes
    let state_machines = [
        Arc::new(prepare_state_machine(1, &format!("{JOIN_CLUSTER_CASE1_DB_ROOT_DIR}/cs/1")).await),
        Arc::new(prepare_state_machine(2, &format!("{JOIN_CLUSTER_CASE1_DB_ROOT_DIR}/cs/2")).await),
        Arc::new(prepare_state_machine(3, &format!("{JOIN_CLUSTER_CASE1_DB_ROOT_DIR}/cs/3")).await),
        Arc::new(prepare_state_machine(4, &format!("{JOIN_CLUSTER_CASE1_DB_ROOT_DIR}/cs/4")).await),
    ];

    // Prepare raft logs for all nodes
    let storage_engines = [
        prepare_storage_engine(1, &format!("{JOIN_CLUSTER_CASE1_DB_ROOT_DIR}/cs/1"), 0),
        prepare_storage_engine(2, &format!("{JOIN_CLUSTER_CASE1_DB_ROOT_DIR}/cs/2"), 0),
        prepare_storage_engine(3, &format!("{JOIN_CLUSTER_CASE1_DB_ROOT_DIR}/cs/3"), 0),
        prepare_storage_engine(4, &format!("{JOIN_CLUSTER_CASE1_DB_ROOT_DIR}/cs/4"), 0),
    ];

    // Initialize logs with test data
    let last_log_id: u64 = 10;
    manipulate_log(&storage_engines[0], vec![1, 2, 3], 1).await;
    init_hard_state(&storage_engines[0], 1, None);
    manipulate_log(&storage_engines[1], vec![1, 2, 3, 4], 1).await;
    init_hard_state(&storage_engines[1], 1, None);
    manipulate_log(&storage_engines[2], (1..=3).collect(), 1).await;
    init_hard_state(&storage_engines[2], 2, None);
    manipulate_log(&storage_engines[2], (4..=last_log_id).collect(), 2).await;

    // Create cluster node definitions with dynamic ports
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
            &format!("{}/cs/{}", JOIN_CLUSTER_CASE1_DB_ROOT_DIR, i + 1),
            JOIN_CLUSTER_CASE1_LOG_DIR,
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
        let (graceful_tx, node_handle) = start_node(
            node_config,
            Some(state_machines[i].clone()),
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

    debug!("Initial cluster started. Running tests...");

    // Wait for snapshot generation on the leader
    sleep(Duration::from_secs(3)).await;
    let leader_snapshot_metadata = state_machines[2].snapshot_metadata().unwrap();

    // Verify snapshot file exists on the leader
    let snapshot_path = format!("{SNAPSHOT_DIR}/3");
    assert!(check_path_contents(&snapshot_path).unwrap_or(false));
    assert!(leader_snapshot_metadata.last_included.unwrap().index >= last_included);
    assert!(!leader_snapshot_metadata.checksum.is_empty());

    // Create cluster definition including the new node
    let full_cluster_nodes: Vec<(u16, u8, u8)> = initial_ports
        .iter()
        .map(|&port| (port, 1, 2))
        .chain(std::iter::once((new_node_port, 3, 0))) // New node as learner
        .collect();

    // Start new node and join it to the cluster
    debug!("Starting new node and joining cluster...");

    // MODIFICATION: Use dynamic port for new node
    let config = create_node_config(
        4,
        new_node_port,
        &full_cluster_nodes,
        &format!("{JOIN_CLUSTER_CASE1_DB_ROOT_DIR}/cs/4"),
        JOIN_CLUSTER_CASE1_LOG_DIR,
    )
    .await;

    let mut node_config = node_config(&config);
    node_config.raft.snapshot.max_log_entries_before_snapshot = 10;
    node_config.raft.snapshot.cleanup_retain_count = 2;
    node_config.raft.snapshot.snapshots_dir = PathBuf::from(format!("{}/{}", SNAPSHOT_DIR, 4));
    node_config.raft.snapshot.chunk_size = 100;

    let (graceful_tx4, node_n4) = start_node(
        node_config,
        Some(state_machines[3].clone()),
        Some(storage_engines[3].clone()),
    )
    .await?;

    ctx.graceful_txs.push(graceful_tx4);
    ctx.node_handles.push(node_n4);

    // Wait for the new node to synchronize
    sleep(Duration::from_secs(3)).await;

    // Validate that the new node has received the snapshot
    let snapshot_path = format!("{SNAPSHOT_DIR}/4");
    assert!(check_path_contents(&snapshot_path).unwrap_or(false));

    // Verify that the new node has all the data
    for i in 1..=10 {
        let value = state_machines[3].get(&safe_kv(i)).unwrap();
        assert_eq!(value, Some(Bytes::from(safe_kv(i).to_vec())));
    }

    debug!("Cluster join test completed successfully");

    // Clean up
    ctx.shutdown().await
}

// MODIFICATION: Updated function signature and implementation to work with dynamic ports
async fn create_node_config(
    node_id: u64,
    port: u16,
    cluster_nodes: &[(u16, u8, u8)], // (port, role, status)
    db_root_dir: &str,
    log_dir: &str,
) -> String {
    debug!(
        "Creating configuration for node {} on port {}",
        node_id, port
    );

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
