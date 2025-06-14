//! Case 1: Verify that commit_index will become 100
//!
//! Scenario:
//!
//! 1. Create a cluster with 3 nodes (A, B, C).
//! 2. Node A appends 3 log entries with Term=1.
//! 3. Node B appends 4 log entries with Term=1.
//! 4. Node C appends 10 log entries with Term=2.
//! 5. Node C becomes Leader
//! 6. Trigger a client write request
//!
//! Expected Result:
//!
//! - Node C becomes the leader
//! - last_commit_index is 10
//! - Node A and B's log-3's term is 2

use crate::client_manager::ClientManager;
use crate::common::check_cluster_is_ready;
use crate::common::init_state_storage;
use crate::common::manipulate_log;
use crate::common::prepare_raft_log;
use crate::common::prepare_state_machine;
use crate::common::prepare_state_storage;
use crate::common::reset;
use crate::common::start_node;
use crate::common::ClientCommands;
use crate::common::ITERATIONS;
use crate::common::LATENCY_IN_MS;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::APPEND_ENNTRIES_PORT_BASE;
use d_engine::storage::StateMachine;
use d_engine::ClientApiError;
use std::sync::Arc;
use std::time::Duration;

#[tracing::instrument]
#[tokio::test]
async fn test_out_of_sync_peer_scenario() -> Result<(), ClientApiError> {
    crate::enable_logger();
    reset("append_entries/case1").await?;

    let port1 = APPEND_ENNTRIES_PORT_BASE + 1;
    let port2 = APPEND_ENNTRIES_PORT_BASE + 2;
    let port3 = APPEND_ENNTRIES_PORT_BASE + 3;

    // 1. Prepare state machine for node 1 so that we could read out the last applied id in this test
    println!("1. Prepare state_machine & raft_log");
    let sm1 = Arc::new(prepare_state_machine(1, "./db/append_entries/case1/cs/1"));
    let r1 = Arc::new(prepare_raft_log("./db/append_entries/case1/cs/1", 0));
    manipulate_log(&r1, vec![1, 2, 3], 1);
    let r2 = Arc::new(prepare_raft_log("./db/append_entries/case1/cs/2", 0));
    manipulate_log(&r2, vec![1, 2, 3, 4], 1);
    let r3 = Arc::new(prepare_raft_log("./db/append_entries/case1/cs/3", 0));
    manipulate_log(&r3, (1..=10).collect(), 2);
    let ss1 = Arc::new(prepare_state_storage("./db/append_entries/case1/cs/1"));
    init_state_storage(&ss1, 1, None);
    let ss2 = Arc::new(prepare_state_storage("./db/append_entries/case1/cs/2"));
    init_state_storage(&ss2, 1, None);
    let ss3 = Arc::new(prepare_state_storage("./db/append_entries/case1/cs/3"));
    init_state_storage(&ss3, 2, None);

    // 2. Start a 3-node cluster and artificially create inconsistent states
    println!("2. Start a 3-node cluster and artificially create inconsistent states");
    let (graceful_tx1, node_n1) = start_node(
        "./tests/append_entries/case1/n1",
        Some(sm1.clone()),
        Some(r1),
        Some(ss1),
    )
    .await?;
    let (graceful_tx2, node_n2) = start_node("./tests/append_entries/case1/n2", None, Some(r2), Some(ss2)).await?;
    let (graceful_tx3, node_n3) = start_node("./tests/append_entries/case1/n3", None, Some(r3), Some(ss3)).await?;

    // Combine all log layers

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    for port in [port1, port2, port3] {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!("Cluster started. Running tests...");

    // 3. Trigger client request
    let bootstrap_urls: Vec<String> = vec![
        format!("http://127.0.0.1:{port1}",),
        format!("http://127.0.0.1:{port2}",),
        format!("http://127.0.0.1:{port3}",),
    ];

    let mut client_manager = ClientManager::new(&bootstrap_urls).await?;
    // Node C becomes Leader
    assert_eq!(client_manager.list_leader_id().await.unwrap(), 3);

    // Trigger client request
    println!("put 11 100");
    assert!(
        client_manager
            .execute_command(ClientCommands::Put, 11, Some(100))
            .await
            .is_ok(),
        "Put command failed!"
    );
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
    client_manager.verify_read(11, 100, ITERATIONS).await;

    // 4.1 Verify global state
    assert_eq!(sm1.len(), 11);

    println!("put 12 200");
    assert!(
        client_manager
            .execute_command(ClientCommands::Put, 12, Some(200))
            .await
            .is_ok(),
        "Put command failed!"
    );
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
    client_manager.verify_read(12, 200, ITERATIONS).await;

    // 4.2 Verify global state
    assert_eq!(sm1.len(), 12);

    graceful_tx3
        .send(())
        .map_err(|_| ClientApiError::general_client_error("failed to shutdown".to_string()))?;
    graceful_tx2
        .send(())
        .map_err(|_| ClientApiError::general_client_error("failed to shutdown".to_string()))?;
    graceful_tx1
        .send(())
        .map_err(|_| ClientApiError::general_client_error("failed to shutdown".to_string()))?;
    node_n3.await??;
    node_n2.await??;
    node_n1.await??;
    Ok(())
}
