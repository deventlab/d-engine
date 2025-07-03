//! Case 1: Verify that commit_index will become 100
//!
//! Scenario:
//!
//! 1. Create a cluster with 3 nodes (1, 2, 3).
//! 2. Node 1 appends 3 log entries with Term=1.
//! 3. Node 2 appends 4 log entries with Term=1.
//! 4. Node 3 appends 10 log entries with Term=2.
//! 5. Node 3 becomes Leader
//! 6. Trigger a client write request
//!
//! Expected Result:
//!
//! - Node 3 becomes the leader
//! - last_commit_index is 10
//! - Node 1 and 2's log-3's term is 2

use crate::client_manager::ClientManager;
use crate::common::check_cluster_is_ready;
use crate::common::create_bootstrap_urls;
use crate::common::create_node_config;
use crate::common::init_state_storage;
use crate::common::manipulate_log;
use crate::common::node_config;
use crate::common::prepare_raft_log;
use crate::common::prepare_state_machine;
use crate::common::prepare_state_storage;
use crate::common::reset;
use crate::common::start_node;
use crate::common::test_put_get;
use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::APPEND_ENNTRIES_PORT_BASE;
use d_engine::storage::StateMachine;
use d_engine::ClientApiError;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

const TEST_CASE_DIR: &str = "append_entries/case1";
const DB_ROOT_DIR: &str = "./db/append_entries/case1";
const LOG_DIR: &str = "./logs/append_entries/case1";

#[tracing::instrument]
#[tokio::test]
async fn test_out_of_sync_peer_scenario() -> Result<(), ClientApiError> {
    crate::enable_logger();
    reset(TEST_CASE_DIR).await?;

    // 1. Prepare node data
    println!("1. Prepare node data");

    let ports = [
        APPEND_ENNTRIES_PORT_BASE + 1,
        APPEND_ENNTRIES_PORT_BASE + 2,
        APPEND_ENNTRIES_PORT_BASE + 3,
    ];

    // Prepare state machine and logs
    println!("Prepare state machine and logs");

    let sm1 = Arc::new(prepare_state_machine(1, &format!("{DB_ROOT_DIR}/cs/1")));
    let raft_logs = [
        Arc::new(prepare_raft_log(&format!("{DB_ROOT_DIR}/cs/1"), 0)),
        Arc::new(prepare_raft_log(&format!("{DB_ROOT_DIR}/cs/2"), 0)),
        Arc::new(prepare_raft_log(&format!("{DB_ROOT_DIR}/cs/3"), 0)),
    ];

    manipulate_log(&raft_logs[0], vec![1, 2, 3], 1);
    manipulate_log(&raft_logs[1], vec![1, 2, 3, 4], 1);
    manipulate_log(&raft_logs[2], (1..=10).collect(), 2);

    // Prepare state storage
    let state_storages = (1..=3)
        .map(|i| {
            let ss = Arc::new(prepare_state_storage(&format!("{DB_ROOT_DIR}/cs/{i}")));
            init_state_storage(&ss, if i == 3 { 2 } else { 1 }, None);
            ss
        })
        .collect::<Vec<_>>();

    // 2. Start the cluster
    println!("2. Start the cluster");
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    println!("{:?}", ports);
    for (i, port) in ports.iter().enumerate() {
        let (graceful_tx, node_handle) = start_node(
            node_config(&create_node_config((i + 1) as u64, *port, &ports, DB_ROOT_DIR, LOG_DIR).await),
            if i == 0 { Some(sm1.clone()) } else { None },
            Some(raft_logs[i].clone()),
            Some(state_storages[i].clone()),
        )
        .await?;

        ctx.graceful_txs.push(graceful_tx);
        ctx.node_handles.push(node_handle);
    }

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Check cluster status
    println!("Check cluster status");
    for port in ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }
    debug!("[test_out_of_sync_peer_scenario] Cluster started. Running tests...");

    // 3. Verify leader election
    let mut client_manager = ClientManager::new(&create_bootstrap_urls(&ports)).await?;
    assert_eq!(client_manager.list_leader_id().await.unwrap(), 3);

    // 4. Test client request
    test_put_get(&mut client_manager, 11, 100).await?;
    assert_eq!(sm1.len(), 11);

    test_put_get(&mut client_manager, 12, 200).await?;
    assert_eq!(sm1.len(), 12);

    // 5. Cleanup
    ctx.shutdown().await
}
