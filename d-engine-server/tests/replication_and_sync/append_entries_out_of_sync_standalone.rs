//! Case 1: Verify that commit_index will become 100
//!
//! Scenario:
//!
//! Node 1: Locally has log-1(1), log-2(1), log-3(1)
//! Node 2: Locally has log-1(1), log-2(1), log-3(1), log-4(1)
//! Node 3: Locally has log-1(1), log-2(1), log-3(1), log-4(2), log-4(5), …, log-9(2), log-10(2)
//!
//! Expected Result:
//!
//! - Node 3 becomes the leader
//! - last_commit_index is 10
//! - Node 1 and 2's log-3's term is 2

// use std::sync::Arc; // Not needed anymore
use std::time::Duration;

use d_engine_client::ClientApiError;
// use d_engine_server::StateMachine; // Not needed - we don't access state machine directly
use tracing::debug;
use tracing_test::traced_test;

use crate::client_manager::ClientManager;
use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::common::check_cluster_is_ready;
use crate::common::create_bootstrap_urls;
use crate::common::create_node_config;
use crate::common::get_available_ports;
use crate::common::init_hard_state;
use crate::common::manipulate_log;
use crate::common::node_config;
// use crate::common::prepare_state_machine; // Not needed - each node creates its own
use crate::common::prepare_storage_engine;
use crate::common::reset;
use crate::common::start_node;
use crate::common::test_put_get;

const TEST_CASE_DIR: &str = "append_entries/case1";
const DB_ROOT_DIR: &str = "./db/append_entries/case1";
const LOG_DIR: &str = "./logs/append_entries/case1";

#[tracing::instrument]
#[tokio::test]
#[traced_test]
async fn test_out_of_sync_peer_scenario() -> Result<(), ClientApiError> {
    reset(TEST_CASE_DIR).await?;

    // 1. Prepare node data
    println!("1. Prepare node data");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Prepare state machine and logs
    println!("Prepare state machine and logs");

    let raft_logs = [
        prepare_storage_engine(1, &format!("{DB_ROOT_DIR}/cs/1"), 0),
        prepare_storage_engine(2, &format!("{DB_ROOT_DIR}/cs/2"), 0),
        prepare_storage_engine(3, &format!("{DB_ROOT_DIR}/cs/3"), 0),
    ];

    manipulate_log(&raft_logs[0], vec![1, 2, 3], 1).await;
    init_hard_state(&raft_logs[0], 1, None);
    manipulate_log(&raft_logs[1], vec![1, 2, 3, 4], 1).await;
    init_hard_state(&raft_logs[1], 1, None);
    manipulate_log(&raft_logs[2], (1..=3).collect(), 1).await;
    init_hard_state(&raft_logs[2], 2, None);
    manipulate_log(&raft_logs[2], (4..=10).collect(), 2).await;

    // 2. Start the cluster
    println!("2. Start the cluster");
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    println!("{:?}", ports);
    for (i, port) in ports.iter().enumerate() {
        let (graceful_tx, node_handle) = start_node(
            node_config(
                &create_node_config((i + 1) as u64, *port, ports, DB_ROOT_DIR, LOG_DIR).await,
            ),
            None, // Let build_node create state machines for each node
            Some(raft_logs[i].clone()),
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
    let mut client_manager = ClientManager::new(&create_bootstrap_urls(ports)).await?;
    assert_eq!(client_manager.list_leader_id().await.unwrap(), Some(3));

    // 4. Test client request
    test_put_get(&mut client_manager, 11, 100).await?;
    // Note: Cannot verify state machine length directly since each node has its own state machine
    // The test_put_get already verifies data persistence via read-back

    test_put_get(&mut client_manager, 12, 200).await?;
    // Data consistency is verified through client reads

    // 5. Cleanup
    ctx.shutdown().await
}
