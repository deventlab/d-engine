//! Case 1: Verify that the Raft leader is elected based on the highest log Term and Index, not
//! merely the number of log entries.
//!
//! Scenario:
//!
//! 1. Create a cluster with 3 nodes (A, B, C).
//! 2. Node A appends 10 log entries with Term=2.
//! 3. Node B appends 8 log entries with Term=3 (higher term).
//! 4. Node C is a new node with no logs.
//! 5. Trigger a leader election.
//!
//! Expected Result:
//!
//! - Node B becomes the leader because its logs have the highest Term (Term=3), even though it has
//!   fewer entries than Node A.
//! - Nodes A and C recognize B as the leader.

use std::sync::Arc;
use std::time::Duration;

use d_engine::ClientApiError;

use crate::client_manager::ClientManager;
use crate::common::check_cluster_is_ready;
use crate::common::create_bootstrap_urls;
use crate::common::create_node_config;
use crate::common::init_state_storage;
use crate::common::manipulate_log;
use crate::common::node_config;
use crate::common::prepare_raft_log;
use crate::common::prepare_state_storage;
use crate::common::reset;
use crate::common::start_node;
use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::ELECTION_PORT_BASE;

// Constants for test configuration
const ELECTION_CASE1_DIR: &str = "election/case1";
const ELECTION_CASE1_DB_ROOT_DIR: &str = "./db/election/case1";
const ELECTION_CASE1_LOG_DIR: &str = "./logs/election/case1";

#[tokio::test]
async fn test_leader_election_based_on_log_term_and_index() -> Result<(), ClientApiError> {
    crate::enable_logger();
    reset(ELECTION_CASE1_DIR).await?;

    let ports = [ELECTION_PORT_BASE + 1, ELECTION_PORT_BASE + 2, ELECTION_PORT_BASE + 3];

    // Prepare state storage
    let ss1 = Arc::new(prepare_state_storage(&format!("{}/cs/1", ELECTION_CASE1_DB_ROOT_DIR)));
    init_state_storage(&ss1, 2, None);
    let ss2 = Arc::new(prepare_state_storage(&format!("{}/cs/2", ELECTION_CASE1_DB_ROOT_DIR)));
    init_state_storage(&ss2, 3, None);

    // Prepare raft logs
    let r1 = Arc::new(prepare_raft_log(1, &format!("{}/cs/1", ELECTION_CASE1_DB_ROOT_DIR), 0));
    manipulate_log(&r1, (1..=10).collect(), 2);
    let r2 = Arc::new(prepare_raft_log(2, &format!("{}/cs/2", ELECTION_CASE1_DB_ROOT_DIR), 0));
    manipulate_log(&r2, (1..=8).collect(), 3);

    // Start cluster nodes
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    for (i, port) in ports.iter().enumerate() {
        let config = create_node_config(
            (i + 1) as u64,
            *port,
            &ports,
            &format!("{}/cs/{}", ELECTION_CASE1_DB_ROOT_DIR, i + 1),
            ELECTION_CASE1_LOG_DIR,
        )
        .await;

        let (raft_log, state_storage) = match i {
            0 => (Some(r1.clone()), Some(ss1.clone())),
            1 => (Some(r2.clone()), Some(ss2.clone())),
            _ => (None, None),
        };

        let (graceful_tx, node_handle) = start_node(node_config(&config), None, raft_log, state_storage).await?;

        ctx.graceful_txs.push(graceful_tx);
        ctx.node_handles.push(node_handle);
    }

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Verify cluster is ready
    for port in ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!("[test_leader_election_based_on_log_term_and_index] Cluster started. Running tests...");

    // Verify Leader is Node 2
    let bootstrap_urls = create_bootstrap_urls(&ports);
    let client_manager = ClientManager::new(&bootstrap_urls).await?;
    assert_eq!(client_manager.list_leader_id().await.unwrap(), 2);

    // Clean up
    ctx.shutdown().await
}
