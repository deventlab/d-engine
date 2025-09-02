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

use std::time::Duration;

use d_engine::ClientApiError;
use tracing::debug;

use crate::client_manager::ClientManager;
use crate::common::check_cluster_is_ready;
use crate::common::create_bootstrap_urls;
use crate::common::create_node_config;
use crate::common::get_available_ports;
use crate::common::init_hard_state;
use crate::common::manipulate_log;
use crate::common::node_config;
use crate::common::prepare_storage_engine;
use crate::common::reset;
use crate::common::start_node;
use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;

// Constants for test configuration
const ELECTION_CASE1_DIR: &str = "election/case1";
const ELECTION_CASE1_DB_ROOT_DIR: &str = "./db/election/case1";
const ELECTION_CASE1_LOG_DIR: &str = "./logs/election/case1";

#[tokio::test]
async fn test_leader_election_based_on_log_term_and_index() -> Result<(), ClientApiError> {
    // enable_logger();

    debug!("...test_leader_election_based_on_log_term_and_index...");
    reset(ELECTION_CASE1_DIR).await?;

    let ports = get_available_ports(3).await;

    // Prepare raft logs
    let r1 = prepare_storage_engine(1, &format!("{ELECTION_CASE1_DB_ROOT_DIR}/cs/1"), 0);
    manipulate_log(&r1, (1..=10).collect(), 2).await;
    init_hard_state(&r1, 2, None);
    let r2 = prepare_storage_engine(2, &format!("{ELECTION_CASE1_DB_ROOT_DIR}/cs/2"), 0);
    manipulate_log(&r2, (1..=2).collect(), 2).await;
    init_hard_state(&r2, 3, None);
    manipulate_log(&r2, (3..=8).collect(), 3).await;
    let r3 = prepare_storage_engine(3, &format!("{ELECTION_CASE1_DB_ROOT_DIR}/cs/3"), 0);
    init_hard_state(&r3, 0, None);

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

        let raft_log = match i {
            0 => Some(r1.clone()),
            1 => Some(r2.clone()),
            _ => Some(r3.clone()),
        };

        let (graceful_tx, node_handle) = start_node(node_config(&config), None, raft_log).await?;

        ctx.graceful_txs.push(graceful_tx);
        ctx.node_handles.push(node_handle);
    }

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Verify cluster is ready
    for port in ports.clone() {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!(
        "[test_leader_election_based_on_log_term_and_index] Cluster started. Running tests..."
    );

    // Verify Leader is Node 2
    let bootstrap_urls = create_bootstrap_urls(&ports);
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(15);

    let client_manager = loop {
        match ClientManager::new(&bootstrap_urls).await {
            Ok(mgr) => break mgr,
            Err(e) => {
                if start.elapsed() > timeout {
                    panic!("Leader not elected within timeout: {e:?}");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    };

    let leader_id = client_manager.list_leader_id().await.unwrap();
    assert_eq!(leader_id, 2);

    // Clean up
    ctx.shutdown().await
}
