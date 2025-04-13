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
//! fewer entries than Node A.
//! - Nodes A and C recognize B as the leader.

use std::time::Duration;

use d_engine::Error;

use crate::client_manager::ClientManager;
use crate::common::check_cluster_is_ready;
use crate::common::init_state_storage;
use crate::common::manipulate_log;
use crate::common::prepare_raft_log;
use crate::common::prepare_state_storage;
use crate::common::reset;
use crate::common::start_node;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::ELECTION_PORT_BASE;

#[tokio::test]
async fn test_leader_election_based_on_log_term_and_index() -> Result<(), Error> {
    crate::enable_logger();
    reset("election/case1").await?;

    let port1 = ELECTION_PORT_BASE + 1;
    let port2 = ELECTION_PORT_BASE + 2;
    let port3 = ELECTION_PORT_BASE + 3;

    // 1. Start a 3-node cluster and artificially create inconsistent states
    println!("1. Start a 3-node cluster and artificially create inconsistent states");

    let r1 = prepare_raft_log("./db/election/case1/cs/1", None);
    manipulate_log(&r1, (1..=10).collect(), 2);
    let r2 = prepare_raft_log("./db/election/case1/cs/2", None);
    manipulate_log(&r2, (1..=8).collect(), 3);
    let ss1 = prepare_state_storage("./db/election/case1/cs/1");
    init_state_storage(&ss1, 2, None);
    let ss2 = prepare_state_storage("./db/election/case1/cs/2");
    init_state_storage(&ss2, 3, None);

    let (graceful_tx1, node_n1) = start_node("./tests/election/case1/n1", None, Some(r1), Some(ss1)).await?;
    let (graceful_tx2, node_n2) = start_node("./tests/election/case1/n2", None, Some(r2), Some(ss2)).await?;
    let (graceful_tx3, node_n3) = start_node("./tests/election/case1/n3", None, None, None).await?;

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    for port in [port1, port2, port3] {
        check_cluster_is_ready(&format!("127.0.0.1:{}", port), 10).await?;
    }

    println!("Cluster started. Running tests...");

    // 2. Verify Leader is Node 2
    let bootstrap_urls: Vec<String> = vec![
        format!("http://127.0.0.1:{}", port1),
        format!("http://127.0.0.1:{}", port2),
        format!("http://127.0.0.1:{}", port3),
    ];

    let client_manager = ClientManager::new(&bootstrap_urls).await?;
    assert_eq!(client_manager.list_leader_id().await.unwrap(), 2);

    graceful_tx3.send(()).map_err(|_| Error::ServerError)?;
    graceful_tx2.send(()).map_err(|_| Error::ServerError)?;
    graceful_tx1.send(()).map_err(|_| Error::ServerError)?;
    node_n3.await??;
    node_n2.await??;
    node_n1.await??;
    Ok(())
}
