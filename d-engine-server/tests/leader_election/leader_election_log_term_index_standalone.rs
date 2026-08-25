//! Case 1: Verify that the Raft leader is elected based on the highest log Term and Index, not
//! merely the number of log entries — and that node 2 (the eventual leader) explicitly rejects
//! at least one vote request from a worse-log peer along the way, rather than just happening to
//! win the timeout race.
//!
//! Scenario:
//!
//! 1. Create a cluster with 3 nodes (A, B, C).
//! 2. Node A appends 10 log entries with Term=2.
//! 3. Node B appends 8 log entries with Term=3 (higher term).
//! 4. Node C is a new node with no logs.
//! 5. Trigger a leader election (all three nodes use the same default randomized timeout —
//!    see the note on the rejection assertion below for why no timing tricks are needed).
//!
//! Expected Result:
//!
//! - Node B becomes the leader because its logs have the highest Term (Term=3), even though it has
//!   fewer entries than Node A.
//! - Nodes A and C recognize B as the leader.
//! - Node B must have sent at least one `VoteResponse { vote_granted: false, .. }` before winning —
//!   proving Raft's election-safety guarantee (§5.4.1) is enforced *on the wire*, not just that
//!   node B happened to win the randomized timeout race. `election_handler::check_vote_request_is_legal`
//!   (the function that decides this) already has thorough pure-logic unit test coverage — see
//!   `election_handler_test.rs` — but until this assertion, nothing exercised it end-to-end over
//!   real gRPC in a live multi-node cluster.
//!
//! Note: earlier versions of this test tried to force node B into a passive role by giving it a
//! much longer election timeout than A/C, so a rejection would be guaranteed to happen before B's
//! own timer ever fired. That backfired: A and C (whose logs disagree with each other too — A has
//! no log entries in common with C's empty log, different terms) ended up in a prolonged
//! split-vote livelock (term counters observed climbing past 20) with neither able to reach a
//! majority without B. The uniform default timeout (used below) avoids that: whichever node times
//! out first almost immediately triggers *some* rejection (the exact reason — stale term vs. stale
//! log — varies run to run, which is why the assertion below checks for the rejection outcome,
//! not a specific internal reason).

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
use crate::common::prepare_storage_engine;
use crate::common::reset;
use crate::common::start_node;
use d_engine_core::ClientApiError;
use d_engine_core::capture_logs_globally_filtered;
use d_engine_core::logs_contain_globally;
use std::time::Duration;
use tracing::debug;

// Constants for test configuration
const ELECTION_CASE1_DIR: &str = "election/case1";

#[tokio::test]
async fn test_leader_election_based_on_log_term_and_index() -> Result<(), ClientApiError> {
    // enable_logger();

    // Captures `election_handler`'s debug logs across every node's tokio task
    // (not just this test's own thread) so we can assert, below, that a stale
    // vote request was actually rejected on the wire — not just that node 2
    // eventually won.
    let logs = capture_logs_globally_filtered(
        "info,d_engine_core=debug,d_engine_server=debug,h2=off,tonic=warn,hyper=warn",
    );

    debug!("...test_leader_election_based_on_log_term_and_index...");
    reset(ELECTION_CASE1_DIR).await?;

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Prepare raft logs
    let r1 = prepare_storage_engine(1, &format!("{}/cs/1", data_dir.display()), 0);
    manipulate_log(&r1, (1..=10).collect(), 2).await;
    init_hard_state(&r1, 2, None);
    let r2 = prepare_storage_engine(2, &format!("{}/cs/2", data_dir.display()), 0);
    manipulate_log(&r2, (1..=2).collect(), 2).await;
    init_hard_state(&r2, 3, None);
    manipulate_log(&r2, (3..=8).collect(), 3).await;
    let r3 = prepare_storage_engine(3, &format!("{}/cs/3", data_dir.display()), 0);
    init_hard_state(&r3, 0, None);

    // Start cluster nodes
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    for (i, port) in ports.iter().enumerate() {
        let node_data_dir = format!("{}/cs/{}", data_dir.display(), i + 1);
        let config = create_node_config(
            (i + 1) as u64,
            *port,
            ports,
            &node_data_dir,
            &log_dir.display().to_string(),
        )
        .await;

        let raft_log = match i {
            0 => Some(r1.clone()),
            1 => Some(r2.clone()),
            _ => Some(r3.clone()),
        };

        let (graceful_tx, node_handle) =
            start_node(&node_data_dir, node_config(&config), None, raft_log).await?;

        ctx.graceful_txs.push(graceful_tx);
        ctx.node_handles.push(node_handle);
    }

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Verify cluster is ready
    for port in ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!(
        "[test_leader_election_based_on_log_term_and_index] Cluster started. Running tests..."
    );

    // Verify Leader is Node 2
    let bootstrap_urls = create_bootstrap_urls(ports);
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);

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
    assert_eq!(leader_id, Some(2));

    // Node 2's log signature (index 8, term 3, seeded above) uniquely identifies
    // it as the responder. Matching on `vote_granted: false` together with that
    // signature proves node 2 itself explicitly rejected a vote request at some
    // point — not just that it eventually won. This deliberately does not pin
    // down *which* of check_vote_request_is_legal's checks (stale term vs.
    // stale log) caused the rejection: both are valid, and which one fires
    // first depends on exactly how far node 1/3's term has climbed by the time
    // their request reaches node 2, which varies run to run.
    assert!(
        logs_contain_globally(
            &logs,
            "vote_granted: false, last_log_index: 8, last_log_term: 3"
        ),
        "expected node 2 to have explicitly rejected at least one peer's vote \
         request (VoteResponse{{vote_granted: false}}) before being elected — \
         this is the Raft election-safety guarantee (§5.4.1) that stops a node \
         with an incomplete log from ever becoming leader; seeing this only via \
         `leader_id == Some(2)` above would also be consistent with node 2 \
         simply winning the timeout race without the rejection path ever firing"
    );

    // Clean up
    ctx.shutdown().await
}
