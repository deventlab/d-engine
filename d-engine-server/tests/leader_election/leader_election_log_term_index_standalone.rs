//! A Raft election compares logs by last term, then index — never by entry count (Raft §5.4.1).
//!
//! Each node's log on disk at startup:
//!
//! | node | log                          | last entry           |
//! |------|------------------------------|----------------------|
//! | 1    | 1..=10, all term 2           | (index 10, term 2)   |
//! | 2    | 1..=2 term 2, 3..=8 term 3   | (index 8, term 3)    |
//! | 3    | same as node 2               | (index 8, term 3)    |
//!
//! Node 1 has the most entries but the oldest last term. The test checks:
//!
//! 1. Node 1 is never elected. Nodes 2 and 3 hold a newer last term, so node 1 gets no vote from
//!    either and cannot reach a majority. Which of nodes 2 and 3 wins depends on election timers;
//!    the test accepts both.
//! 2. Node 2 rejects a vote request carrying node 1's log (index 10, term 2): term is compared
//!    first, so 10 entries lose to 8.
//!
//! Why node 3 holds node 2's log: with an empty log, node 3 would vote for node 1 as readily as for
//! node 2, and nodes 1 and 3 are a majority — node 1 could then win legally.
//!
//! Why check 2 sends its own vote request instead of waiting for node 1 to ask: node 2 receives
//! one only if node 1's election timer fires before node 2's, which is timer luck. The test sends
//! it through the same gRPC client real peers use, under a made-up candidate id, at term 10 so the
//! rejection can only come from the log comparison, not from a stale term. Nodes 2 and 3 may
//! already have elected a leader by then; a request at a higher term makes that leader step down,
//! so a second election can follow. Both checks allow for it.

use std::time::Duration;

use d_engine_core::ClientApiError;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::raft_election_service_client::RaftElectionServiceClient;
use tracing::debug;

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

// Constants for test configuration
const ELECTION_CASE1_DIR: &str = "election/case1";

#[tokio::test]
async fn test_leader_election_based_on_log_term_and_index() -> Result<(), ClientApiError> {
    // enable_logger();

    debug!("...test_leader_election_based_on_log_term_and_index...");
    reset(ELECTION_CASE1_DIR).await?;

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Logs on disk before startup (see the table in the module doc).
    let r1 = prepare_storage_engine(1, &format!("{}/cs/1", data_dir.display()), 0);
    manipulate_log(&r1, (1..=10).collect(), 2).await;
    init_hard_state(&r1, 2, None);
    let r2 = prepare_storage_engine(2, &format!("{}/cs/2", data_dir.display()), 0);
    manipulate_log(&r2, (1..=2).collect(), 2).await;
    init_hard_state(&r2, 3, None);
    manipulate_log(&r2, (3..=8).collect(), 3).await;
    // Node 3 holds the same log as node 2, so a majority has a higher last term than node 1.
    let r3 = prepare_storage_engine(3, &format!("{}/cs/3", data_dir.display()), 0);
    manipulate_log(&r3, (1..=2).collect(), 2).await;
    init_hard_state(&r3, 3, None);
    manipulate_log(&r3, (3..=8).collect(), 3).await;

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

    // Check 2: node 2 must reject a vote request carrying node 1's log (see the module doc).
    let node2_addr = format!("http://127.0.0.1:{}", ports[1]);
    let inject_deadline = std::time::Instant::now() + Duration::from_secs(10);
    let vote_response = loop {
        let attempt = async {
            let mut client = RaftElectionServiceClient::connect(node2_addr.clone())
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
            client
                .request_vote(tonic::Request::new(VoteRequest {
                    term: 10,
                    candidate_id: 99,   // not a cluster member
                    last_log_index: 10, // node 1's log: more entries...
                    last_log_term: 2,   // ...but an older last term than node 2's (3)
                }))
                .await
        }
        .await;

        match attempt {
            Ok(resp) => break resp.into_inner(),
            Err(e) if std::time::Instant::now() < inject_deadline => {
                debug!("synthetic vote request to node 2 not ready yet, retrying: {e:?}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("synthetic vote request to node 2 never succeeded: {e:?}"),
        }
    };
    assert!(
        !vote_response.vote_granted,
        "node 2 must reject a candidate with a lower log term (2) even though \
         that candidate has more log entries (10) than node 2's own log \
         (index=8, term=3) — Raft's election-safety guarantee (§5.4.1) compares \
         term first, then index; entry count never matters"
    );
    assert!(
        (vote_response.last_log_term, vote_response.last_log_index) >= (3, 8),
        "rejection response must carry node 2's own log, at least its initial \
         (index=8, term=3) — it may already include the no-op node 2 appends if it \
         was elected first; got (index={}, term={})",
        vote_response.last_log_index,
        vote_response.last_log_term
    );

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    // Verify cluster is ready
    for port in ports {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!(
        "[test_leader_election_based_on_log_term_and_index] Cluster started. Running tests..."
    );

    // Check 1: node 1 must not be the leader.
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
    assert!(
        matches!(leader_id, Some(2) | Some(3)),
        "the leader must be node 2 or node 3, whose last log term (3) is higher than node 1's (2); \
         node 1's extra entries must not win the election, got {leader_id:?}"
    );

    // Clean up
    ctx.shutdown().await
}
