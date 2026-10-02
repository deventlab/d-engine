//! Case 1: Verify that the Raft leader is elected based on the highest log Term and Index, not
//! merely the number of log entries — and that node 2 (the eventual leader) rejects a vote
//! request from a worse-log peer, proving Raft's election-safety guarantee (§5.4.1) is enforced
//! *on the wire*, not just that node 2 happened to win the timeout race.
//!
//! Scenario:
//!
//! 1. Create a cluster with 3 nodes (A, B, C).
//! 2. Node A appends 10 log entries with Term=2.
//! 3. Node B appends 8 log entries with Term=3 (higher term).
//! 4. Node C is a new node with no logs.
//! 5. Trigger a leader election (all three nodes use the same default randomized timeout —
//!    the rejection is no longer sourced from this race; see below).
//!
//! Expected Result:
//!
//! - Node B becomes the leader because its logs have the highest Term (Term=3), even though it has
//!   fewer entries than Node A.
//! - Nodes A and C recognize B as the leader.
//! - Node B rejects a synthetic vote request built with node A's real log profile (more entries,
//!   lower term) — proving the comparison is term-then-index, not entry count.
//!
//! ## Why the rejection is injected directly, not observed from the natural election
//!
//! Earlier versions of this test relied on the natural 3-node election race to produce the
//! rejection organically: whichever of A/C's randomized timers fired first would ask node B for
//! a vote and (having a worse log) get rejected. Two problems with that:
//!
//! 1. It's not guaranteed. If node B's own timer happens to fire first, it becomes candidate
//!    itself and never receives a vote request to reject — the assertion would then have nothing
//!    to observe, purely due to timer luck (observed in CI: ~1/9 runs).
//! 2. An earlier attempt to fix (1) by giving node B a much longer timeout than A/C — forcing A/C
//!    to go first — backfired: A and C's logs disagree with each other too (A has no entries in
//!    common with C's empty log, different terms), and they ended up in a prolonged split-vote
//!    livelock (term counters observed climbing past 20) with neither able to reach a majority
//!    without B.
//!
//! Fix: stop depending on the natural race for the rejection half of the proof. The test itself
//! opens a raw `RaftElectionServiceClient` to node B's real gRPC endpoint (the same generated
//! client real peers use — nothing mocked) and sends a `VoteRequest` carrying node A's exact log
//! signature (last_log_index=10, last_log_term=2) under a fake `candidate_id`, at `term=10` —
//! comfortably above any term the natural 3-node election reaches in this test's runtime
//! (observed: single digits) — so the rejection is unambiguously the log check, not term
//! staleness. This is sent immediately after the nodes start, before node B could plausibly have
//! become leader itself, so there is no leader-step-down side effect from the injected term bump.
//! The natural election (who the cluster actually elects) is untouched and still runs exactly as
//! before.

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
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::raft_election_service_client::RaftElectionServiceClient;
use std::time::Duration;
use tracing::debug;

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

    // Prove node 2 rejects a worse-log candidate on the wire (§5.4.1) — see the
    // module doc for why this is injected directly instead of relied on from
    // the natural election race. Sent now, immediately after node startup and
    // before the readiness sleep below, so node 2 cannot yet be leader (no
    // step-down side effect from the term bump this induces).
    let node2_addr = format!("http://127.0.0.1:{}", ports[1]);
    let inject_deadline = std::time::Instant::now() + Duration::from_secs(10);
    let vote_response = loop {
        let attempt = async {
            let mut client = RaftElectionServiceClient::connect(node2_addr.clone())
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
            client
                .request_vote(tonic::Request::new(VoteRequest {
                    term: 10,           // comfortably above any term the natural election reaches here
                    candidate_id: 99,   // synthetic candidate — not a real cluster member
                    last_log_index: 10, // node A's real log profile: more entries...
                    last_log_term: 2,   // ...but a lower term than node 2's (3)
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
    assert_eq!(
        (vote_response.last_log_index, vote_response.last_log_term),
        (8, 3),
        "rejection response must carry node 2's own real log signature, \
         confirming node 2 itself (not some other node) is the responder"
    );

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

    // Clean up
    ctx.shutdown().await
}
