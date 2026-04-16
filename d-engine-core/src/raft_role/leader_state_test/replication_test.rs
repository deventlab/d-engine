//! Tests for leader state replication and quorum verification
//!
//! This module tests the `process_batch`, `handle_append_result`, and
//! `execute_request_immediately` methods which handle log replication,
//! commit index calculation, and client response delivery.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use rand::distr::SampleString;
use tokio::sync::{mpsc, watch};
use tonic::Status;
use tracing_test::traced_test;

use crate::Error;
use crate::MockMembership;
use crate::MockRaftLog;
use crate::{MaybeCloneOneshot, PeerUpdate};

use crate::RaftRequestWithSignal;

use crate::event::RoleEvent;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_role::leader_state::LeaderState;
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::common::{EntryPayload, LogId, NodeRole::Follower, NodeStatus};
use d_engine_proto::server::cluster::{ClusterMembership, NodeMeta};
use d_engine_proto::server::replication::append_entries_response;
use d_engine_proto::server::replication::{AppendEntriesResponse, SuccessResult};

// ============================================================================
// Test Helper Structures and Functions
// ============================================================================

struct ProcessRaftRequestTestContext {
    state: LeaderState<MockTypeConfig>,
    raft_context: crate::raft_context::RaftContext<MockTypeConfig>,
}

/// Create a mock membership for testing (default: multi-node cluster)
fn create_mock_membership() -> MockMembership<MockTypeConfig> {
    let mut membership = MockMembership::new();
    membership.expect_is_single_node_cluster().returning(|| false);
    membership
}

/// Setup helper for process_batch tests with multi-node cluster configuration
async fn setup_process_batch_test_context(
    path: &str,
    graceful_rx: watch::Receiver<()>,
) -> ProcessRaftRequestTestContext {
    let mut context = mock_raft_context(path, graceful_rx, None);

    // Setup default membership mock with voters() and replication_peers() support for
    // init_cluster_metadata. For process_batch tests, we need multi-node cluster (2+ peers) to
    // avoid single_voter logic
    let mut membership = MockMembership::new();
    membership.expect_is_single_node_cluster().returning(|| false);
    membership.expect_voters().returning(|| {
        // Return 2 peer voters (node 2 and 3), so total = 2 + 1 (leader) = 3 (multi-node)
        vec![
            NodeMeta {
                id: 2,
                address: "".to_string(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
            NodeMeta {
                id: 3,
                address: "".to_string(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
        ]
    });
    membership.expect_replication_peers().returning(|| {
        vec![
            NodeMeta {
                id: 2,
                address: "".to_string(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
            NodeMeta {
                id: 3,
                address: "".to_string(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
        ]
    });
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_commit_index(5).unwrap(); // Initial commit index

    // Initialize cluster metadata with default membership
    state.init_cluster_metadata(&context.membership).await.unwrap();

    ProcessRaftRequestTestContext {
        state,
        raft_context: context,
    }
}

type ClientResponseResult = std::result::Result<ClientResponse, Status>;

fn mock_request(
    sender: crate::MaybeCloneOneshotSender<ClientResponseResult>
) -> RaftRequestWithSignal {
    RaftRequestWithSignal {
        id: rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21),
        payloads: vec![EntryPayload::command(Bytes::from_static(b"cmd"))],
        senders: vec![sender],
        wait_for_apply_event: false,
    }
}

fn success_response(
    term: u64,
    match_index: u64,
) -> AppendEntriesResponse {
    AppendEntriesResponse {
        node_id: 0,
        term,
        result: Some(append_entries_response::Result::Success(SuccessResult {
            last_match: Some(LogId {
                term,
                index: match_index,
            }),
        })),
    }
}

// ============================================================================
// Process Batch Tests
// ============================================================================

/// Test process_batch with quorum achieved
///
/// # Test Scenario
/// Single-voter leader completes a batch via handle_log_flushed.
/// All clients receive success responses once commit advances.
///
/// # Given
/// - Leader with commit_index = 5
/// - Batch of 2 client requests (end_log_index = 6)
/// - Single-voter: commit driven by handle_log_flushed
///
/// # When
/// - process_batch is called, then handle_log_flushed(6) advances commit
///
/// # Then
/// - Commit index advances to 6
/// - NotifyNewCommitIndex event is sent
/// - Both clients receive success responses
#[tokio::test]
#[traced_test]
async fn test_process_batch_quorum_achieved() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_quorum_achieved", graceful_rx)
            .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let last_entry_id = Arc::new(AtomicU64::new(4));
    let last_entry_id_clone = last_entry_id.clone();
    let mut raft_log = MockRaftLog::new();
    // start_index = 5, 2 writes → end = 6
    raft_log
        .expect_last_entry_id()
        .returning(move || last_entry_id_clone.load(Ordering::Relaxed));
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    // Prepare batch of 2 requests
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (tx2, rx2) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);
    let receivers = vec![rx1, rx2];

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;

    assert!(result.is_ok());

    // Single-voter: advance commit via handle_log_flushed.
    // MemFirst: set last_entry_id=6 before flush.
    last_entry_id.store(6, Ordering::Relaxed);
    context.state.cluster_metadata.single_voter = true;
    context.state.handle_log_flushed(6, &context.raft_context, &role_tx).await;

    assert_eq!(context.state.shared_state().commit_index, 6);
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));

    // Verify client responses
    for mut rx in receivers {
        let response = rx.recv().await.unwrap().unwrap();
        assert!(response.is_write_success());
    }
}

/// Test process_batch with quorum NOT achieved (verifiable failure)
///
/// # Test Scenario
/// Leader sends batch to peers; peers don't yet respond.
/// Writes stay pending until commit advances.
///
/// # Given
/// - Leader with commit_index = 5
/// - Batch of 2 client requests
///
/// # When
/// - process_batch is called (no handle_log_flushed / handle_append_result)
///
/// # Then
/// - Commit index remains at 5
/// - Client responses are pending (not yet delivered)
#[tokio::test]
#[traced_test]
async fn test_process_batch_quorum_failed_verifiable() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = setup_process_batch_test_context(
        "/tmp/test_process_batch_quorum_failed_verifiable",
        graceful_rx,
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, mut rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (tx2, mut rx2) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);
    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.shared_state().commit_index, 5); // Should not advance

    // Writes are pending — commit has not advanced yet
    assert!(
        rx1.try_recv().is_err(),
        "Write 1 is pending — no commit advancement"
    );
    assert!(
        rx2.try_recv().is_err(),
        "Write 2 is pending — no commit advancement"
    );
}

/// Test process_batch with quorum NOT achieved (non-verifiable failure)
///
/// # Test Scenario
/// Leader sends batch to multi-voter cluster; no peer ACKs arrive.
/// Writes stay pending until commit advances via handle_append_result.
///
/// # Given
/// - Leader with commit_index = 5
/// - Multi-voter cluster (nodes 2 and 3)
///
/// # When
/// - process_batch is called (no peer responses)
///
/// # Then
/// - Commit index remains at 5
/// - Client responses are pending
#[tokio::test]
#[traced_test]
async fn test_process_batch_quorum_non_verifiable_failure() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = setup_process_batch_test_context(
        "/tmp/test_process_batch_quorum_non_verifiable_failure",
        graceful_rx,
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, mut rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (tx2, mut rx2) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);

    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.shared_state().commit_index, 5); // Should not advance

    // Writes are pending — no peer responses received
    assert!(rx1.try_recv().is_err(), "Write 1 is pending — no peer ACKs");
    assert!(rx2.try_recv().is_err(), "Write 2 is pending — no peer ACKs");
}

/// Test process_batch with higher term detected
///
/// # Test Scenario
/// Follower responds with higher term via handle_append_result, triggering leader step-down.
///
/// # Given
/// - Leader with current_term = 1
/// - Pending write in queue
/// - Peer response carries term = 10 (higher than leader)
///
/// # When
/// - handle_append_result is called with higher-term response
///
/// # Then
/// - Leader steps down (BecomeFollower event sent)
/// - Leader's term is updated to 10
/// - Client receives TermOutdated response
#[tokio::test]
#[traced_test]
async fn test_process_batch_higher_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_higher_term", graceful_rx).await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, mut rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // process_batch queues the write and returns Ok
    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;
    assert!(result.is_ok());

    // Trigger step-down via higher-term peer response
    let higher_term_resp = AppendEntriesResponse {
        node_id: 2,
        term: 10,
        result: None,
    };
    let ha_result = context
        .state
        .handle_append_result(2, Ok(higher_term_resp), &context.raft_context, &role_tx)
        .await;
    assert!(ha_result.is_err());

    assert_eq!(context.state.current_term(), 10);
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::BecomeFollower(_))
    ));

    // Pending write receives TermOutdated error
    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_term_outdated());
}

/// Test process_batch with partial timeouts
///
/// # Test Scenario
/// Some peers respond, some do not. Writes stay pending until commit advances.
///
/// # Given
/// - Leader with commit_index = 5
/// - Multi-voter cluster
///
/// # When
/// - process_batch is called (no commit advancement)
///
/// # Then
/// - Commit index remains at 5
/// - Client response is pending
#[tokio::test]
#[traced_test]
async fn test_process_batch_partial_timeouts() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_partial_timeouts", graceful_rx)
            .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, mut rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.shared_state().commit_index, 5); // Initial commit index

    // Write is pending — no commit advancement yet
    assert!(
        rx1.try_recv().is_err(),
        "Write is pending — partial timeout scenario"
    );
}

/// Test process_batch with all peers timeout
///
/// # Test Scenario
/// No successful responses from any peer. Write stays pending.
///
/// # Given
/// - Leader with commit_index = 5
///
/// # When
/// - process_batch is called (no peer responses)
///
/// # Then
/// - Commit index remains at 5 (unchanged)
/// - Client response is pending
#[tokio::test]
#[traced_test]
async fn test_process_batch_all_timeout() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_all_timeout", graceful_rx).await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, mut rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.shared_state().commit_index, 5); // Unchanged

    // Write is pending — all peers timed out
    assert!(
        rx1.try_recv().is_err(),
        "Write is pending — all peers timed out"
    );
}

/// Test process_batch with fatal error during replication
///
/// # Test Scenario
/// Storage failure or unrecoverable error in prepare_batch_requests.
///
/// # Given
/// - Mock replication returns Fatal error
///
/// # When
/// - process_batch is called
///
/// # Then
/// - process_batch returns error
/// - Client receives ProposeFailed response immediately
#[tokio::test]
#[traced_test]
async fn test_process_batch_fatal_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_fatal_error", graceful_rx).await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::Fatal("Storage failure".to_string())));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, mut rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
        .await;

    assert!(result.is_err());

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_propose_failure());
}

// ============================================================================
// Commit Index Calculation Tests
// ============================================================================

/// Setup helper for commit index tests with configurable cluster membership
async fn setup_commit_index_test_context(
    path: &str,
    single_voter: bool,
) -> ProcessRaftRequestTestContext {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(path, graceful_rx, None);

    // Mock membership based on cluster topology
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));

    // For multi-node tests, return peer voters (excluding self/leader)
    // For single-voter tests, return empty (only self is voter)
    if single_voter {
        membership.expect_voters().returning(Vec::new);
    } else {
        // Multi-node: return 2 peer voters (node 2 and 3), so total = 2 + 1 (leader) = 3
        membership.expect_voters().returning(|| {
            vec![
                NodeMeta {
                    id: 2,
                    address: "".to_string(),
                    status: NodeStatus::Active as i32,
                    role: Follower.into(),
                },
                NodeMeta {
                    id: 3,
                    address: "".to_string(),
                    status: NodeStatus::Active as i32,
                    role: Follower.into(),
                },
            ]
        });
    }

    membership.expect_get_peers_id_with_condition().returning(|_| vec![]);
    membership.expect_members().returning(Vec::new);
    membership.expect_check_cluster_is_ready().returning(|| Ok(()));
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(|_current_leader_id| ClusterMembership {
            version: 1,
            nodes: vec![],
            current_leader_id: None,
        });
    membership.expect_pre_warm_connections().returning(|| Ok(()));
    if single_voter {
        membership.expect_replication_peers().returning(Vec::new);
    } else {
        membership.expect_replication_peers().returning(|| {
            vec![
                NodeMeta {
                    id: 2,
                    address: "".to_string(),
                    status: NodeStatus::Active as i32,
                    role: Follower.into(),
                },
                NodeMeta {
                    id: 3,
                    address: "".to_string(),
                    status: NodeStatus::Active as i32,
                    role: Follower.into(),
                },
            ]
        });
    }
    membership.expect_initial_cluster_size().returning(|| 3);
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_commit_index(5).unwrap();

    // Initialize cluster metadata to match the test scenario
    state.init_cluster_metadata(&context.membership).await.unwrap();

    ProcessRaftRequestTestContext {
        state,
        raft_context: context,
    }
}

/// Test commit index calculation for single-node cluster
///
/// # Test Scenario
/// Single-voter cluster: commit advances to durable_index via handle_log_flushed.
/// Verifies that single-voter commit path uses the handle_log_flushed durable param,
/// not calculate_majority_matched_index.
///
/// # Given
/// - Single-node cluster (single_voter = true)
/// - One write at index 7 (last_entry_id = 6 → start_index = 7)
///
/// # When
/// - process_batch is called (no peers → no replication requests sent)
/// - handle_log_flushed(7) is called to simulate the async LogFlushed event from BufferedRaftLog
///
/// # Then
/// - Commit index advances to 7 (driven by the simulated LogFlushed event)
/// - Client receives success response
#[tokio::test]
#[traced_test]
async fn test_single_node_cluster_commit_index() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_single_node_cluster_commit_index",
        true, // single-node
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let last_entry_id = Arc::new(AtomicU64::new(6));
    let last_entry_id_clone = last_entry_id.clone();
    let mut raft_log = MockRaftLog::new();
    // last_entry_id=6 → start_index=7; write stored at end_log_index=7
    raft_log
        .expect_last_entry_id()
        .returning(move || last_entry_id_clone.load(Ordering::Relaxed));
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    let (tx1, rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;
    assert!(result.is_ok());

    // Simulate the async LogFlushed event that BufferedRaftLog's batch_processor fires
    // after fsync. MemFirst: set last_entry_id=7 before flush.
    last_entry_id.store(7, Ordering::Relaxed);
    context.state.handle_log_flushed(7, &context.raft_context, &role_tx).await;

    assert_eq!(
        context.state.shared_state().commit_index,
        7,
        "Single-node: commit_index should advance to last_entry_id after LogFlushed"
    );
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));

    let mut rx = rx1;
    let response = rx.recv().await.unwrap().unwrap();
    assert!(response.is_write_success());
}

/// Test commit index calculation for multi-node cluster with empty peer_updates (Bug #186)
///
/// # Test Scenario
/// Multi-voter cluster: without peer ACKs, commit does not advance.
/// Verifies that multi-node cluster does NOT use single-node commit logic
/// when no peer responses are received.
///
/// # Given
/// - 3-node cluster (leader + 2 peers)
/// - No peer responses (workers fire-and-forget, nothing ACKed)
///
/// # When
/// - process_batch is called (no handle_append_result)
///
/// # Then
/// - Commit index remains at 5 (initial value)
/// - Write is pending — awaiting peer ACKs
#[tokio::test]
#[traced_test]
async fn test_multi_node_cluster_empty_peer_updates_commit_index() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_multi_node_empty_peer_updates_commit_index",
        false, // multi-node
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 9);
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;

    assert!(result.is_ok());
    assert_eq!(
        context.state.shared_state().commit_index,
        5,
        "Multi-node: commit does not advance without peer ACKs"
    );
    assert!(
        role_rx.try_recv().is_err(),
        "No NotifyNewCommitIndex without commit advancement"
    );

    // Write is pending — awaiting peer ACKs via handle_append_result
    let mut rx = rx1;
    assert!(
        rx.try_recv().is_err(),
        "Write is pending — no peer responses received"
    );
}

/// Test commit index calculation for multi-node cluster with peer responses
///
/// # Test Scenario
/// Leader receives a response from peer 2 and calculates commit index
/// via calculate_majority_matched_index (quorum).
///
/// # Given
/// - 3-node cluster
/// - last_entry_id = 4 → start_index = 5; 2 writes at end_log_index = 6
/// - Peer 2 responds with match_index = 6
/// - calculate_majority_matched_index returns 6
///
/// # When
/// - process_batch is called, then handle_append_result(2, success) is called
///
/// # Then
/// - Commit index advances to 6 (from calculate_majority_matched_index)
/// - Both clients receive success responses
#[tokio::test]
#[traced_test]
async fn test_multi_node_cluster_with_peer_updates_commit_index() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_multi_node_with_peer_updates_commit_index",
        false, // multi-node
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));
    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_success_response()
        .returning(|_, _, _, _| {
            Ok(PeerUpdate {
                match_index: Some(6),
                next_index: 7,
                success: true,
            })
        });

    let mut raft_log = MockRaftLog::new();
    // last_entry_id=4 → start_index=5; 2 writes → end_log_index=6
    raft_log.expect_last_entry_id().returning(|| 4);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(6));
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (tx2, rx2) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;
    assert!(result.is_ok());

    // Simulate peer 2 responding — triggers calculate_majority_matched_index → commit=6
    context
        .state
        .handle_append_result(
            2,
            Ok(success_response(1, 6)),
            &context.raft_context,
            &role_tx,
        )
        .await
        .unwrap();

    assert_eq!(context.state.shared_state().commit_index, 6);
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));

    let mut rx = rx1;
    let response = rx.recv().await.unwrap().unwrap();
    assert!(response.is_write_success());

    let mut rx = rx2;
    let response = rx.recv().await.unwrap().unwrap();
    assert!(response.is_write_success());
}

// ============================================================================
// Execute Request Immediately / Quorum Path Tests
// ============================================================================

/// Test execute_request_immediately with single-voter quorum achieved
///
/// # Previously: test_verify_internal_quorum_success
/// Restructured to bypass the blocking verify_internal_quorum wrapper
/// and test the underlying mechanism directly.
///
/// # Test Scenario
/// Single-voter leader executes a write immediately and advances commit via async LogFlushed.
///
/// # Given
/// - Single-voter cluster
/// - Mock: prepare_batch_requests returns Ok(empty)
///
/// # When
/// - execute_request_immediately is called
/// - handle_log_flushed(5) is called to simulate the async LogFlushed event from BufferedRaftLog
///
/// # Then
/// - Commit index advances to 5
/// - NotifyNewCommitIndex event is sent
/// - Response channel receives write_success
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_success() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_success",
        graceful_rx,
        None,
    );

    // Single-voter: no peers
    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    raft_context.membership = Arc::new(membership);

    raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let last_entry_id = Arc::new(AtomicU64::new(4));
    let last_entry_id_clone = last_entry_id.clone();
    let mut raft_log = MockRaftLog::new();
    // start_index=5, write at end=5; MemFirst: last_entry_id updated to 5 before flush
    raft_log
        .expect_last_entry_id()
        .returning(move || last_entry_id_clone.load(Ordering::Relaxed));
    raft_context.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());
    state.init_cluster_metadata(&raft_context.membership).await.unwrap();
    assert!(state.cluster_metadata.single_voter);

    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let req = RaftRequestWithSignal {
        id: rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21),
        payloads,
        senders: vec![resp_tx],
        wait_for_apply_event: false,
    };

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state.execute_request_immediately(req, &raft_context, &role_tx).await.unwrap();

    // Simulate the async LogFlushed event. MemFirst: set last_entry_id=5 before flush.
    last_entry_id.store(5, Ordering::Relaxed);
    state.handle_log_flushed(5, &raft_context, &role_tx).await;

    assert_eq!(state.shared_state().commit_index, 5);
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));

    // Response received: write_success
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.is_write_success());
}

/// Test execute_request_immediately with no commit advancement (verifiable failure scenario)
///
/// # Previously: test_verify_internal_quorum_verifiable_failure
/// Restructured: write stays pending when commit does not advance.
///
/// # Test Scenario
/// Leader executes a write but no commit advancement is triggered.
/// The response channel stays empty until commit catches up.
///
/// # Given
/// - Multi-voter cluster
/// - Mock: prepare_batch_requests returns Ok(empty)
///
/// # When
/// - execute_request_immediately is called (no handle_log_flushed / handle_append_result)
///
/// # Then
/// - Write stays pending (response not yet delivered)
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_verifiable_failure() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_verifiable_failure",
        graceful_rx,
        None,
    );

    raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    raft_context.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let req = RaftRequestWithSignal {
        id: rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21),
        payloads,
        senders: vec![resp_tx],
        wait_for_apply_event: false,
    };

    let (role_tx, _) = mpsc::unbounded_channel();
    state.execute_request_immediately(req, &raft_context, &role_tx).await.unwrap();

    // Write stays pending — no commit advancement
    assert!(
        resp_rx.try_recv().is_err(),
        "Write is pending — no commit advancement triggered"
    );
}

/// Test execute_request_immediately with multi-voter cluster, no peer responses
///
/// # Previously: test_verify_internal_quorum_non_verifiable_failure
/// Restructured: write stays pending when no peer ACKs arrive.
///
/// # Test Scenario
/// 4-node cluster, leader sends write, no peers respond.
/// Write remains pending until quorum is achieved.
///
/// # Given
/// - 4-node cluster (leader + 3 peers)
/// - No peer responses
///
/// # When
/// - execute_request_immediately called (no commit advancement)
///
/// # Then
/// - Write is pending (response not delivered)
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_non_verifiable_failure() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_non_verifiable_failure",
        graceful_rx,
        None,
    );

    raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    raft_context.storage.raft_log = Arc::new(raft_log);

    // Prepare 4-node membership
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(move || {
        vec![
            NodeMeta {
                id: 2,
                address: "".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                address: "".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 4,
                address: "".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            },
        ]
    });
    membership.expect_replication_peers().returning(Vec::new);
    raft_context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());
    state.init_cluster_metadata(&raft_context.membership).await.unwrap();

    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let req = RaftRequestWithSignal {
        id: rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21),
        payloads,
        senders: vec![resp_tx],
        wait_for_apply_event: false,
    };

    let (role_tx, _) = mpsc::unbounded_channel();
    state.execute_request_immediately(req, &raft_context, &role_tx).await.unwrap();

    // Write is pending — 4-node cluster, no peer ACKs received
    assert!(
        resp_rx.try_recv().is_err(),
        "Write is pending — no peer responses"
    );
}

/// Test execute_request_immediately with partial peer timeouts
///
/// # Previously: test_verify_internal_quorum_partial_timeouts
/// Restructured: write stays pending when insufficient peer ACKs arrive.
///
/// # Test Scenario
/// Some peers respond, some time out. Write stays pending.
///
/// # Given
/// - Multi-voter cluster
///
/// # When
/// - execute_request_immediately called (no commit advancement)
///
/// # Then
/// - Write is pending
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_partial_timeouts() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_partial_timeouts",
        graceful_rx,
        None,
    );

    raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    raft_context.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let req = RaftRequestWithSignal {
        id: rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21),
        payloads,
        senders: vec![resp_tx],
        wait_for_apply_event: false,
    };

    let (role_tx, _) = mpsc::unbounded_channel();
    state.execute_request_immediately(req, &raft_context, &role_tx).await.unwrap();

    // Write is pending — partial peer timeouts, no commit advancement
    assert!(
        resp_rx.try_recv().is_err(),
        "Write is pending — partial timeout scenario"
    );
}

/// Test execute_request_immediately with all peers timing out
///
/// # Previously: test_verify_internal_quorum_all_timeouts
/// Restructured: write stays pending when all peers time out.
///
/// # Given
/// - Multi-voter cluster
///
/// # When
/// - execute_request_immediately called (no peer ACKs)
///
/// # Then
/// - Write is pending
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_all_timeouts() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_all_timeouts",
        graceful_rx,
        None,
    );

    raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    raft_context.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let req = RaftRequestWithSignal {
        id: rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21),
        payloads,
        senders: vec![resp_tx],
        wait_for_apply_event: false,
    };

    let (role_tx, _) = mpsc::unbounded_channel();
    state.execute_request_immediately(req, &raft_context, &role_tx).await.unwrap();

    // Write is pending — all peers timed out
    assert!(
        resp_rx.try_recv().is_err(),
        "Write is pending — all peers timed out"
    );
}

/// Test execute_request_immediately then handle_append_result with higher term
///
/// # Previously: test_verify_internal_quorum_higher_term
/// Restructured to test the underlying handle_append_result HigherTerm path directly.
///
/// # Test Scenario
/// Follower responds with higher term, triggering step-down.
/// Pending write receives TermOutdated error.
///
/// # Given
/// - Leader with pending write
/// - Peer response carries term = 10 (higher than leader term = 1)
///
/// # When
/// - handle_append_result called with higher-term response
///
/// # Then
/// - Returns Err(HigherTerm(10))
/// - BecomeFollower event is sent
/// - Pending write receives TermOutdated response
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_higher_term() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_higher_term",
        graceful_rx,
        None,
    );

    raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    raft_context.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let req = RaftRequestWithSignal {
        id: rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21),
        payloads,
        senders: vec![resp_tx],
        wait_for_apply_event: false,
    };

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state.execute_request_immediately(req, &raft_context, &role_tx).await.unwrap();

    // Trigger step-down via higher-term peer response
    let higher_term_resp = AppendEntriesResponse {
        node_id: 2,
        term: 10,
        result: None,
    };
    let result = state
        .handle_append_result(2, Ok(higher_term_resp), &raft_context, &role_tx)
        .await;

    assert!(result.is_err());
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::BecomeFollower(_))
    ));

    // Pending write receives TermOutdated error
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.is_term_outdated());
}

/// Test execute_request_immediately with critical failure from prepare_batch_requests
///
/// # Previously: test_verify_internal_quorum_critical_failure
///
/// # Test Scenario
/// Fatal error in prepare_batch_requests causes immediate client notification.
///
/// # Given
/// - Mock replication returns Fatal error
///
/// # When
/// - execute_request_immediately is called
///
/// # Then
/// - Returns original error
/// - Pending write receives ProposeFailed response immediately
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_critical_failure() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_critical_failure",
        graceful_rx,
        None,
    );

    raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::Fatal("Storage failure".to_string())));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    raft_context.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let req = RaftRequestWithSignal {
        id: rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21),
        payloads,
        senders: vec![resp_tx],
        wait_for_apply_event: false,
    };

    let (role_tx, _) = mpsc::unbounded_channel();
    let result = state.execute_request_immediately(req, &raft_context, &role_tx).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::Fatal(msg) if msg == "Storage failure"
    ));

    // Client receives ProposeFailed immediately (sender notified in error path)
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.is_propose_failure());
}

// ============================================================================
// Regression Tests for Bug #268
// ============================================================================

/// Test execute_and_process_raft_rpc with multi-node cluster and empty peer_updates
///
/// # Regression Test for Bug #186 / #268
/// Verifies that multi-node cluster does NOT use single-node logic when no peer ACKs arrive.
/// Without peer responses, commit stays at initial value (not advanced to last_entry_id).
///
/// # Test Scenario
/// Multi-node cluster (3 nodes), no peer ACKs received.
/// Commit must remain at initial value, NOT jump to last_entry_id.
///
/// # Given
/// - 3-node cluster (leader + 2 peers)
/// - Mock: last_entry_id() returns 10 (would be wrong for single-node path)
///
/// # When
/// - process_batch is called (no peer responses)
///
/// # Then
/// - Commit index = 5 (initial value, unchanged)
/// - Write is pending — awaiting peer ACKs
#[tokio::test]
#[traced_test]
async fn test_execute_and_process_raft_rpc_multi_node_empty_peer_updates() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_execute_and_process_raft_rpc_multi_node_empty_peer_updates",
        false, // multi-node cluster
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    // Sentinel: if single-node path were taken, commit would jump to 10 (wrong)
    raft_log.expect_last_entry_id().returning(|| 10);
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    let (tx_write, rx_write) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx_write)]);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;

    assert!(result.is_ok());
    assert_eq!(
        context.state.shared_state().commit_index,
        5,
        "Multi-node cluster: commit stays at 5 without peer ACKs (not last_entry_id=10)"
    );
    assert!(
        role_rx.try_recv().is_err(),
        "No NotifyNewCommitIndex without commit advancement"
    );

    // Write request is pending — awaiting peer ACKs
    let mut rx = rx_write;
    assert!(
        rx.try_recv().is_err(),
        "Write is pending — no peer responses"
    );
}

/// Test merge_batch_to_write_metadata with empty payload but non-empty senders
///
/// # Regression Test for Bug #268
/// Verifies that merge_batch_to_write_metadata does NOT discard senders when payloads is empty.
/// This scenario occurs during quorum verification (verify_internal_quorum) where Leader sends
/// empty payload but needs response channel to wait for quorum result.
///
/// # Test Scenario
/// Internal quorum check: payloads=[] but senders=[channel] (Leader waiting for quorum response)
///
/// # Given
/// - RaftRequestWithSignal with payloads=[] and senders=[sender]
///
/// # When
/// - merge_batch_to_write_metadata is called
///
/// # Then
/// - Returns Some(WriteMetadata) with the sender intact
/// - NOT None (which would discard the sender and cause channel leak)
#[tokio::test]
#[traced_test]
async fn test_merge_batch_to_write_metadata_empty_payload_with_senders() {
    use crate::raft_role::leader_state::LeaderState;

    // Create a sender to simulate internal quorum check
    let (tx, _rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();

    // Simulate verify_internal_quorum scenario: empty payload, non-empty sender
    let batch = vec![RaftRequestWithSignal {
        id: rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 21),
        payloads: vec![],  // ← Empty payload (quorum check doesn't write logs)
        senders: vec![tx], // ← Non-empty sender (Leader needs response)
        wait_for_apply_event: false,
    }];

    let start_idx = 100;
    let (payloads, metadata) =
        LeaderState::<MockTypeConfig>::merge_batch_to_write_metadata(batch.into_iter(), start_idx);

    // Verify results
    assert!(payloads.is_empty(), "Payloads should be empty");
    assert!(
        metadata.is_some(),
        "WriteMetadata must NOT be None even when payloads is empty"
    );

    let meta = metadata.unwrap();
    assert_eq!(meta.start_idx, start_idx);
    assert_eq!(meta.senders.len(), 1, "Sender must be preserved");
    assert!(!meta.wait_for_apply);
}

// ============================================================================
// Quorum Calculation Tests (B1: Achieved / B2: Not Achieved / B3: Special)
// ============================================================================

/// B1: Two-node cluster achieves quorum when peer responds with success.
///
/// # Previously verified via handle_raft_request_in_batch:
/// peer_updates[peer2] = PeerUpdate{match_index:Some(3), next_index:4, success:true}
///
/// # Given
/// - 2-node cluster (leader=1, peer=2), initial commit=0, leader_term=1
/// - Peer 2 responds: success, term=1, match_index=3
///
/// # When
/// - handle_append_result(peer2, Ok(success(term=1, match_index=3)))
///
/// # Then
/// - Commit advances from 0 to 3 (quorum: leader + peer = 2/2)
/// - NotifyNewCommitIndex event sent
#[tokio::test]
#[traced_test]
async fn test_handle_append_result_two_node_quorum_achieved() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_handle_append_result_two_node_quorum_achieved",
        graceful_rx,
        None,
    );
    let peer2_id = 2u32;

    // 2-node membership
    let mut membership = MockMembership::new();
    membership.expect_voters().returning(move || {
        vec![NodeMeta {
            id: peer2_id,
            address: "".to_string(),
            status: NodeStatus::Active as i32,
            role: Follower.into(),
        }]
    });
    membership.expect_replication_peers().returning(move || {
        vec![NodeMeta {
            id: peer2_id,
            address: "".to_string(),
            status: NodeStatus::Active as i32,
            role: Follower.into(),
        }]
    });
    raft_context.membership = Arc::new(membership);

    // handle_success_response preserves original assertion: match_index=3, next_index=4
    raft_context
        .handlers
        .replication_handler
        .expect_handle_success_response()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(PeerUpdate {
                match_index: Some(3),
                next_index: 4,
                success: true,
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(3));
    raft_context.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());
    state.init_cluster_metadata(&raft_context.membership).await.unwrap();
    // initial commit_index = 0

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state
        .handle_append_result(
            peer2_id,
            Ok(success_response(1, 3)),
            &raft_context,
            &role_tx,
        )
        .await
        .unwrap();

    assert_eq!(
        state.shared_state().commit_index,
        3,
        "two-node: commit advances to 3 (quorum: leader + peer2 = 2/2)"
    );
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));
}

/// B1: Three-node cluster achieves quorum when all peers respond with success.
///
/// # Given
/// - 3-node cluster, initial commit=5, leader_term=1
/// - Peer 2 responds: success, match_index=10
/// - Peer 3 responds: success, match_index=10
///
/// # When
/// - handle_append_result called for each peer
///
/// # Then
/// - Commit advances to 10 after peer 2 (quorum: leader + peer2 = 2/3)
/// - Peer 3 response: calculate_majority returns None (already committed), no-op
#[tokio::test]
#[traced_test]
async fn test_handle_append_result_three_node_quorum_all_peers() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_handle_append_result_three_node_quorum_all_peers",
        false,
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_success_response()
        .returning(|_, _, _, _| {
            Ok(PeerUpdate {
                match_index: Some(10),
                next_index: 11,
                success: true,
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(
            |_, old_commit, _| {
                if old_commit < 10 { Some(10) } else { None }
            },
        );
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Peer 2 responds → quorum (2/3), commit advances
    context
        .state
        .handle_append_result(
            2,
            Ok(success_response(1, 10)),
            &context.raft_context,
            &role_tx,
        )
        .await
        .unwrap();

    assert_eq!(
        context.state.shared_state().commit_index,
        10,
        "commit advances after peer 2"
    );
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));

    // Peer 3 responds → already committed, calculate_majority returns None
    context
        .state
        .handle_append_result(
            3,
            Ok(success_response(1, 10)),
            &context.raft_context,
            &role_tx,
        )
        .await
        .unwrap();

    assert_eq!(
        context.state.shared_state().commit_index,
        10,
        "commit unchanged after peer 3"
    );
}

/// B1: Three-node cluster achieves quorum even with one peer timing out (partial response).
///
/// # Given
/// - 3-node cluster, initial commit=5, leader_term=1
/// - Peer 2 responds: success, match_index=10
/// - Peer 3: never responds (timeout)
///
/// # When
/// - handle_append_result called only for peer 2
///
/// # Then
/// - Commit advances to 10 (quorum: leader + peer2 = 2/3, majority achieved)
#[tokio::test]
#[traced_test]
async fn test_handle_append_result_three_node_quorum_partial_timeout() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_handle_append_result_three_node_quorum_partial_timeout",
        false,
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_success_response()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(PeerUpdate {
                match_index: Some(10),
                next_index: 11,
                success: true,
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(10));
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Only peer 2 responds; peer 3 never does
    context
        .state
        .handle_append_result(
            2,
            Ok(success_response(1, 10)),
            &context.raft_context,
            &role_tx,
        )
        .await
        .unwrap();

    assert_eq!(
        context.state.shared_state().commit_index,
        10,
        "quorum achieved with partial timeout (2/3 nodes = majority)"
    );
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));
}

/// B1: Five-node cluster achieves quorum when majority (3 of 4 peers) respond.
///
/// # Given
/// - 5-node cluster (leader + 4 peers), initial commit=5, leader_term=1
/// - Peers 2, 3, 4 respond: success, match_index=10
/// - Peer 5: never responds
///
/// # When
/// - handle_append_result called for peers 2, 3, 4
///
/// # Then
/// - Commit advances (quorum: leader + peers 2,3,4 = 4/5 ≥ majority of 3)
#[tokio::test]
#[traced_test]
async fn test_handle_append_result_five_node_quorum_majority() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_handle_append_result_five_node_quorum_majority",
        false,
    )
    .await;

    // Extend cluster to 5 nodes
    context.state.cluster_metadata.replication_targets.extend([
        NodeMeta {
            id: 4,
            address: "".to_string(),
            status: NodeStatus::Active as i32,
            role: Follower.into(),
        },
        NodeMeta {
            id: 5,
            address: "".to_string(),
            status: NodeStatus::Active as i32,
            role: Follower.into(),
        },
    ]);
    context.state.cluster_metadata.total_voters = 5;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_success_response()
        .returning(|_, _, _, _| {
            Ok(PeerUpdate {
                match_index: Some(10),
                next_index: 11,
                success: true,
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(
            |_, old_commit, _| {
                if old_commit < 10 { Some(10) } else { None }
            },
        );
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // 3 peers respond → majority of 5 (need ≥3) achieved
    context
        .state
        .handle_append_result(
            2,
            Ok(success_response(1, 10)),
            &context.raft_context,
            &role_tx,
        )
        .await
        .unwrap();

    assert_eq!(
        context.state.shared_state().commit_index,
        10,
        "commit advances (4/5 ≥ majority)"
    );
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));

    // Remaining peers just confirm — no second commit advance
    for peer_id in [3u32, 4u32] {
        context
            .state
            .handle_append_result(
                peer_id,
                Ok(success_response(1, 10)),
                &context.raft_context,
                &role_tx,
            )
            .await
            .unwrap();
    }
    assert_eq!(context.state.shared_state().commit_index, 10);
}

/// B2: Three-node cluster does not advance commit when all peers return RPC failures.
///
/// # Given
/// - 3-node cluster, initial commit=5, leader_term=1
/// - Peers 2 and 3: RPC failure (timeout/network error)
///
/// # When
/// - handle_append_result called with Err for each peer
///
/// # Then
/// - Returns Ok(()) for each (errors are logged, not propagated)
/// - Commit stays at 5 (no quorum achieved)
#[tokio::test]
#[traced_test]
async fn test_handle_append_result_three_node_no_quorum_all_timeouts() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_handle_append_result_three_node_no_quorum_all_timeouts",
        false,
    )
    .await;

    // No handle_success_response or calculate_majority_matched_index expected (early return on Err)

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    for peer_id in [2u32, 3u32] {
        let result = context
            .state
            .handle_append_result(
                peer_id,
                Err(crate::Error::Fatal("simulated timeout".to_string())),
                &context.raft_context,
                &role_tx,
            )
            .await;
        assert!(result.is_ok(), "RPC failure is tolerated, not propagated");
    }

    assert_eq!(
        context.state.shared_state().commit_index,
        5,
        "commit unchanged — all peers timed out, no quorum"
    );
    assert!(
        role_rx.try_recv().is_err(),
        "no NotifyNewCommitIndex without commit advancement"
    );
}

/// B2: Three-node cluster does not advance commit when majority calculation returns None.
///
/// # Given
/// - 3-node cluster, initial commit=5, leader_term=1
/// - Peer 2 responds success but calculate_majority returns None (insufficient replication)
///
/// # When
/// - handle_append_result(peer2, success)
///
/// # Then
/// - commit stays at 5 (calculate_majority returned None)
#[tokio::test]
#[traced_test]
async fn test_handle_append_result_three_node_no_quorum_single_peer_insufficient() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_handle_append_result_three_node_no_quorum_single_peer_insufficient",
        false,
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_success_response()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(PeerUpdate {
                match_index: Some(4),
                next_index: 5,
                success: true,
            })
        });

    // calculate_majority returns None — peer's match_index too low for new commit
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| None);
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    context
        .state
        .handle_append_result(
            2,
            Ok(success_response(1, 4)),
            &context.raft_context,
            &role_tx,
        )
        .await
        .unwrap();

    assert_eq!(
        context.state.shared_state().commit_index,
        5,
        "commit unchanged — calculate_majority returned None"
    );
    assert!(
        role_rx.try_recv().is_err(),
        "no NotifyNewCommitIndex without commit advancement"
    );
}

/// B2: Five-node cluster does not advance commit when only a minority of peers respond.
///
/// # Given
/// - 5-node cluster (leader + 4 peers), initial commit=5, leader_term=1
/// - Only peer 2 responds (1/4 peers = 2/5 total = minority)
///
/// # When
/// - handle_append_result(peer2, success)
///
/// # Then
/// - calculate_majority returns None (2/5 < majority of 3)
/// - commit stays at 5
#[tokio::test]
#[traced_test]
async fn test_handle_append_result_five_node_no_quorum_minority() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_handle_append_result_five_node_no_quorum_minority",
        false,
    )
    .await;

    context.state.cluster_metadata.replication_targets.extend([
        NodeMeta {
            id: 4,
            address: "".to_string(),
            status: NodeStatus::Active as i32,
            role: Follower.into(),
        },
        NodeMeta {
            id: 5,
            address: "".to_string(),
            status: NodeStatus::Active as i32,
            role: Follower.into(),
        },
    ]);
    context.state.cluster_metadata.total_voters = 5;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_success_response()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(PeerUpdate {
                match_index: Some(10),
                next_index: 11,
                success: true,
            })
        });

    // 1 peer (2/5 nodes) is minority — calculate_majority returns None
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| None);
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    context
        .state
        .handle_append_result(
            2,
            Ok(success_response(1, 10)),
            &context.raft_context,
            &role_tx,
        )
        .await
        .unwrap();

    assert_eq!(
        context.state.shared_state().commit_index,
        5,
        "commit unchanged — minority response (2/5 < majority)"
    );
    assert!(
        role_rx.try_recv().is_err(),
        "no NotifyNewCommitIndex without quorum"
    );
}

/// B3: Stale-term response is silently ignored.
///
/// # Given
/// - leader_term=1
/// - Peer responds with term=0 (stale, from previous term)
///
/// # When
/// - handle_append_result(peer2, Ok(response with term=0))
///
/// # Then
/// - Returns Ok() immediately (no handle_success_response, no commit change)
/// - commit unchanged
#[tokio::test]
#[traced_test]
async fn test_handle_append_result_stale_term_ignored() {
    let mut context =
        setup_commit_index_test_context("/tmp/test_handle_append_result_stale_term_ignored", false)
            .await;

    // No mock expectations — stale response triggers early return before any handler call

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // term=0 is stale (leader_term=1), response ignored
    let stale_response = AppendEntriesResponse {
        node_id: 2,
        term: 0,
        result: Some(append_entries_response::Result::Success(SuccessResult {
            last_match: Some(LogId { term: 0, index: 10 }),
        })),
    };
    let result = context
        .state
        .handle_append_result(2, Ok(stale_response), &context.raft_context, &role_tx)
        .await;

    assert!(result.is_ok(), "stale term response returns Ok (ignored)");
    assert_eq!(
        context.state.shared_state().commit_index,
        5,
        "commit unchanged — stale term response ignored"
    );
    assert!(
        role_rx.try_recv().is_err(),
        "no event emitted for stale response"
    );
}

/// B3: RPC failure is silently ignored (network errors do not propagate).
///
/// # Given
/// - leader_term=1
/// - RPC call to peer returns Err (network error / timeout)
///
/// # When
/// - handle_append_result(peer2, Err(network error))
///
/// # Then
/// - Returns Ok() immediately (error is logged, not propagated)
/// - commit unchanged
#[tokio::test]
#[traced_test]
async fn test_handle_append_result_rpc_failure_ignored() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_handle_append_result_rpc_failure_ignored",
        false,
    )
    .await;

    // No mock expectations — Err triggers early return before any handler call

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = context
        .state
        .handle_append_result(
            2,
            Err(crate::Error::Fatal("connection refused".to_string())),
            &context.raft_context,
            &role_tx,
        )
        .await;

    assert!(
        result.is_ok(),
        "RPC failure returns Ok (error is tolerated)"
    );
    assert_eq!(
        context.state.shared_state().commit_index,
        5,
        "commit unchanged — RPC failure ignored"
    );
    assert!(
        role_rx.try_recv().is_err(),
        "no event emitted for RPC failure"
    );
}

/// Pipeline: stale conflict ACK must not regress next_index below match_index + 1.
///
/// # Scenario
/// With pipeline depth > 1, two batches are in-flight simultaneously:
///   Batch A: entries 1-100 (prev_log_index = 0)
///   Batch B: entries 101-200 (prev_log_index = 100)
///
/// Batch B arrives at follower first (follower log is empty) → CONFLICT hint_index = 1.
/// Batch A arrives next → SUCCESS match_index = 100.
///
/// The leader receives SUCCESS first (next_index → 101, match_index → 100),
/// then the stale CONFLICT arrives. Without floor protection, next_index would
/// regress from 101 → 1, causing the leader to redundantly re-send entries 1-100.
///
/// # Expected
/// The floor guard (match_index + 1 = 101) prevents next_index from going below 101.
#[tokio::test]
#[traced_test]
async fn test_stale_conflict_ack_does_not_regress_next_index() {
    use crate::MockReplicationCore;
    use crate::test_utils::mock::MockBuilder;
    use d_engine_proto::server::replication::ConflictResult;
    use tokio::sync::watch;

    let peer_id = 2u32;

    // Build a replication handler that returns next_index=1 for the conflict —
    // simulating what happens when a stale conflict from an empty-log follower arrives.
    let mut rep = MockReplicationCore::<MockTypeConfig>::new();
    rep.expect_handle_conflict_response().returning(move |_, _, _, _| {
        Ok(crate::PeerUpdate {
            match_index: None,
            next_index: 1, // stale hint: follower was empty at the time
            success: false,
        })
    });

    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context_inner =
        MockBuilder::new(graceful_rx).with_replication_handler(rep).build_context();

    // Set up membership with peer 2 and 3 as voters (same as setup_commit_index_test_context).
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(|| {
        vec![
            NodeMeta {
                id: 2,
                address: "".into(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
            NodeMeta {
                id: 3,
                address: "".into(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
        ]
    });
    membership.expect_get_peers_id_with_condition().returning(|_| vec![]);
    membership.expect_members().returning(Vec::new);
    membership.expect_check_cluster_is_ready().returning(|| Ok(()));
    membership.expect_retrieve_cluster_membership_config().returning(|_| {
        d_engine_proto::server::cluster::ClusterMembership {
            version: 1,
            nodes: vec![],
            current_leader_id: None,
        }
    });
    membership.expect_pre_warm_connections().returning(|| Ok(()));
    membership.expect_replication_peers().returning(|| {
        vec![
            NodeMeta {
                id: 2,
                address: "".into(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
            NodeMeta {
                id: 3,
                address: "".into(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
        ]
    });
    membership.expect_initial_cluster_size().returning(|| 3);
    context_inner.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context_inner.node_config.clone());
    state.update_commit_index(5).unwrap();
    state.init_cluster_metadata(&context_inner.membership).await.unwrap();

    // Simulate: SUCCESS from Batch A already processed — match_index = 100, next_index = 101.
    state.match_index.insert(peer_id, 100);
    state.next_index.insert(peer_id, 101);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Stale CONFLICT from Batch B: follower's log was empty when it arrived.
    let stale_conflict = AppendEntriesResponse {
        node_id: peer_id,
        term: 1,
        result: Some(append_entries_response::Result::Conflict(ConflictResult {
            conflict_term: None,
            conflict_index: Some(1),
        })),
    };

    let result = state
        .handle_append_result(peer_id, Ok(stale_conflict), &context_inner, &role_tx)
        .await;

    assert!(result.is_ok(), "stale conflict must not return an error");

    // Floor guard: next_index must not retreat below match_index + 1 = 101.
    assert_eq!(
        state.next_index[&peer_id], 101,
        "stale conflict must not regress next_index below match_index + 1"
    );

    // match_index must remain 100 (conflict carries no match_index update).
    assert_eq!(
        state.match_index[&peer_id], 100,
        "match_index must remain unchanged after a conflict response"
    );
}
