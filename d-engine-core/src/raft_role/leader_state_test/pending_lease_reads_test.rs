//! Tests for `pending_lease_reads` queue.
//!
//! Covers: queue push on expired lease, drain on quorum ACK (multi-voter),
//! immediate serve for single-voter, deadline cleanup in tick(),
//! cleanup on step-down, batching of multiple requests, and
//! Unavailable delivery when leadership is lost via higher-term step-down.

use std::sync::Arc;
use std::time::Duration;

use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::SuccessResult;
use d_engine_proto::server::replication::append_entries_response;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tonic::Code;
use tracing_test::traced_test;

use crate::ClientCmd;
use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::PeerUpdate;
use crate::RaftNodeConfig;
use crate::ReadConsistencyPolicy;
use crate::convert::safe_kv_bytes;
use crate::maybe_clone_oneshot::{MaybeCloneOneshot, RaftOneshot};
use crate::raft_role::leader_state::{LeaderState, PendingLeaseRead};
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;

// ============================================================================
// Helpers
// ============================================================================

fn make_lease_read_cmd(
    sender: crate::MaybeCloneOneshotSender<
        std::result::Result<d_engine_proto::client::ClientResponse, tonic::Status>,
    >
) -> ClientCmd {
    let req = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
        keys: vec![safe_kv_bytes(1)],
    };
    ClientCmd::Read(req, sender)
}

fn make_lease_read_request() -> ClientReadRequest {
    ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
        keys: vec![safe_kv_bytes(1)],
    }
}

fn quorum_ack(
    term: u64,
    match_index: u64,
) -> AppendEntriesResponse {
    AppendEntriesResponse {
        node_id: 2,
        term,
        result: Some(append_entries_response::Result::Success(SuccessResult {
            last_match: Some(LogId {
                term,
                index: match_index,
            }),
        })),
    }
}

/// Build a 3-voter context (leader=1, peers=2,3) with expired lease.
/// Default SM handler returns None for reads (unwrap_or_default gives empty vec).
async fn setup_multi_voter_expired_lease(
    path: &str
) -> (
    LeaderState<MockTypeConfig>,
    crate::raft_context::RaftContext<MockTypeConfig>,
    mpsc::UnboundedSender<crate::event::RoleEvent>,
    mpsc::UnboundedReceiver<crate::event::RoleEvent>,
) {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 10);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(10));

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;
    node_config.raft.batching.max_batch_size = 1;
    node_config.raft.general_raft_timeout_duration_in_ms = 2000;

    let context = MockBuilder::new(graceful_rx)
        .with_db_path(path)
        .with_replication_handler(replication_handler)
        .with_raft_log(raft_log)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    let mut membership = MockMembership::new();
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
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();
    // lease is expired by default (timestamp = 0)

    let (role_tx, role_rx) = mpsc::unbounded_channel();
    (state, context, role_tx, role_rx)
}

// ============================================================================
// Test 1: expired lease pushes request into pending_lease_reads, not blocked
// ============================================================================

/// When lease is expired in a multi-voter cluster, a LeaseRead request must be
/// placed into `pending_lease_reads` and flush_cmd_buffers must return without
/// blocking (no timeout-await on quorum).
#[tokio::test]
#[traced_test]
async fn test_lease_read_expired_pushes_to_pending_lease_reads() {
    let (mut state, context, role_tx, _role_rx) = setup_multi_voter_expired_lease(
        "/tmp/test_lease_read_expired_pushes_to_pending_lease_reads",
    )
    .await;

    assert!(
        !state.is_lease_valid(&context),
        "precondition: lease expired"
    );

    let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
    state.push_client_cmd(make_lease_read_cmd(resp_tx), &context);

    // flush_cmd_buffers must return quickly (must not block awaiting quorum)
    let flush_result = tokio::time::timeout(
        Duration::from_millis(200),
        state.flush_cmd_buffers(&context, &role_tx),
    )
    .await;

    assert!(
        flush_result.is_ok(),
        "flush_cmd_buffers must not block on expired lease"
    );
    assert!(flush_result.unwrap().is_ok());

    // Request must be queued, not discarded or served yet
    assert_eq!(
        state.pending_lease_reads.len(),
        1,
        "expired lease read must be queued in pending_lease_reads"
    );
}

// ============================================================================
// Test 2: quorum ACK via handle_append_result drains pending_lease_reads
// ============================================================================

/// After quorum ACK arrives (handle_append_result), pending_lease_reads must be
/// drained: lease refreshed, SM read executed, sender notified with success.
#[tokio::test]
#[traced_test]
async fn test_pending_lease_reads_drained_on_quorum_ack() {
    let (mut state, mut context, role_tx, _role_rx) =
        setup_multi_voter_expired_lease("/tmp/test_pending_lease_reads_drained_on_quorum_ack")
            .await;

    context.handlers.replication_handler.expect_handle_success_response().returning(
        |_, _, _, _| {
            Ok(PeerUpdate {
                match_index: Some(10),
                next_index: 11,
                success: true,
            })
        },
    );

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    state.push_client_cmd(make_lease_read_cmd(resp_tx), &context);
    state.flush_cmd_buffers(&context, &role_tx).await.unwrap();

    assert_eq!(
        state.pending_lease_reads.len(),
        1,
        "precondition: request is queued"
    );

    // Simulate quorum ACK from peer 2 with match_index=10.
    // Majority = 2/3: leader(self) + peer 2 is sufficient.
    state
        .handle_append_result(2, Ok(quorum_ack(1, 10)), &context, &role_tx)
        .await
        .unwrap();

    // Queue must be drained
    assert_eq!(
        state.pending_lease_reads.len(),
        0,
        "pending_lease_reads must be empty after quorum ACK"
    );

    // Sender must receive success response
    let result = resp_rx.recv().await.unwrap();
    assert!(
        result.is_ok(),
        "sender must receive success response after quorum ACK"
    );
}

// ============================================================================
// Test 3: single-voter serves lease read immediately (no queue)
// ============================================================================

/// For single-voter cluster, quorum = self, so expired lease must be refreshed
/// immediately and the read served without going through pending_lease_reads.
#[tokio::test]
#[traced_test]
async fn test_single_voter_lease_read_served_immediately_on_expired_lease() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 5);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;
    node_config.raft.batching.max_batch_size = 1;

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_single_voter_lease_read_served_immediately")
        .with_replication_handler(replication_handler)
        .with_raft_log(raft_log)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    assert!(
        state.cluster_metadata.single_voter,
        "precondition: single-voter"
    );
    assert!(
        !state.is_lease_valid(&context),
        "precondition: lease expired"
    );

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    state.push_client_cmd(make_lease_read_cmd(resp_tx), &context);
    state.flush_cmd_buffers(&context, &role_tx).await.unwrap();

    // Must not be queued — single-voter serves immediately
    assert_eq!(
        state.pending_lease_reads.len(),
        0,
        "single-voter must not queue in pending_lease_reads"
    );

    // Sender must receive response
    let result = resp_rx.recv().await.unwrap();
    assert!(
        result.is_ok(),
        "single-voter must serve lease read immediately"
    );
}

// ============================================================================
// Test 4: tick() cleans up expired pending_lease_reads
// ============================================================================

/// Requests in pending_lease_reads that exceed their deadline must be removed
/// by tick() and their senders notified with DeadlineExceeded.
#[tokio::test]
#[traced_test]
async fn test_pending_lease_reads_timeout_on_tick() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_pending_lease_reads_timeout_on_tick")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();

    // Inject a request with an already-expired deadline directly
    state.pending_lease_reads.push_back(PendingLeaseRead {
        request: make_lease_read_request(),
        sender: resp_tx,
        deadline: Instant::now() - Duration::from_secs(1),
    });

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let (raft_tx, _raft_rx) = mpsc::channel(1);
    state.tick(&role_tx, &raft_tx, &context).await.unwrap();

    // Queue must be cleared
    assert_eq!(
        state.pending_lease_reads.len(),
        0,
        "tick() must remove expired pending_lease_reads entries"
    );

    // Sender must receive DeadlineExceeded
    let result = resp_rx.recv().await.unwrap();
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        Code::DeadlineExceeded,
        "expired lease read must receive DeadlineExceeded"
    );
}

// ============================================================================
// Test 5: step-down drains pending_lease_reads with Unavailable error
// ============================================================================

/// When the leader steps down, all pending_lease_reads must be drained with
/// Unavailable (not left hanging).
#[tokio::test]
#[traced_test]
async fn test_pending_lease_reads_cleared_on_stepdown() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_pending_lease_reads_cleared_on_stepdown")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    state.pending_lease_reads.push_back(PendingLeaseRead {
        request: make_lease_read_request(),
        sender: resp_tx,
        deadline: Instant::now() + Duration::from_secs(5),
    });

    // drain_read_buffer is called during role transition (step-down)
    state.drain_read_buffer().unwrap();

    assert_eq!(
        state.pending_lease_reads.len(),
        0,
        "step-down must drain pending_lease_reads"
    );

    let result = resp_rx.recv().await.unwrap();
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        Code::Unavailable,
        "step-down must send Unavailable to pending lease reads"
    );
}

// ============================================================================
// Test 6: multiple lease reads all served on single quorum ACK (batching)
// ============================================================================

/// Multiple expired lease reads queued before quorum ACK arrives must all be
/// served together when a single quorum ACK comes in — not one at a time.
#[tokio::test]
#[traced_test]
async fn test_multiple_lease_reads_batched_on_single_quorum_ack() {
    let (mut state, mut context, role_tx, _role_rx) = setup_multi_voter_expired_lease(
        "/tmp/test_multiple_lease_reads_batched_on_single_quorum_ack",
    )
    .await;

    context.handlers.replication_handler.expect_handle_success_response().returning(
        |_, _, _, _| {
            Ok(PeerUpdate {
                match_index: Some(10),
                next_index: 11,
                success: true,
            })
        },
    );

    // Push 3 lease reads while lease is expired
    let mut receivers = vec![];
    for _ in 0..3 {
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        state.push_client_cmd(make_lease_read_cmd(resp_tx), &context);
        state.flush_cmd_buffers(&context, &role_tx).await.unwrap();
        receivers.push(resp_rx);
    }

    assert_eq!(
        state.pending_lease_reads.len(),
        3,
        "precondition: 3 requests queued"
    );

    // Single quorum ACK must drain all 3
    state
        .handle_append_result(2, Ok(quorum_ack(1, 10)), &context, &role_tx)
        .await
        .unwrap();

    assert_eq!(
        state.pending_lease_reads.len(),
        0,
        "all 3 must be drained by one ACK"
    );

    for mut rx in receivers {
        let result = rx.recv().await.unwrap();
        assert!(result.is_ok(), "each lease read must receive success");
    }
}

// ============================================================================
// Test 7: leadership lost via higher-term step-down drains pending_lease_reads
// ============================================================================

/// When a peer responds with a higher term, the leader steps down. Any requests
/// sitting in `pending_lease_reads` must be drained with `Unavailable` — the
/// client must not hang waiting for a quorum ACK that will never arrive.
///
/// # Flow
/// 1. Expired lease → request queued in `pending_lease_reads`
/// 2. Peer 2 responds with term=10 (higher than leader term=1)
/// 3. `handle_append_result` detects higher term → `BecomeFollower` event sent
///    → returns `Err(HigherTerm)`
/// 4. Raft loop processes `BecomeFollower` → calls `drain_read_buffer()`
/// 5. All pending lease reads receive `Unavailable`
///
/// # Why this test exists
/// The drain path on step-down (`drain_read_buffer`) is separate from the normal
/// quorum-ACK drain path (`drain_pending_lease_reads`). Without this test, a
/// regression could leave clients hanging when the leader loses an election.
#[tokio::test]
#[traced_test]
async fn test_pending_lease_reads_drained_on_higher_term_stepdown() {
    use crate::event::RoleEvent;
    use d_engine_proto::server::replication::AppendEntriesResponse;

    let (mut state, mut context, role_tx, mut role_rx) = setup_multi_voter_expired_lease(
        "/tmp/test_pending_lease_reads_drained_on_higher_term_stepdown",
    )
    .await;

    context.handlers.replication_handler.expect_handle_success_response().returning(
        |_, _, _, _| {
            Ok(PeerUpdate {
                match_index: Some(10),
                next_index: 11,
                success: true,
            })
        },
    );

    // Queue one lease read while lease is expired
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    state.push_client_cmd(make_lease_read_cmd(resp_tx), &context);
    state.flush_cmd_buffers(&context, &role_tx).await.unwrap();
    assert_eq!(
        state.pending_lease_reads.len(),
        1,
        "precondition: request is queued"
    );

    // Peer 2 responds with higher term — triggers step-down
    let higher_term_resp = AppendEntriesResponse {
        node_id: 2,
        term: 10, // higher than leader term=1
        result: None,
    };
    let result = state.handle_append_result(2, Ok(higher_term_resp), &context, &role_tx).await;

    // handle_append_result must return Err(HigherTerm)
    assert!(result.is_err(), "higher term must return Err");

    // BecomeFollower event must be sent to the Raft loop
    assert!(
        matches!(role_rx.try_recv(), Ok(RoleEvent::BecomeFollower(_))),
        "BecomeFollower event must be queued"
    );

    // Simulate Raft loop completing the step-down: drain_read_buffer is called
    // during role transition from Leader to Follower.
    state.drain_read_buffer().unwrap();

    // pending_lease_reads must be empty after step-down
    assert_eq!(
        state.pending_lease_reads.len(),
        0,
        "step-down must drain pending_lease_reads"
    );

    // Client must receive Unavailable — not hang forever
    let result = resp_rx.recv().await.unwrap();
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        Code::Unavailable,
        "client must receive Unavailable when leader steps down"
    );
}
