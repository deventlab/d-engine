//! Tests verifying that `become_follower()` revokes the read lease.
//!
//! Safety invariant: a stepped-down leader must not allow ReadActor to serve
//! stale lease reads. The shared `Arc<ReadLease>` must be invalid immediately
//! after `become_follower()` returns so any concurrent ReadActor check fails.
//!
//! # Coverage
//! - Direct call: `become_follower()` revokes lease (unit pin)
//! - End-to-end: higher-term VoteRequest → BecomeFollower → become_follower() → lease invalid
//! - End-to-end: higher-term AppendEntries → BecomeFollower → become_follower() → lease invalid
//! - End-to-end: higher-term AppendResult → BecomeFollower → become_follower() → lease invalid

use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use crate::RaftNodeConfig;
use crate::event::{InboundEvent, InternalEvent};
use crate::maybe_clone_oneshot::{MaybeCloneOneshot, RaftOneshot};
use crate::now_ms;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::replication::{AppendEntriesRequest, AppendEntriesResponse};

/// become_follower() revokes the shared Arc<ReadLease> so ReadActor immediately
/// returns LeaseInvalid on the very next is_valid() check.
///
/// This pins the safety contract: after step-down the ReadActor fast path is
/// blocked regardless of when (before or after) its thread checks the lease.
#[tokio::test]
async fn test_become_follower_revokes_read_lease() {
    let mut state = LeaderState::<MockTypeConfig>::new(1, RaftNodeConfig::default().into());

    // Clone Arc before step-down to observe the shared lease from the outside
    // (simulates what ReadActor holds).
    let lease = Arc::clone(&state.shared_state.lease);

    // Renew with a generous 60-second deadline.
    state.test_update_lease_timestamp();
    assert!(
        state.is_lease_valid(),
        "precondition: lease must be valid before become_follower()"
    );
    assert!(
        lease.is_valid(now_ms()),
        "precondition: same Arc must also report valid"
    );

    // Transition to follower — must call lease.revoke() atomically.
    let _ = state.become_follower().expect("become_follower must succeed");

    // The shared Arc<ReadLease> must now be invalid.
    assert!(
        !lease.is_valid(now_ms()),
        "become_follower() must revoke the shared Arc<ReadLease>"
    );
}

/// Higher-term VoteRequest → BecomeFollower event → become_follower() → lease revoked.
///
/// End-to-end pin for the path:
///   ReceiveVoteRequest(term > current) → send_become_follower_event()
///   → Raft loop calls become_follower() → lease.revoke()
///
/// The ReadActor holds the same Arc<ReadLease>. After this chain the fast path
/// must return LeaseInvalid on the very next is_valid() check.
#[tokio::test]
async fn test_receive_higher_term_vote_request_revokes_lease() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_vote_request_revokes_lease")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let lease = Arc::clone(&state.shared_state.lease);

    state.test_update_lease_timestamp();
    assert!(
        lease.is_valid(now_ms()),
        "precondition: lease must be valid"
    );

    let (resp_tx, _resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let event = InboundEvent::ReceiveVoteRequest(
        VoteRequest {
            term: 999,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        },
        resp_tx,
    );
    state.handle_inbound_event(event, &context, internal_event_tx).await.ok();

    assert!(
        matches!(
            internal_event_rx.try_recv(),
            Ok(InternalEvent::BecomeFollower(_))
        ),
        "higher-term VoteRequest must emit BecomeFollower"
    );

    // Simulate Raft loop processing BecomeFollower.
    let _ = state.become_follower().expect("become_follower must succeed");

    assert!(
        !lease.is_valid(now_ms()),
        "lease must be revoked after VoteRequest step-down — ReadActor must not serve stale reads"
    );
}

/// Higher-term AppendEntries → BecomeFollower event → become_follower() → lease revoked.
///
/// End-to-end pin for the path:
///   AppendEntries(term > current) → send_become_follower_event()
///   → Raft loop calls become_follower() → lease.revoke()
#[tokio::test]
async fn test_receive_higher_term_append_entries_revokes_lease() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_append_entries_revokes_lease")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(10);
    let lease = Arc::clone(&state.shared_state.lease);

    state.test_update_lease_timestamp();
    assert!(
        lease.is_valid(now_ms()),
        "precondition: lease must be valid"
    );

    let (resp_tx, _resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let event = InboundEvent::AppendEntries(
        AppendEntriesRequest {
            term: 11, // higher than current term 10
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit_index: 0,
        },
        vec![resp_tx],
    );
    state.handle_inbound_event(event, &context, internal_event_tx).await.ok();

    assert!(
        matches!(
            internal_event_rx.try_recv(),
            Ok(InternalEvent::BecomeFollower(_))
        ),
        "higher-term AppendEntries must emit BecomeFollower"
    );

    // Simulate Raft loop processing BecomeFollower.
    let _ = state.become_follower().expect("become_follower must succeed");

    assert!(
        !lease.is_valid(now_ms()),
        "lease must be revoked after AppendEntries step-down"
    );
}

/// Higher-term VoteRequest → lease revoked BEFORE Raft loop calls become_follower().
///
/// Pins the window-period safety contract: the Raft loop sends BecomeFollower into
/// internal_event_tx but may not process it immediately (async event-driven). During that gap a
/// concurrent ReadActor task can observe the old valid lease and serve a stale LeaseRead.
///
/// This test verifies the fix: revoke() is called at the detection point itself, so
/// the lease is invalid as soon as handle_inbound_event() returns — before become_follower()
/// is ever called by the Raft loop.
///
/// Expected to FAIL before the fix (lease still valid in window), PASS after.
#[tokio::test]
async fn test_vote_request_higher_term_lease_revoked_before_become_follower() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_vote_request_revokes_lease_early")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let lease = Arc::clone(&state.shared_state.lease);

    state.test_update_lease_timestamp();
    assert!(
        lease.is_valid(now_ms()),
        "precondition: lease must be valid"
    );

    let (resp_tx, _resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let event = InboundEvent::ReceiveVoteRequest(
        VoteRequest {
            term: 999,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        },
        resp_tx,
    );
    state.handle_inbound_event(event, &context, internal_event_tx).await.ok();

    assert!(
        matches!(
            internal_event_rx.try_recv(),
            Ok(InternalEvent::BecomeFollower(_))
        ),
        "higher-term VoteRequest must emit BecomeFollower"
    );

    // become_follower() is intentionally NOT called here — the Raft loop has not yet
    // processed the BecomeFollower event. The lease must already be invalid.
    assert!(
        !lease.is_valid(now_ms()),
        "lease must be revoked at detection point, not waiting for become_follower() \
         — window-period fix required in handle_inbound_event VoteRequest branch"
    );
}

/// Higher-term AppendEntries → lease revoked BEFORE Raft loop calls become_follower().
///
/// Same window-period contract as test_vote_request_higher_term_lease_revoked_before_become_follower,
/// for the AppendEntries detection path.
#[tokio::test]
async fn test_append_entries_higher_term_lease_revoked_before_become_follower() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_append_entries_revokes_lease_early")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(10);
    let lease = Arc::clone(&state.shared_state.lease);

    state.test_update_lease_timestamp();
    assert!(
        lease.is_valid(now_ms()),
        "precondition: lease must be valid"
    );

    let (resp_tx, _resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let event = InboundEvent::AppendEntries(
        AppendEntriesRequest {
            term: 11,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit_index: 0,
        },
        vec![resp_tx],
    );
    state.handle_inbound_event(event, &context, internal_event_tx).await.ok();

    assert!(
        matches!(
            internal_event_rx.try_recv(),
            Ok(InternalEvent::BecomeFollower(_))
        ),
        "higher-term AppendEntries must emit BecomeFollower"
    );

    // become_follower() intentionally NOT called — pinning the window-period invariant.
    assert!(
        !lease.is_valid(now_ms()),
        "lease must be revoked at detection point, not waiting for become_follower() \
         — window-period fix required in handle_inbound_event AppendEntries branch"
    );
}

/// Higher-term AppendResult → lease revoked BEFORE Raft loop calls become_follower().
///
/// Same window-period contract for the handle_append_result detection path.
#[tokio::test]
async fn test_append_result_higher_term_lease_revoked_before_become_follower() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_append_result_revokes_lease_early")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let lease = Arc::clone(&state.shared_state.lease);

    state.test_update_lease_timestamp();
    assert!(
        lease.is_valid(now_ms()),
        "precondition: lease must be valid"
    );

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let higher_term_response = AppendEntriesResponse {
        node_id: 2,
        term: 999,
        result: None,
    };
    let result = state
        .handle_append_result(2, Ok(higher_term_response), &context, &internal_event_tx)
        .await;

    assert!(result.is_err(), "higher-term AppendResult must return Err");
    assert!(
        matches!(
            internal_event_rx.try_recv(),
            Ok(InternalEvent::BecomeFollower(_))
        ),
        "higher-term AppendResult must emit BecomeFollower"
    );

    // become_follower() intentionally NOT called — pinning the window-period invariant.
    assert!(
        !lease.is_valid(now_ms()),
        "lease must be revoked at detection point, not waiting for become_follower() \
         — window-period fix required in handle_append_result HigherTerm branch"
    );
}

/// Higher-term AppendResult → BecomeFollower event → become_follower() → lease revoked.
///
/// End-to-end pin for the path:
///   handle_append_result(response.term > current) → send_become_follower_event()
///   → Raft loop calls become_follower() → lease.revoke()
#[tokio::test]
async fn test_append_result_higher_term_revokes_lease() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_append_result_revokes_lease")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let lease = Arc::clone(&state.shared_state.lease);

    state.test_update_lease_timestamp();
    assert!(
        lease.is_valid(now_ms()),
        "precondition: lease must be valid"
    );

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let higher_term_response = AppendEntriesResponse {
        node_id: 2,
        term: 999, // higher than leader term=1
        result: None,
    };
    let result = state
        .handle_append_result(2, Ok(higher_term_response), &context, &internal_event_tx)
        .await;

    assert!(result.is_err(), "higher-term AppendResult must return Err");
    assert!(
        matches!(
            internal_event_rx.try_recv(),
            Ok(InternalEvent::BecomeFollower(_))
        ),
        "higher-term AppendResult must emit BecomeFollower"
    );

    // Simulate Raft loop processing BecomeFollower.
    let _ = state.become_follower().expect("become_follower must succeed");

    assert!(
        !lease.is_valid(now_ms()),
        "lease must be revoked after AppendResult step-down"
    );
}
