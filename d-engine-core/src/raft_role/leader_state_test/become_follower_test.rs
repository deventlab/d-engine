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

/// Simulates a quorum ACK of a heartbeat sent just now, with the noop already applied.
/// Goes through the real `update_lease_timestamp`, so whatever it records (read lease,
/// quorum-ack time) is recorded exactly as in production.
fn simulate_quorum_ack(state: &mut LeaderState<MockTypeConfig>) {
    state.noop_log_id = Some(1);
    state.last_heartbeat_send_ts = now_ms();
    state.test_renew_lease_from_send_ts(60_000, 1);
}

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
    let lease = Arc::clone(&state.shared_state.read_lease);

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

/// A leader with a valid lease rejects a higher-term VoteRequest instead of stepping down.
///
/// Scenario:
/// - Leader (term 1) has a valid lease: a quorum acknowledged it within the lease window.
/// - A node returning from a partition asks for a vote with an inflated term (999).
///
/// Expected:
/// - No `BecomeFollower` event: the cluster is healthy, so the returning node must not
///   force a re-election.
/// - Response is a rejection carrying the leader's own term.
#[tokio::test]
async fn test_receive_higher_term_vote_request_with_valid_lease_is_rejected() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_vote_request_valid_lease_rejected")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let leader_term = state.current_term();
    simulate_quorum_ack(&mut state);
    assert!(state.is_lease_valid(), "precondition: lease must be valid");

    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
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
    state.handle_inbound_event(event, &context, internal_event_tx).await.unwrap();

    assert!(
        internal_event_rx.try_recv().is_err(),
        "a leader with a valid lease must not step down on a higher-term VoteRequest"
    );
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.vote_granted);
    assert_eq!(response.term, leader_term);
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
    let lease = Arc::clone(&state.shared_state.read_lease);

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

/// Rejecting a higher-term VoteRequest has no side effects on a leader with a valid lease.
///
/// Expected:
/// - Leader term unchanged: adopting 999 would make the leader reject its own followers'
///   term-1 traffic and break a healthy cluster.
/// - The read lease is untouched, so LeaseRead keeps being served; revoking it here would
///   punish readers for a vote request the leader ignored.
#[tokio::test]
async fn test_vote_request_higher_term_with_valid_lease_leaves_term_and_lease_untouched() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_vote_request_valid_lease_untouched")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let lease = Arc::clone(&state.shared_state.read_lease);
    let term_before = state.current_term();
    simulate_quorum_ack(&mut state);
    assert!(
        lease.is_valid(now_ms()),
        "precondition: lease must be valid"
    );

    let (resp_tx, _resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let event = InboundEvent::ReceiveVoteRequest(
        VoteRequest {
            term: 999,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        },
        resp_tx,
    );
    state.handle_inbound_event(event, &context, internal_event_tx).await.unwrap();

    assert_eq!(state.current_term(), term_before, "term must not change");
    assert!(lease.is_valid(now_ms()), "lease must stay valid");
}

/// A leader without a valid lease still steps down on a higher-term VoteRequest.
///
/// Expected:
/// - The guard is bounded by the lease: with no quorum acknowledgement in the window the
///   leader may really have lost leadership, so Raft's usual "higher term wins" applies.
/// - After the Raft loop runs `become_follower()`, the lease stays invalid.
#[tokio::test]
async fn test_receive_higher_term_vote_request_without_valid_lease_steps_down() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_vote_request_no_lease_steps_down")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let lease = Arc::clone(&state.shared_state.read_lease);
    assert!(
        !lease.is_valid(now_ms()),
        "precondition: lease was never renewed, so it is invalid"
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
    state.handle_inbound_event(event, &context, internal_event_tx).await.unwrap();

    assert!(
        matches!(
            internal_event_rx.try_recv(),
            Ok(InternalEvent::BecomeFollower(_))
        ),
        "higher-term VoteRequest without a valid lease must emit BecomeFollower"
    );

    let _ = state.become_follower().expect("become_follower must succeed");
    assert!(!lease.is_valid(now_ms()), "lease must stay invalid");
}

/// Stepping down on a higher-term VoteRequest adopts the new term and replays the request.
///
/// Expected (leader without a valid lease):
/// - The term becomes the candidate's term at the detection point.
/// - The VoteRequest is replayed to the new Follower role, which decides on the vote.
#[tokio::test]
async fn test_vote_request_higher_term_without_valid_lease_adopts_term_and_replays() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_vote_request_no_lease_replays")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    assert!(!state.is_lease_valid(), "precondition: lease invalid");

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
    state.handle_inbound_event(event, &context, internal_event_tx).await.unwrap();

    assert_eq!(state.current_term(), 999, "term must be adopted");
    assert!(matches!(
        internal_event_rx.try_recv(),
        Ok(InternalEvent::BecomeFollower(_))
    ));
    assert!(matches!(
        internal_event_rx.try_recv(),
        Ok(InternalEvent::ReprocessEvent(_))
    ));
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
    let lease = Arc::clone(&state.shared_state.read_lease);

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
    let lease = Arc::clone(&state.shared_state.read_lease);

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

/// Test: Leader persists hard_state when an AppendEntries response reveals a
/// higher term, BEFORE stepping down to Follower.
///
/// Scenario:
/// - Leader is at term=1, receives an AppendEntries response from a follower
///   carrying a higher term (999) — this is the response-side term check
///   (distinct from the request-side ones already covered in candidate/follower
///   tests), flagged during review as previously uncovered by any test.
///
/// Expected:
/// - `save_hard_state` is called exactly once with term=999.
/// - The call happens BEFORE `InternalEvent::BecomeFollower` is sent.
#[tokio::test]
async fn test_handle_append_result_higher_term_persists_hard_state_before_stepping_down() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let (internal_event_tx, internal_event_rx) = mpsc::unbounded_channel();
    let internal_event_rx = Arc::new(std::sync::Mutex::new(internal_event_rx));
    let rx_for_ordering_check = Arc::clone(&internal_event_rx);

    let mut raft_log = crate::MockRaftLog::new();
    raft_log
        .expect_save_hard_state()
        .withf(move |s| {
            assert!(
                rx_for_ordering_check.lock().unwrap().try_recv().is_err(),
                "save_hard_state must be called BEFORE BecomeFollower is sent"
            );
            s.current_term == 999
        })
        .times(1)
        .returning(|_| Ok(()));

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_append_result_higher_term_persists_hard_state")
        .with_raft_log(raft_log)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

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
            internal_event_rx.lock().unwrap().try_recv(),
            Ok(InternalEvent::BecomeFollower(_))
        ),
        "higher-term AppendResult must emit BecomeFollower"
    );
    assert_eq!(state.current_term(), 999, "Term should update to 999");
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
    let lease = Arc::clone(&state.shared_state.read_lease);

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

/// A new leader is protected from a higher-term VoteRequest as soon as a quorum has
/// acknowledged it, even while the read lease is still withheld.
///
/// Scenario:
/// - Leader just elected: noop at index 101, state machine applied only up to 100.
/// - A quorum ACK arrives. The read lease stays withheld (readers fall back to the Raft loop),
///   but a quorum has just confirmed this leader.
/// - A node returning from a partition asks for a vote with term 999.
///
/// Expected:
/// - The vote request is rejected and the leader stays leader. "A quorum recently confirmed me"
///   is what makes a step-down pointless; it must not depend on the state machine catching up.
#[tokio::test]
async fn test_leader_before_noop_applied_still_rejects_higher_term_vote_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_vote_request_before_noop_applied")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let lease = Arc::clone(&state.shared_state.read_lease);
    let leader_term = state.current_term();
    state.noop_log_id = Some(101);
    state.last_heartbeat_send_ts = now_ms();
    state.test_renew_lease_from_send_ts(60_000, 100);
    assert!(
        !lease.is_valid(now_ms()),
        "precondition: read lease withheld until the noop is applied"
    );

    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
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
    state.handle_inbound_event(event, &context, internal_event_tx).await.unwrap();

    assert!(
        internal_event_rx.try_recv().is_err(),
        "a leader a quorum just acknowledged must not step down on a higher-term VoteRequest"
    );
    assert_eq!(state.current_term(), leader_term, "term must not change");
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.vote_granted);
    assert_eq!(response.term, leader_term);
}

/// Once the quorum acknowledgement is older than the lease window, a higher-term
/// VoteRequest makes the leader step down again.
///
/// Expected:
/// - The protection is time-bounded: a leader nobody has confirmed recently may really have
///   lost leadership, so "higher term wins" applies.
#[tokio::test]
async fn test_leader_with_expired_quorum_ack_steps_down_on_higher_term_vote_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_vote_request_expired_quorum_ack")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.noop_log_id = Some(1);
    state.last_heartbeat_send_ts = now_ms();
    // 1 ms window, then let it pass.
    state.test_renew_lease_from_send_ts(1, 1);
    std::thread::sleep(std::time::Duration::from_millis(5));

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
    state.handle_inbound_event(event, &context, internal_event_tx).await.unwrap();

    assert!(matches!(
        internal_event_rx.try_recv(),
        Ok(InternalEvent::BecomeFollower(_))
    ));
}

// ============================================================================
// PreVote requests received by a Leader
//
// The election handler decides (it is mocked here); the Leader's job is to tell it whether a
// quorum recently confirmed it, to forward the answer unchanged, and to change nothing: a
// PreVote never makes a leader step down, take a term, or persist anything.
// ============================================================================

/// An election mock that expects exactly one PreVote and records the `leader_active` it got.
fn pre_vote_election_mock(
    seen_leader_active: Arc<std::sync::atomic::AtomicBool>
) -> crate::MockElectionCore<MockTypeConfig> {
    let mut election_handler = crate::MockElectionCore::<MockTypeConfig>::new();
    election_handler.expect_handle_pre_vote_request().times(1).returning(
        move |_, current_term, _, leader_active| {
            seen_leader_active.store(leader_active, std::sync::atomic::Ordering::SeqCst);
            d_engine_proto::server::election::VoteResponse {
                term: current_term,
                vote_granted: !leader_active,
                last_log_index: 7,
                last_log_term: 3,
            }
        },
    );
    election_handler
}

/// Sends a PreVote (term 999) to the leader and returns what the handler was told, the
/// response, whether any internal event (e.g. BecomeFollower) was emitted, and the leader's
/// term afterwards.
async fn leader_pre_vote(
    state: &mut LeaderState<MockTypeConfig>,
    context: &crate::RaftContext<MockTypeConfig>,
) -> (d_engine_proto::server::election::VoteResponse, bool) {
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    state
        .handle_inbound_event(
            InboundEvent::ReceivePreVoteRequest(
                VoteRequest {
                    term: 999,
                    candidate_id: 2,
                    last_log_index: 0,
                    last_log_term: 0,
                },
                resp_tx,
            ),
            context,
            internal_event_tx,
        )
        .await
        .unwrap();
    let response = resp_rx.recv().await.unwrap().unwrap();
    (response, internal_event_rx.try_recv().is_ok())
}

/// Test: a leader that no quorum has confirmed yet tells the handler no leader is active,
/// but still does not step down.
///
/// Expected:
/// - `leader_active == false`, the handler's answer is forwarded, no event is emitted, the
///   term is unchanged.
#[tokio::test]
async fn test_leader_pre_vote_without_quorum_ack_reports_inactive_and_does_not_step_down() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let seen = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_leader_pre_vote_no_ack")
        .with_election_handler(pre_vote_election_mock(Arc::clone(&seen)))
        .build_context();
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let term_before = state.current_term();

    let (response, emitted_event) = leader_pre_vote(&mut state, &context).await;

    assert!(!seen.load(std::sync::atomic::Ordering::SeqCst));
    assert!(response.vote_granted);
    assert!(
        !emitted_event,
        "a PreVote must never make a leader step down"
    );
    assert_eq!(state.current_term(), term_before);
}

/// Test: a leader that a quorum just confirmed tells the handler a leader is active, so the
/// PreVote is denied, and does not step down.
#[tokio::test]
async fn test_leader_pre_vote_with_recent_quorum_ack_reports_active_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_leader_pre_vote_recent_ack")
        .with_election_handler(pre_vote_election_mock(Arc::clone(&seen)))
        .build_context();
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    simulate_quorum_ack(&mut state);
    let term_before = state.current_term();

    let (response, emitted_event) = leader_pre_vote(&mut state, &context).await;

    assert!(seen.load(std::sync::atomic::Ordering::SeqCst));
    assert!(!response.vote_granted);
    assert!(!emitted_event);
    assert_eq!(state.current_term(), term_before);
}

/// Test: before the noop is applied the read lease is withheld, but a quorum has confirmed the
/// leader, so a PreVote must still be reported as "leader active".
///
/// Expected:
/// - `leader_active == true`. The protection must not depend on the state machine catching up.
#[tokio::test]
async fn test_leader_pre_vote_before_noop_applied_still_reports_active_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_leader_pre_vote_before_noop_applied")
        .with_election_handler(pre_vote_election_mock(Arc::clone(&seen)))
        .build_context();
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.noop_log_id = Some(101);
    state.last_heartbeat_send_ts = now_ms();
    state.test_renew_lease_from_send_ts(60_000, 100);

    leader_pre_vote(&mut state, &context).await;

    assert!(seen.load(std::sync::atomic::Ordering::SeqCst));
}

/// Test: once the quorum acknowledgement is older than the lease window the leader no longer
/// reports itself active (the protection is time-bounded).
#[tokio::test]
async fn test_leader_pre_vote_with_expired_quorum_ack_reports_inactive() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let seen = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_leader_pre_vote_expired_ack")
        .with_election_handler(pre_vote_election_mock(Arc::clone(&seen)))
        .build_context();
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.noop_log_id = Some(1);
    state.last_heartbeat_send_ts = now_ms();
    state.test_renew_lease_from_send_ts(1, 1);
    std::thread::sleep(std::time::Duration::from_millis(5));

    leader_pre_vote(&mut state, &context).await;

    assert!(!seen.load(std::sync::atomic::Ordering::SeqCst));
}
