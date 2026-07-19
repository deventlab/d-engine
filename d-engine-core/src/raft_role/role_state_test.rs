//! Tests for role_state free functions
//!
//! Covers:
//! - schedule_and_execute_purge: two-phase log purge orchestration (Phase 1 = schedule, Phase 2 = execute)

use tokio::sync::{mpsc, watch};

use d_engine_proto::common::LogId;

use d_engine_proto::server::election::VotedFor;

use crate::Error;
use crate::InternalEvent;
use crate::MockPurgeExecutor;
use crate::MockRaftLog;
use crate::raft_role::candidate_state::CandidateState;
use crate::test_utils::mock::{MockBuilder, MockTypeConfig, mock_raft_log};
use crate::test_utils::node_config;

use super::role_state::RaftRoleState;
use super::role_state::schedule_and_execute_purge;

// ============================================================================
// schedule_and_execute_purge Tests
// ============================================================================

/// Happy path: snapshot at index 50, retained=1 → purge_upto=49.
/// Both phases execute: Phase 1 schedules, Phase 2 purges and dispatches LogPurgeCompleted.
#[tokio::test]
async fn test_schedule_and_execute_purge_happy_path() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_log = mock_raft_log();
    raft_log
        .expect_entry_term()
        .withf(|&idx| idx == 49)
        .times(1)
        .returning(|_| Some(1));

    let ctx = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let last_included = LogId { index: 50, term: 1 };
    let mut scheduled_purge_upto: Option<LogId> = None;

    let result = schedule_and_execute_purge(
        last_included,
        &ctx,
        100,
        None,
        &mut scheduled_purge_upto,
        &internal_event_tx,
    )
    .await;

    assert!(result.is_ok());
    assert_eq!(
        scheduled_purge_upto,
        Some(LogId { index: 49, term: 1 }),
        "Phase 1 must schedule purge_upto = last_included.index - retained"
    );
    let event = internal_event_rx
        .try_recv()
        .expect("LogPurgeCompleted must be dispatched after successful purge");
    assert!(
        matches!(
            event,
            InternalEvent::LogPurgeCompleted(LogId { index: 49, .. })
        ),
        "expected LogPurgeCompleted(49), got: {event:?}"
    );
}

/// Zero boundary: retained_log_entries >= last_included.index → saturating_sub = 0.
/// Guard `idx > 0` rejects the purge — no schedule, no execute, no event.
#[tokio::test]
async fn test_schedule_and_execute_purge_zero_boundary_skips_purge() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // entry_term(0) called but returns None — index 0 does not exist
    let mut raft_log = mock_raft_log();
    raft_log.expect_entry_term().withf(|&idx| idx == 0).times(0).returning(|_| None);

    let _temp_dir = tempfile::tempdir().unwrap();
    let mut nc = node_config(_temp_dir.path().to_str().unwrap());
    nc.raft.snapshot.retained_log_entries = 50; // >= last_included.index(50) → purge_upto=0

    let ctx = MockBuilder::new(graceful_rx)
        .with_raft_log(raft_log)
        .with_node_config(nc)
        .build_context();

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let last_included = LogId { index: 50, term: 1 };
    let mut scheduled_purge_upto: Option<LogId> = None;

    let result = schedule_and_execute_purge(
        last_included,
        &ctx,
        100,
        None,
        &mut scheduled_purge_upto,
        &internal_event_tx,
    )
    .await;

    assert!(result.is_ok());
    assert!(
        scheduled_purge_upto.is_none(),
        "zero purge_upto must not be scheduled"
    );
    assert!(
        internal_event_rx.try_recv().is_err(),
        "no LogPurgeCompleted must be sent when purge is skipped"
    );
}

/// commit_index gap rule: purge_upto(99) >= commit_index(95) → guard rejects.
/// No schedule update, no event.
#[tokio::test]
async fn test_schedule_and_execute_purge_rejects_when_purge_upto_exceeds_commit_index() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // retained=1, last_included.index=100 → purge_upto_index=99
    let mut raft_log = mock_raft_log();
    raft_log
        .expect_entry_term()
        .withf(|&idx| idx == 99)
        .times(1)
        .returning(|_| Some(1));

    let ctx = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let last_included = LogId {
        index: 100,
        term: 1,
    };
    let mut scheduled_purge_upto: Option<LogId> = None;

    let result = schedule_and_execute_purge(
        last_included,
        &ctx,
        95, // commit_index = 95 < purge_upto_index(99)
        None,
        &mut scheduled_purge_upto,
        &internal_event_tx,
    )
    .await;

    assert!(result.is_ok());
    assert!(
        scheduled_purge_upto.is_none(),
        "purge_upto >= commit_index must be rejected"
    );
    assert!(
        internal_event_rx.try_recv().is_err(),
        "no event when commit_index gap rule rejects"
    );
}

/// Monotonicity: last_purged_index(60) >= purge_upto(49) → backward purge rejected.
/// scheduled_purge_upto stays None, no event.
#[tokio::test]
async fn test_schedule_and_execute_purge_monotonicity_rejects_backward_purge() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_log = mock_raft_log();
    raft_log
        .expect_entry_term()
        .withf(|&idx| idx == 49)
        .times(1)
        .returning(|_| Some(1));

    let ctx = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let last_included = LogId { index: 50, term: 1 };
    let mut scheduled_purge_upto: Option<LogId> = None;

    let result = schedule_and_execute_purge(
        last_included,
        &ctx,
        100,
        Some(LogId { index: 60, term: 1 }), // already purged beyond purge_upto(49)
        &mut scheduled_purge_upto,
        &internal_event_tx,
    )
    .await;

    assert!(result.is_ok());
    assert!(
        scheduled_purge_upto.is_none(),
        "backward purge must not update scheduled_purge_upto"
    );
    assert!(
        internal_event_rx.try_recv().is_err(),
        "no event when monotonicity guard rejects"
    );
}

/// Fault recovery: entry_term returns None (log compacted) so Phase 1 is skipped.
/// Pre-existing scheduled_purge_upto is retried in Phase 2 → LogPurgeCompleted dispatched.
#[tokio::test]
async fn test_schedule_and_execute_purge_fault_recovery_retries_existing_scheduled() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // entry_term returns None → Phase 1 skipped
    let mut raft_log = mock_raft_log();
    raft_log
        .expect_entry_term()
        .withf(|&idx| idx == 49)
        .times(1)
        .returning(|_| None);

    let ctx = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let last_included = LogId { index: 50, term: 1 };
    // Pre-existing scheduled from a prior Phase 1 that never executed
    let mut scheduled_purge_upto: Option<LogId> = Some(LogId { index: 45, term: 1 });

    let result = schedule_and_execute_purge(
        last_included,
        &ctx,
        100,
        None,
        &mut scheduled_purge_upto,
        &internal_event_tx,
    )
    .await;

    assert!(result.is_ok());
    assert_eq!(
        scheduled_purge_upto,
        Some(LogId { index: 45, term: 1 }),
        "scheduled_purge_upto must not change when Phase 1 is skipped"
    );
    let event = internal_event_rx
        .try_recv()
        .expect("Phase 2 must retry existing scheduled_purge_upto");
    assert!(
        matches!(
            event,
            InternalEvent::LogPurgeCompleted(LogId { index: 45, .. })
        ),
        "expected LogPurgeCompleted(45), got: {event:?}"
    );
}

/// Schedule monotonicity: new purge_upto(49) < existing scheduled(60) → no regression.
/// Phase 2 still executes with the existing scheduled(60).
#[tokio::test]
async fn test_schedule_and_execute_purge_does_not_regress_scheduled_purge_upto() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_log = mock_raft_log();
    raft_log
        .expect_entry_term()
        .withf(|&idx| idx == 49)
        .times(1)
        .returning(|_| Some(1));

    let ctx = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let last_included = LogId { index: 50, term: 1 };
    // scheduled already ahead of new purge_upto(49)
    let mut scheduled_purge_upto: Option<LogId> = Some(LogId { index: 60, term: 1 });

    let result = schedule_and_execute_purge(
        last_included,
        &ctx,
        100,
        None,
        &mut scheduled_purge_upto,
        &internal_event_tx,
    )
    .await;

    assert!(result.is_ok());
    assert_eq!(
        scheduled_purge_upto,
        Some(LogId { index: 60, term: 1 }),
        "scheduled_purge_upto must not regress from 60 to 49"
    );
    let event = internal_event_rx
        .try_recv()
        .expect("Phase 2 must execute existing scheduled(60)");
    assert!(
        matches!(
            event,
            InternalEvent::LogPurgeCompleted(LogId { index: 60, .. })
        ),
        "expected LogPurgeCompleted(60), got: {event:?}"
    );
}

/// Phase 2 failure: execute_purge returns Err.
/// Function returns Ok(()), LogPurgeCompleted is NOT dispatched.
#[tokio::test]
async fn test_schedule_and_execute_purge_execute_failure_suppresses_completion_event() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Phase 1 skipped — entry_term returns None
    let mut raft_log = mock_raft_log();
    raft_log
        .expect_entry_term()
        .withf(|&idx| idx == 49)
        .times(1)
        .returning(|_| None);

    let mut purge_executor = MockPurgeExecutor::new();
    purge_executor
        .expect_execute_purge()
        .times(1)
        .returning(|_| Err(Error::Fatal("disk error".to_string())));

    let mut builder = MockBuilder::new(graceful_rx);
    builder.purge_executor = Some(purge_executor);
    let ctx = builder.with_raft_log(raft_log).build_context();

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let last_included = LogId { index: 50, term: 1 };
    let mut scheduled_purge_upto: Option<LogId> = Some(LogId { index: 49, term: 1 });

    let result = schedule_and_execute_purge(
        last_included,
        &ctx,
        100,
        None,
        &mut scheduled_purge_upto,
        &internal_event_tx,
    )
    .await;

    assert!(
        result.is_ok(),
        "errors from execute_purge must not propagate"
    );
    assert!(
        internal_event_rx.try_recv().is_err(),
        "LogPurgeCompleted must not be sent when execute_purge fails"
    );
    // scheduled_purge_upto preserved — fault recovery will retry on next snapshot
    assert_eq!(scheduled_purge_upto, Some(LogId { index: 49, term: 1 }));
}

// ============================================================================
// commit_hard_state / commit_vote_reset Tests
//
// CandidateState is used as the host — commit_hard_state/commit_vote_reset
// are role-agnostic default methods on RaftRoleState, only touching
// shared_state_mut() and ctx.raft_log(), so any concrete state works.
//
// IMPORTANT: use MockRaftLog::new() fresh, NOT mock_raft_log() — the latter
// pre-registers a permissive save_hard_state stub with no call-count limit,
// which (per mockall's FIFO expectation matching) silently absorbs every
// call before a stricter .times(n) expectation added afterward ever runs.
// ============================================================================

/// Both term and voted_for are None → no-op: no persist, returns Ok(false).
#[tokio::test]
async fn test_commit_hard_state_noop_when_both_none() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_save_hard_state().times(0);

    let context = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    let result = state.commit_hard_state(&context, None, None);

    assert!(
        matches!(result, Ok(false)),
        "expected Ok(false), got {result:?}"
    );
}

/// Calling commit_hard_state twice with the identical voted_for value must
/// only persist once — the second call is a redundant re-confirmation (e.g.
/// a routine heartbeat from an already-known leader) and must not trigger a
/// second disk write. Locks in the `changed` gate on HardStateChange.
#[tokio::test]
async fn test_commit_hard_state_skips_persist_when_value_unchanged() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_log = MockRaftLog::new();
    // Not 2 — the second call must be skipped since nothing changed.
    raft_log.expect_save_hard_state().times(1).returning(|_| Ok(()));

    let context = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    let vote = VotedFor {
        voted_for_id: 2,
        voted_for_term: 1,
        committed: true,
    };

    state.commit_hard_state(&context, None, Some(vote)).unwrap();
    state.commit_hard_state(&context, None, Some(vote)).unwrap();
}

/// term changes, voted_for untouched (None) → persists with the new term.
/// Returns Ok(false) — no vote involved, so is_new_leader_commitment is false
/// even though the write genuinely happened (this is the exact case that a
/// naive "gate on is_new_leader_commitment instead of changed" refactor
/// would silently break — see conversation history).
#[tokio::test]
async fn test_commit_hard_state_persists_when_term_changes() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_save_hard_state()
        .withf(|s| s.current_term == 5)
        .times(1)
        .returning(|_| Ok(()));

    let context = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    let result = state.commit_hard_state(&context, Some(5), None);

    assert!(
        matches!(result, Ok(false)),
        "expected Ok(false), got {result:?}"
    );
    assert_eq!(state.current_term(), 5);
}

/// voted_for changes to a genuinely different value each time → both calls
/// persist (contrast with test_commit_hard_state_skips_persist_when_value_unchanged
/// above, where the second call is a no-op because the value repeats).
#[tokio::test]
async fn test_commit_hard_state_persists_when_vote_value_changes() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_save_hard_state().times(2).returning(|_| Ok(()));

    let context = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    let v1 = VotedFor {
        voted_for_id: 2,
        voted_for_term: 1,
        committed: true,
    };
    let v2 = VotedFor {
        voted_for_id: 3,
        voted_for_term: 1,
        committed: true,
    };

    state.commit_hard_state(&context, None, Some(v1)).unwrap();
    state.commit_hard_state(&context, None, Some(v2)).unwrap();
}

/// is_new_leader_commitment is true only for a genuine committed transition
/// (e.g. None -> Some(committed: true)), false for a provisional self-vote
/// (committed: false) and false for reconfirming the same committed leader.
#[tokio::test]
async fn test_commit_hard_state_is_new_leader_commitment_semantics() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_log = MockRaftLog::new();
    // Only 2 persists: case 1 (None -> provisional) and case 2 (provisional ->
    // committed) both change the value. Case 3 repeats case 2's exact value,
    // so it's a no-op — no third persist, even though is_new_leader_commitment
    // is still computed correctly from the in-memory transition.
    raft_log.expect_save_hard_state().times(2).returning(|_| Ok(()));

    let context = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    // 1) Provisional self-vote (committed: false) — never a "new commitment".
    let provisional = VotedFor {
        voted_for_id: 1,
        voted_for_term: 1,
        committed: false,
    };
    let result1 = state.commit_hard_state(&context, None, Some(provisional));
    assert!(
        matches!(result1, Ok(false)),
        "provisional vote must not report new commitment, got {result1:?}"
    );

    // 2) First committed vote — genuine new commitment.
    let committed = VotedFor {
        voted_for_id: 2,
        voted_for_term: 1,
        committed: true,
    };
    let result2 = state.commit_hard_state(&context, None, Some(committed));
    assert!(
        matches!(result2, Ok(true)),
        "first committed vote must report new commitment, got {result2:?}"
    );
    // Mirrors the real caller (role_state.rs): commit_hard_state is checked
    // BEFORE set_current_leader is called. Without this, current_leader()
    // stays None forever in this test and the "node restart" branch of
    // is_new_leader_commitment would fire on every subsequent call.
    state.shared_state().set_current_leader(committed.voted_for_id);

    // 3) Reconfirming the identical committed vote — not new.
    let result3 = state.commit_hard_state(&context, None, Some(committed));
    assert!(
        matches!(result3, Ok(false)),
        "reconfirming the same committed vote must not report new commitment, got {result3:?}"
    );
}

/// commit_vote_reset clears voted_for to None and persists exactly once.
#[tokio::test]
async fn test_commit_vote_reset_clears_and_persists() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_log = MockRaftLog::new();
    // One persist to seed an initial vote, one more for the reset itself.
    raft_log.expect_save_hard_state().times(2).returning(|_| Ok(()));

    let context = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    let vote = VotedFor {
        voted_for_id: 2,
        voted_for_term: 1,
        committed: true,
    };
    state.commit_hard_state(&context, None, Some(vote)).unwrap();
    assert_eq!(
        state.voted_for().unwrap(),
        Some(vote),
        "precondition: vote is set"
    );

    state.commit_vote_reset(&context).unwrap();

    assert_eq!(
        state.voted_for().unwrap(),
        None,
        "voted_for must be cleared by commit_vote_reset"
    );
}
