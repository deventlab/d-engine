//! Tests for role_state free functions
//!
//! Covers:
//! - schedule_and_execute_purge: two-phase log purge orchestration (Phase 1 = schedule, Phase 2 = execute)

use crate::CapturedLocalSnapshot;
use crate::ConsensusError;
use crate::Error;
use crate::InternalEvent;
use crate::MockPurgeExecutor;
use crate::MockRaftLog;
use crate::MockStateMachineHandler;
use crate::OwnedSnapshotDir;
use crate::SnapshotError;
use crate::StateMachineCommand;
use crate::StateMachineCommandSender;
use crate::raft_role::candidate_state::CandidateState;
use crate::raft_role::follower_state::FollowerState;
use crate::test_utils::mock::{MockBuilder, MockTypeConfig, mock_raft_log};
use crate::test_utils::node_config;
use d_engine_proto::common::LogId;
use d_engine_proto::server::election::VotedFor;
use d_engine_proto::server::storage::SnapshotMetadata;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

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

    let _temp_dir = tempfile::tempdir().unwrap();
    let mut nc = node_config(_temp_dir.path().to_str().unwrap());
    nc.raft.snapshot.retained_log_entries = 1;
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

    let _temp_dir = tempfile::tempdir().unwrap();
    let mut nc = node_config(_temp_dir.path().to_str().unwrap());
    nc.raft.snapshot.retained_log_entries = 1;
    let ctx = MockBuilder::new(graceful_rx)
        .with_raft_log(raft_log)
        .with_node_config(nc)
        .build_context();

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

    let _temp_dir = tempfile::tempdir().unwrap();
    let mut nc = node_config(_temp_dir.path().to_str().unwrap());
    nc.raft.snapshot.retained_log_entries = 1;
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

    let _temp_dir = tempfile::tempdir().unwrap();
    let mut nc = node_config(_temp_dir.path().to_str().unwrap());
    nc.raft.snapshot.retained_log_entries = 1;
    let ctx = MockBuilder::new(graceful_rx)
        .with_raft_log(raft_log)
        .with_node_config(nc)
        .build_context();

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

    let _temp_dir = tempfile::tempdir().unwrap();
    let mut nc = node_config(_temp_dir.path().to_str().unwrap());
    nc.raft.snapshot.retained_log_entries = 1;
    let ctx = MockBuilder::new(graceful_rx)
        .with_raft_log(raft_log)
        .with_node_config(nc)
        .build_context();

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
    let _temp_dir = tempfile::tempdir().unwrap();
    let mut nc = node_config(_temp_dir.path().to_str().unwrap());
    nc.raft.snapshot.retained_log_entries = 1;
    let ctx = builder.with_raft_log(raft_log).with_node_config(nc).build_context();

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

// ============================================================================
// handle_create_snapshot Tests (#436)
// ============================================================================

/// Spawns a fake Worker that answers exactly one `CaptureLocalSnapshot` command with
/// `result`, then exits. Stands in for the real `StateMachineWorker`, which isn't
/// running in these role-layer unit tests.
fn fake_command_sink_capture_local_snapshot(
    result: crate::Result<CapturedLocalSnapshot>
) -> StateMachineCommandSender {
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        if let Some(StateMachineCommand::CaptureLocalSnapshot { response }) = rx.recv().await {
            let _ = response.send(result);
        }
    });
    StateMachineCommandSender::new(tx)
}

/// Candidate has no snapshot_in_progress flag (default trait impl returns None),
/// so handle_create_snapshot must short-circuit with RoleViolation and never
/// call create_snapshot.
#[tokio::test]
async fn test_handle_create_snapshot_candidate_role_violation() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_try_begin_local_snapshot_capture().never();

    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(sm_handler)
        .build_context();

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    let result = state.handle_create_snapshot(&context, &internal_event_tx).await;

    match result {
        Err(Error::Consensus(ConsensusError::RoleViolation { current_role, .. })) => {
            assert_eq!(current_role, "Candidate");
        }
        other => panic!("expected RoleViolation, got: {other:?}"),
    }
}

/// If a snapshot build is already in progress (flag=true), a second call must
/// be a no-op: return Ok(()) immediately, never call create_snapshot again,
/// and leave the flag untouched.
#[tokio::test]
async fn test_handle_create_snapshot_dedup_skips_when_already_in_progress() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_try_begin_local_snapshot_capture().never();

    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(sm_handler)
        .build_context();

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.snapshot_in_progress.store(true, std::sync::atomic::Ordering::SeqCst);

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();

    let result = state.handle_create_snapshot(&context, &internal_event_tx).await;

    assert!(
        result.is_ok(),
        "dedup path must return Ok(()), not an error"
    );
    assert!(
        internal_event_rx.try_recv().is_err(),
        "no SnapshotCreated event must be sent when the call was deduped"
    );
    assert!(
        state.snapshot_in_progress.load(std::sync::atomic::Ordering::SeqCst),
        "flag must remain true — dedup must not reset it"
    );
}

/// Normal path: flag starts false, handle_create_snapshot flips it to true and
/// returns immediately (does not wait for the spawned build), and the spawned
/// task eventually reports success via InternalEvent::SnapshotCreated carrying
/// the exact metadata/path create_snapshot produced.
#[tokio::test]
async fn test_handle_create_snapshot_success_sets_flag_and_emits_event() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let expected_metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 10, term: 2 }),
        checksum: bytes::Bytes::from_static(b"abc"),
    };
    let expected_path = std::path::PathBuf::from("/tmp/snap-10");

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler
        .expect_try_begin_local_snapshot_capture()
        .times(1)
        .returning(|| Ok(()));
    sm_handler.expect_end_local_snapshot_capture().times(1).returning(|| {});
    {
        let expected_metadata = expected_metadata.clone();
        let expected_path = expected_path.clone();
        sm_handler
            .expect_build_local_snapshot()
            .times(1)
            .returning(move |_captured| Ok((expected_metadata.clone(), expected_path.clone())));
    }

    // `build_local_snapshot` is mocked below and never actually reads this directory,
    // but `OwnedSnapshotDir::from_existing` still requires a real path on disk (it
    // adopts, never creates).
    let captured_dir = tempfile::tempdir().unwrap().keep();
    let captured = CapturedLocalSnapshot {
        metadata: SnapshotMetadata::default(),
        temp_dir: OwnedSnapshotDir::from_existing(captured_dir).unwrap(),
    };
    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(sm_handler)
        .with_state_machine_commands(fake_command_sink_capture_local_snapshot(Ok(captured)))
        .build_context();

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    assert!(!state.snapshot_in_progress.load(std::sync::atomic::Ordering::SeqCst));

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();

    let result = state.handle_create_snapshot(&context, &internal_event_tx).await;
    assert!(result.is_ok());
    assert!(
        state.snapshot_in_progress.load(std::sync::atomic::Ordering::SeqCst),
        "flag must be set to true before the background build starts"
    );

    let event = tokio::time::timeout(Duration::from_secs(2), internal_event_rx.recv())
        .await
        .expect("SnapshotCreated must arrive within 2s")
        .expect("channel must not close before the event is sent");

    match event {
        InternalEvent::SnapshotCreated(Ok((metadata, path))) => {
            assert_eq!(metadata, expected_metadata);
            assert_eq!(path, expected_path);
        }
        other => panic!("expected SnapshotCreated(Ok(..)), got: {other:?}"),
    }
}

/// Failure path: create_snapshot returns Err — the error must be propagated
/// via InternalEvent::SnapshotCreated(Err(..)), not swallowed or panicked on.
#[tokio::test]
async fn test_handle_create_snapshot_failure_emits_error_event() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler
        .expect_try_begin_local_snapshot_capture()
        .times(1)
        .returning(|| Ok(()));
    sm_handler.expect_end_local_snapshot_capture().times(1).returning(|| {});
    sm_handler.expect_build_local_snapshot().never();

    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(sm_handler)
        .with_state_machine_commands(fake_command_sink_capture_local_snapshot(Err(
            SnapshotError::OperationFailed("disk full".to_string()).into(),
        )))
        .build_context();

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();

    let result = state.handle_create_snapshot(&context, &internal_event_tx).await;
    assert!(result.is_ok(), "spawn dispatch itself must still succeed");

    let event = tokio::time::timeout(Duration::from_secs(2), internal_event_rx.recv())
        .await
        .expect("SnapshotCreated must arrive within 2s")
        .expect("channel must not close before the event is sent");

    match event {
        InternalEvent::SnapshotCreated(Err(_)) => {}
        other => panic!("expected SnapshotCreated(Err(..)), got: {other:?}"),
    }
}

// ============================================================================
// A vote belongs to the term it was cast in (Raft §5.1: a new term starts with votedFor = null)
//
// raft-rs `reset(term)` clears the vote whenever the term changes; openraft replaces the whole
// (term, node) vote. If an old vote survives a term change, it blocks a legitimate vote request
// in the new term even though this node has not voted in that term.
// ============================================================================

fn recording_raft_log() -> (
    MockRaftLog,
    std::sync::Arc<std::sync::Mutex<Vec<crate::HardState>>>,
) {
    let saved = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorder = std::sync::Arc::clone(&saved);
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_save_hard_state().returning(move |state| {
        recorder.lock().unwrap().push(*state);
        Ok(())
    });
    (raft_log, saved)
}

fn vote_for(
    id: u32,
    term: u64,
) -> VotedFor {
    VotedFor {
        voted_for_id: id,
        voted_for_term: term,
        committed: false,
    }
}

/// Test: adopting a higher term (without a new vote) clears the vote cast in the older term,
/// in memory and on disk.
///
/// Scenario:
/// - The node voted for node 3 in term 1.
/// - It adopts term 2 because of a request it did not grant.
///
/// Expected:
/// - `voted_for` is None and the persisted hard state says term 2 with no vote.
#[tokio::test]
async fn test_commit_hard_state_clears_older_vote_when_term_increases() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (raft_log, saved) = recording_raft_log();
    let context = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.commit_hard_state(&context, None, Some(vote_for(3, 1))).unwrap();

    state.commit_hard_state(&context, Some(2), None).unwrap();

    assert_eq!(state.current_term(), 2);
    assert_eq!(
        state.voted_for().unwrap(),
        None,
        "the term-1 vote must not survive term 2"
    );
    let last_saved = *saved.lock().unwrap().last().unwrap();
    assert_eq!((last_saved.current_term, last_saved.voted_for), (2, None));
}

/// Test: confirming the SAME term keeps the vote (e.g. a repeated request at the current term).
#[tokio::test]
async fn test_commit_hard_state_keeps_vote_when_term_is_unchanged() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (raft_log, _saved) = recording_raft_log();
    let context = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.commit_hard_state(&context, None, Some(vote_for(3, 1))).unwrap();
    let term = state.current_term();

    state.commit_hard_state(&context, Some(term), None).unwrap();

    assert_eq!(state.voted_for().unwrap(), Some(vote_for(3, 1)));
}

/// Test: when the term changes together with a new vote, the new vote wins (the clearing must not
/// swallow a vote supplied by the caller, e.g. a vote granted in the new term).
#[tokio::test]
async fn test_commit_hard_state_applies_new_vote_given_with_a_higher_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (raft_log, saved) = recording_raft_log();
    let context = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.commit_hard_state(&context, None, Some(vote_for(3, 1))).unwrap();

    state.commit_hard_state(&context, Some(2), Some(vote_for(5, 2))).unwrap();

    assert_eq!(state.voted_for().unwrap(), Some(vote_for(5, 2)));
    let last_saved = *saved.lock().unwrap().last().unwrap();
    assert_eq!(
        (last_saved.current_term, last_saved.voted_for),
        (2, Some(vote_for(5, 2)))
    );
}

// ============================================================================
// Vote protocol, end to end through the real handler and the real hard state
//
// The single-step tests above mock the other half. These run the exact two lines a Follower
// executes for every VoteRequest (`handle_vote_request`, then `commit_hard_state` with its
// answer) against the real implementations, and "restart" a node by rebuilding it from the hard
// state it persisted.
// ============================================================================

/// Applies one VoteRequest the way `FollowerState` does and returns whether it was granted.
async fn grant_or_deny(
    state: &mut impl RaftRoleState<T = MockTypeConfig>,
    context: &crate::RaftContext<MockTypeConfig>,
    request_term: u64,
    candidate_id: u32,
    candidate_log: (u64, u64),
) -> bool {
    use crate::ElectionCore as _;

    let update = crate::election::ElectionHandler::<MockTypeConfig>::new(1)
        .handle_vote_request(
            d_engine_proto::server::election::VoteRequest {
                term: request_term,
                candidate_id,
                last_log_index: candidate_log.0,
                last_log_term: candidate_log.1,
            },
            state.current_term(),
            state.voted_for().unwrap(),
            context.raft_log(),
        )
        .await
        .unwrap();
    state
        .commit_hard_state(context, update.term_update, update.new_voted_for)
        .unwrap();
    update.new_voted_for.is_some()
}

fn vote_sequence_context() -> (
    crate::RaftContext<MockTypeConfig>,
    std::sync::Arc<std::sync::Mutex<Vec<crate::HardState>>>,
    watch::Sender<()>,
) {
    let (graceful_tx, graceful_rx) = watch::channel(());
    let (mut raft_log, saved) = recording_raft_log();
    // The voter's own log: index 5, term 2.
    raft_log
        .expect_last_log_id()
        .returning(|| Some(d_engine_proto::common::LogId { index: 5, term: 2 }));
    let context = MockBuilder::new(graceful_rx).with_raft_log(raft_log).build_context();
    (context, saved, graceful_tx)
}

/// Test: Election Safety. At most one candidate gets this node's vote in a term, also across a
/// restart.
///
/// Scenario:
/// - Candidate 5 asks for a vote in term 2 (good log) and gets it.
/// - Candidate 6 asks in the same term: denied.
/// - The node restarts from the hard state it persisted. Candidate 6 asks again: still denied.
///   Candidate 5 asking again is granted again (a retry of the same request).
///
/// Expected:
/// - Two different candidates never both hold this node's vote for the same term. If they could,
///   both can reach a majority and the cluster has two leaders in one term.
#[tokio::test]
async fn test_vote_is_granted_to_one_candidate_per_term_also_across_a_restart() {
    let (context, saved, _graceful_tx) = vote_sequence_context();
    let mut node = FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    node.shared_state_mut().update_current_term(1);

    assert!(
        grant_or_deny(&mut node, &context, 2, 5, (5, 2)).await,
        "first candidate"
    );
    assert!(
        !grant_or_deny(&mut node, &context, 2, 6, (5, 2)).await,
        "second candidate"
    );

    let persisted = *saved.lock().unwrap().last().expect("the vote must have been persisted");
    let mut restarted =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), Some(persisted), None);

    assert!(
        !grant_or_deny(&mut restarted, &context, 2, 6, (5, 2)).await,
        "after a restart the node must remember its vote and still deny candidate 6"
    );
    assert!(
        grant_or_deny(&mut restarted, &context, 2, 5, (5, 2)).await,
        "a retry from the candidate that already holds the vote is granted again"
    );
}

/// Test: the business timeline of the stale-vote defect, through the real code.
///
/// Scenario:
/// - The node votes for candidate 3 in term 1.
/// - Candidate 4 asks in term 2 with a log that is behind: the node adopts term 2 and denies.
/// - Candidate 5 asks in the same term 2 with a good log.
///
/// Expected:
/// - Candidate 5 gets the vote: the node has not voted in term 2. (Before the fix the leftover
///   term-1 vote blocked it and candidate 5 had to retry in term 3.)
#[tokio::test]
async fn test_vote_from_an_older_term_does_not_block_a_good_candidate_in_the_next_term() {
    let (context, _saved, _graceful_tx) = vote_sequence_context();
    let mut node = FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    node.shared_state_mut().update_current_term(1);

    assert!(
        grant_or_deny(&mut node, &context, 1, 3, (5, 2)).await,
        "vote in term 1"
    );
    assert!(
        !grant_or_deny(&mut node, &context, 2, 4, (1, 1)).await,
        "candidate 4's log is behind"
    );
    assert_eq!(node.current_term(), 2, "the higher term is still adopted");

    assert!(
        grant_or_deny(&mut node, &context, 2, 5, (5, 2)).await,
        "candidate 5 must get the vote: nothing was cast in term 2"
    );
}

/// Test: a higher-term request with a stale log is denied but its term is persisted, so the node
/// does not fall back to the old term after a restart.
#[tokio::test]
async fn test_adopted_term_survives_a_restart_even_when_the_vote_was_denied() {
    let (context, saved, _graceful_tx) = vote_sequence_context();
    let mut node = FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    node.shared_state_mut().update_current_term(1);

    assert!(
        !grant_or_deny(&mut node, &context, 4, 9, (1, 1)).await,
        "stale log, denied"
    );

    let persisted = *saved.lock().unwrap().last().expect("the term change must be persisted");
    assert_eq!((persisted.current_term, persisted.voted_for), (4, None));
    let restarted =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), Some(persisted), None);
    assert_eq!(restarted.current_term(), 4);
}
