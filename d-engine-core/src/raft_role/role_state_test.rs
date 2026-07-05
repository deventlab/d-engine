//! Tests for role_state free functions
//!
//! Covers:
//! - schedule_and_execute_purge: two-phase log purge orchestration (Phase 1 = schedule, Phase 2 = execute)

use tokio::sync::{mpsc, watch};

use d_engine_proto::common::LogId;

use crate::Error;
use crate::InternalEvent;
use crate::MockPurgeExecutor;
use crate::test_utils::mock::{MockBuilder, mock_raft_log};
use crate::test_utils::node_config;

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
