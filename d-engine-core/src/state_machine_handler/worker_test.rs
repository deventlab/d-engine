//! Unit tests for StateMachineWorker
//!
//! Tests verify SM Worker's core responsibilities:
//! - Apply entries asynchronously via apply_chunk
//! - Send ApplyCompleted events on success via internal_event_tx (P2)
//! - Send FatalError events on apply failures via internal_event_tx (P2)
//! - Gracefully drain remaining entries on shutdown

use std::sync::Arc;

use d_engine_proto::common::{Entry, LogId};
use d_engine_proto::server::storage::SnapshotMetadata;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, watch};

use super::StateMachineWorker;
use crate::{
    ApplyResult, CapturedLocalSnapshot, ConsensusError, Error, InternalEvent,
    MockStateMachineWriterOps, MockTypeConfig, PreparedSnapshot, SnapshotApplyResult,
    SnapshotError, StateMachineCommand,
};

/// Helper: build an InstallSnapshot command backed by a real (empty) temp dir.
/// Returns the command plus the response receiver to await on.
fn make_install_snapshot_command() -> (
    StateMachineCommand,
    oneshot::Receiver<crate::Result<SnapshotApplyResult>>,
) {
    let (response, response_rx) = oneshot::channel();
    let command = StateMachineCommand::InstallSnapshot {
        snapshot: PreparedSnapshot {
            metadata: SnapshotMetadata::default(),
            temp_dir: tempfile::tempdir().unwrap(),
        },
        response,
    };
    (command, response_rx)
}

/// Helper: Create test entry
fn create_test_entry(
    index: u64,
    term: u64,
) -> Entry {
    Entry {
        index,
        term,
        payload: Some(d_engine_proto::common::EntryPayload::command(
            bytes::Bytes::from("test_data"),
        )),
    }
}

/// Test: Apply Success - Sends ApplyCompleted Event
///
/// # Test Objective
/// Verify that when apply_chunk succeeds, SM Worker sends
/// InternalEvent::ApplyCompleted with correct results via internal_event_tx (P2, unbounded).
///
/// # Given
/// - Mock StateMachineHandler with successful apply_chunk
/// - SM Worker running in background
///
/// # When
/// - apply_chunk returns Ok(results)
///
/// # Then
/// - InternalEvent::ApplyCompleted is sent to internal_event_tx
/// - Event contains correct last_index and results
#[tokio::test]
async fn test_apply_success_sends_apply_completed() {
    let mut mock_smw = MockStateMachineWriterOps::new();
    mock_smw.expect_apply_chunk().times(1).returning(|entries| {
        let results: Vec<crate::ApplyResult> =
            entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect();
        Ok(results)
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );

    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    sm_apply_tx
        .send(StateMachineCommand::ApplyEntries {
            entries: vec![create_test_entry(1, 1), create_test_entry(2, 1)],
        })
        .unwrap();

    match tokio::time::timeout(
        std::time::Duration::from_millis(100),
        internal_event_rx.recv(),
    )
    .await
    {
        Ok(Some(InternalEvent::ApplyCompleted {
            last_index,
            results,
        })) => {
            assert_eq!(last_index, 2, "last_index should match last entry");
            assert_eq!(results.len(), 2, "results should contain 2 entries");
            assert_eq!(results[0].index, 1);
            assert_eq!(results[1].index, 2);
        }
        Ok(Some(other)) => panic!("Expected ApplyCompleted, got {other:?}"),
        Ok(None) => panic!("Event channel closed unexpectedly"),
        Err(_) => panic!("Timeout waiting for ApplyCompleted event"),
    }
}

/// Test: Apply Failure - Sends FatalError Event
///
/// # Test Objective
/// Verify that when apply_chunk fails, SM Worker sends
/// InternalEvent::FatalError via internal_event_tx (P2, unbounded) and exits with error.
///
/// # Given
/// - Mock StateMachineHandler with failing apply_chunk
///
/// # When
/// - apply_chunk returns Err(Error::Fatal)
///
/// # Then
/// - InternalEvent::FatalError is sent to internal_event_tx
/// - Worker returns error (exits run loop)
#[tokio::test]
async fn test_apply_failure_sends_fatal_error() {
    let mut mock_smw = MockStateMachineWriterOps::new();
    mock_smw.expect_apply_chunk().times(1).returning(|_| {
        Err(Error::Fatal(
            "Disk failure - cannot write to storage".to_string(),
        ))
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );

    let worker_handle = tokio::spawn(async move { worker.run().await });

    sm_apply_tx
        .send(StateMachineCommand::ApplyEntries {
            entries: vec![create_test_entry(1, 1)],
        })
        .unwrap();

    match tokio::time::timeout(
        std::time::Duration::from_millis(100),
        internal_event_rx.recv(),
    )
    .await
    {
        Ok(Some(InternalEvent::FatalError { source, error })) => {
            assert_eq!(source, "StateMachine");
            assert!(
                error.contains("Disk failure") || error.contains("storage"),
                "error should contain failure details, got: {error}"
            );
        }
        Ok(Some(other)) => panic!("Expected FatalError, got {other:?}"),
        Ok(None) => panic!("Event channel closed unexpectedly"),
        Err(_) => panic!("Timeout waiting for FatalError event"),
    }

    let result = worker_handle.await.unwrap();
    assert!(
        result.is_err(),
        "Worker should return error after fatal failure"
    );
}

/// Test: Shutdown - Drains Remaining Entries
///
/// # Test Objective
/// Verify that when shutdown signal is received, SM Worker drains all
/// pending entries before exit (no data loss).
///
/// # Success Criteria
/// - All 3 entries applied (verified via ApplyCompleted count on internal_event_rx)
/// - Worker run() returns Ok(())
#[tokio::test]
async fn test_shutdown_drains_remaining_entries() {
    let mut mock_smw = MockStateMachineWriterOps::new();
    mock_smw.expect_apply_chunk().times(3).returning(|entries| {
        let results: Vec<crate::ApplyResult> =
            entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect();
        Ok(results)
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );

    let worker_handle = tokio::spawn(async move { worker.run().await });

    for i in 1..=3 {
        sm_apply_tx
            .send(StateMachineCommand::ApplyEntries {
                entries: vec![create_test_entry(i, 1)],
            })
            .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    shutdown_tx.send(()).unwrap();

    let mut apply_count = 0;
    while apply_count < 3 {
        match tokio::time::timeout(
            std::time::Duration::from_millis(100),
            internal_event_rx.recv(),
        )
        .await
        {
            Ok(Some(InternalEvent::ApplyCompleted { .. })) => apply_count += 1,
            Ok(Some(other)) => panic!("Expected ApplyCompleted, got {other:?}"),
            Ok(None) => break,
            Err(_) => panic!("Timeout waiting for ApplyCompleted events"),
        }
    }

    assert_eq!(
        apply_count, 3,
        "All 3 entries should be applied during shutdown drain"
    );

    let result = worker_handle.await.unwrap();
    assert!(result.is_ok(), "Worker should exit cleanly after shutdown");
}

/// #436: Worker now holds its own clone of `sm_apply_tx` (to route
/// `LocalSnapshotReady` back to itself), so dropping every *external* clone no
/// longer closes the channel — Worker's own clone keeps the sender count above
/// zero for as long as it's running. This replaces the old
/// `test_channel_closed_worker_exits`, which asserted the opposite (now-false)
/// behavior. Shutdown must go through `shutdown_signal` — see
/// `test_worker_exits_when_shutdown_sender_dropped_without_send` below for the
/// mechanism embedders actually rely on when they drop their handle without
/// calling `stop()`.
#[tokio::test]
async fn test_dropping_external_senders_alone_does_not_stop_worker() {
    let mut mock_smw = MockStateMachineWriterOps::new();
    mock_smw
        .expect_apply_chunk()
        .times(1)
        .returning(|entries| Ok(entries.iter().map(|e| ApplyResult::success(e.index)).collect()));

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );
    let worker_handle = tokio::spawn(async move { worker.run().await });

    // Drop every external clone — Worker's own internal clone is still alive.
    drop(sm_apply_tx.clone());
    drop(sm_apply_tx);

    // Worker must still be alive and functional: it can't be sent a command
    // through the now-dropped external sender, but the task itself must not
    // have exited. Prove liveness indirectly via the shutdown path instead —
    // if the task had already exited, this would just be a no-op join.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !worker_handle.is_finished(),
        "Worker exited after external senders dropped — self-sender no longer \
         keeps it alive, channel-closed detection is back (update this test \
         and the doc comment above if that's an intentional design change)"
    );

    // Clean up: use the (still valid) internal_event_rx as a proxy for confirming
    // the spawned worker task exists and isn't leaking test resources.
    drop(internal_event_rx);
    worker_handle.abort();
}

/// #436 / embedded-mode safety net: if the caller drops the `shutdown_signal`
/// Sender without ever calling `.send(())` — this is exactly what happens when
/// an embedder drops `EmbeddedEngine` without awaiting `stop()` first, since
/// `Inner::shutdown_tx` then drops as part of the struct's own teardown — Worker
/// must still exit. `tokio::sync::watch::Receiver::changed()` resolves (with an
/// error) once its Sender is dropped, and Worker's shutdown arm (`_ =
/// shutdown_signal.changed()`) doesn't discriminate Ok from Err, so this must be
/// sufficient on its own — independent of whether `sm_apply_tx` has any activity,
/// since Worker now permanently holds its own clone (see test above).
#[tokio::test]
async fn test_worker_exits_when_shutdown_sender_dropped_without_send() {
    let mock_smw = MockStateMachineWriterOps::new();

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );
    let worker_handle = tokio::spawn(async move { worker.run().await });

    // Keep sm_apply_tx alive throughout, unlike the old channel-closed test —
    // this proves shutdown works even when the command channel is untouched.
    let _keep_alive = sm_apply_tx;

    // Drop the sender without ever calling .send(()) — no explicit shutdown signal.
    drop(shutdown_tx);

    match tokio::time::timeout(std::time::Duration::from_millis(200), worker_handle).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => panic!("Worker returned error: {e:?}"),
        Ok(Err(e)) => panic!("Worker task panicked: {e:?}"),
        Err(_) => panic!(
            "Worker did not exit when shutdown_signal's Sender was dropped \
             without .send() — embedders who drop EmbeddedEngine without \
             calling stop() would leak the sm-apply OS thread forever"
        ),
    }
}

/// Test: Multiple Batches - Sequential Processing
///
/// Verify SM Worker processes batches in order, last_index increases monotonically.
///
/// # Success Criteria
/// - 3 ApplyCompleted events received on internal_event_rx with last_index: 2, 4, 6
#[tokio::test]
async fn test_multiple_batches_sequential_processing() {
    let mut mock_smw = MockStateMachineWriterOps::new();
    mock_smw.expect_apply_chunk().times(3).returning(|entries| {
        let results: Vec<crate::ApplyResult> =
            entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect();
        Ok(results)
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );

    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    sm_apply_tx
        .send(StateMachineCommand::ApplyEntries {
            entries: vec![create_test_entry(1, 1), create_test_entry(2, 1)],
        })
        .unwrap();
    sm_apply_tx
        .send(StateMachineCommand::ApplyEntries {
            entries: vec![create_test_entry(3, 1), create_test_entry(4, 1)],
        })
        .unwrap();
    sm_apply_tx
        .send(StateMachineCommand::ApplyEntries {
            entries: vec![create_test_entry(5, 1), create_test_entry(6, 1)],
        })
        .unwrap();

    for expected_index in [2u64, 4, 6] {
        match tokio::time::timeout(
            std::time::Duration::from_millis(100),
            internal_event_rx.recv(),
        )
        .await
        {
            Ok(Some(InternalEvent::ApplyCompleted { last_index, .. })) => {
                assert_eq!(
                    last_index, expected_index,
                    "last_index should match expected"
                );
            }
            Ok(Some(other)) => panic!("Expected ApplyCompleted, got {other:?}"),
            Ok(None) => panic!("Event channel closed unexpectedly"),
            Err(_) => panic!("Timeout waiting for ApplyCompleted event"),
        }
    }
}

// ============================================================================
// InstallSnapshot command tests (#436)
// ============================================================================

/// Worker forwards `Applied` results from `install_prepared_snapshot` back to
/// the caller via the oneshot response, unmodified.
///
/// # Given
/// - Mock handler's install_prepared_snapshot returns Ok(Applied)
///
/// # When
/// - InstallSnapshot command is sent
///
/// # Then
/// - response_rx receives exactly that Applied result
#[tokio::test]
async fn test_install_snapshot_applied_forwards_result_via_response() {
    let mut mock_smw = MockStateMachineWriterOps::new();
    mock_smw.expect_install_prepared_snapshot().times(1).returning(|_, _| {
        Ok(SnapshotApplyResult::Applied {
            last_included: LogId { index: 10, term: 2 },
        })
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );
    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    let (command, response_rx) = make_install_snapshot_command();
    sm_apply_tx.send(command).unwrap();

    let result = tokio::time::timeout(std::time::Duration::from_millis(100), response_rx)
        .await
        .expect("response must arrive within 100ms")
        .expect("response sender must not be dropped");

    match result {
        Ok(SnapshotApplyResult::Applied { last_included }) => {
            assert_eq!(last_included, LogId { index: 10, term: 2 });
        }
        other => panic!("expected Ok(Applied), got: {other:?}"),
    }
}

/// A skipped install (stale/duplicate) is not an error — Worker must forward
/// it as Ok, not translate it into a failure.
///
/// # Given
/// - Mock handler's install_prepared_snapshot returns Ok(IgnoredStale)
///
/// # Then
/// - response_rx receives Ok(IgnoredStale), not Err
#[tokio::test]
async fn test_install_snapshot_ignored_stale_forwards_as_ok_not_error() {
    let mut mock_smw = MockStateMachineWriterOps::new();
    mock_smw.expect_install_prepared_snapshot().times(1).returning(|_, _| {
        Ok(SnapshotApplyResult::IgnoredStale {
            current: LogId { index: 20, term: 3 },
        })
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );
    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    let (command, response_rx) = make_install_snapshot_command();
    sm_apply_tx.send(command).unwrap();

    let result = tokio::time::timeout(std::time::Duration::from_millis(100), response_rx)
        .await
        .expect("response must arrive within 100ms")
        .expect("response sender must not be dropped");

    assert!(
        matches!(result, Ok(SnapshotApplyResult::IgnoredStale { .. })),
        "expected Ok(IgnoredStale), got: {result:?}"
    );
}

/// A hard failure (e.g. BoundaryConflict) is forwarded as Err via the
/// response — and, unlike an ApplyEntries failure, must NOT bring the whole
/// Worker down: InstallSnapshot errors are per-request, not fatal to the
/// worker task itself.
///
/// # Given
/// - Mock handler's install_prepared_snapshot returns Err
///
/// # Then
/// - response_rx receives Err
/// - Worker keeps running afterward (verified by successfully processing a
///   subsequent ApplyEntries command on the same worker)
#[tokio::test]
async fn test_install_snapshot_error_forwards_as_err_worker_survives() {
    let mut mock_smw = MockStateMachineWriterOps::new();
    mock_smw
        .expect_install_prepared_snapshot()
        .times(1)
        .returning(|_, _| Err(Error::Fatal("boundary conflict".to_string())));
    mock_smw.expect_apply_chunk().times(1).returning(|entries| {
        let results: Vec<crate::ApplyResult> =
            entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect();
        Ok(results)
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );
    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    let (command, response_rx) = make_install_snapshot_command();
    sm_apply_tx.send(command).unwrap();

    let result = tokio::time::timeout(std::time::Duration::from_millis(100), response_rx)
        .await
        .expect("response must arrive within 100ms")
        .expect("response sender must not be dropped");
    assert!(result.is_err(), "expected Err, got: {result:?}");

    // Worker must still be alive: a subsequent ApplyEntries command is processed normally.
    sm_apply_tx
        .send(StateMachineCommand::ApplyEntries {
            entries: vec![create_test_entry(1, 1)],
        })
        .unwrap();
    match tokio::time::timeout(
        std::time::Duration::from_millis(100),
        internal_event_rx.recv(),
    )
    .await
    {
        Ok(Some(InternalEvent::ApplyCompleted { .. })) => {}
        other => panic!("expected worker to keep processing after install error, got: {other:?}"),
    }
}

/// ApplyEntries and InstallSnapshot share one queue and are processed
/// strictly in send order — the invariant the whole StateMachineWorker
/// design depends on.
///
/// # Given
/// - Send order: ApplyEntries(A) -> InstallSnapshot -> ApplyEntries(B)
///
/// # Then
/// - Both apply_chunk calls and the install call are observed in that exact
///   order (recorded via a shared, mutex-guarded log)
#[tokio::test]
async fn test_apply_and_install_share_single_queue_fifo_order() {
    let order: Arc<std::sync::Mutex<Vec<&'static str>>> = Arc::new(std::sync::Mutex::new(vec![]));

    let mut mock_smw = MockStateMachineWriterOps::new();
    {
        let order = order.clone();
        mock_smw.expect_apply_chunk().times(2).returning(move |entries| {
            order.lock().unwrap().push("apply");
            let results: Vec<crate::ApplyResult> =
                entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect();
            Ok(results)
        });
    }
    {
        let order = order.clone();
        mock_smw.expect_install_prepared_snapshot().times(1).returning(move |_, _| {
            order.lock().unwrap().push("install");
            Ok(SnapshotApplyResult::Applied {
                last_included: LogId::default(),
            })
        });
    }

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );
    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    sm_apply_tx
        .send(StateMachineCommand::ApplyEntries {
            entries: vec![create_test_entry(1, 1)],
        })
        .unwrap();
    let (install_command, install_response_rx) = make_install_snapshot_command();
    sm_apply_tx.send(install_command).unwrap();
    sm_apply_tx
        .send(StateMachineCommand::ApplyEntries {
            entries: vec![create_test_entry(2, 1)],
        })
        .unwrap();

    // Drain: 2 ApplyCompleted events + 1 install response, in FIFO order.
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        internal_event_rx.recv(),
    )
    .await
    .expect("first ApplyCompleted must arrive");
    let _ = tokio::time::timeout(std::time::Duration::from_millis(100), install_response_rx)
        .await
        .expect("install response must arrive");
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        internal_event_rx.recv(),
    )
    .await
    .expect("second ApplyCompleted must arrive");

    assert_eq!(
        *order.lock().unwrap(),
        vec!["apply", "install", "apply"],
        "commands must execute in send order, not be reordered"
    );
}

// ============================================================================
// call_with_timing_guard tests (#436) — written before the helper exists (TDD).
//
// This wraps Worker's synchronous calls into StateMachine trait methods
// (apply_chunk, apply_snapshot_from_file, and the future create_snapshot capture
// step). Third-party StateMachine implementations aren't guaranteed to be fast —
// this makes a slow call observable (warn) without ever cancelling it: a
// tokio::time::timeout here would drop the underlying future mid-flight, which
// could abandon a destructive storage operation (e.g. a WriteBatch commit or a
// CF drop/rebuild) in an unknown state — worse than not having a timeout at all.
// ============================================================================

/// Under the threshold: returns the correct result promptly, no warning needed.
#[tokio::test]
async fn test_call_with_timing_guard_returns_result_when_fast() {
    let result = super::worker::call_with_timing_guard(
        "test_op",
        std::time::Duration::from_millis(200),
        async { 42 },
    )
    .await;
    assert_eq!(result, 42);
}

/// Over the threshold: still delivers the correct result — proves the guard
/// doesn't lose or corrupt the outcome, it only observes.
#[tokio::test]
#[tracing_test::traced_test]
async fn test_call_with_timing_guard_warns_but_still_returns_result_when_slow() {
    let result = super::worker::call_with_timing_guard(
        "slow_op",
        std::time::Duration::from_millis(20),
        async {
            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
            99
        },
    )
    .await;
    assert_eq!(result, 99, "result must still be delivered, not lost");
    assert!(
        logs_contain("slow_op"),
        "must log a warning identifying which operation exceeded the threshold"
    );
}

/// The core safety property: the wrapped future must run to genuine completion
/// even after the threshold fires — not be dropped/cancelled at that point.
#[tokio::test]
async fn test_call_with_timing_guard_never_drops_the_underlying_future() {
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let completed_clone = completed.clone();
    let _ = super::worker::call_with_timing_guard(
        "long_op",
        std::time::Duration::from_millis(10),
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            completed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        },
    )
    .await;
    assert!(
        completed.load(std::sync::atomic::Ordering::SeqCst),
        "the wrapped future must run to completion, not be cancelled at the threshold"
    );
}

/// #436 follow-up: a node generating its own local snapshot must not stall the
/// application of newly committed entries — `CaptureLocalSnapshot` and `ApplyEntries`
/// are unrelated (the storage engine is responsible for its own internal isolation
/// between the two, see `StateMachine::generate_snapshot_data`'s doc comment), so they
/// must not share Worker's FIFO position.
///
/// TDD red: today `handle_command` awaits `capture_local_snapshot()` inline in the main
/// loop, so a slow/in-flight capture blocks `recv()` from ever reaching a subsequent
/// `ApplyEntries`. This test proves it with a capture that's still gated (hasn't
/// returned) when `ApplyEntries` is sent — `ApplyCompleted` must still arrive promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_capture_local_snapshot_does_not_block_subsequent_apply_entries() {
    // std::sync::mpsc (blocking, not tokio) — mockall's `.returning()` closure for an
    // `#[async_trait]` method is synchronous, so it can't `.await` a tokio channel to
    // model an in-flight call. Blocking the OS thread here is safe under the
    // multi-thread runtime: the test's own driving code runs on a different worker
    // thread and isn't affected.
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(Some(release_rx));

    let mut mock_smw = MockStateMachineWriterOps::new();
    mock_smw.expect_capture_local_snapshot().times(1).returning(move || {
        let rx = release_rx.lock().unwrap().take().expect("called once");
        let _ = rx.recv();
        Ok(CapturedLocalSnapshot {
            metadata: SnapshotMetadata::default(),
            temp_dir: std::path::PathBuf::from("/tmp/gated-capture-test"),
        })
    });
    mock_smw
        .expect_apply_chunk()
        .times(1)
        .returning(|entries| Ok(entries.iter().map(|e| ApplyResult::success(e.index)).collect()));

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );
    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    // Send CaptureLocalSnapshot first — it will hang on the gate (not released yet).
    let (capture_response, _capture_response_rx) = oneshot::channel();
    sm_apply_tx
        .send(StateMachineCommand::CaptureLocalSnapshot {
            response: capture_response,
        })
        .unwrap();

    // Give Worker a moment to actually start processing it (not just enqueue it).
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Now send ApplyEntries — must be processed without waiting for capture to finish.
    sm_apply_tx
        .send(StateMachineCommand::ApplyEntries {
            entries: vec![create_test_entry(1, 1)],
        })
        .unwrap();

    let apply_result = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        internal_event_rx.recv(),
    )
    .await;

    // Release the gate regardless of outcome so the spawned capture (if any) can finish
    // and doesn't leak past the test.
    let _ = release_tx.send(());

    assert!(
        matches!(apply_result, Ok(Some(InternalEvent::ApplyCompleted { .. }))),
        "ApplyEntries must be processed while CaptureLocalSnapshot is still in-flight, \
         got: {apply_result:?}"
    );
}

/// #436: local capture must never wait for the capture/install lock — it's periodic
/// maintenance, cheap to skip. If the lock is already held, a second
/// `CaptureLocalSnapshot` must report `CaptureSkipped` without ever calling
/// `capture_local_snapshot` on the writer. `try_lock()` doesn't care who's holding
/// the lock, so a second capture is a faithful (and, unlike Install, deterministic)
/// stand-in for "Install already holds it" — see the ordering note below for why
/// Install can't be used directly here.
///
/// Deterministic by construction: the first capture is sent and gated *inside*
/// `capture_local_snapshot` — since the lock is acquired before that call, blocking
/// on the gate only after entering it proves the lock is already held. The second
/// capture is only sent after observing that gate, so there's no race for it to lose.
///
/// Ordering note: Install can't play the "already holds it" role deterministically
/// here, because `InstallSnapshot` is handled *inline* on Worker's own loop (see
/// `handle_command`) — while it's blocked waiting for/using the lock, Worker can't
/// even dequeue a subsequently-sent `CaptureLocalSnapshot`, so that capture wouldn't
/// attempt `try_lock()` until *after* install finishes and releases it. Capture, by
/// contrast, is spawned off Worker's loop, so a second capture's `try_lock()` can
/// genuinely race a first capture's held lock while Worker's loop stays free.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_capture_skipped_when_lock_already_held() {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(Some(release_rx));
    let (gate_reached_tx, gate_reached_rx) = std::sync::mpsc::channel::<()>();

    let mut mock_smw = MockStateMachineWriterOps::new();
    mock_smw.expect_capture_local_snapshot().times(1).returning(move || {
        let _ = gate_reached_tx.send(());
        let rx = release_rx.lock().unwrap().take().expect("called once");
        let _ = rx.recv();
        Ok(CapturedLocalSnapshot {
            metadata: SnapshotMetadata::default(),
            temp_dir: std::path::PathBuf::from("/tmp/lock-held-test"),
        })
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );
    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    let (first_response, _first_response_rx) = oneshot::channel();
    sm_apply_tx
        .send(StateMachineCommand::CaptureLocalSnapshot {
            response: first_response,
        })
        .unwrap();

    // Blocks until the first capture has entered the mock — i.e. the lock is
    // provably held — so sending the second capture next is not racing for it.
    gate_reached_rx.recv().unwrap();

    let (second_response, second_response_rx) = oneshot::channel();
    sm_apply_tx
        .send(StateMachineCommand::CaptureLocalSnapshot {
            response: second_response,
        })
        .unwrap();

    let second_result =
        tokio::time::timeout(std::time::Duration::from_millis(400), second_response_rx).await;

    let _ = release_tx.send(());

    match second_result {
        Ok(Ok(Err(Error::Consensus(ConsensusError::Snapshot(SnapshotError::CaptureSkipped))))) => {}
        other => panic!("expected CaptureSkipped, got {other:?}"),
    }
}

/// #436: InstallSnapshot is leader-authoritative — it must never skip; it waits for
/// the lock instead. This proves it actually blocks until Capture (which got the lock
/// first) releases it, rather than failing/skipping immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_install_waits_for_capture_to_release_lock() {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(Some(release_rx));

    let mut mock_smw = MockStateMachineWriterOps::new();
    mock_smw.expect_capture_local_snapshot().times(1).returning(move || {
        let rx = release_rx.lock().unwrap().take().expect("called once");
        let _ = rx.recv();
        Ok(CapturedLocalSnapshot {
            metadata: SnapshotMetadata::default(),
            temp_dir: std::path::PathBuf::from("/tmp/install-waits-test"),
        })
    });
    mock_smw.expect_install_prepared_snapshot().times(1).returning(|_, _| {
        Ok(SnapshotApplyResult::Applied {
            last_included: LogId { index: 1, term: 1 },
        })
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );
    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    let (capture_response, _capture_response_rx) = oneshot::channel();
    sm_apply_tx
        .send(StateMachineCommand::CaptureLocalSnapshot {
            response: capture_response,
        })
        .unwrap();

    // Let the spawned capture task actually start and grab the lock.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let (install_response, install_response_rx) = oneshot::channel();
    sm_apply_tx
        .send(StateMachineCommand::InstallSnapshot {
            snapshot: PreparedSnapshot {
                metadata: SnapshotMetadata::default(),
                temp_dir: tempfile::tempdir().unwrap(),
            },
            response: install_response,
        })
        .unwrap();

    // Install must not resolve while capture still holds the lock.
    let still_waiting =
        tokio::time::timeout(std::time::Duration::from_millis(100), install_response_rx).await;
    assert!(
        still_waiting.is_err(),
        "InstallSnapshot must wait for the capture/install lock, got: {still_waiting:?}"
    );

    // Release capture — install must now complete.
    let _ = release_tx.send(());
}

/// #436 product decision: a leader-pushed InstallSnapshot
/// always wins over a concurrently-running local CaptureLocalSnapshot — unconditionally,
/// regardless of which one's boundary is actually more advanced. This is a deliberate
/// choice of the simpler of two possible rules ("semantic A": any install completing
/// during a capture supersedes it) over the alternative ("semantic B": compare LogId
/// boundaries and only reject a capture that would regress the current snapshot).
///
/// Business scenario this test encodes: a follower fell behind by the leader's (slightly
/// stale) accounting, so the leader pushes it an Install at boundary index=90. Meanwhile
/// the follower had already caught up on its own and started a routine local Capture at
/// index=100 — genuinely *more* advanced than what the leader is pushing. Even so, the
/// capture must be discarded: the leader is treated as the authoritative source for
/// snapshot content, never the follower's own local judgment, so "install happened during
/// my capture" is sufficient to supersede it without comparing boundaries at all.
///
/// Mechanism under test: `CaptureLocalSnapshot`'s background task only performs the slow
/// export; the "is this still fresh" decision is made back on the Worker's own loop when
/// it processes `LocalSnapshotReady`, which travels through the *same* `sm_apply_tx` FIFO
/// queue as `InstallSnapshot` (not a separate channel — a separate channel would make
/// `tokio::select!`'s ordering between the two non-deterministic). Both commands are sent
/// before the capture's gate is released, with `InstallSnapshot` sent first, so by FIFO
/// ordering the Worker is guaranteed to fully process the install (bumping `install_epoch`)
/// before it ever dequeues `LocalSnapshotReady` — deterministic, no `yield_now()`, no sleep
/// racing the scheduler.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_capture_superseded_by_install_even_when_capture_is_more_advanced() {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(Some(release_rx));

    let mut mock_smw = MockStateMachineWriterOps::new();
    // Capture's own boundary (index=100) is ahead of what the leader is pushing
    // (index=90) — proves rejection is about "install happened", not "who's newer".
    mock_smw.expect_capture_local_snapshot().times(1).returning(move || {
        let rx = release_rx.lock().unwrap().take().expect("called once");
        let _ = rx.recv();
        Ok(CapturedLocalSnapshot {
            metadata: SnapshotMetadata {
                last_included: Some(LogId {
                    index: 100,
                    term: 1,
                }),
                ..Default::default()
            },
            temp_dir: std::path::PathBuf::from("/tmp/superseded-test"),
        })
    });
    mock_smw.expect_install_prepared_snapshot().times(1).returning(|_, _| {
        Ok(SnapshotApplyResult::Applied {
            last_included: LogId { index: 90, term: 1 },
        })
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );
    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    let (capture_response, capture_response_rx) = oneshot::channel();
    sm_apply_tx
        .send(StateMachineCommand::CaptureLocalSnapshot {
            response: capture_response,
        })
        .unwrap();

    // Let capture start and grab the lock.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Install is sent (queued into the same sm_apply_tx FIFO) while capture still
    // holds the lock — its send() precedes capture's eventual LocalSnapshotReady
    // send(), which is the only ordering guarantee this test relies on.
    let (install_response, install_response_rx) = oneshot::channel();
    sm_apply_tx
        .send(StateMachineCommand::InstallSnapshot {
            snapshot: PreparedSnapshot {
                metadata: SnapshotMetadata::default(),
                temp_dir: tempfile::tempdir().unwrap(),
            },
            response: install_response,
        })
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Release capture — it drops the lock (install proceeds inline on Worker's loop
    // and fully completes, bumping install_epoch, before Worker ever dequeues capture's
    // LocalSnapshotReady message — guaranteed by FIFO order, not scheduler luck).
    let _ = release_tx.send(());

    let install_result =
        tokio::time::timeout(std::time::Duration::from_millis(200), install_response_rx).await;
    assert!(
        matches!(
            install_result,
            Ok(Ok(Ok(SnapshotApplyResult::Applied { .. })))
        ),
        "install must complete, got: {install_result:?}"
    );

    let capture_result =
        tokio::time::timeout(std::time::Duration::from_millis(200), capture_response_rx).await;
    match capture_result {
        Ok(Ok(Err(Error::Consensus(ConsensusError::Snapshot(
            SnapshotError::CaptureSuperseded,
        ))))) => {}
        other => panic!(
            "expected CaptureSuperseded (leader install always wins, even though \
             capture's own boundary index=100 > install's index=90), got: {other:?}"
        ),
    }
}

/// #436: shutdown must wait for an in-flight CaptureLocalSnapshot task instead of
/// returning while it's still writing to disk — see `call_with_timing_guard`'s doc
/// comment on why a storage op can't just be abandoned mid-flight (same reasoning
/// applies here: don't leave a capture "orphaned" past the point something else,
/// e.g. an embedder's `stop()` caller, assumes everything has already stopped and
/// goes on to close storage or reopen the same data dir).
///
/// Proves two things: (1) `run()` does NOT return while the capture is still gated
/// — shutdown is blocked on it, not racing past it — and (2) once released, `run()`
/// does complete. The capture's own `response` still resolves to a dropped-channel
/// error either way (the final `LocalSnapshotReady` message never gets processed —
/// `run()` awaits the task's `JoinHandle`, not another `sm_apply_rx` read, so that
/// message is dropped along with the channel on return) — that's fine: the node is
/// shutting down regardless, nothing downstream still needs a fresh/superseded
/// verdict. What actually matters — the disk write finishing before `run()` returns
/// — is what this test checks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shutdown_waits_for_in_flight_capture_before_returning() {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(Some(release_rx));
    let (gate_reached_tx, gate_reached_rx) = std::sync::mpsc::channel::<()>();

    let mut mock_smw = MockStateMachineWriterOps::new();
    mock_smw.expect_capture_local_snapshot().times(1).returning(move || {
        let _ = gate_reached_tx.send(());
        let rx = release_rx.lock().unwrap().take().expect("called once");
        let _ = rx.recv();
        Ok(CapturedLocalSnapshot {
            metadata: SnapshotMetadata::default(),
            temp_dir: std::path::PathBuf::from("/tmp/shutdown-waits-test"),
        })
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smw),
        sm_apply_tx.clone(),
        sm_apply_rx,
        internal_event_tx,
        shutdown_rx,
    );
    let worker_handle = tokio::spawn(async move { worker.run().await });

    let (capture_response, capture_response_rx) = oneshot::channel();
    sm_apply_tx
        .send(StateMachineCommand::CaptureLocalSnapshot {
            response: capture_response,
        })
        .unwrap();

    // Blocks until capture has entered the mock — i.e. the export is genuinely
    // "in flight" — before triggering shutdown.
    gate_reached_rx.recv().unwrap();

    shutdown_tx.send(()).unwrap();

    // Give shutdown handling a real chance to run, then prove it hasn't returned —
    // it must be blocked waiting on the still-gated capture, not racing past it.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !worker_handle.is_finished(),
        "Worker returned from run() while the capture task was still gated mid-export \
         — shutdown must wait for it, not leave it orphaned"
    );

    // Release the gate — the capture finishes, and shutdown can now complete.
    let _ = release_tx.send(());

    let run_result = tokio::time::timeout(std::time::Duration::from_millis(200), worker_handle)
        .await
        .expect("Worker must exit once the in-flight capture finishes");
    assert!(matches!(run_result, Ok(Ok(()))), "got: {run_result:?}");

    // The capture's own response is a dropped-channel error (see doc comment above)
    // — expected, not a hang.
    let capture_result =
        tokio::time::timeout(std::time::Duration::from_millis(200), capture_response_rx).await;
    assert!(
        matches!(capture_result, Ok(Err(_))),
        "expected the response channel to resolve (not hang), got: {capture_result:?}"
    );
}
