//! Tests for `SmWorkerHandle`: starting the worker thread, stopping it through its queue, and the
//! cases where the stop does not go to plan (never started, stuck, panicked, owner dropped).
//!
//! The worker is real; only the state machine writer is mocked. The multi-thread runtime is
//! deliberate: the worker thread drives its future on the runtime of the test.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use d_engine_proto::common::Entry;
use tokio::sync::mpsc;

use super::SmWorkerHandle;
use super::StateMachineCommand;
use super::StateMachineWorker;
use super::StopError;
use crate::ApplyResult;
use crate::InternalEvent;
use crate::MockStateMachineWriterOps;
use crate::MockTypeConfig;

type Applied = Arc<Mutex<Vec<u64>>>;

/// A handle around a real worker whose writer records the indices it applies. The test keeps its
/// own sender (`tx`) to feed the queue the way the Raft loop would.
struct Rig {
    handle: SmWorkerHandle<MockTypeConfig>,
    tx: mpsc::UnboundedSender<StateMachineCommand>,
    applied: Applied,
    // Kept alive so the worker's sends of apply results never fail.
    _events: mpsc::UnboundedReceiver<InternalEvent>,
}

fn rig_with(
    configure: impl FnOnce(&mut MockStateMachineWriterOps<MockTypeConfig>, Applied)
) -> Rig {
    let applied: Applied = Arc::default();
    let mut writer = MockStateMachineWriterOps::new();
    configure(&mut writer, applied.clone());
    // Added last, so a test's own `expect_close` keeps precedence.
    writer.expect_close().returning(|| Ok(()));

    let (tx, rx) = mpsc::unbounded_channel();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let worker =
        StateMachineWorker::<MockTypeConfig>::new(1, Arc::new(writer), tx.clone(), rx, events_tx);
    Rig {
        handle: SmWorkerHandle::new(worker, tx.clone()),
        tx,
        applied,
        _events: events_rx,
    }
}

/// A writer that records every applied index and succeeds.
fn recording(
    writer: &mut MockStateMachineWriterOps<MockTypeConfig>,
    applied: Applied,
) {
    writer.expect_apply_chunk().returning(move |entries| {
        applied.lock().unwrap().extend(entries.iter().map(|e| e.index));
        Ok(entries.iter().map(|e| ApplyResult::success(e.index)).collect())
    });
}

fn apply(index: u64) -> StateMachineCommand {
    StateMachineCommand::ApplyEntries {
        sent_at: tokio::time::Instant::now(),
        entries: vec![Entry {
            index,
            term: 1,
            payload: Some(d_engine_proto::common::EntryPayload::command(
                bytes::Bytes::from("data"),
            )),
        }],
    }
}

async fn eventually(mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    condition()
}

fn state_of(handle: &SmWorkerHandle<MockTypeConfig>) -> String {
    format!("{handle:?}")
}

/// Business scenario: a node starts. Purpose: `start` brings the worker to life on its own
/// thread, so entries the Raft loop queues are applied without anyone driving the worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_started_worker_applies_entries_queued_by_the_raft_loop() {
    let rig = rig_with(recording);
    rig.handle.start().expect("start");

    rig.tx.send(apply(1)).unwrap();
    rig.tx.send(apply(2)).unwrap();

    assert!(
        eventually(|| *rig.applied.lock().unwrap() == vec![1, 2]).await,
        "queued entries were not applied: {:?}",
        rig.applied.lock().unwrap()
    );
    assert!(state_of(&rig.handle).contains("Running"));
    rig.handle.stop_and_join(Duration::from_secs(2)).await.expect("stop");
}

/// Business scenario: the node shuts down right after the Raft loop queued its last committed
/// batches. Purpose: stopping applies everything queued before the stop request, in order, and
/// only then ends the thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stop_and_join_applies_everything_queued_before_it() {
    let rig = rig_with(recording);
    rig.handle.start().expect("start");
    for index in 1..=3 {
        rig.tx.send(apply(index)).unwrap();
    }

    rig.handle.stop_and_join(Duration::from_secs(2)).await.expect("clean stop");

    assert_eq!(*rig.applied.lock().unwrap(), vec![1, 2, 3]);
    assert!(state_of(&rig.handle).contains("Stopped"));
}

/// Business scenario: the node's shutdown path runs unconditionally, so it may run twice and may
/// follow a start that was already done. Purpose: a second `start` and a second `stop_and_join`
/// are harmless.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_start_and_stop_and_join_are_idempotent() {
    let rig = rig_with(recording);
    rig.handle.start().expect("start");
    rig.handle.start().expect("second start is a no-op");
    assert!(state_of(&rig.handle).contains("Running"));

    rig.handle.stop_and_join(Duration::from_secs(2)).await.expect("first stop");
    rig.handle.stop_and_join(Duration::from_secs(2)).await.expect("second stop");
    assert!(state_of(&rig.handle).contains("Stopped"));
}

/// Business scenario: the state machine is wedged in a slow storage call during shutdown.
/// Purpose: `stop_and_join` gives up at the timeout instead of hanging the node, says so, keeps
/// the thread's handle, and a later call succeeds once the worker is free again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stop_and_join_times_out_on_a_stuck_worker_and_can_be_retried() {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = Mutex::new(release_rx);
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let started_tx = Mutex::new(started_tx);
    let rig = rig_with(|writer, _| {
        writer.expect_apply_chunk().returning(move |entries| {
            let _ = started_tx.lock().unwrap().send(());
            let _ = release_rx.lock().unwrap().recv(); // blocks the worker thread
            Ok(entries.iter().map(|e| ApplyResult::success(e.index)).collect())
        });
    });
    rig.handle.start().expect("start");
    rig.tx.send(apply(1)).unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).expect("apply started");

    let timeout = Duration::from_millis(100);
    assert_eq!(
        rig.handle.stop_and_join(timeout).await,
        Err(StopError::TimedOut(timeout))
    );
    assert!(
        state_of(&rig.handle).contains("Running"),
        "thread handle must be kept"
    );

    release_tx.send(()).unwrap();
    rig.handle
        .stop_and_join(Duration::from_secs(2))
        .await
        .expect("a free worker stops on the retry");
}

/// Business scenario: start-up fails before the node ever starts its worker. Purpose: stopping
/// a handle that was never started succeeds, never runs the worker, and a later `start` cannot
/// revive it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stop_before_start_discards_the_worker() {
    // No expectation on apply_chunk: any call to it would panic.
    let rig = rig_with(|_, _| {});
    assert!(state_of(&rig.handle).contains("Pending"));

    rig.handle.stop_and_join(Duration::from_secs(1)).await.expect("stop");
    assert!(state_of(&rig.handle).contains("Stopped"));

    rig.handle.start().expect("start after stop is a no-op");
    assert!(state_of(&rig.handle).contains("Stopped"));
}

/// Business scenario: the task running the node is aborted, or panics, so the unconditional
/// shutdown path never runs and the handle is simply dropped. Purpose: dropping the handle still
/// stops the worker, after it applied what was queued, without waiting for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dropping_the_handle_stops_the_worker_after_what_was_queued() {
    let rig = rig_with(recording);
    rig.handle.start().expect("start");
    rig.tx.send(apply(1)).unwrap();
    rig.tx.send(apply(2)).unwrap();

    let Rig {
        handle,
        tx,
        applied,
        ..
    } = rig;
    drop(handle);

    // The receiver is dropped when the worker thread ends.
    assert!(
        eventually(|| tx.is_closed()).await,
        "the worker did not stop after its handle was dropped"
    );
    assert_eq!(*applied.lock().unwrap(), vec![1, 2]);
}

/// Business scenario: a bug in a custom state machine panics inside `apply_chunk`. Purpose: the
/// node's shutdown path learns about it (`Panicked`) instead of believing the stop was clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stop_and_join_reports_a_panicked_worker() {
    let rig = rig_with(|writer, _| {
        writer.expect_apply_chunk().returning(|_| panic!("state machine bug"));
    });
    rig.handle.start().expect("start");
    rig.tx.send(apply(1)).unwrap();

    assert_eq!(
        rig.handle.stop_and_join(Duration::from_secs(2)).await,
        Err(StopError::Panicked)
    );
}

/// Business scenario: the node shuts down while the disk is full, so the state machine cannot
/// persist its final state in `close`. Purpose: the node's shutdown path learns about it
/// (`Failed`, carrying the reason) instead of believing the stop was clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stop_and_join_reports_a_failed_close() {
    let rig = rig_with(|writer, _| {
        writer
            .expect_close()
            .times(1)
            .returning(|| Err(crate::SnapshotError::OperationFailed("disk full".into()).into()));
    });
    rig.handle.start().expect("start");

    let result = rig.handle.stop_and_join(Duration::from_secs(2)).await;

    match result {
        Err(StopError::Failed(reason)) => {
            assert!(
                reason.contains("disk full"),
                "reason lost the cause: {reason}"
            )
        }
        other => panic!("a failed close must be reported as Failed, got {other:?}"),
    }
}

/// Same as above for a worker that was never started: its state machine is still closed on stop,
/// and a failure there must not be swallowed just because no thread ever ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stop_before_start_reports_a_failed_close() {
    let rig = rig_with(|writer, _| {
        writer
            .expect_close()
            .times(1)
            .returning(|| Err(crate::SnapshotError::OperationFailed("disk full".into()).into()));
    });

    let result = rig.handle.stop_and_join(Duration::from_secs(1)).await;

    assert!(
        matches!(&result, Err(StopError::Failed(reason)) if reason.contains("disk full")),
        "got {result:?}"
    );
    assert!(state_of(&rig.handle).contains("Stopped"));
}
