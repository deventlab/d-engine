//! Direct, isolated unit tests for `FsyncWorker`.
//!
//! This module is a child of `fsync_worker` (see the `#[path = ...] mod
//! fsync_worker_test;` at the bottom of `fsync_worker.rs`), so it can construct
//! `FsyncWorker` directly and inspect its private fields (`inflight` /
//! `pending_max` / `pending_replies` / `generation`) without going through
//! `RaftLogCore`'s append/flush pipeline at all. `run_until_caught_up` is a
//! plain sync fn, so most tests need no tokio runtime.
//!
//! Scope: protocol/logic correctness of `FsyncWorker`'s own state machine.
//! Not performance — see `benches/` for throughput regression guards.

use super::*;
use crate::InternalEvent;
use crate::MockLogStore;
use crate::MockStorageEngine;
use crate::Result;
use crate::StorageEngine;
use d_engine_proto::common::LogId;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{mpsc, oneshot};

/// Build a bare `FsyncWorker` over a mock log store — no tokio runtime, no
/// `RaftLogCore`. `internal_event_tx` is `None`, so the worker never sends
/// `FsyncCompleted`; tests that need to observe completion wire their own.
fn minimal_worker(log_store: Arc<MockLogStore>) -> Arc<FsyncWorker<MockLogStore>> {
    Arc::new(FsyncWorker::new(
        1,
        log_store,
        Arc::new(AtomicBool::new(false)),
        None,
    ))
}

// ── Initial state ──────────────────────────────────────────────────────────

/// `FsyncWorker::new()` starts with no work pending and no fence armed.
///
/// Expected:
///   - `inflight == false`, `pending_max == 0`, `pending_replies` empty,
///     `generation == 0`.
#[test]
fn test_new_initializes_empty_state() {
    let worker = minimal_worker(Arc::new(MockLogStore::new()));

    assert!(
        !worker.inflight.load(Ordering::Acquire),
        "inflight must start false"
    );
    assert_eq!(
        worker.pending_max.lock().index,
        0,
        "pending_max must start at 0"
    );
    assert!(
        worker.pending_replies.lock().is_empty(),
        "pending_replies must start empty"
    );
    assert_eq!(
        worker.generation.load(Ordering::Acquire),
        0,
        "generation must start at 0"
    );
}

// ── fence_reset() ────────────────────────────────────────────────────────────

/// `fence_reset()` zeroes `pending_max` — any batch size recorded before
/// reset must not leak into a post-reset round.
#[test]
fn test_fence_reset_zeroes_pending_max() {
    let worker = minimal_worker(Arc::new(MockLogStore::new()));
    *worker.pending_max.lock() = LogId { term: 1, index: 10 };

    worker.fence_reset();

    assert_eq!(
        worker.pending_max.lock().index,
        0,
        "fence_reset() must zero pending_max"
    );
}

/// `fence_reset()` drains `pending_replies` and answers each with `Err` —
/// callers queued before reset must not be silently dropped nor receive a
/// stale `Ok(())`.
#[test]
fn test_fence_reset_drains_pending_replies_with_err() {
    let worker = minimal_worker(Arc::new(MockLogStore::new()));

    let (tx1, mut rx1) = oneshot::channel();
    let (tx2, mut rx2) = oneshot::channel();
    worker.pending_replies.lock().extend([tx1, tx2]);

    worker.fence_reset();

    assert!(
        worker.pending_replies.lock().is_empty(),
        "pending_replies must be empty after fence_reset()"
    );
    assert!(
        rx1.try_recv().expect("tx1 must have been answered").is_err(),
        "queued reply must receive Err, not a silent drop or stale Ok"
    );
    assert!(
        rx2.try_recv().expect("tx2 must have been answered").is_err(),
        "queued reply must receive Err, not a silent drop or stale Ok"
    );
}

/// `fence_reset()` increments `generation` — the fence itself.
#[test]
fn test_fence_reset_increments_generation() {
    let worker = minimal_worker(Arc::new(MockLogStore::new()));

    worker.fence_reset();
    assert_eq!(
        worker.generation.load(Ordering::Acquire),
        1,
        "first fence_reset() must bump to 1"
    );
    worker.fence_reset();
    assert_eq!(
        worker.generation.load(Ordering::Acquire),
        2,
        "second fence_reset() must bump to 2"
    );
}

/// `fence_reset()` is safe to call with nothing pending.
#[test]
fn test_fence_reset_is_safe_with_nothing_pending() {
    let worker = minimal_worker(Arc::new(MockLogStore::new()));

    worker.fence_reset();

    assert_eq!(
        worker.generation.load(Ordering::Acquire),
        1,
        "generation must still increment"
    );
    assert_eq!(
        worker.pending_max.lock().index,
        0,
        "pending_max must stay 0"
    );
    assert!(
        worker.pending_replies.lock().is_empty(),
        "pending_replies must stay empty"
    );
}

// ── submit() ─────────────────────────────────────────────────────────────

/// `submit()` records the high-water mark — a smaller, later `mark` must not
/// regress it.
#[test]
fn test_submit_pending_max_uses_fetch_max_not_last_write() {
    let worker = minimal_worker(Arc::new(MockLogStore::new()));

    // Pretend a round is in flight so submit() only records state.
    worker.inflight.store(true, Ordering::Release);

    worker.submit(
        LogId {
            term: 1,
            index: 100,
        },
        vec![],
    );
    worker.submit(LogId { term: 1, index: 50 }, vec![]);

    assert_eq!(
        worker.pending_max.lock().index,
        100,
        "pending_max must stay at the high-water mark (100), not regress to 50"
    );
}

/// The first `submit()` call wins the CAS and flips `inflight` to `true`
/// synchronously.
#[tokio::test]
async fn test_submit_first_call_sets_inflight_true() {
    let (storage, _flush_gate) =
        MockStorageEngine::not_durable_gated_flush("submit_first_call_sets_inflight_true".into());
    let worker = minimal_worker(storage.log_store());

    worker.submit(LogId { term: 1, index: 1 }, vec![]);

    assert!(
        worker.inflight.load(Ordering::Acquire),
        "inflight must be true immediately after the first submit() wins the CAS"
    );
}

/// A `submit()` call while a round is in flight must not spawn a second task.
#[test]
fn test_submit_second_call_does_not_spawn_second_task_while_inflight() {
    let (storage, flush_call_count) = MockStorageEngine::not_durable(
        "submit_second_call_does_not_spawn_second_task_while_inflight".into(),
    );
    let worker = minimal_worker(storage.log_store());

    worker.inflight.store(true, Ordering::Release);

    let (tx, mut rx) = oneshot::channel::<Result<()>>();
    worker.submit(LogId { term: 1, index: 1 }, vec![tx]);

    assert_eq!(
        worker.pending_max.lock().index,
        1,
        "submit() must record pending_max"
    );
    assert_eq!(
        worker.pending_replies.lock().len(),
        1,
        "submit() must queue the reply"
    );
    assert!(rx.try_recv().is_err(), "queued reply must still be pending");
    assert_eq!(
        flush_call_count.load(Ordering::Acquire),
        0,
        "losing the CAS must not trigger a physical flush"
    );
}

// ── run_until_caught_up() ────────────────────────────────────────────────

/// With nothing pending, `run_until_caught_up` clears `inflight` and returns
/// without flushing.
#[test]
fn test_run_until_caught_up_returns_immediately_when_nothing_pending() {
    let (storage, flush_call_count) = MockStorageEngine::not_durable(
        "run_until_caught_up_returns_immediately_when_nothing_pending".into(),
    );
    let worker = minimal_worker(storage.log_store());
    worker.inflight.store(true, Ordering::Release);

    worker.run_until_caught_up();

    assert!(
        !worker.inflight.load(Ordering::Acquire),
        "inflight must be cleared"
    );
    assert_eq!(
        flush_call_count.load(Ordering::Acquire),
        0,
        "no flush when nothing pending"
    );
}

/// A successful physical flush sends `FsyncCompleted` for the round's mark.
#[test]
fn test_run_until_caught_up_sends_fsync_completed_on_success() {
    let (storage, _flush_call_count) = MockStorageEngine::not_durable(
        "run_until_caught_up_sends_fsync_completed_on_success".into(),
    );
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Arc::new(FsyncWorker::new(
        1,
        storage.log_store(),
        Arc::new(AtomicBool::new(false)),
        Some(tx),
    ));

    worker.inflight.store(true, Ordering::Release);
    *worker.pending_max.lock() = LogId { term: 1, index: 5 };

    worker.run_until_caught_up();

    match rx.try_recv().expect("must emit FsyncCompleted") {
        InternalEvent::FsyncCompleted { mark, sent_at: _ } => {
            assert_eq!(
                mark,
                LogId { term: 1, index: 5 },
                "must report the round's mark"
            );
        }
        other => panic!("expected FsyncCompleted, got {other:?}"),
    }
}

/// A failed physical flush does NOT send `FsyncCompleted` — it poisons and
/// notifies a `FatalError` instead.
#[test]
fn test_run_until_caught_up_does_not_send_fsync_completed_on_flush_failure() {
    let storage = MockStorageEngine::not_durable_always_failing_flush(
        "run_until_caught_up_does_not_send_fsync_completed_on_flush_failure".into(),
    );
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Arc::new(FsyncWorker::new(
        1,
        storage.log_store(),
        Arc::new(AtomicBool::new(false)),
        Some(tx),
    ));

    worker.inflight.store(true, Ordering::Release);
    *worker.pending_max.lock() = LogId { term: 1, index: 5 };

    worker.run_until_caught_up();

    while let Ok(event) = rx.try_recv() {
        assert!(
            !matches!(event, InternalEvent::FsyncCompleted { .. }),
            "a failed flush must not emit FsyncCompleted, got {event:?}"
        );
    }
}

/// A failed physical flush sends `Err` (not a hang, not `Ok`) to queued replies.
#[test]
fn test_run_until_caught_up_sends_err_to_replies_on_flush_failure() {
    let storage = MockStorageEngine::not_durable_always_failing_flush(
        "run_until_caught_up_sends_err_to_replies_on_flush_failure".into(),
    );
    let worker = minimal_worker(storage.log_store());

    let (tx, mut rx) = oneshot::channel::<Result<()>>();
    worker.inflight.store(true, Ordering::Release);
    *worker.pending_max.lock() = LogId { term: 1, index: 5 };
    worker.pending_replies.lock().push(tx);

    worker.run_until_caught_up();

    assert!(
        rx.try_recv().expect("reply must have been answered").is_err(),
        "a failed flush must send Err to queued replies"
    );
}

/// The reset fence: a round whose generation no longer matches at completion
/// must be discarded — no `FsyncCompleted`, replies get `Err`.
#[test]
fn test_run_until_caught_up_discards_stale_generation_result_without_advancing() {
    // `Arc::new_cyclic` lets the mock's flush() closure bump the worker's own
    // `generation` field — the worker owns the mock, so the closure needs a
    // `Weak` upgraded at flush time.
    let worker: Arc<FsyncWorker<MockLogStore>> =
        Arc::new_cyclic(|weak: &std::sync::Weak<FsyncWorker<MockLogStore>>| {
            let weak_in_mock = weak.clone();
            let mut mock_log_store = MockLogStore::new();
            mock_log_store.expect_is_write_durable().returning(|| false);
            mock_log_store.expect_flush().returning(move || {
                if let Some(w) = weak_in_mock.upgrade() {
                    w.generation.fetch_add(1, Ordering::AcqRel);
                }
                Ok(())
            });
            FsyncWorker::new(
                1,
                Arc::new(mock_log_store),
                Arc::new(AtomicBool::new(false)),
                None,
            )
        });

    let (tx, mut rx) = oneshot::channel::<Result<()>>();
    worker.inflight.store(true, Ordering::Release);
    *worker.pending_max.lock() = LogId { term: 1, index: 5 };
    worker.pending_replies.lock().push(tx);

    worker.run_until_caught_up();

    assert!(
        rx.try_recv().expect("reply must have been answered").is_err(),
        "a stale-generation result must send Err, not a resurrected Ok"
    );
}

/// The other half of the fence: a matching generation accepts the result —
/// `FsyncCompleted` emitted, replies resolve `Ok`.
#[test]
fn test_run_until_caught_up_accepts_result_when_generation_unchanged() {
    let (storage, _flush_call_count) = MockStorageEngine::not_durable(
        "run_until_caught_up_accepts_result_when_generation_unchanged".into(),
    );
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Arc::new(FsyncWorker::new(
        1,
        storage.log_store(),
        Arc::new(AtomicBool::new(false)),
        Some(tx),
    ));

    // Two unrelated fences happened earlier — generation is 2, not 0.
    worker.fence_reset();
    worker.fence_reset();
    assert_eq!(worker.generation.load(Ordering::Acquire), 2);

    let (reply_tx, mut reply_rx) = oneshot::channel::<Result<()>>();
    worker.inflight.store(true, Ordering::Release);
    *worker.pending_max.lock() = LogId { term: 1, index: 5 };
    worker.pending_replies.lock().push(reply_tx);

    worker.run_until_caught_up();

    match rx.try_recv().expect("must emit FsyncCompleted") {
        InternalEvent::FsyncCompleted { mark, sent_at: _ } => {
            assert_eq!(mark, LogId { term: 1, index: 5 });
        }
        other => panic!("expected FsyncCompleted, got {other:?}"),
    }
    assert!(
        reply_rx.try_recv().expect("reply must have been answered").is_ok(),
        "a matching generation must resolve replies as Ok"
    );
}

/// Multiple queued `submit()`s are coalesced into one physical `flush()` call.
#[test]
fn test_run_until_caught_up_coalesces_queued_submits_into_one_flush() {
    let (storage, flush_call_count) = MockStorageEngine::not_durable(
        "run_until_caught_up_coalesces_queued_submits_into_one_flush".into(),
    );
    let worker = minimal_worker(storage.log_store());

    let (tx1, mut rx1) = oneshot::channel::<Result<()>>();
    let (tx2, mut rx2) = oneshot::channel::<Result<()>>();
    worker.inflight.store(true, Ordering::Release);
    *worker.pending_max.lock() = LogId { term: 1, index: 10 };
    worker.pending_replies.lock().extend([tx1, tx2]);

    worker.run_until_caught_up();

    assert_eq!(
        flush_call_count.load(Ordering::Acquire),
        1,
        "two submits must share one flush()"
    );
    assert!(
        rx1.try_recv().expect("tx1 answered").is_ok(),
        "tx1 must resolve Ok"
    );
    assert!(
        rx2.try_recv().expect("tx2 answered").is_ok(),
        "tx2 must resolve Ok"
    );
}

/// Term-first ordering: a newer term's mark wins over an older term's higher
/// index, so a stale pre-truncation batch can't swallow the valid tail.
#[test]
fn test_submit_term_first_keeps_valid_mark_over_stale_higher_index() {
    let (storage, _flush_call_count) = MockStorageEngine::not_durable(
        "submit_term_first_keeps_valid_mark_over_stale_higher_index".into(),
    );
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Arc::new(FsyncWorker::new(
        1,
        storage.log_store(),
        Arc::new(AtomicBool::new(false)),
        Some(tx),
    ));

    worker.inflight.store(true, Ordering::Release);
    worker.submit(LogId { term: 1, index: 10 }, vec![]); // stale pre-truncation
    worker.submit(LogId { term: 2, index: 2 }, vec![]); // valid post-truncation

    assert_eq!(
        *worker.pending_max.lock(),
        LogId { term: 2, index: 2 },
        "term-first: the newer-term mark must win over the stale higher index"
    );

    worker.run_until_caught_up();

    match rx.try_recv().expect("must emit FsyncCompleted") {
        InternalEvent::FsyncCompleted { mark, sent_at: _ } => {
            assert_eq!(
                mark,
                LogId { term: 2, index: 2 },
                "must report the valid tail"
            );
        }
        other => panic!("expected FsyncCompleted, got {other:?}"),
    }
}
