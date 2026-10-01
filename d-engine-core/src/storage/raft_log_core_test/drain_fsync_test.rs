//! Tests for the drain-then-fsync IO architecture (Method C).
//!
//! These tests verify three core properties:
//!
//! 1. **Write/flush separation**: `durable_index` advances only after fsync, not
//!    after the write-only phase. This ensures crash-safety semantics are preserved.
//!
//! 2. **Batch efficiency**: rapid concurrent writes are covered by far fewer fsyncs
//!    than individual writes. The fsync execution time itself acts as the batch window.
//!
//! 3. **Explicit flush barrier**: `flush()` waits until all entries written before
//!    the call are durable, regardless of how many internal fsyncs occurred.

use crate::Error;
use crate::HardState;
use crate::MockLogStore;
use crate::MockMetaStore;
use crate::MockStorageEngine;
use crate::MockTypeConfig;
use crate::RaftLogCore;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio::time::{Duration, sleep};

use crate::RaftLog;
use crate::test_utils::RaftLogCoreTestContext;

/// Each `append_entries` persists the new tail and auto-submits it to `FsyncWorker`
/// for fsync — no timer, no explicit `flush()` required.
///
/// After `append_entries`, the new tail is written to the page cache and handed to
/// `FsyncWorker`, which fsyncs it. `durable_index` advances automatically once the
/// fsync completes and raft.rs drains the `FsyncCompleted` event.
#[tokio::test]
async fn test_writes_become_durable_via_io_thread() {
    let (mut ctx, flush_count) =
        RaftLogCoreTestContext::new_not_durable("writes_become_durable_via_io_thread");

    // Append 5 entries — each persists and submits the new tail to FsyncWorker.
    for i in 1u64..=5 {
        ctx.raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: None,
            }])
            .await
            .unwrap();
    }

    // Entries must be readable from SkipMap immediately (MemFirst invariant).
    assert_eq!(ctx.raft_log.last_entry_id(), 5);
    for i in 1u64..=5 {
        assert!(
            ctx.raft_log.entry(i).unwrap().is_some(),
            "entry {i} must be in memory"
        );
    }

    // Give FsyncWorker time to process the submit and fsync.
    sleep(Duration::from_millis(50)).await;
    ctx.drain_fsync_completions();

    // durable_index must have advanced via FsyncWorker auto-fsync (no explicit flush).
    assert_eq!(
        ctx.raft_log.durable_index(),
        5,
        "durable_index must advance via FsyncWorker fsync"
    );

    // FsyncWorker called log_store.flush() at least once.
    assert!(
        flush_count.load(Ordering::Relaxed) >= 1,
        "FsyncWorker must have called flush at least once"
    );
}

/// A single `append_entries` call with 100 entries batches into far fewer fsyncs
/// than individual writes.
///
/// `append_entries` persists the whole batch and submits one fsync regardless of how many
/// entries are in the batch. The new tail is written to page cache once, then fsynced via
/// `FsyncWorker`. The explicit `flush()` call
/// may race with the spawned fsync task: if it observes `durable_index` before the
/// first task completes, it submits a second round (coalesced by the coordinator).
/// N entries in one call → ≤2 fsyncs (not N), regardless of storage speed.
#[tokio::test]
async fn test_batch_append_produces_one_flush() {
    let (mut ctx, flush_count) = RaftLogCoreTestContext::new_not_durable("batch_append_one_flush");

    // All 100 entries in one append_entries call.
    let entries: Vec<Entry> = (1u64..=100)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: None,
        })
        .collect();
    ctx.raft_log.append_entries(entries).await.unwrap();

    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();

    assert_eq!(ctx.raft_log.durable_index(), 100);

    // One append → one fsync submit → far fewer fsyncs than entries.
    // With FsyncWorker the explicit flush() may add one extra round if it
    // races with the in-flight spawned task; the invariant is "not N flushes".
    let flushes = flush_count.load(Ordering::Relaxed);
    assert!(
        (1..=2).contains(&flushes),
        "one append_entries batch must produce ≤2 flushes, not {flushes}"
    );
}

/// `reset()` must not let a stale `pending_max` corrupt `durable_index`.
///
/// ## Background
/// `FsyncWorker::pending_max` holds the highest mark written to the OS page cache
/// but not yet fsynced. `reset()` calls `fence_reset()`, which zeroes it.
///
/// ## Original bug (fixed pre-#422, in the since-removed IO-thread design)
/// The IO thread's reset path wiped the on-disk log but did NOT zero
/// `pending_max`. On the next wakeup the IO thread would compute:
/// ```
/// pending_max = pending_max.max(new_end)   // stale 10 wins over new 3
/// fsync_and_advance(10)                    // advances durable_index to 10 — WRONG
/// ```
/// `durable_index (10) >= max_index (3)` would then make every subsequent `flush()`
/// return immediately without syncing the new entries — silently lost on a crash.
///
/// ## Update (2026-07-19): now structurally unreachable
/// A failed fsync now poisons the log permanently, so writes after Phase 2
/// are rejected outright — the corruption repro path no longer exists.
/// Assertions updated to check for that rejection instead.
#[tokio::test]
async fn test_pending_max_zeroed_on_reset_preventing_durable_index_corruption() {
    let storage = Arc::new(MockStorageEngine::not_durable_first_flush_fails(
        "pending_max_zeroed_on_reset".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    std::thread::sleep(Duration::from_millis(10)); // let the spawned fsync task start

    // Phase 1: append 10 entries.
    // FsyncWorker persists then fsyncs: persist succeeds, fsync FAILS (first call).
    // Failed fsync leaves pending_max = 10 (not zeroed — only success path zeros
    // it) AND now poisons the log permanently (see FsyncWorker).
    let entries: Vec<Entry> = (1u64..=10)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: None,
        })
        .collect();
    raft_log.append_entries(entries).await.unwrap();
    // Wait for FsyncWorker to process the submit (persist ok, fsync fails).
    sleep(Duration::from_millis(20)).await;
    assert!(
        raft_log.is_poisoned(),
        "the fsync failure above must have poisoned the log"
    );

    // Phase 2: reset — disk wiped. Still permitted (reset doesn't promise
    // durability), but does NOT clear poisoned (see test_poisoned_survives_reset).
    raft_log.reset().await.unwrap();
    assert!(
        raft_log.is_poisoned(),
        "poisoned must survive reset — this is what closes off the original bug"
    );

    // Phase 3: attempt to append 3 new entries starting from index 1 — this is
    // the exact sequence that used to reproduce the durable_index corruption.
    // It must now be rejected outright, never reaching FsyncWorker at all.
    let new_entries: Vec<Entry> = (1u64..=3)
        .map(|i| Entry {
            index: i,
            term: 2,
            payload: None,
        })
        .collect();
    let result = raft_log.append_entries(new_entries).await;
    assert!(
        result.is_err(),
        "a poisoned log must reject writes even after reset — the original \
         'stale pending_max causes silent data loss' bug can no longer be \
         reached because there is no post-poisoning write path left to corrupt"
    );

    raft_log.close().await;
}

/// flush() acts as a strict durability barrier: all entries appended before the
/// flush() call must be durable when flush() returns, regardless of internal batching.
#[tokio::test]
async fn test_flush_is_strict_durability_barrier() {
    let (mut ctx, _flush_count) =
        RaftLogCoreTestContext::new_not_durable("flush_durability_barrier");

    // First batch.
    for i in 1u64..=20 {
        ctx.raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: None,
            }])
            .await
            .unwrap();
    }
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();
    assert_eq!(
        ctx.raft_log.durable_index(),
        20,
        "first batch must be fully durable"
    );

    // Second batch after the barrier.
    for i in 21u64..=50 {
        ctx.raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: None,
            }])
            .await
            .unwrap();
    }
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();
    assert_eq!(
        ctx.raft_log.durable_index(),
        50,
        "second batch must be fully durable"
    );
}

/// flush() must return Err when the underlying fsync fails — not hang indefinitely.
///
/// ## Bug (pre-fix, in the since-removed IO-thread design)
/// `flush()` sent a fire-and-forget flush request, then registered a durable waiter.
/// If the IO thread's fsync failed, it logged the error and moved on — `durable_index`
/// never advanced, the waiter was never notified, and `flush()` blocked forever.
///
/// ## Invariant (#331)
/// `flush()` submits to `FsyncWorker` together with a reply oneshot. The worker sends
/// the fsync result (Ok or Err) directly back through it, so `flush()` always returns
/// within bounded time.
#[tokio::test]
async fn test_flush_propagates_io_error() {
    // Every flush() call on the underlying log store returns an error.
    // This covers both the auto-fsync triggered by append_entries and the explicit
    // flush() call, so timing between the two does not affect the outcome.
    let storage = Arc::new(MockStorageEngine::not_durable_always_failing_flush(
        "flush_propagates_io_error".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    std::thread::sleep(Duration::from_millis(10));

    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();

    // flush() must return Err, not hang.
    // A 2 s timeout distinguishes the fixed path (Err returned quickly) from
    // the pre-fix hang (WaitDurable waiter never notified).
    let result = timeout(Duration::from_secs(2), raft_log.flush()).await;

    match result {
        Err(_elapsed) => {
            panic!(
                "flush() hung: IO error was not propagated back to the caller (pre-fix behaviour)"
            );
        }
        Ok(Ok(())) => {
            panic!("flush() returned Ok but the fsync mock always fails");
        }
        Ok(Err(_e)) => {
            // Expected: flush() surfaces the fsync failure to the caller.
        }
    }

    raft_log.close().await;
}

/// Test: a real fsync failure poisons the log end-to-end (via the actual
/// `FsyncWorker` failure path, not by forcing the flag directly like
/// `test_poisoned_survives_reset` does), and poisoning survives a
/// subsequent `reset()` — writes afterward are rejected.
///
/// ## History
/// Originally written (pre-#422 fatal-poisoning fix) to prove a `flush()`
/// reply queued after one failed fsync round still resolves once a later
/// round succeeds — i.e. the log recovers from a transient failure. That
/// property no longer exists: one fsync failure is now permanent. Rewritten
/// to check for the new (correct) behavior instead.
#[tokio::test]
async fn test_fsync_failure_poisons_and_rejects_writes_after_reset() {
    let storage = Arc::new(MockStorageEngine::not_durable_first_flush_fails(
        "fsync_failure_poisons_and_rejects_writes_after_reset".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    std::thread::sleep(Duration::from_millis(10)); // let the spawned fsync task start

    // Trigger a real fsync failure via the actual FsyncWorker path.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await;
    assert!(
        raft_log.is_poisoned(),
        "a real fsync failure must poison the log"
    );

    raft_log.reset().await.unwrap();
    assert!(raft_log.is_poisoned(), "poisoned must survive reset");

    let result = raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 2,
            payload: None,
        }])
        .await;
    assert!(
        result.is_err(),
        "writes after reset must still be rejected while poisoned"
    );

    raft_log.close().await;
}

/// A `replace_range()` failure (conflict-resolution truncate+write) poisons
/// the log, same as persist_entries/fsync failures — disk state is uncertain
/// either way. Exercised end-to-end via `filter_out_conflicts_and_append`'s
/// real conflict-truncation path, not by forcing the flag directly.
#[tokio::test]
async fn test_replace_range_failure_poisons() {
    let storage = Arc::new(MockStorageEngine::not_durable_replace_range_fails(
        "replace_range_failure_poisons".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    std::thread::sleep(Duration::from_millis(10)); // let the spawned fsync task start

    // Base log: 3 entries at term 1.
    raft_log
        .append_entries(vec![
            Entry {
                index: 1,
                term: 1,
                payload: None,
            },
            Entry {
                index: 2,
                term: 1,
                payload: None,
            },
            Entry {
                index: 3,
                term: 1,
                payload: None,
            },
        ])
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await;

    // A new leader (term 2) overwrites from index 2 onward — a real conflict
    // that must truncate + replace, routing through `replace_range_and_submit`.
    let result = raft_log
        .filter_out_conflicts_and_append(
            1,
            1,
            vec![
                Entry {
                    index: 2,
                    term: 2,
                    payload: None,
                },
                Entry {
                    index: 3,
                    term: 2,
                    payload: None,
                },
            ],
        )
        .await;
    assert!(
        result.is_err(),
        "the failed replace_range() must surface as an error"
    );

    assert!(
        raft_log.is_poisoned(),
        "a replace_range() failure must poison the log — disk state is uncertain"
    );

    let append_result = raft_log
        .append_entries(vec![Entry {
            index: 4,
            term: 2,
            payload: None,
        }])
        .await;
    assert!(
        append_result.is_err(),
        "writes must be rejected once poisoned"
    );
}

/// A `purge()` failure poisons the log, same as the other storage-layer
/// failures, and now propagates as a real `Err` to the caller (unlike the
/// old `oneshot::Sender<()>` done-channel, `purge_and_advance` returns
/// `Result<()>` directly).
#[tokio::test]
async fn test_purge_failure_poisons() {
    let storage = Arc::new(MockStorageEngine::not_durable_purge_fails(
        "purge_failure_poisons".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    std::thread::sleep(Duration::from_millis(10));

    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await;

    let result = raft_log.purge_logs_up_to(LogId { term: 1, index: 1 }).await;
    assert!(
        result.is_err(),
        "a real purge() failure must now be propagated to the caller"
    );

    assert!(
        raft_log.is_poisoned(),
        "a purge() failure must poison the log"
    );

    let result = raft_log
        .append_entries(vec![Entry {
            index: 2,
            term: 1,
            payload: None,
        }])
        .await;
    assert!(result.is_err(), "writes must be rejected once poisoned");
}

/// A `reset()` failure poisons the log, same as the other storage-layer
/// failures.
#[tokio::test]
async fn test_reset_failure_poisons() {
    let storage = Arc::new(MockStorageEngine::not_durable_reset_fails(
        "reset_failure_poisons".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    std::thread::sleep(Duration::from_millis(10));

    let result = raft_log.reset().await;
    assert!(
        result.is_err(),
        "a failed reset() must surface as an error to the caller"
    );

    assert!(
        raft_log.is_poisoned(),
        "a reset() failure must poison the log"
    );

    let append_result = raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await;
    assert!(
        append_result.is_err(),
        "writes must be rejected once poisoned"
    );
}

/// A `save_hard_state()` failure (persisting current_term/voted_for) poisons
/// the log — this is the Election Safety gap the expert flagged as highest
/// priority: a silently-failed vote write, if not caught, lets a restarted
/// node believe it never voted this term and cast a second vote, breaking
/// "at most one leader per term."
#[tokio::test]
async fn test_save_hard_state_failure_poisons() {
    let storage = Arc::new(MockStorageEngine::not_durable_save_hard_state_fails(
        "save_hard_state_failure_poisons".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    std::thread::sleep(Duration::from_millis(10));

    let result = raft_log.save_hard_state(&HardState {
        current_term: 1,
        voted_for: None,
    });
    assert!(
        result.is_err(),
        "a failed save_hard_state() must surface as an error to the caller"
    );

    assert!(
        raft_log.is_poisoned(),
        "a save_hard_state() failure must poison the log"
    );

    let append_result = raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await;
    assert!(
        append_result.is_err(),
        "writes must be rejected once poisoned"
    );
}

/// Once already poisoned, `save_hard_state()` must be rejected immediately
/// without ever calling `meta_store.save_hard_state()`.
#[tokio::test]
async fn test_poisoned_rejects_save_hard_state() {
    let storage = Arc::new(MockStorageEngine::with_id(
        "poisoned_rejects_save_hard_state".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    std::thread::sleep(Duration::from_millis(10));

    raft_log.poisoned.store(true, Ordering::SeqCst);

    let result = raft_log.save_hard_state(&HardState {
        current_term: 1,
        voted_for: None,
    });
    match result {
        Err(Error::Fatal(msg)) => assert!(
            msg.contains("poisoned"),
            "expected the poisoned short-circuit to fire before save_hard_state() \
             was ever called, got: {msg}"
        ),
        other => panic!("expected Err(Fatal(\"...poisoned...\")), got: {other:?}"),
    }
}

// ============================================================================
// Gap fix: run_storage_tasks now checks is_poisoned() before executing
// ReplaceRange/Purge/Reset, instead of only checking it in run_batch_turn's
// drain loop (which missed the direct-dispatch path in batch_processor's
// top-level select, and the "just poisoned mid-turn" race).
// ============================================================================

/// Once already poisoned, `ReplaceRange` must be rejected immediately
/// without ever calling `log_store.replace_range()`. Proven by checking the
/// error message: if the underlying mock's own failure ("simulated
/// replace_range failure") had been reached, the message would differ from
/// the immediate "raft log storage is poisoned" rejection.
#[tokio::test]
async fn test_poisoned_skips_replace_range() {
    let storage = Arc::new(MockStorageEngine::not_durable_replace_range_fails(
        "poisoned_skips_replace_range".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    std::thread::sleep(Duration::from_millis(10));

    // Base log, written while still healthy.
    raft_log
        .append_entries(vec![
            Entry {
                index: 1,
                term: 1,
                payload: None,
            },
            Entry {
                index: 2,
                term: 1,
                payload: None,
            },
            Entry {
                index: 3,
                term: 1,
                payload: None,
            },
        ])
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await;

    raft_log.poisoned.store(true, Ordering::SeqCst);

    let result = raft_log
        .filter_out_conflicts_and_append(
            1,
            1,
            vec![
                Entry {
                    index: 2,
                    term: 2,
                    payload: None,
                },
                Entry {
                    index: 3,
                    term: 2,
                    payload: None,
                },
            ],
        )
        .await;

    match result {
        Err(Error::Fatal(msg)) => assert!(
            msg.contains("poisoned"),
            "expected the poisoned short-circuit to fire before replace_range() \
             was ever called, got: {msg}"
        ),
        other => panic!("expected Err(Fatal(\"...poisoned...\")), got: {other:?}"),
    }
}

/// Once already poisoned, `Reset` is deliberately NOT short-circuited —
/// unlike `ReplaceRange`/`Purge`, it makes no new durability promise (it's a
/// clean wipe, not a write the cluster will rely on), so it's allowed to run
/// even when poisoned, letting the node reach a known-clean state before it
/// exits. This test proves `reset()` actually executes (reaches
/// `log_store.reset()`) instead of being rejected — using a mock whose
/// `reset()` always succeeds, so the call completing with `Ok(())` is proof
/// it wasn't skipped.
#[tokio::test]
async fn test_poisoned_does_not_skip_reset() {
    let storage = Arc::new(MockStorageEngine::with_id(
        "poisoned_does_not_skip_reset".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    std::thread::sleep(Duration::from_millis(10));

    raft_log.poisoned.store(true, Ordering::SeqCst);

    let result = raft_log.reset().await;
    assert!(
        result.is_ok(),
        "Reset must not be short-circuited by poisoned — got: {result:?}"
    );
    assert!(
        raft_log.is_poisoned(),
        "poisoned must remain true — Reset succeeding doesn't clear it"
    );
}

/// Once already poisoned, `Purge` must be rejected immediately without ever
/// calling `log_store.purge()`. Proven by checking the error message: if the
/// underlying mock's own failure had been reached, the message would differ
/// from the immediate "raft log storage is poisoned" rejection — the same
/// pattern `test_poisoned_skips_replace_range` uses. `purge_and_advance` now
/// returns `Result<()>` directly, so this is no longer just a weak check.
#[tokio::test]
async fn test_poisoned_skips_purge() {
    let storage = Arc::new(MockStorageEngine::not_durable_purge_fails(
        "poisoned_skips_purge".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    std::thread::sleep(Duration::from_millis(10));

    // try_advance_durable_index() clamps against memory_max_index — simulate
    // a log that already has the entry this test purges up to.
    raft_log.set_memory_max_index_for_test(1);
    raft_log.poisoned.store(true, Ordering::SeqCst);

    let result = raft_log.purge_logs_up_to(LogId { term: 1, index: 1 }).await;
    match result {
        Err(Error::Fatal(msg)) => assert!(
            msg.contains("poisoned"),
            "expected the poisoned short-circuit to fire before purge() was \
             ever called, got: {msg}"
        ),
        other => panic!("expected Err(Fatal(\"...poisoned...\")), got: {other:?}"),
    }
    assert!(raft_log.is_poisoned(), "poisoned must remain true");
}

// ============================================================================
// Poisoned-state tests (#422 follow-up: fsync/persist failure must be fatal,
// not silently retried — see decision discussion 2026-07-19)
// ============================================================================

/// A freshly constructed log is never born poisoned.
///
/// Guards against a constructor regression (e.g. a copy-paste default flip)
/// that would make every node refuse writes from the very first call.
#[tokio::test]
async fn test_new_raft_log_core_starts_unpoisoned() {
    let (storage, _flush_call_count) =
        MockStorageEngine::not_durable("new_raft_log_core_starts_unpoisoned".into());
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), None, 5000);
    assert!(
        !raft_log.is_poisoned(),
        "A freshly constructed log is never born poisoned"
    );
}

/// Once poisoned, the state survives `reset()` — it must NOT be cleared by
/// `reset_internal()` / `FsyncWorker::fence_reset()`.
///
/// Why this matters: `reset()` is also invoked mid-flight for legitimate
/// reasons (snapshot install, log conflict rewind). If poisoned were treated
/// as just another piece of "current generation" state and wiped on reset,
/// a node with an unconfirmed/corrupted WAL could silently resume accepting
/// writes right after a snapshot install — exactly the silent-continue bug
/// this whole fix exists to close.
#[tokio::test]
async fn test_poisoned_survives_reset() {
    let storage = MockStorageEngine::with_id("poisoned_survives_reset".into());
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), None, 5000);

    raft_log.poisoned.store(true, Ordering::SeqCst);
    assert!(raft_log.reset().await.is_ok());
    assert!(
        raft_log.is_poisoned(),
        "reset shoud not cleaned poisoned flag"
    );
}

/// A `persist_entries()` (page-cache write) failure poisons the log, exactly
/// like an fsync failure does — these are two independent failure surfaces
/// (`persist_pending_range` vs `FsyncWorker::run_until_caught_up`) and both
/// must reach the same fatal outcome.
///
/// `persist_pending_range` now runs inline, awaited directly inside
/// `append_entries`, so the poison lands synchronously — this same call
/// returns `Err`, not just some later one.
///
/// Without this test, a bug that only wires up ONE of the two poisoning
/// paths (e.g. fsync failures poison correctly, but persist_entries
/// failures are still silently swallowed) would go unnoticed —
/// `test_poisoned_survives_reset` alone only exercises the fsync-failure
/// surface via `not_durable_first_flush_fails`.
#[tokio::test]
async fn test_persist_entries_failure_poisons() {
    let storage = MockStorageEngine::not_durable_first_persist_fails(
        "persist_entries_failure_poisons".into(),
    );
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), None, 5000);
    std::thread::sleep(Duration::from_millis(10));

    // persist_pending_range hits the mock's first (failing) persist_entries()
    // inline — this call itself must fail, not a later one.
    let result = raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await;
    assert!(
        result.is_err(),
        "the append_entries() call whose persist_entries fails must itself return Err"
    );

    assert!(
        raft_log.is_poisoned(),
        "a persist_entries() failure must poison the log, same as an fsync failure"
    );

    // Black-box confirmation from the caller's point of view: the log now
    // refuses further writes, not just an internal flag flip.
    let result = raft_log
        .append_entries(vec![Entry {
            index: 2,
            term: 1,
            payload: None,
        }])
        .await;
    assert!(
        result.is_err(),
        "append_entries() must reject writes once poisoned"
    );
}

/// If `notify_fatal`'s underlying channel is already closed when a failure
/// happens, the node must not fail *silently* — poisoned must still end up
/// `true`, and the failure must be visible somewhere (log line), even though
/// nothing can drive `raft.rs::run()` to exit via this specific event.
///
/// This test intentionally does not assert an exit or a state transition —
/// there isn't one to observe from here. It only pins down: (1) poisoning
/// itself is independent of whether the notification channel is alive, and
/// (2) the failure-to-deliver path is not a silent no-op.
#[tokio::test]
async fn test_notify_fatal_channel_closed_still_poisons_and_logs() {
    // tracing-test's #[traced_test] only captures events on this test's own
    // thread — the fsync failure and its log line happen on FsyncWorker's
    // own execution thread, so a cross-thread-capable global subscriber is
    // needed instead (see test_utils::log_capture).
    let logs = crate::test_utils::capture_logs_globally();

    let storage = MockStorageEngine::not_durable_first_flush_fails(
        "notify_fatal_channel_closed_still_poisons_and_logs".into(),
    );

    // Build the InternalEvent channel but drop the receiver immediately —
    // by the time notify_fatal() runs, log_flush_tx.send() hits a closed
    // channel and returns Err.
    let (tx, rx) = mpsc::unbounded_channel();
    drop(rx);

    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), Some(tx), 5000);

    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await; // let the spawned fsync task hit the fsync failure

    assert!(
        raft_log.is_poisoned(),
        "poisoning must not depend on whether the FatalError channel is still alive"
    );
    assert!(
        crate::test_utils::logs_contain_globally(&logs, "FatalError delivery failed"),
        "a channel-closed failure to notify must still be observable via logs, \
         not a silent no-op — see notify_fatal()'s error! call"
    );
}

/// Efficiency: the persist scan must start from its own page-cache
/// frontier, not from `durable_index`. Since #446 `durable_index` only advances
/// after an `FsyncCompleted` round-trips through raft.rs's event loop; under
/// load it lags far behind what has already been written. If the scan
/// restarted from `durable_index + 1` on every wakeup, each of N appends would
/// re-scan and re-`persist_entries` the whole not-yet-durable window — O(N^2)
/// total work.
///
/// This test pins `durable_index` at 0 (no `log_flush_tx`, so no
/// `FsyncCompleted` is ever consumed) and appends N entries one at a time. The
/// total number of entries handed to `persist_entries` across all calls must
/// stay ~N, not ~N^2/2.
#[tokio::test]
async fn test_persist_scan_tracks_frontier_not_stuck_durable_index() {
    let persisted_total = Arc::new(AtomicU64::new(0));
    let persisted_total_c = persisted_total.clone();

    let mut log_store = MockLogStore::new();
    log_store.expect_last_index().returning(|| 0);
    log_store.expect_persist_entries().returning(move |entries| {
        persisted_total_c.fetch_add(entries.len() as u64, Ordering::Relaxed);
        Ok(())
    });
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_get_entries().returning(|_| Ok(vec![]));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store.expect_replace_range().returning(|from, new_entries| {
        Ok(new_entries.last().map(|e| e.index).unwrap_or(from.saturating_sub(1)))
    });
    log_store.expect_is_write_durable().returning(|| false);
    log_store.expect_flush().returning(|| Ok(()));
    log_store.expect_flush_async().returning(|| Ok(()));

    let mut meta_store = MockMetaStore::new();
    meta_store.expect_save_hard_state().returning(|_| Ok(()));
    meta_store.expect_load_hard_state().returning(|| Ok(None));
    meta_store.expect_flush().returning(|| Ok(()));
    meta_store.expect_flush_async().returning(|| Ok(()));

    let storage = Arc::new(MockStorageEngine::from(log_store, meta_store));
    // No log_flush_tx: FsyncCompleted is never consumed, so durable_index
    // stays pinned at 0 for the whole test.
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);

    const N: u64 = 100;
    for i in 1..=N {
        raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: None,
            }])
            .await
            .unwrap();
        // flush() forces the persist up to memory_max_index right
        // now, so the scan boundary is exercised once per append — deterministic,
        // no sleeps.
        raft_log.flush().await.unwrap();
    }

    assert_eq!(
        raft_log.durable_index(),
        0,
        "durable_index must stay stuck for this test to be meaningful"
    );
    let total = persisted_total.load(Ordering::Relaxed);
    assert!(
        total < 3 * N,
        "persist_entries received {total} entries for {N} appends; a frontier-tracking \
         scan is ~{N}, a durable_index-relative scan would be ~{} (O(N^2))",
        N * (N + 1) / 2
    );
}

/// Cold start: after a restart, `durable_index` starts at the disk length and
/// the persist frontier must start *past* it. The first write's
/// persist scan begins at `durable_index + 1` — an already-durable entry on
/// disk must never be handed back to `persist_entries`.
///
/// Guards the frontier initialization (`= durable_index`, scans use `+ 1`).
#[tokio::test]
async fn test_cold_start_persist_frontier_starts_past_durable_index() {
    let persist_calls: Arc<Mutex<Vec<Vec<u64>>>> = Arc::new(Mutex::new(Vec::new()));

    let mut log_store = MockLogStore::new();
    log_store.expect_last_index().returning(|| 5); // disk already holds 1..=5
    log_store.expect_get_entries().returning(|range| {
        Ok(range
            .map(|i| Entry {
                index: i,
                term: 1,
                payload: None,
            })
            .collect())
    });
    {
        let calls = persist_calls.clone();
        log_store.expect_persist_entries().returning(move |entries| {
            calls.lock().unwrap().push(entries.iter().map(|e| e.index).collect());
            Ok(())
        });
    }
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store
        .expect_replace_range()
        .returning(|from, e| Ok(e.last().map(|x| x.index).unwrap_or(from.saturating_sub(1))));
    log_store.expect_is_write_durable().returning(|| false);
    log_store.expect_flush().returning(|| Ok(()));
    log_store.expect_flush_async().returning(|| Ok(()));

    let mut meta_store = MockMetaStore::new();
    meta_store.expect_save_hard_state().returning(|_| Ok(()));
    meta_store.expect_load_hard_state().returning(|| Ok(None));
    meta_store.expect_flush().returning(|| Ok(()));
    meta_store.expect_flush_async().returning(|| Ok(()));

    let storage = Arc::new(MockStorageEngine::from(log_store, meta_store));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    std::thread::sleep(Duration::from_millis(10));

    assert_eq!(
        raft_log.durable_index(),
        5,
        "restart: disk length 5 is treated as durable"
    );

    // First write after restart. Its persist scan must start at 6.
    raft_log
        .append_entries(vec![Entry {
            index: 6,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let calls = persist_calls.lock().unwrap().clone();
    assert!(!calls.is_empty(), "entry 6 must have been persisted");
    assert!(
        calls.iter().flatten().all(|&idx| idx >= 6),
        "cold start: the first persist must scan from durable_index+1 (6), never \
         re-scan already-durable entry 5. Got: {calls:?}"
    );
}
