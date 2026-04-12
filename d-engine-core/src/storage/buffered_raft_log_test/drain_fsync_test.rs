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

use std::sync::atomic::Ordering;

use d_engine_proto::common::Entry;
use tokio::time::{Duration, sleep};

use crate::test_utils::BufferedRaftLogTestContext;
use crate::{FlushPolicy, RaftLog};

/// The IO thread auto-fsyncs on each write_notify wakeup without any timer.
///
/// After `append_entries`, the IO thread is notified via `write_notify`, reads
/// pending entries from the SkipMap, and calls fsync. `durable_index` advances
/// automatically — no explicit `flush()` required.
#[tokio::test]
async fn test_writes_become_durable_via_io_thread() {
    let (ctx, flush_count) = BufferedRaftLogTestContext::new_not_durable(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 60_000,
        },
        "writes_become_durable_via_io_thread",
    );

    // Append 5 entries — each calls write_notify.notify_one().
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

    // Give IO thread time to process write_notify wakeup and fsync.
    sleep(Duration::from_millis(50)).await;

    // durable_index must have advanced via IO thread auto-fsync (no explicit flush).
    assert_eq!(
        ctx.raft_log.durable_index(),
        5,
        "durable_index must advance via IO thread drain-then-fsync"
    );

    // IO thread called log_store.flush() at least once.
    assert!(
        flush_count.load(Ordering::Relaxed) >= 1,
        "IO thread must have called flush at least once"
    );
}

/// A single `append_entries` call with 100 entries produces exactly one fsync.
///
/// `append_entries` calls `write_notify.notify_one()` once regardless of how many
/// entries are in the batch. The IO thread wakes once and calls fsync exactly once.
/// N entries in one call → 1 fsync, always, regardless of storage speed.
#[tokio::test]
async fn test_batch_append_produces_one_flush() {
    let (ctx, flush_count) = BufferedRaftLogTestContext::new_not_durable(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 60_000,
        },
        "batch_append_one_flush",
    );

    // All 100 entries in one append_entries call → one notify_one() → one fsync.
    let entries: Vec<Entry> = (1u64..=100)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: None,
        })
        .collect();
    ctx.raft_log.append_entries(entries).await.unwrap();

    ctx.raft_log.flush().await.unwrap();

    assert_eq!(ctx.raft_log.durable_index(), 100);

    // One notify_one() → IO thread wakes once → exactly one flush() call.
    let flushes = flush_count.load(Ordering::Relaxed);
    assert_eq!(
        flushes, 1,
        "one append_entries batch must produce exactly one flush (got {flushes})"
    );
}

/// `IOTask::Reset` must zero `pending_max`; stale value corrupts `durable_index`.
///
/// ## Background
/// `batch_processor` tracks `pending_max`: the highest log index written to the OS
/// page cache but not yet fsynced. After a successful `fsync_and_advance`, it is
/// zeroed (`pending_max = 0`). After a **failed** fsync, it is NOT zeroed — the
/// `else { pending_max = 0 }` branch is skipped.
///
/// ## Bug
/// `handle_non_write_cmd(IOTask::Reset)` wipes the on-disk log but does NOT zero
/// `pending_max`. On the next `write_notify` wakeup the IO thread computes:
/// ```
/// pending_max = pending_max.max(new_end)   // stale 10 wins over new 3
/// fsync_and_advance(10)                    // advances durable_index to 10 — WRONG
/// ```
/// Now `durable_index (10) >= max_index (3)`, so every subsequent `flush()` returns
/// immediately without syncing the new entries. They are silently lost on a crash.
///
/// ## Deterministic trigger
/// Making flush fail once leaves `pending_max` non-zero when `IOTask::Reset` arrives
/// — no concurrent-race needed. The safety-net timer is disabled (60 s) to ensure
/// only the `write_notify` path is exercised.
#[tokio::test]
async fn test_pending_max_zeroed_on_reset_preventing_durable_index_corruption() {
    use crate::{
        BufferedRaftLog, MockStorageEngine, MockTypeConfig, PersistenceConfig, PersistenceStrategy,
    };
    use std::sync::Arc;

    let storage = Arc::new(MockStorageEngine::not_durable_first_flush_fails(
        "pending_max_zeroed_on_reset".into(),
    ));
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            strategy: PersistenceStrategy::MemFirst,
            // Safety-net disabled: only write_notify triggers fsync.
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            max_buffered_entries: 1000,
        },
        storage,
    );
    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    // Phase 1: append 10 entries.
    // IO thread wakes on write_notify: persist succeeds, fsync FAILS (first call).
    // Failed fsync leaves pending_max = 10 (not zeroed — only success path zeros it).
    let entries: Vec<Entry> = (1u64..=10)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: None,
        })
        .collect();
    raft_log.append_entries(entries).await.unwrap();
    // Wait for IO thread to process write_notify (persist ok, fsync fails).
    sleep(Duration::from_millis(20)).await;

    // Phase 2: reset — disk wiped.
    // Bug: handle_non_write_cmd(IOTask::Reset) does NOT zero pending_max.
    // Fix: *pending_max = 0 is added in the Reset branch.
    raft_log.reset().await.unwrap();

    // Phase 3: append 3 new entries starting from index 1.
    let new_entries: Vec<Entry> = (1u64..=3)
        .map(|i| Entry {
            index: i,
            term: 2,
            payload: None,
        })
        .collect();
    raft_log.append_entries(new_entries).await.unwrap();
    raft_log.flush().await.unwrap();

    // Without the fix: pending_max = max(stale_10, 3) = 10
    //   → fsync_and_advance(10) → durable_index = 10  ← WRONG
    //   → subsequent flush() sees durable_index(10) >= max_index(3), returns early
    //   → new entries never actually fsynced (data loss)
    //
    // With the fix: pending_max = max(0, 3) = 3
    //   → fsync_and_advance(3)  → durable_index = 3   ← CORRECT
    assert_eq!(
        raft_log.durable_index(),
        3,
        "durable_index must be 3 (post-reset entries only); stale pending_max must be zeroed on Reset"
    );
    raft_log.close().await;
}

/// flush() acts as a strict durability barrier: all entries appended before the
/// flush() call must be durable when flush() returns, regardless of internal batching.
#[tokio::test]
async fn test_flush_is_strict_durability_barrier() {
    let (ctx, _flush_count) = BufferedRaftLogTestContext::new_not_durable(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 60_000,
        },
        "flush_durability_barrier",
    );

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
    assert_eq!(
        ctx.raft_log.durable_index(),
        50,
        "second batch must be fully durable"
    );
}

/// flush() must return Err when the underlying fsync fails — not hang indefinitely.
///
/// ## Bug (pre-fix)
/// `flush()` sends `IOTask::FlushNow` (fire-and-forget), then calls `wait_durable()`
/// which registers a `WaitDurable` waiter. If the IO thread's fsync fails, it logs the
/// error and moves on — `durable_index` never advances, the waiter is never notified,
/// and `flush()` blocks forever.
///
/// ## Fix (#331)
/// Replace `FlushNow` + `WaitDurable` with a single `IOTask::Flush(oneshot::Sender<Result<()>>)`.
/// The IO thread performs fsync and sends the result (Ok or Err) directly back to the
/// caller via the oneshot channel, so `flush()` always returns within bounded time.
#[tokio::test]
async fn test_flush_propagates_io_error() {
    use crate::{
        BufferedRaftLog, MockStorageEngine, MockTypeConfig, PersistenceConfig, PersistenceStrategy,
    };
    use std::sync::Arc;
    use tokio::time::timeout;

    // Every flush() call on the underlying log store returns an error.
    // This covers both the auto-fsync triggered by write_notify and the explicit
    // flush() call, so timing between the two does not affect the outcome.
    let storage = Arc::new(MockStorageEngine::not_durable_always_failing_flush(
        "flush_propagates_io_error".into(),
    ));
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            strategy: PersistenceStrategy::MemFirst,
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            max_buffered_entries: 1000,
        },
        storage,
    );
    let raft_log = raft_log.start(receiver, None);
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
