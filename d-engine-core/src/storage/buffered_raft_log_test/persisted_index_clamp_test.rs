//! `persisted_index` must never claim a follower has written more to its
//! storage engine than what its log actually contains right now.
//!
//! Scenario: a follower has replicated entries 1-10 from an old leader and
//! synchronously written them to its storage engine (page cache), but hasn't
//! fsynced yet. A new leader is elected, finds entries 2-10 don't match its
//! own history, and tells the follower to truncate everything from index=2
//! onward — the follower's real log now only has index=1. The new leader then
//! sends one brand-new entry that happens to land at index=2 again (different
//! content, new term).
//!
//! `persisted_index` only ever moves up (`fetch_max`), so without clamping it
//! on truncation, it would still remember "wrote up to 10" from before the
//! truncation — a stale high-water mark that the small index=2 write can't
//! pull back down. The next disk sync would then advertise `durable_index=10`
//! to the rest of the engine, even though the follower's log — and its
//! storage engine — genuinely only holds entries 1 and 2. A power loss at
//! that moment would prove the claim false: the follower reboots with only
//! [1, 2], not [1..=10], yet something upstream may already have acted on
//! "this follower is durable through 10" (e.g. deciding it's safe to purge
//! earlier log entries elsewhere in the cluster).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use d_engine_proto::common::Entry;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
use crate::{BufferedRaftLog, FlushPolicy, MockStorageEngine, MockTypeConfig, PersistenceConfig};

fn entry(
    index: u64,
    term: u64,
) -> Entry {
    Entry {
        index,
        term,
        payload: None,
    }
}

/// `durable_index()` must never exceed `last_entry_id()` — it must never
/// claim durability for an index that doesn't exist in the log anymore.
#[tokio::test]
async fn test_durable_index_never_exceeds_log_after_truncation_and_resync() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 60_000, // isolate from the safety-net timer
        },
        "durable_index_never_exceeds_log_after_truncation_and_resync",
    );

    // Old leader (term=1) replicates entries 1..=10. append_entries() persists
    // them to the storage engine synchronously, but nothing fsyncs them yet.
    ctx.append_entries(1, 10, 1).await;
    assert_eq!(ctx.raft_log.last_entry_id(), 10);
    assert_eq!(ctx.raft_log.durable_index(), 0, "nothing fsynced yet");

    // New leader (term=2) finds index=2 doesn't match its history (term=1
    // there, should be term=2) and truncates from index=2 onward, replacing
    // it with one brand-new entry — real log becomes just [1, 2]. This goes
    // through filter_out_conflicts_and_append's term-conflict slow path:
    // remove_range(2..=MAX) (the clamp under test fires here, since it drops
    // max_index from 10 down to 1) followed by inserting the new index=2.
    ctx.raft_log
        .filter_out_conflicts_and_append(1, 1, vec![entry(2, 2)])
        .await
        .unwrap();
    assert_eq!(
        ctx.raft_log.last_entry_id(),
        2,
        "log truncated and replaced down to [1, 2]"
    );

    // Trigger a disk sync and give it time to complete.
    ctx.raft_log.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    ctx.drain_fsync_completions();

    // The follower must never advertise durability for an index it doesn't
    // actually have. If persisted_index wasn't clamped down during the
    // truncation, this would report 10 here — a lie.
    assert!(
        ctx.raft_log.durable_index() <= ctx.raft_log.last_entry_id(),
        "durable_index ({}) must never exceed last_entry_id ({}) — it must not \
         claim durability for entries the truncation already discarded",
        ctx.raft_log.durable_index(),
        ctx.raft_log.last_entry_id()
    );
    assert_eq!(
        ctx.raft_log.durable_index(),
        2,
        "durable_index must reach the log's true end (2), not a stale pre-truncation watermark"
    );
}

/// Different ordering from the test above: there, `remove_range`'s clamp ran
/// *before* anything else touched `persisted_index`. Here, a `Persist` task
/// dispatched *before* the truncation is still stuck on the IO thread (write
/// not yet reached the storage engine) when the truncation's own clamp runs —
/// and only *afterward* does that stale `Persist` complete and call
/// `persisted_index.fetch_max(10, ..)` (the line under review in
/// `handle_write_cmd`'s `IOTask::Persist` arm), using an index from entries
/// the truncation already discarded. `fetch_max` only ever moves up, so if
/// this call isn't fenced the same way `advance_durable_and_notify` fences a
/// stale fsync (see `truncation_fsync_fence_test.rs`), it silently
/// resurrects the clamp remove_range just applied.
///
/// Scenario:
/// 1. Old leader (term=1) replicates entries 1..=10. `append_entries()`
///    inserts them into memory immediately, then blocks inside
///    `persist_entries()` on a gate — the write hasn't reached the storage
///    engine yet.
/// 2. New leader (term=2): index=2 conflicts. `filter_out_conflicts_and_append`
///    runs — `remove_range(2..=MAX)` (in-memory, synchronous, not routed
///    through the IO thread) executes and clamps immediately; the task then
///    blocks on the IO thread for its own queued `ReplaceRange`, which can't
///    run yet because the IO thread is still stuck on step 1's gate.
/// 3. Release the gate. The stale `Persist` for entries 1..=10 completes and
///    calls `persisted_index.fetch_max(10, ..)` — after the truncation's
///    clamp already ran, using entries that no longer exist. The queued
///    `ReplaceRange` runs next but does not re-clamp (its clamp already
///    fired once, in step 2, at truncation time).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_persisted_index_does_not_adopt_a_stale_persist_after_truncation() {
    let (storage, persist_gate) = MockStorageEngine::not_durable_gated_persist(
        "persisted_index_does_not_adopt_a_stale_persist_after_truncation".into(),
    );
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 5000,
        },
        Arc::new(storage),
    );
    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10));

    let entries: Vec<Entry> = (1..=10).map(|i| entry(i, 1)).collect();
    let append_task = {
        let raft_log = raft_log.clone();
        tokio::spawn(async move { raft_log.append_entries(entries).await })
    };
    // Let append_task reach the gate inside persist_entries().
    tokio::time::sleep(Duration::from_millis(50)).await;

    let truncate_task = {
        let raft_log = raft_log.clone();
        tokio::spawn(async move {
            raft_log.filter_out_conflicts_and_append(1, 1, vec![entry(2, 2)]).await
        })
    };
    // Let truncate_task run remove_range()'s synchronous clamp and reach
    // its own await point (queued behind the still-gated Persist).
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        raft_log.last_entry_id(),
        2,
        "remove_range()'s in-memory truncation must be visible immediately, \
         without waiting for the gated Persist or the queued ReplaceRange"
    );

    // Release the stale Persist — it completes and calls
    // persisted_index.fetch_max(10, ..) using now-discarded entries.
    persist_gate.send(()).expect("IO thread should still be waiting on the gate");
    append_task.await.unwrap().unwrap();
    truncate_task.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        raft_log.persisted_index.load(Ordering::Acquire) <= raft_log.last_entry_id(),
        "persisted_index ({}) must never exceed last_entry_id ({}) — the stale \
         Persist for entries 1..=10 must not be adopted after truncation shrank \
         the log to [1, 2]",
        raft_log.persisted_index.load(Ordering::Acquire),
        raft_log.last_entry_id()
    );
}
