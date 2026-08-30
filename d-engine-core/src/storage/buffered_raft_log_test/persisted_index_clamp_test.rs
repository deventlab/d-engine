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

use std::time::Duration;

use d_engine_proto::common::Entry;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
use crate::{FlushPolicy, PersistenceStrategy};

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
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
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
    assert_eq!(ctx.raft_log.last_entry_id(), 2, "log truncated and replaced down to [1, 2]");

    // Trigger a disk sync and give it time to complete.
    ctx.raft_log.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

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
