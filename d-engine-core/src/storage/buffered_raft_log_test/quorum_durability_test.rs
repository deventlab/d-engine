//! Quorum Durability Tests
//!
//! MemFirst design: leader contributes `last_entry_id` (in-memory) to quorum.
//! IO thread persistence is async and NOT on the commit critical path.
//!
//! Follower ACK path: followers ACK immediately after memory write (no `wait_durable`).
//! IO thread fsyncs asynchronously; crash safety is guaranteed by quorum, not per-follower durability.

use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
use crate::{FlushPolicy, PersistenceStrategy};
use std::time::Duration;

/// Flush policy with a far-future safety timer — IO thread only fsyncs on WriteNotify.
/// In current_thread test runtime, durable_index stays at 0 immediately after append_entries
/// because the IO thread task has no chance to run until the test yields.
fn no_auto_flush_policy() -> FlushPolicy {
    FlushPolicy::Batch {
        idle_flush_interval_ms: 999_999,
    }
}

// ── Leader quorum uses last_entry_id (in-memory), not durable_index ──

/// MemFirst: leader's quorum contribution is last_entry_id (in-memory), not durable_index.
///
/// Even when durable_index=0 (IO thread has not flushed), quorum must be satisfied
/// as soon as last_entry_id + follower ACKs form a majority. IO persistence is async
/// and must NOT block commit.
///
/// This test FAILS if calculate_majority_matched_index uses durable_index (the bug
/// introduced by fix #329 which incorrectly put IO thread latency on the commit
/// critical path, causing +617µs avg latency regression in 3-node embedded bench).
#[tokio::test]
async fn test_memfirst_quorum_uses_last_entry_id_not_durable_index() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        no_auto_flush_policy(), // durable_index stays 0 — IO thread won't run
        "test_memfirst_quorum_last_entry_id",
    );

    // Entry written to SkipMap (in memory). IO thread has not flushed yet.
    ctx.append_entries(1, 1, 1).await;

    assert_eq!(ctx.raft_log.last_entry_id(), 1);
    assert_eq!(ctx.raft_log.durable_index(), 0); // IO thread hasn't run

    let result = ctx.raft_log.calculate_majority_matched_index(
        1,
        0,
        vec![1], // one follower acked index=1; together with leader = majority of 3
    );

    // MemFirst: leader contributes last_entry_id=1.
    // quorum = [leader=1, follower=1] → majority of {leader, f1, f2} satisfied → Some(1).
    assert_eq!(
        result,
        Some(1),
        "MemFirst: quorum must use last_entry_id, not durable_index — IO must not block commit"
    );
}

/// Once the leader flushes, quorum calculation should succeed.
///
/// This test verifies the positive case: after flush, durable_index=1,
/// quorum should proceed normally.
#[tokio::test]
async fn test_quorum_succeeds_after_leader_flush() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            idle_flush_interval_ms: 999_999, // only threshold trigger, no timer
        },
        "test_quorum_after_flush",
    );

    // Append entry 1 — threshold=1 so flush fires immediately
    ctx.append_entries(1, 1, 1).await;

    // Wait for flush to complete
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(ctx.raft_log.last_entry_id(), 1);
    assert_eq!(
        ctx.raft_log.durable_index(),
        1,
        "durable_index must be 1 after threshold flush"
    );

    let new_commit = ctx.raft_log.calculate_majority_matched_index(
        1,
        0,
        vec![1], // follower acked
    );

    // Correct: durable_index=1 = last_entry_id=1, quorum should pass
    assert_eq!(
        new_commit,
        Some(1),
        "quorum must succeed after leader flush"
    );
}

// ── Bug 2: gap between last_entry_id and durable_index ──

/// Demonstrates that after append_entries with MemFirst + no-auto-flush,
/// last_entry_id and durable_index diverge.
///
/// This is the root condition enabling the bug: both values exist,
/// but quorum calculation only uses the unsafe one.
#[tokio::test]
async fn test_last_entry_id_diverges_from_durable_index_with_mem_first() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        no_auto_flush_policy(),
        "test_diverge_mem_first",
    );

    ctx.append_entries(1, 5, 1).await; // entries 1..=5, no flush

    assert_eq!(ctx.raft_log.last_entry_id(), 5, "memory index should be 5");
    assert_eq!(
        ctx.raft_log.durable_index(),
        0,
        "durable_index must remain 0: no flush has run"
    );
    // This gap (5 vs 0) is exactly what the quorum bug exploits.
}

/// After explicit flush, durable_index must equal last_entry_id.
#[tokio::test]
async fn test_durable_index_equals_last_entry_id_after_flush() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_no_diverge_after_flush",
    );

    ctx.append_entries(1, 5, 1).await;
    ctx.raft_log.flush().await.unwrap();

    let last = ctx.raft_log.last_entry_id();
    let durable = ctx.raft_log.durable_index();

    assert_eq!(last, 5);
    assert_eq!(
        durable, last,
        "durable_index must equal last_entry_id after flush"
    );
}
