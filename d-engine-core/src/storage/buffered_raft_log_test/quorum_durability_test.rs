//! Quorum Durability Tests
//!
//! Verifies that `durable_index` (crash-safe) — not `last_entry_id` (in-memory) —
//! gates quorum and commit decisions. Fixed by ticket #329.

use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
use crate::{FlushPolicy, PersistenceStrategy};
use std::time::Duration;

/// Flush policy that never auto-flushes: threshold too high to reach, timer far in future.
/// Guarantees durable_index stays at 0 after append_entries, isolating the memory/disk split.
fn no_auto_flush_policy() -> FlushPolicy {
    FlushPolicy::Batch {
        threshold: 999_999,
        interval_ms: 999_999,
    }
}

// ── Quorum uses durable_index, not last_entry_id ──

/// Quorum must not advance when leader's entry is in memory but not yet crash-safe.
#[tokio::test]
async fn test_quorum_must_not_count_undurable_leader_entry() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        no_auto_flush_policy(),
        "test_quorum_no_undurable",
    );

    // Append entry 1 to memory only — durable_index stays 0
    ctx.append_entries(1, 1, 1).await;

    assert_eq!(ctx.raft_log.last_entry_id(), 1);
    assert_eq!(ctx.raft_log.durable_index(), 0);

    let new_commit = ctx.raft_log.calculate_majority_matched_index(
        1,
        0,
        vec![1], // one follower acked — would be quorum if leader counts itself
    );

    // CORRECT: leader's contribution is durable_index=0, not last_entry_id=1
    // majority cannot be reached until leader flushes
    assert_eq!(
        new_commit, None,
        "CORRECT: quorum must not be reached with leader durable_index=0"
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
            threshold: 1,
            interval_ms: 999_999, // only threshold trigger, no timer
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

/// DiskFirst must never diverge: every append is immediately durable.
#[tokio::test]
async fn test_last_entry_id_equals_durable_index_with_disk_first() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::DiskFirst,
        no_auto_flush_policy(), // irrelevant for DiskFirst but required by constructor
        "test_no_diverge_disk_first",
    );

    ctx.append_entries(1, 5, 1).await;

    let last = ctx.raft_log.last_entry_id();
    let durable = ctx.raft_log.durable_index();

    assert_eq!(last, 5);
    assert_eq!(
        durable, last,
        "DiskFirst: durable_index must equal last_entry_id after every append"
    );
}

// ── Follower must wait for durable_index before ACK ──

/// Follower ACK flow: after filter_out_conflicts_and_append + wait_durable,
/// durable_index must equal the appended index before any success response is sent.
///
/// threshold=1 ensures the batch_processor flushes immediately on first entry,
/// so wait_durable(1) returns as soon as the flush completes.
#[tokio::test]
async fn test_follower_must_wait_for_durable_before_ack() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 1, // flush fires on first entry
            interval_ms: 999_999,
        },
        "test_follower_must_wait_durable",
    );

    let log_id = ctx
        .raft_log
        .filter_out_conflicts_and_append(
            0,
            0,
            vec![d_engine_proto::common::Entry {
                index: 1,
                term: 1,
                payload: None,
            }],
        )
        .await
        .unwrap()
        .expect("append must return a log id");

    // Mirrors what replication_handler does before building the success response.
    ctx.raft_log.wait_durable(log_id.index).await.unwrap();

    // CORRECT: follower only ACKs after durable_index >= appended index
    assert_eq!(
        ctx.raft_log.durable_index(),
        1,
        "CORRECT: follower must only ack after durable_index >= appended index"
    );
}
