//! `RaftLog::truncate_from` drops every entry from a given index on and returns only once the
//! removal is on disk.
//!
//! A follower calls it before installing a leader's snapshot whose boundary disagrees with the
//! local log: entries after that boundary come from a history the leader does not share and were
//! never committed, so they must go — and must not come back after a restart (#400).

use std::sync::atomic::Ordering;

use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::RaftLogCoreTestContext;

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

/// Entries from `from` on disappear; everything before it stays untouched.
///
/// Scenario: a follower holds 1..12 from term 2 and must drop 10..12.
#[tokio::test]
async fn test_truncate_from_removes_the_suffix_and_keeps_the_prefix() {
    let ctx = RaftLogCoreTestContext::new("truncate_from_removes_the_suffix");
    ctx.append_entries(1, 12, 2).await;

    ctx.raft_log.truncate_from(10).await.unwrap();

    assert_eq!(ctx.raft_log.last_entry_id(), 9);
    assert_eq!(
        ctx.raft_log.last_log_id(),
        Some(LogId { index: 9, term: 2 })
    );
    for index in 10..=12 {
        assert_eq!(
            ctx.raft_log.entry(index).unwrap(),
            None,
            "entry {index} must be gone"
        );
        assert_eq!(
            ctx.raft_log.entry_term(index),
            None,
            "term of {index} must be gone"
        );
    }
    for index in 1..=9 {
        assert_eq!(
            ctx.raft_log.entry_term(index),
            Some(2),
            "entry {index} must be kept"
        );
    }
}

/// The removal is on disk when the call returns: it syncs before returning, and a restart does
/// not bring the removed entries back.
///
/// Without the sync, a power loss right after the call could resurrect 10..12, and the follower
/// would again report a last log position from a history the leader does not share.
#[tokio::test]
async fn test_truncate_from_is_durable_on_return_and_survives_restart() {
    let (mut ctx, flush_count) =
        RaftLogCoreTestContext::new_with_flush_counter("truncate_from_is_durable_on_return");
    ctx.append_entries(1, 12, 2).await;
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();
    let syncs_before = flush_count.load(Ordering::Relaxed);

    ctx.raft_log.truncate_from(10).await.unwrap();

    assert!(
        flush_count.load(Ordering::Relaxed) > syncs_before,
        "truncate_from must sync the removal before it returns"
    );
    let restarted = ctx.recover_from_crash();
    assert_eq!(
        restarted.raft_log.last_log_id(),
        Some(LogId { index: 9, term: 2 })
    );
    assert_eq!(restarted.raft_log.entry(10).unwrap(), None);
}

/// After the removal, the freed indexes can be filled by a newer term, and the per-term index
/// bookkeeping follows: the old term ends at 9, the new one starts at 10.
#[tokio::test]
async fn test_truncate_from_lets_a_newer_term_reuse_the_indexes() {
    let ctx = RaftLogCoreTestContext::new("truncate_from_lets_a_newer_term_reuse");
    ctx.append_entries(1, 12, 2).await;

    ctx.raft_log.truncate_from(10).await.unwrap();
    ctx.raft_log.append_entries(vec![entry(10, 4), entry(11, 4)]).await.unwrap();

    assert_eq!(ctx.raft_log.entry_term(10), Some(4));
    assert_eq!(ctx.raft_log.entry_term(11), Some(4));
    assert_eq!(
        ctx.raft_log.last_log_id(),
        Some(LogId { index: 11, term: 4 })
    );
    assert_eq!(ctx.raft_log.last_index_for_term(2), Some(9));
    assert_eq!(ctx.raft_log.first_index_for_term(4), Some(10));
}

/// `durable_index` never claims more than the log holds: after dropping 10..12 it must not still
/// say 12 is durable.
#[tokio::test]
async fn test_truncate_from_never_reports_removed_entries_as_durable() {
    let (mut ctx, _flush_count) = RaftLogCoreTestContext::new_with_flush_counter(
        "truncate_from_never_reports_removed_durable",
    );
    ctx.append_entries(1, 12, 2).await;
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();
    assert_eq!(
        ctx.raft_log.durable_index(),
        12,
        "precondition: 1..12 durable"
    );

    ctx.raft_log.truncate_from(10).await.unwrap();
    ctx.drain_fsync_completions();

    assert!(
        ctx.raft_log.durable_index() <= 9,
        "durable_index must not cover removed entries, got {}",
        ctx.raft_log.durable_index()
    );
}

/// `from` past the last entry removes nothing and is not an error.
#[tokio::test]
async fn test_truncate_from_past_the_last_entry_changes_nothing() {
    let ctx = RaftLogCoreTestContext::new("truncate_from_past_the_last_entry");
    ctx.append_entries(1, 9, 2).await;

    ctx.raft_log.truncate_from(10).await.unwrap();
    ctx.raft_log.truncate_from(20).await.unwrap();

    assert_eq!(
        ctx.raft_log.last_log_id(),
        Some(LogId { index: 9, term: 2 })
    );
    for index in 1..=9 {
        assert_eq!(ctx.raft_log.entry_term(index), Some(2));
    }
}

/// Entries up to the purge boundary are committed and already covered by a snapshot. Asking to
/// truncate from there is a caller bug, so the call fails fatally and the log is left alone.
#[tokio::test]
async fn test_truncate_from_at_or_below_the_purge_boundary_is_fatal() {
    let ctx = RaftLogCoreTestContext::new("truncate_from_at_or_below_purge_boundary");
    ctx.append_entries(1, 12, 2).await;
    ctx.raft_log.purge_logs_up_to(LogId { index: 5, term: 2 }).await.unwrap();

    for from in [5, 3] {
        let err = ctx
            .raft_log
            .truncate_from(from)
            .await
            .expect_err("truncating at or below the purge boundary must fail");
        assert!(
            err.is_fatal(),
            "from={from}: expected a fatal error, got {err:?}"
        );
    }

    assert_eq!(
        ctx.raft_log.last_log_id(),
        Some(LogId { index: 12, term: 2 })
    );
    assert_eq!(
        ctx.raft_log.entry_term(5),
        Some(2),
        "the purge boundary must be kept"
    );
}

/// Truncating right after the purge boundary empties the log; the last log position then falls
/// back to the boundary, which the snapshot still covers.
#[tokio::test]
async fn test_truncate_from_right_after_the_purge_boundary_empties_the_log() {
    let ctx = RaftLogCoreTestContext::new("truncate_from_right_after_purge_boundary");
    ctx.append_entries(1, 12, 2).await;
    ctx.raft_log.purge_logs_up_to(LogId { index: 5, term: 2 }).await.unwrap();

    ctx.raft_log.truncate_from(6).await.unwrap();

    assert_eq!(ctx.raft_log.entry(6).unwrap(), None);
    assert_eq!(
        ctx.raft_log.last_log_id(),
        Some(LogId { index: 5, term: 2 })
    );
    assert_eq!(ctx.raft_log.entry_term(5), Some(2));
}
