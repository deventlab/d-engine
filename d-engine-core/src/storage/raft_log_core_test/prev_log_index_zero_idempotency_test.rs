//! Tests for `filter_out_conflicts_and_append` when `prev_log_index == 0`.
//!
//! Background: `prev_log_index == 0` is Raft's sentinel for "nothing before the start of the
//! log" — Rule 2 (term-at-prev-index check) is trivially satisfied because there is no real
//! entry 0 to compare. That does NOT license skipping Rules 3/4: the receiver must still
//! compare incoming entries against whatever it already has, starting at index 1, and only
//! touch the entries that actually conflict (differing term at the same index). A batch that
//! fully matches existing content must be a no-op — this is exactly what
//! `pipeline_overlap_test.rs` already proves for `prev_log_index > 0`.
//!
//! The current implementation special-cases `prev_log_index == 0` to unconditionally
//! `reset()` (wipe the whole log, `durable_index` included) before re-appending — regardless
//! of whether the incoming entries are a pure duplicate of what's already durably stored. A
//! leader that resends a `prev_log_index=0` probe (no backpressure, a retry, a reconnect) before
//! learning the follower already caught up will repeatedly destroy real, already-durable
//! progress. These tests are RED until `prev_log_index == 0` is folded into the same
//! overlap/conflict comparison used for `prev_log_index > 0`.

use crate::storage::raft_log::RaftLog;
use crate::test_utils::RaftLogCoreTestContext;
use d_engine_proto::common::Entry;
use std::time::Duration;

fn ctx(name: &str) -> RaftLogCoreTestContext {
    RaftLogCoreTestContext::new(name)
}

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

/// A genuinely fresh follower (no prior `append_entries` calls at all) receiving its very
/// first `prev_log_index=0` probe must accept and append normally.
///
/// # Why this needs its own test, not just implicit coverage
/// The other tests in this file all pre-populate the log before calling
/// `filter_out_conflicts_and_append`, so none of them exercise the case the
/// `is_virtual_log_start` guard exists to protect: without it, `entry_term(0)` returns `None`
/// unconditionally (there is no real entry 0 to look up), so `entry_term(0) != Some(0)` would
/// be true and this — the single most basic, legitimate case — would be wrongly rejected as a
/// conflict. This test pins that guard directly.
///
/// # Expected (holds both before and after the fix — this is a regression guard for the
/// `is_virtual_log_start` skip, not a RED/GREEN discriminator for the reset() removal)
#[tokio::test]
async fn test_filter_conflicts_zero_prev_on_genuinely_empty_log_appends_all() {
    let ctx = ctx("zero_prev_genuinely_empty_log_appends_all");
    assert_eq!(
        ctx.raft_log.last_entry_id(),
        0,
        "precondition: log must be untouched"
    );

    // Act: the very first AppendEntries this follower ever receives.
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(
            0,
            0,
            vec![
                entry(1, 1),
                entry(2, 1),
                entry(3, 1),
                entry(4, 1),
                entry(5, 1),
            ],
        )
        .await
        .unwrap();

    assert_eq!(result.unwrap().index, 5);
    assert_eq!(ctx.raft_log.last_entry_id(), 5);
    for i in 1u64..=5 {
        assert_eq!(ctx.raft_log.entry(i).unwrap().unwrap().term, 1);
    }
}

/// A leader resending `prev_log_index=0` with entries the follower already has — durably —
/// must be a no-op. This is the exact T4/T5 scenario from the #446 investigation: the
/// follower's first response to a `prev_log_index=0` probe is withheld pending its own
/// `durable_index` catching up (RPO=0); if the leader resends the identical probe before that
/// withheld ACK is released, the follower must not throw away the progress it already made.
///
/// # Expected (RED until fixed)
/// `durable_index()` and `last_entry_id()` stay at 5 — the duplicate probe changes nothing.
#[tokio::test]
async fn test_filter_conflicts_zero_prev_duplicate_resend_preserves_durable_index() {
    let mut ctx = ctx("zero_prev_duplicate_preserves_durable_index");

    // Arrange: follower already durably has [1..5], all term=1 — simulating a prior
    // `prev_log_index=0` probe that succeeded and finished fsyncing.
    for i in 1u64..=5 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    ctx.drain_fsync_completions();
    assert_eq!(
        ctx.raft_log.durable_index(),
        5,
        "precondition: [1..5] must be durable"
    );

    // Act: leader resends the identical prev_log_index=0 probe — same entries, same terms.
    // This is what happens when the leader's next_index[peer] never advanced (the leader
    // hasn't processed a response yet), not a genuinely new peer.
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(
            0,
            0,
            vec![
                entry(1, 1),
                entry(2, 1),
                entry(3, 1),
                entry(4, 1),
                entry(5, 1),
            ],
        )
        .await
        .unwrap();

    // Assert: no-op — durable progress must survive a duplicate zero-prev probe.
    assert_eq!(result.unwrap().index, 5);
    assert_eq!(
        ctx.raft_log.last_entry_id(),
        5,
        "last_entry_id must be unchanged"
    );
    assert_eq!(
        ctx.raft_log.durable_index(),
        5,
        "a duplicate prev_log_index=0 resend must not regress durable_index — this is what \
         re-arms the withheld-ACK deadlock (RPO=0 withhold never resolves once durable_index \
         is wiped out from under it)"
    );
    for i in 1u64..=5 {
        assert_eq!(
            ctx.raft_log.entry(i).unwrap().unwrap().term,
            1,
            "index={i} must not be touched by a duplicate zero-prev probe"
        );
    }
}

/// A `prev_log_index=0` batch that overlaps existing content but also carries genuinely new
/// entries beyond it must append only the new tail — mirrors
/// `pipeline_overlap_test::test_filter_conflicts_pipeline_overlap_no_truncation`, anchored at
/// prev=0 instead of prev>0, to prove the same comparison logic applies uniformly regardless
/// of which branch computed `prev_log_index`.
///
/// # Expected (RED until fixed)
/// [1..5] untouched, [6,7] appended.
#[tokio::test]
async fn test_filter_conflicts_zero_prev_overlap_appends_only_new_tail() {
    let ctx = ctx("zero_prev_overlap_appends_only_new_tail");

    // Arrange: follower has [1..5], term=1 (not necessarily durable yet — overlap detection
    // must work purely off in-memory content, independent of durability).
    for i in 1u64..=5 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    assert_eq!(ctx.raft_log.last_entry_id(), 5);

    // Act: leader sends prev_log_index=0 with [1..7] — [1..5] match, [6,7] are new.
    let new_entries: Vec<_> = (1u64..=7).map(|i| entry(i, 1)).collect();
    let result = ctx.raft_log.filter_out_conflicts_and_append(0, 0, new_entries).await.unwrap();

    // Assert: existing [1..5] untouched, new tail [6,7] appended.
    assert_eq!(result.unwrap().index, 7);
    assert_eq!(ctx.raft_log.last_entry_id(), 7);
    for i in 1u64..=5 {
        assert_eq!(
            ctx.raft_log.entry(i).unwrap().unwrap().term,
            1,
            "index={i} was already present and must not be truncated"
        );
    }
    assert_eq!(ctx.raft_log.entry(6).unwrap().unwrap().term, 1);
    assert_eq!(ctx.raft_log.entry(7).unwrap().unwrap().term, 1);
}

/// A genuine conflict at index 1 (different term than what the follower already has) must
/// still truncate and replace — proves the fix is a precise reuse of the existing
/// overlap/conflict comparison, not "prev_log_index=0 always becomes a no-op."
///
/// # Scenario
/// Follower has [1..5] term=1 (stale, from a since-superseded leader). A new leader with no
/// prior knowledge of this follower (or after a purge/snapshot boundary reset) sends
/// prev_log_index=0 with [1..3] all term=2 — a real conflict at index=1.
///
/// # Expected (should already hold both before and after the fix — this is the control case)
/// [1..3] replaced with term=2; nothing beyond index=3 survives.
#[tokio::test]
async fn test_filter_conflicts_zero_prev_real_conflict_truncates_and_replaces() {
    let ctx = ctx("zero_prev_real_conflict_truncates_and_replaces");

    // Arrange: follower has [1..5], all term=1.
    for i in 1u64..=5 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    assert_eq!(ctx.raft_log.last_entry_id(), 5);

    // Act: prev_log_index=0, entries=[1(t2), 2(t2), 3(t2)] — conflicts at index=1 immediately.
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(0, 0, vec![entry(1, 2), entry(2, 2), entry(3, 2)])
        .await
        .unwrap();

    // Assert: [1..3] replaced with term=2; stale [4,5] from the old leader must not survive.
    assert_eq!(result.unwrap().index, 3);
    assert_eq!(
        ctx.raft_log.last_entry_id(),
        3,
        "stale tail beyond the new leader's log must be gone"
    );
    for i in 1u64..=3 {
        assert_eq!(
            ctx.raft_log.entry(i).unwrap().unwrap().term,
            2,
            "index={i} must be term=2"
        );
    }
    assert!(
        ctx.raft_log.entry(4).unwrap().is_none(),
        "stale index=4 must not survive"
    );
    assert!(
        ctx.raft_log.entry(5).unwrap().is_none(),
        "stale index=5 must not survive"
    );
}
