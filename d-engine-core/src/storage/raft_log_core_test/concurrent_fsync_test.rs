//! Tests for the concurrent fsync pipeline introduced in #422.
//!
//! Covers three correctness dimensions:
//! - **Protocol**: Raft invariants must hold regardless of fsync timing
//! - **Logic**: `FsyncWorker` (submit / run_until_caught_up) / try_advance_durable_index contract
//! - **Concurrency**: Reset races, out-of-order completion, crash recovery

use crate::test_utils::drain_and_apply_fsync_completions;
use crate::test_utils::wait_for_durable_index;
use crate::{MockStorageEngine, MockTypeConfig, RaftLog, RaftLogCore};
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

// ── Protocol correctness ──────────────────────────────────────────────────────

/// `durable_index` must not advance before the physical fsync actually completes.
///
/// Use a MockLogStore with a controllable-delay flush(). While the delay is active,
/// assert `durable_index()` equals its pre-write value. After unblocking fsync,
/// assert `durable_index()` reaches the expected index.
///
/// This guards the core Level-3 contract: entries are only counted as durable
/// after fdatasync, not after page-cache write.
#[tokio::test]
async fn test_durable_index_not_advanced_before_fsync_completes() {
    // Gate closed: the first flush() call will block until we send () on `flush_gate`.
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "durable_index_not_advanced_before_fsync_completes".into(),
    );
    // A real log_flush_tx is required now: durable_index only advances when
    // something drains InternalEvent::FsyncCompleted and calls
    // try_advance_durable_index — see drain_and_apply_fsync_completions.
    let (log_flush_tx, mut log_flush_rx) = mpsc::unbounded_channel();
    let raft_log =
        RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), Some(log_flush_tx), 5000);

    let pre_write_durable_index = raft_log.durable_index();

    // append_entries persists inline, then hands the mark to FsyncWorker::submit(),
    // which spawns run_until_caught_up() on its own thread — log_store.flush()
    // blocks on flush_gate there.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();

    // Give the fsync thread time to reach the gated flush() call.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // While the gate is closed, durable_index must still equal
    // its pre-write value — fsync hasn't physically completed yet.
    assert_eq!(
        raft_log.durable_index(),
        pre_write_durable_index,
        "durable_index must not advance before fsync completes"
    );

    // Release the gate — flush() returns, notify_fsync_completed(1, 1) fires.
    flush_gate.send(()).unwrap();

    wait_for_durable_index(&raft_log, &mut log_flush_rx, 1, Duration::from_secs(5)).await;

    assert_eq!(
        raft_log.durable_index(),
        1,
        "durable_index must reach the expected index after fsync completes"
    );
}

/// `calculate_majority_matched_index` uses `durable_index` (fsync-confirmed), not the
/// in-memory `last_entry_id` — even when a follower already reports the index, the
/// leader's own contribution must not count toward quorum until it has itself fsynced.
///
/// Stall every flush() call via a MockLogStore barrier, append entries, then verify that
/// majority-matched calculation does NOT advance while fsync is stalled, and does advance
/// once fsync completes.
///
/// Regression guard: RPO=0 (#446) requires the leader's own copy to be durable before it
/// counts toward commit — if this ever reverts to using `last_entry_id`, this test will
/// catch it before it reaches production.
#[tokio::test]
async fn test_majority_matched_index_uses_durable_not_memory() {
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "majority_matched_index_uses_durable_not_memory".into(),
    );
    let (log_flush_tx, mut log_flush_rx) = mpsc::unbounded_channel();
    let raft_log =
        RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), Some(log_flush_tx), 5000);

    let pre_write_durable_index = raft_log.durable_index();
    let pre_last_entry_id = raft_log.last_entry_id();

    let entries = vec![
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
    ];
    let size = entries.len() as u64;
    raft_log.append_entries(entries).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        raft_log.durable_index(),
        pre_write_durable_index,
        "durable_index must not advance before fsync completes"
    );

    assert_eq!(
        raft_log.last_entry_id(),
        pre_last_entry_id + size,
        "in-memory tail should still advance even while fsync is stalled"
    );

    // One follower already matched index 2; the other is still behind at 0 — asymmetric
    // on purpose. If the leader's own un-fsynced entry counted (the old MemFirst
    // behavior), 2 out of 3 voters would reach index 2 — but RPO=0 requires the
    // leader's own copy to be durable first, so this must return None while fsync
    // is still gated.
    let result = raft_log.calculate_majority_matched_index(1, 1, vec![2, 0]);
    assert_eq!(
        result, None,
        "RPO=0: the leader's own un-fsynced entry must not count toward quorum, even \
         when a follower already reports it"
    );

    flush_gate.send(()).unwrap();
    wait_for_durable_index(
        &raft_log,
        &mut log_flush_rx,
        pre_write_durable_index + size,
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(
        raft_log.durable_index(),
        pre_write_durable_index + size,
        "durable_index must reach the expected index after fsync completes"
    );

    let result_after_fsync = raft_log.calculate_majority_matched_index(1, 1, vec![2, 0]);
    assert_eq!(
        result_after_fsync,
        Some(2),
        "once the leader's own entry is durable, majority index must advance to 2"
    );
}

/// `entry_term()` returns the correct term during high-concurrency writes
/// with artificially delayed fsyncs.
///
/// Term correctness is a memory-only invariant (TermSegments / SkipMap). Fsync
/// timing must not affect it. Run 10 concurrent writers with a delayed fsync,
/// then verify every entry's term matches what was written.
///
/// Doubles as a concurrency check for the inline architecture: `append_entries`
/// now executes `persist_pending_range` + `persisted_index.fetch_max` directly
/// on the caller's own task, so this test exercises genuinely concurrent
/// callers racing that path, not just concurrent readers.
#[tokio::test]
async fn test_entry_term_correct_during_concurrent_fsync_delay() {
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "entry_term_correct_during_concurrent_fsync_delay".into(),
    );
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), None, 5000);

    // 10 concurrent writers, each appending one entry. Term boundary at 5/6
    // exercises TermSegments::on_append's "new term" branch, not just the
    // flat single-term hot path.
    let handles: Vec<_> = (1u64..=10)
        .map(|index| {
            let raft_log = raft_log.clone();
            let term = if index <= 5 { 1 } else { 2 };
            tokio::spawn(async move {
                raft_log
                    .append_entries(vec![Entry {
                        index,
                        term,
                        payload: None,
                    }])
                    .await
                    .unwrap();
            })
        })
        .collect();
    futures::future::join_all(handles).await;

    let expected_term = |index: u64| if index <= 5 { 1 } else { 2 };

    // While the gate is closed (fsync still in flight / not even started for
    // some entries), term lookups must already be correct — TermSegments/SkipMap
    // are populated on append, independent of fsync completion.
    for index in 1u64..=10 {
        assert_eq!(
            raft_log.entry_term(index),
            Some(expected_term(index)),
            "entry {index} term must be correct while fsync is still gated"
        );
    }

    // Release the gate and let fsync complete.
    flush_gate.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Term lookups must remain correct after fsync completes too — fsync
    // timing must not affect this memory-only invariant either way.
    for index in 1u64..=10 {
        assert_eq!(
            raft_log.entry_term(index),
            Some(expected_term(index)),
            "entry {index} term must be correct after fsync completes"
        );
    }
}

// ── Logic correctness ─────────────────────────────────────────────────────────

/// `try_advance_durable_index` is monotonic: a late-arriving lower index is a no-op.
///
/// Directly call `try_advance_durable_index(150, 1)`, then `try_advance_durable_index(100, 1)`.
/// Assert:
///   - final `durable_index() == 150` (not 100)
///   - the 150 call returns `Some(150)` (it fired), the 100 call returns `None` (no-op)
///
/// Verifies the `fetch_max` invariant that makes out-of-order concurrent fsyncs safe.
#[tokio::test]
async fn test_durable_index_monotonic_when_fsyncs_complete_out_of_order() {
    // Storage engine choice doesn't matter here — try_advance_durable_index is
    // called directly, bypassing the real fsync pipeline entirely.
    let storage = Arc::new(MockStorageEngine::with_id(
        "durable_index_monotonic_when_fsyncs_complete_out_of_order".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);

    // try_advance_durable_index() content-validates against entry_term(index) —
    // needs real entries in memory, not just a raw max_index poke.
    let entries: Vec<Entry> = (1..=150)
        .map(|index| Entry {
            index,
            term: 1,
            payload: None,
        })
        .collect();
    raft_log.append_entries(entries).await.unwrap();

    // Simulates a fsync task completing with index 150, then a second, older
    // fsync task (dispatched earlier, finishing later) completing with 100.
    let result_150 = raft_log.try_advance_durable_index(LogId {
        term: 1,
        index: 150,
    });
    let result_100 = raft_log.try_advance_durable_index(LogId {
        term: 1,
        index: 100,
    });

    assert_eq!(
        result_150,
        Some(150),
        "the 150 call must fire — it's the first advance"
    );
    assert_eq!(
        result_100, None,
        "the later, lower 100 call must be a no-op (None), not a regression"
    );
    assert_eq!(
        raft_log.durable_index(),
        150,
        "durable_index must reflect the highest index seen (150), not the \
         later-arriving lower one (100)"
    );
}

/// A `flush()` caller receives `Ok(())` only after its batch is physically on disk.
///
/// Use a MockLogStore with a release-gate on flush(). Call `flush()`, verify the future
/// is still pending while the gate is closed, open the gate, verify the future resolves
/// to `Ok(())`.
///
/// Core contract of `FsyncWorker::run_until_caught_up`: the reply is sent after
/// `log_store.flush()` returns — never before.
#[tokio::test]
async fn test_flush_caller_blocked_until_fsync_completes() {
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "flush_caller_blocked_until_fsync_completes".into(),
    );
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), None, 5000);

    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();

    let raft_log_clone = raft_log.clone();
    let flush_handle = tokio::spawn(async move { raft_log_clone.flush().await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !flush_handle.is_finished(),
        "flush() must not resolve while the fsync gate is closed"
    );

    flush_gate.send(()).unwrap();

    let result = flush_handle.await.unwrap();
    assert!(
        result.is_ok(),
        "flush() must resolve to Ok(()) after the fsync gate opens"
    );
}

/// `flush()` callers whose requests arrive WHILE a fsync task is already running
/// are coalesced into that same in-flight task — not into a second, competing
/// physical fsync.
///
/// `FsyncWorker::submit()` records new work into `pending_max`/`pending_replies`
/// and returns immediately if `inflight` is already `true`; `run_until_caught_up`
/// picks that accumulated work up on its next loop iteration, before clearing
/// `inflight`. This is the mechanism this whole rearchitecture depends on for
/// keeping physical fsync counts low — see §9 in the design discussion.
///
/// Configure a MockLogStore with a flush call counter and a release-gate on the
/// first `flush()` call. Append one entry — the inline persist path submits a
/// fsync round on its own (round 1), winning the CAS and blocking on the gate.
/// While round 1 is gated, call `raft_log.flush()` three times (A, B, C) — none
/// of them can win the CAS (round 1 is still in flight), so all three just
/// extend `pending_max`/`pending_replies` and wait. Release the gate and assert:
/// round 1 finishes and immediately picks up A/B/C as a single round 2, so
/// `flush_call_count` is 2, not 4 — and all three futures resolve to `Ok(())`.
#[tokio::test]
async fn test_flush_callers_arriving_during_inflight_fsync_are_coalesced() {
    let (storage, flush_gate, flush_call_count) =
        MockStorageEngine::not_durable_gated_flush_counted(
            "flush_callers_arriving_during_inflight_fsync_are_coalesced".into(),
        );
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), None, 5000);

    // Append one entry: the inline persist path submits its own fsync round
    // (round 1) and wins the CAS — flush() itself early-returns Ok(()) with
    // nothing to do while memory_max_index is still 0, so a real write is
    // needed to get a round in flight for A/B/C to arrive during.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();

    // Give the fsync thread time to submit round 1 and block on the gate
    // before A/B/C are dispatched — otherwise they could race the automatic
    // round for the CAS instead of deterministically losing it.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // A, B, C: all arrive while round 1 is still gated — none can win the
    // CAS, so all three just extend pending_max/pending_replies and wait.
    let raft_log_a = raft_log.clone();
    let a = tokio::spawn(async move { raft_log_a.flush().await });
    let raft_log_b = raft_log.clone();
    let b = tokio::spawn(async move { raft_log_b.flush().await });
    let raft_log_c = raft_log.clone();
    let c = tokio::spawn(async move { raft_log_c.flush().await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !a.is_finished() && !b.is_finished() && !c.is_finished(),
        "all three flush() calls must still be pending while the gate is closed"
    );

    flush_gate.send(()).unwrap();

    let (ra, rb, rc) = tokio::join!(a, b, c);
    assert!(ra.unwrap().is_ok(), "A's flush() must resolve to Ok(())");
    assert!(rb.unwrap().is_ok(), "B's flush() must resolve to Ok(())");
    assert!(rc.unwrap().is_ok(), "C's flush() must resolve to Ok(())");

    assert_eq!(
        flush_call_count.load(std::sync::atomic::Ordering::Acquire),
        2,
        "expected exactly 2 physical flush() calls: round 1 (append's own \
         automatic fsync) plus round 2 (A, B, and C served together) — not 4 \
         (one per append + one per explicit caller)"
    );
}

/// A `flush()` caller queued behind an already in-flight fsync round still
/// gets a real reply after `close()` gives up waiting — not a silent
/// channel-closed error.
///
/// `close()`'s wait is bounded by `shutdown_timeout_ms` (a short value here so
/// the test doesn't burn real wall-clock seconds); it does not cancel the
/// in-flight `FsyncWorker` round, which keeps running on its own thread and
/// eventually serves any callers queued behind it, independent of whether
/// `close()` has already returned.
#[tokio::test]
async fn test_shutdown_with_pending_flush_caller_still_receives_ok_reply() {
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "shutdown_with_pending_flush_caller_still_receives_ok_reply".into(),
    );
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), None, 100);

    // Append triggers the automatic round (round 1), which wins the CAS and
    // blocks on the gate.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // X arrives while round 1 is gated — loses the CAS, gets queued into
    // FsyncWorker's own pending_replies.
    let raft_log_x = raft_log.clone();
    let x = tokio::spawn(async move { raft_log_x.flush().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !x.is_finished(),
        "X's flush() must still be pending before shutdown"
    );

    // The gate stays closed here: close() must give up after
    // shutdown_timeout_ms rather than hanging forever.
    tokio::time::timeout(Duration::from_secs(1), raft_log.close())
        .await
        .expect("close() must return within shutdown_timeout_ms, not hang");
    assert!(
        !x.is_finished(),
        "X must still be pending right after close() times out"
    );

    // Release the gate: round 1 completes on its own, loops back, and picks
    // up X's queued reply as round 2 — independent of close() having already
    // returned.
    flush_gate.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), x)
        .await
        .expect("X must not hang")
        .unwrap();
    assert!(
        result.is_ok(),
        "X must receive a real Ok(()) reply, not a channel-closed error, \
         even though close() already returned"
    );
}

/// Same as above, but the queued fsync round fails once unblocked — the
/// caller must receive the real `Err`, not a silent channel-closed error.
#[tokio::test]
async fn test_shutdown_with_pending_flush_caller_still_receives_err_reply() {
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush_failing(
        "shutdown_with_pending_flush_caller_still_receives_err_reply".into(),
    );
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), None, 100);

    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let raft_log_x = raft_log.clone();
    let x = tokio::spawn(async move { raft_log_x.flush().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !x.is_finished(),
        "X's flush() must still be pending before shutdown"
    );

    tokio::time::timeout(Duration::from_secs(1), raft_log.close())
        .await
        .expect("close() must return within shutdown_timeout_ms, not hang");
    assert!(
        !x.is_finished(),
        "X must still be pending right after close() times out"
    );

    flush_gate.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), x)
        .await
        .expect("X must not hang")
        .unwrap();
    assert!(
        result.is_err(),
        "X must receive the real fsync failure, not a channel-closed error"
    );
}

// ── Distributed / concurrency ─────────────────────────────────────────────────

/// An in-flight fsync task completing AFTER `reset()` must not "resurrect" the
/// pre-reset `durable_index`, and must not silently report success to any
/// caller waiting on that stale round.
///
/// Scenario:
///   1. Write an entry, gate the physical `flush()` call so the round stays
///      in flight.
///   2. While gated, call `reset()` — bumps `FsyncWorker`'s fence generation,
///      then clears `durable_index`/`memory_max_index`/entries to 0.
///   3. Release the gate — the stale round's `flush()` returns, but its
///      generation no longer matches; it must discard its result instead of
///      resolving durable_index or the queued reply.
#[tokio::test]
async fn test_reset_during_inflight_fsync_does_not_resurrect_stale_durable_index() {
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "reset_during_inflight_fsync_does_not_resurrect_stale_durable_index".into(),
    );
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), None, 5000);

    // Append triggers the automatic round (round 1), which wins the CAS and
    // blocks on the gate.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // X arrives while round 1 is gated — queues behind it in FsyncWorker.
    let raft_log_x = raft_log.clone();
    let x = tokio::spawn(async move { raft_log_x.flush().await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // reset() while round 1 is still gated: bumps the fence generation, then
    // clears durable_index/memory_max_index/entries to 0.
    raft_log.reset().await.unwrap();

    // Release the gate: round 1's flush() call returns, but its generation no
    // longer matches — it must discard instead of resurrecting durable_index.
    flush_gate.send(()).unwrap();

    let result = tokio::time::timeout(Duration::from_secs(1), x)
        .await
        .expect("x must not hang")
        .unwrap();
    assert!(
        result.is_err(),
        "X's flush() must receive Err — its data was wiped by reset, not a \
         resurrected Ok(())"
    );

    assert_eq!(
        raft_log.durable_index(),
        0,
        "durable_index must stay 0 — the stale round must not resurrect the \
         pre-reset value, not even transiently"
    );
}

/// The reset fence must not over-trigger: writes that happen AFTER `reset()`
/// (in a fresh round, not the stale one) must still advance `durable_index`
/// normally — the fence only discards the round that was in flight *before*
/// the reset, not everything that comes after it.
#[tokio::test]
async fn test_post_reset_writes_are_not_discarded_by_stale_fence() {
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "post_reset_writes_are_not_discarded_by_stale_fence".into(),
    );
    let (log_flush_tx, mut log_flush_rx) = mpsc::unbounded_channel();
    let raft_log =
        RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), Some(log_flush_tx), 5000);

    // Append triggers the automatic round (round 1), which wins the CAS and
    // blocks on the gate.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // reset() while round 1 is still gated: bumps the fence generation and
    // drains anything already queued.
    raft_log.reset().await.unwrap();

    // New, post-reset entry — a fresh write, unrelated to the stale round.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let raft_log_y = raft_log.clone();
    let y = tokio::spawn(async move { raft_log_y.flush().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !y.is_finished(),
        "Y must still be pending behind the still-gated stale round"
    );

    // Release the gate: the stale round discards itself (generation
    // mismatch, per the test above); run_until_caught_up loops back,
    // captures a fresh generation, and processes Y's post-reset round.
    flush_gate.send(()).unwrap();

    let result = tokio::time::timeout(Duration::from_secs(1), y)
        .await
        .expect("y must not hang")
        .unwrap();
    assert!(
        result.is_ok(),
        "Y's flush() must succeed — its data was written after reset, not stale"
    );
    drain_and_apply_fsync_completions(&raft_log, &mut log_flush_rx);
    assert_eq!(
        raft_log.durable_index(),
        1,
        "durable_index must advance to reflect the post-reset entry"
    );
}
