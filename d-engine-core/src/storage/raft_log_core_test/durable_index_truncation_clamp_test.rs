//! `durable_index` must never claim more of the log survived to disk than the
//! log actually holds right now. The danger case: a term-conflict truncation
//! shrinks the log while a persist / fsync for the old, longer log is still in
//! flight — the stale in-flight write must not push `durable_index` past the
//! truncation point. Two guards cover this: `try_advance_durable_index`'s term
//! check, and the `FsyncWorker` generation fence bumped by `remove_range`.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::RaftLogCoreTestContext;
use crate::{MockLogStore, MockMetaStore, MockStorageEngine, MockTypeConfig, RaftLogCore};

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

/// After a drastic truncate-then-regrow, `durable_index` must land exactly on
/// the new tail — never above it (would claim durability for discarded
/// entries), never stuck below it (the new tail must actually become durable).
#[tokio::test]
async fn test_durable_index_lands_on_new_tail_after_truncate_and_resync() {
    let mut ctx =
        RaftLogCoreTestContext::new("durable_index_lands_on_new_tail_after_truncate_and_resync");

    // Old leader (term 1) replicates 1..=10. append_entries inserts them into
    // memory and notifies the IO thread; nothing is fsync-confirmed until the
    // FsyncCompleted events are drained below.
    ctx.append_entries(1, 10, 1).await;
    assert_eq!(ctx.raft_log.last_entry_id(), 10);
    assert_eq!(
        ctx.raft_log.durable_index(),
        0,
        "no fsync report drained yet"
    );

    // New leader (term 2): index 2 conflicts, so the log is truncated from 2
    // and replaced with a single new entry — real log becomes [1, 2]. Slow
    // path: remove_range(2..) drops memory_max_index to 1 and clamps
    // durable_index down, then the new index 2 is inserted.
    ctx.raft_log
        .filter_out_conflicts_and_append(1, 1, vec![entry(2, 2)])
        .await
        .unwrap();
    assert_eq!(ctx.raft_log.last_entry_id(), 2, "log is now [1, 2]");

    // flush() only returns after its own fsync-completion event is enqueued,
    // so draining right here is deterministic — no sleep needed.
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();

    // The stale report for index 10 must be rejected (index 10 no longer
    // exists); the report for index 2 must be accepted.
    assert!(
        ctx.raft_log.durable_index() <= ctx.raft_log.last_entry_id(),
        "durable_index ({}) must not exceed last_entry_id ({})",
        ctx.raft_log.durable_index(),
        ctx.raft_log.last_entry_id()
    );
    assert_eq!(
        ctx.raft_log.durable_index(),
        2,
        "durable_index must reach the true tail (2), not a stale pre-truncation watermark"
    );
}

/// A persist whose entry set was captured *before* a truncation but finishes
/// *after* it must not let a stale fsync-completion report advance
/// `durable_index` into the range the truncation discarded.
///
/// Timeline (deterministic via the persist gate):
/// 1. Old leader (term 1) replicates 1..=10 — on its own task, since the
///    inline persist path now blocks the calling task on the gate (unlike
///    the old dedicated-IO-thread design, where `append_entries` returned
///    immediately and the gated persist ran elsewhere).
/// 2. New leader (term 2): index 2 conflicts, on a second task, concurrent
///    with the still-gated persist from step 1. In-memory truncation
///    (`remove_range`) is synchronous, so it's visible immediately; the
///    `replace_range_and_submit` await behind it does not depend on step 1's
///    gate at all — the two tasks touch independent code paths, only
///    `entries`'s `RwLock` briefly serializes them.
/// 3. Release the gate: the stale persist (step 1) completes and reports a
///    stale fsync-completion for index 10; a flush() drives one legitimate
///    fsync-completion for index 2.
///
/// Expected: draining the fsync completions advances `durable_index` to 2 and
/// rejects the stale report for index 10.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stale_persist_after_truncation_does_not_advance_durable_index() {
    let (storage, persist_gate) =
        MockStorageEngine::not_durable_gated_persist("stale_persist_after_truncation".into());
    let (log_flush_tx, mut log_flush_rx) = tokio::sync::mpsc::unbounded_channel();
    let raft_log =
        RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), Some(log_flush_tx), 5000);

    // Step 1: replicate 1..=10 on its own task — this task blocks on the
    // gated persist_entries call until step 3 releases it.
    let entries: Vec<Entry> = (1..=10).map(|i| entry(i, 1)).collect();
    let persist = {
        let raft_log = raft_log.clone();
        tokio::spawn(async move { raft_log.append_entries(entries).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Step 2: term conflict at index 2 — on a second, independent task. Its
    // in-memory truncation does not wait on step 1's gate at all.
    let truncate = {
        let raft_log = raft_log.clone();
        tokio::spawn(async move {
            raft_log.filter_out_conflicts_and_append(1, 1, vec![entry(2, 2)]).await
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        raft_log.last_entry_id(),
        2,
        "in-memory truncation is synchronous — visible without waiting on step 1's gate"
    );

    // Step 3: release the stale persist, let both tasks finish, then flush.
    persist_gate
        .send(())
        .expect("step 1's task should still be waiting on the gate");
    persist.await.unwrap().unwrap();
    truncate.await.unwrap().unwrap();
    raft_log.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain fsync completions the way raft.rs's event loop would.
    while let Ok(event) = log_flush_rx.try_recv() {
        if let crate::InternalEvent::FsyncCompleted { mark, sent_at: _ } = event {
            raft_log.try_advance_durable_index(mark);
        }
    }

    assert!(
        raft_log.durable_index() <= raft_log.last_entry_id(),
        "durable_index ({}) must never exceed last_entry_id ({}) — the stale persist \
         for 1..=10 must not be reported durable after truncation shrank the log to [1, 2]",
        raft_log.durable_index(),
        raft_log.last_entry_id()
    );
    assert_eq!(
        raft_log.durable_index(),
        2,
        "durable_index must land on the post-truncation tail (2), not the stale 10"
    );
}

/// `persist_pending_range` must report the highest index it *actually wrote*,
/// not the upper scan bound it was handed. The two differ during a truncation
/// race: the IO thread latched `memory_max_index` = 10 (an old leader had sent
/// 8, 9, 10), then a term-conflict truncation removed everything above 7 before
/// the SkipMap scan ran. Asking to persist `(4, 10]` then writes only 5, 6, 7.
///
/// Returning the bound (10) would push the caller's `persisted_index` and the
/// fsync target past entries that never reached disk — a redundant fdatasync
/// plus a spurious `FsyncCompleted{10}` that the term check then has to reject.
/// Reporting 7 keeps every downstream watermark on real data.
#[tokio::test]
async fn test_persist_pending_range_reports_written_max_not_scan_bound() {
    let storage = Arc::new(MockStorageEngine::with_id(
        "persist_pending_range_reports_written_max".into(),
    ));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);

    raft_log.append_entries((1..=7).map(|i| entry(i, 1)).collect()).await.unwrap();

    // Scan bound is 10 (stale latch); the SkipMap holds only 1..=7.
    let written = raft_log.persist_pending_range(5, 10, "test").await.unwrap();

    assert_eq!(
        written,
        Some(LogId { term: 1, index: 7 }),
        "must report the highest index actually written (7), not the scan bound (10)"
    );
}

/// After a term-conflict truncation, the persist frontier (`persisted_index`) must land
/// *past* the new tail — not on it. `replace_range_and_submit` already wrote the new
/// tail via `replace_range` and submitted its fsync; the next write's persist scan must
/// start at `new_tail + 1`. If the frontier is left *at* `new_tail`, every
/// subsequent write re-scans and re-`persist_entries` that one boundary entry
/// (and re-submits a redundant fsync for it) — the exact waste #446 removes.
///
/// Guards the "highest-persisted" watermark semantics: `ReplaceRange` sets the
/// watermark to `new_tail`, and scans start at `watermark + 1`.
#[tokio::test]
async fn test_persist_frontier_skips_new_tail_after_truncation() {
    // Records the index list of every persist_entries() call.
    let persist_calls: Arc<Mutex<Vec<Vec<u64>>>> = Arc::new(Mutex::new(Vec::new()));

    let mut log_store = MockLogStore::new();
    log_store.expect_last_index().returning(|| 0);
    {
        let calls = persist_calls.clone();
        log_store.expect_persist_entries().returning(move |entries| {
            calls.lock().unwrap().push(entries.iter().map(|e| e.index).collect());
            Ok(())
        });
    }
    log_store.expect_replace_range().returning(|from, new_entries| {
        Ok(new_entries.last().map(|e| e.index).unwrap_or(from.saturating_sub(1)))
    });
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_get_entries().returning(|_| Ok(vec![]));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
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

    // Old leader (term 1): entries 1..=10, persisted.
    raft_log.append_entries((1..=10).map(|i| entry(i, 1)).collect()).await.unwrap();
    raft_log.flush().await.unwrap();

    // New leader (term 2): conflict at index 6 → truncate [6..], replace with
    // [6, 7] (term 2). `filter_out_conflicts_and_append` awaits
    // `replace_range_and_submit`, so the frontier is at new-tail 7 on return.
    raft_log
        .filter_out_conflicts_and_append(5, 1, vec![entry(6, 2), entry(7, 2)])
        .await
        .unwrap();

    // Only care about persist calls from here on — no flush() in between, so the
    // next append's persist is the first thing to touch the frontier.
    persist_calls.lock().unwrap().clear();

    // Next write extends the log. Its persist scan must start at 8, not 7.
    raft_log.append_entries((8..=10).map(|i| entry(i, 2)).collect()).await.unwrap();

    // Wait until the new entries were actually handed to persist_entries; an empty
    // record would make the "no re-persist" check below pass without observing anything.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !persist_calls.lock().unwrap().iter().flatten().any(|&idx| idx == 10) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "entries 8..=10 were never passed to persist_entries. Got calls: {:?}",
            persist_calls.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let calls = persist_calls.lock().unwrap().clone();
    let re_persisted_tail = calls.iter().flatten().any(|&idx| idx <= 7);
    assert!(
        !re_persisted_tail,
        "after ReplaceRange set the frontier at new-tail 7, the next persist must \
         start at 8 — entry 7 (or below) must not be handed to persist_entries again. \
         Got calls: {calls:?}"
    );
}

/// Regression test for a bug where a *shrinking* truncation left
/// `persisted_index` stuck above the new (shorter) tail. `replace_range_and_submit`
/// used to update the frontier with `fetch_max(new_tail)`, which — unlike a
/// plain store — can never pull a stale higher value back down. Every append
/// after the truncation then computed `start = persisted_index + 1 > end =
/// memory_max_index`, so `persist_pending_range` silently no-op'd and the new
/// entry was never handed to `persist_entries` at all. Worse: `flush()` would
/// still see `persisted_index >= target` and report the entry durable off pure
/// (stale) bookkeeping — `durable_index` advancing past data that was never
/// actually written to disk. The fix is a plain `store(new_tail)`, matching
/// `reset_internal`'s treatment of the same "authoritative reset, not an
/// advance" situation.
#[tokio::test]
async fn test_persisted_index_corrected_downward_after_shrinking_truncation() {
    let persist_calls: Arc<Mutex<Vec<Vec<u64>>>> = Arc::new(Mutex::new(Vec::new()));

    let mut log_store = MockLogStore::new();
    log_store.expect_last_index().returning(|| 0);
    {
        let calls = persist_calls.clone();
        log_store.expect_persist_entries().returning(move |entries| {
            calls.lock().unwrap().push(entries.iter().map(|e| e.index).collect());
            Ok(())
        });
    }
    log_store.expect_replace_range().returning(|from, new_entries| {
        Ok(new_entries.last().map(|e| e.index).unwrap_or(from.saturating_sub(1)))
    });
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_get_entries().returning(|_| Ok(vec![]));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
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

    // Old leader (term 1): 1..=10, all persisted — persisted_index reaches 10.
    raft_log.append_entries((1..=10).map(|i| entry(i, 1)).collect()).await.unwrap();
    raft_log.flush().await.unwrap();

    // New leader (term 2): conflict at index 3, replaced with a SHORT tail
    // ([3] only, term 2) — new_tail (3) lands far below the old persisted_index
    // (10). This is realistic, not contrived: a new leader's AppendEntries
    // batch is often much shorter than a stale follower's uncommitted tail.
    raft_log.filter_out_conflicts_and_append(2, 1, vec![entry(3, 2)]).await.unwrap();
    assert_eq!(raft_log.last_entry_id(), 3, "log is now [1, 2, 3]");

    persist_calls.lock().unwrap().clear();

    // Append one more entry past the new (shorter) tail.
    raft_log.append_entries(vec![entry(4, 2)]).await.unwrap();

    let calls = persist_calls.lock().unwrap().clone();
    assert!(
        calls.iter().flatten().any(|&idx| idx == 4),
        "entry 4 must have been persisted — persisted_index must have been \
         corrected down to the new (shorter) tail (3) after the truncation, not \
         left stuck above it at the pre-truncation value (10). Got persist \
         calls: {calls:?}"
    );
}

/// `persist_pending_range` must refuse to touch storage once the log is
/// poisoned — this is its *own* guard, not something callers must each
/// remember to check first. `flush()` is the one `RaftLog` method that calls
/// `persist_pending_range` without checking `is_poisoned()` itself, so this
/// guard being missing would let `flush()` write to an already-untrusted disk.
///
/// The setup relies on `append_entries`'s own ordering: `insert_to_memory`
/// runs *before* the persist attempt, so a failed persist leaves
/// `memory_max_index` ahead of `persisted_index` — exactly the condition
/// `flush()` needs to decide there's something to persist and reach
/// `persist_pending_range` at all.
#[tokio::test]
async fn test_flush_after_poisoned_does_not_call_persist_entries() {
    let persist_calls: Arc<Mutex<Vec<Vec<u64>>>> = Arc::new(Mutex::new(Vec::new()));
    let persist_should_fail = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut log_store = MockLogStore::new();
    log_store.expect_last_index().returning(|| 0);
    {
        let calls = persist_calls.clone();
        let should_fail = persist_should_fail.clone();
        log_store.expect_persist_entries().returning(move |entries| {
            calls.lock().unwrap().push(entries.iter().map(|e| e.index).collect());
            if should_fail.load(std::sync::atomic::Ordering::Acquire) {
                Err(crate::Error::Fatal("simulated disk failure".into()))
            } else {
                Ok(())
            }
        });
    }
    log_store.expect_replace_range().returning(|_, _| Ok(0));
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_get_entries().returning(|_| Ok(vec![]));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
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

    // First append succeeds — persisted_index catches up to memory_max_index (1).
    raft_log.append_entries(vec![entry(1, 1)]).await.unwrap();

    // Now make persist_entries fail. append_entries calls insert_to_memory
    // (memory_max_index -> 2) *before* the persist attempt, so the failure
    // leaves persisted_index at 1 — one behind memory_max_index — and poisons.
    persist_should_fail.store(true, std::sync::atomic::Ordering::Release);
    let append_result = raft_log.append_entries(vec![entry(2, 1)]).await;
    assert!(append_result.is_err(), "failed persist must propagate");
    assert!(
        raft_log.is_poisoned(),
        "failed persist_entries must poison the log"
    );
    assert_eq!(
        raft_log.last_entry_id(),
        2,
        "entry 2 is in memory even though its persist failed"
    );

    persist_calls.lock().unwrap().clear();

    // flush() must refuse once poisoned. persisted_index (1) < memory_max_index
    // (2) here, so without persist_pending_range's own is_poisoned() guard,
    // flush() would call persist_entries again on the now-untrusted disk.
    let flush_result = raft_log.flush().await;
    assert!(
        flush_result.is_err(),
        "flush() must refuse to proceed once the log is poisoned"
    );
    assert!(
        persist_calls.lock().unwrap().is_empty(),
        "persist_entries must never be called again once the log is poisoned"
    );
}

/// `flush()` must surface the error of its own catch-up persist, not swallow it.
///
/// Entries that sit in memory but were never persisted (`persisted_index <
/// memory_max_index`) make `flush()` persist them itself. If that persist fails, the
/// caller must get the underlying disk error directly. Before the explicit `?`,
/// the error was dropped and `flush()` only failed later, with a generic
/// "storage is poisoned" reply from the fsync worker — correct outcome, but it relied
/// on that indirect chain and hid the root cause.
#[tokio::test]
async fn test_flush_returns_catch_up_persist_error_instead_of_swallowing_it() {
    let mut log_store = MockLogStore::new();
    log_store.expect_last_index().returning(|| 0);
    log_store
        .expect_persist_entries()
        .returning(|_| Err(crate::Error::Fatal("simulated disk failure".into())));
    log_store.expect_replace_range().returning(|_, _| Ok(0));
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_get_entries().returning(|_| Ok(vec![]));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
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

    // In memory only: persisted_index (0) < memory_max_index (1), log not poisoned yet.
    raft_log.insert_to_memory(&[entry(1, 1)]);
    assert!(!raft_log.is_poisoned());

    let err = raft_log.flush().await.expect_err("catch-up persist failed, flush must fail");

    assert!(
        format!("{err:?}").contains("simulated disk failure"),
        "flush() must return the catch-up persist error itself, got: {err:?}"
    );
    assert!(
        raft_log.is_poisoned(),
        "a failed persist must poison the log"
    );
}
