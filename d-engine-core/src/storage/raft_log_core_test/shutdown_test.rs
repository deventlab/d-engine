use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;

use crate::MockStorageEngine;
use crate::MockTypeConfig;
use crate::RaftLogCore;
use crate::storage::raft_log::RaftLog;
use crate::test_utils::RaftLogCoreTestContext;
use crate::test_utils::drain_and_apply_fsync_completions;

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

/// Verifies `close()` completes without hanging and leaves pending writes
/// durable — the explicit shutdown path replaces what used to be verified via
/// `Drop` (there is no dedicated IO thread left to join; `close()` is the
/// only shutdown mechanism now).
#[tokio::test]
async fn test_close_completes_without_hanging() {
    let mut ctx = RaftLogCoreTestContext::new("test_shutdown_channel");

    for i in 1..=10 {
        ctx.raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: None,
            }])
            .await
            .unwrap();
    }

    ctx.raft_log.close().await;
    ctx.drain_fsync_completions();

    assert_eq!(
        ctx.raft_log.durable_index(),
        10,
        "close() must flush pending writes to durable before returning"
    );
}

/// Verifies `close()` waits for in-flight persistence work rather than
/// returning while writes are still pending.
#[tokio::test]
async fn test_close_awaits_pending_writes() {
    let mut ctx = RaftLogCoreTestContext::new("test_shutdown_await_workers");

    for i in 1..=50 {
        ctx.raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: Some(EntryPayload::command(Bytes::from(vec![0u8; 100]))),
            }])
            .await
            .unwrap();
    }

    let close_start = std::time::Instant::now();
    ctx.raft_log.close().await;
    let close_duration = close_start.elapsed();
    ctx.drain_fsync_completions();

    assert_eq!(
        ctx.raft_log.durable_index(),
        50,
        "close() must have flushed all pending writes to durable"
    );
    assert!(
        close_duration < Duration::from_millis(500),
        "close() took too long: {close_duration:?}",
    );
}

/// Verifies `close()` completes in bounded time even with a full backlog of
/// unflushed writes.
#[tokio::test]
async fn test_close_completes_in_bounded_time() {
    let storage = Arc::new(MockStorageEngine::with_id(
        "test_shutdown_slow_workers".to_string(),
    ));
    let (log_flush_tx, mut log_flush_rx) = tokio::sync::mpsc::unbounded_channel();
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, Some(log_flush_tx), 5000);

    for i in 1..=10 {
        raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: Some(EntryPayload::command(Bytes::from(vec![0u8; 50]))),
            }])
            .await
            .unwrap();
    }

    let close_start = std::time::Instant::now();
    raft_log.close().await;
    let close_duration = close_start.elapsed();
    drain_and_apply_fsync_completions(&raft_log, &mut log_flush_rx);

    assert_eq!(raft_log.durable_index(), 10);
    assert!(
        close_duration < Duration::from_millis(1000),
        "close() with pending writes took too long"
    );
}

/// Verifies `close()` still leaves everything durable after several prior
/// explicit `flush()` calls — closing is not a no-op just because the caller
/// already flushed once.
#[tokio::test]
async fn test_close_flushes_multiple_batches() {
    let mut ctx = RaftLogCoreTestContext::new("test_shutdown_multiple_flushes");

    for batch in 0..5 {
        for i in 1..=10 {
            let index = batch * 10 + i;
            ctx.raft_log
                .append_entries(vec![Entry {
                    index,
                    term: 1,
                    payload: Some(EntryPayload::command(Bytes::from(vec![0u8; 100]))),
                }])
                .await
                .unwrap();
        }
        ctx.raft_log.flush().await.unwrap();
    }
    ctx.drain_fsync_completions();

    let close_start = std::time::Instant::now();
    ctx.raft_log.close().await;
    let close_duration = close_start.elapsed();
    ctx.drain_fsync_completions();

    assert_eq!(
        ctx.raft_log.durable_index(),
        50,
        "close() must leave all batches durable"
    );
    assert!(
        close_duration < Duration::from_millis(500),
        "close() with multiple prior flushes took too long"
    );
}

/// Verifies that a fatal `truncate` failure while resolving a term conflict:
/// 1. Propagates the error synchronously to the caller.
/// 2. Poisons the log — writes after the failure are rejected outright, not silently accepted into
///    memory with nothing left to ever persist them.
#[tokio::test]
async fn test_conflict_truncate_failure_poisons_log_and_rejects_future_writes() {
    let mut mock = MockStorageEngine::new();

    // truncate always fails — simulates an unrecoverable disk error.
    mock.expect_truncate()
        .returning(|_| Err(crate::Error::Fatal("simulated disk failure".into())));

    mock.expect_last_index().returning(|| 0);
    mock.expect_persist_entries().returning(|_| Ok(()));
    mock.expect_get_entries().returning(|_| Ok(vec![]));
    mock.expect_purge().returning(|_| Ok(()));
    mock.expect_load_purge_boundary().returning(|| Ok(None));
    mock.expect_flush().returning(|| Ok(()));

    mock.expect_save_hard_state().returning(|_| Ok(()));
    mock.expect_load_hard_state().returning(|| Ok(None));

    let storage = Arc::new(mock);
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);

    // Append [1..4] term=1 — persisted inline.
    raft_log
        .append_entries(vec![entry(1, 1), entry(2, 1), entry(3, 1), entry(4, 1)])
        .await
        .unwrap();

    // Trigger the conflict path: term conflict at index 3 (term 1 → 2).
    let result = raft_log
        .filter_out_conflicts_and_append(2, 1, vec![entry(3, 2), entry(4, 2)], 0)
        .await;

    assert!(
        result.is_err(),
        "expected truncate failure to be propagated to caller"
    );
    assert!(
        raft_log.is_poisoned(),
        "a truncate() failure must poison the log"
    );

    let append_result = raft_log.append_entries(vec![entry(5, 2)]).await;
    assert!(
        append_result.is_err(),
        "writes must be rejected once poisoned"
    );
}

/// `close()` must return within `shutdown_timeout_ms` even when the log has nothing left to make
/// durable and the disk's flush is stuck.
///
/// A node that is shut down before it ever wrote an entry (or after everything is already durable)
/// still goes through `close()`. If the disk stalls at that moment, a flush outside the timeout
/// would keep the shutdown waiting forever.
#[tokio::test]
async fn test_close_with_nothing_to_flush_returns_while_disk_flush_is_stuck() {
    let (storage, flush_gate) =
        MockStorageEngine::gated_flush("close_with_nothing_to_flush".into());
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, Arc::new(storage), None, 100);

    // The gate stays closed: any call to the disk's flush() blocks its thread. Run close() on the
    // blocking pool so a blocked call cannot also stop this test's timer.
    let handle = tokio::runtime::Handle::current();
    let close = tokio::task::spawn_blocking(move || handle.block_on(raft_log.close()));
    let returned = tokio::time::timeout(Duration::from_secs(1), close).await;

    // Release the gate before asserting, so a stuck close() can finish and the runtime can shut down.
    let _ = flush_gate.send(());
    assert!(
        returned.is_ok(),
        "close() must return within shutdown_timeout_ms, not wait on a stuck disk flush"
    );
}
