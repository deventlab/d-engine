use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::{RaftLogCoreTestContext, drain_and_apply_fsync_completions};
use crate::{MockLogStore, MockMetaStore, MockStorageEngine, MockTypeConfig, RaftLogCore};
use d_engine_proto::common::{Entry, EntryPayload};

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

/// Verifies that a fatal `replace_range` failure:
/// 1. Propagates the error synchronously to the caller.
/// 2. Poisons the log — writes after the failure are rejected outright, not
///    silently accepted into memory with nothing left to ever persist them.
#[tokio::test]
async fn test_replace_range_failure_poisons_log_and_rejects_future_writes() {
    let mut log_store = MockLogStore::new();

    // replace_range always fails — simulates an unrecoverable disk error.
    log_store
        .expect_replace_range()
        .returning(|_, _| Err(crate::Error::Fatal("simulated disk failure".into())));

    log_store.expect_last_index().returning(|| 0);
    log_store.expect_persist_entries().returning(|_| Ok(()));
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_get_entries().returning(|_| Ok(vec![]));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store.expect_is_write_durable().returning(|| true);
    log_store.expect_flush().returning(|| Ok(()));
    log_store.expect_flush_async().returning(|| Ok(()));

    let mut meta_store = MockMetaStore::new();
    meta_store.expect_save_hard_state().returning(|_| Ok(()));
    meta_store.expect_load_hard_state().returning(|| Ok(None));
    meta_store.expect_flush().returning(|| Ok(()));
    meta_store.expect_flush_async().returning(|| Ok(()));

    let storage = Arc::new(MockStorageEngine::from(log_store, meta_store));
    let raft_log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);

    // Append [1..4] term=1 — persisted inline.
    raft_log
        .append_entries(vec![entry(1, 1), entry(2, 1), entry(3, 1), entry(4, 1)])
        .await
        .unwrap();

    // Trigger replace_range: term conflict at index 3 (term 1 → 2).
    let result = raft_log
        .filter_out_conflicts_and_append(2, 1, vec![entry(3, 2), entry(4, 2)])
        .await;

    assert!(
        result.is_err(),
        "expected replace_range failure to be propagated to caller"
    );
    assert!(
        raft_log.is_poisoned(),
        "a replace_range() failure must poison the log"
    );

    let append_result = raft_log.append_entries(vec![entry(5, 2)]).await;
    assert!(
        append_result.is_err(),
        "writes must be rejected once poisoned"
    );
}
