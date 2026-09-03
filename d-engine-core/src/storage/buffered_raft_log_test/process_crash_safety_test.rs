//! Process-crash safety of entries counted toward quorum.
//!
//! `calculate_majority_matched_index` counts a leader's own write via
//! `last_entry_id()` — the in-memory SkipMap — as soon as `append_entries()`
//! returns. That's fine for power-loss safety (fsync is deliberately async,
//! see quorum_durability_test.rs) as long as the entry has at least reached the
//! storage engine (OS-managed page cache / WAL), which survives an ordinary
//! process crash even without fsync.
//!
//! These tests pin down whether `append_entries()` actually waits for the
//! storage engine (`LogStore::persist_entries`) before returning. Today it does
//! not — persistence happens later, asynchronously, on the IO thread — so an
//! entry can be quorum-eligible while a process crash between `append_entries()`
//! returning and the IO thread's next wakeup would lose it.

use std::sync::Arc;
use std::time::Duration;

use d_engine_proto::common::Entry;

use crate::{
    BufferedRaftLog, FlushPolicy, LogStore, MockStorageEngine, MockTypeConfig, PersistenceConfig,
    RaftLog, StorageEngine,
};

/// `append_entries()` must not return before the entry reaches the storage
/// engine — otherwise a quorum-eligible write exists only in memory and is
/// lost on an ordinary process crash (not just power loss).
///
/// Gates `LogStore::persist_entries()` so it never completes during the test.
/// Today, `append_entries()` only inserts into the in-memory SkipMap and
/// notifies the IO thread — it does not call `persist_entries()` itself — so
/// it returns immediately regardless of the gate, and the storage engine never
/// sees the entry. After the fix, `append_entries()` must call
/// `persist_entries()` synchronously before returning, so with the gate closed
/// it must still be pending.
///
/// Needs `flavor = "multi_thread"`: the gate blocks on a synchronous
/// `std::sync::mpsc::Receiver::recv()` inside `persist_entries()`, which now
/// runs directly on whichever task calls `append_entries()`. On the default
/// single-threaded runtime that would freeze the only executor thread —
/// including this test's own `sleep()` below — for the gate's entire
/// lifetime, an unrelated deadlock, not the behavior under test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_append_entries_waits_for_storage_engine_before_returning() {
    let (storage, persist_gate) =
        MockStorageEngine::not_durable_gated_persist("append_waits_for_storage_engine".into());
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 5000,
        },
        Arc::new(storage.clone()),
    );
    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    let entry = Entry {
        index: 1,
        term: 1,
        payload: None,
    };

    let append_task = tokio::spawn({
        let raft_log = raft_log.clone();
        async move { raft_log.append_entries(vec![entry]).await }
    });

    // Long enough that, if append_entries() were waiting on persist_entries(),
    // it would still be pending; short enough to keep the suite fast.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // FIXED: append_entries() now routes the write through the IO thread
    // (IOTask::Persist + oneshot) and does not return until it completes —
    // with the gate closed, it must still be pending.
    assert!(
        !append_task.is_finished(),
        "append_entries() must not return before persist_entries() completes"
    );

    // Ground truth: query the storage engine directly, not raft_log's own
    // SkipMap-backed accessor (which would show the entry regardless).
    assert!(
        storage.log_store().entry(1).await.unwrap().is_none(),
        "entry must not be visible in the storage engine while persist_entries() is gated"
    );

    persist_gate.send(()).expect("IO thread should still be waiting on the gate");
    append_task.await.unwrap().unwrap();

    // append_entries() only returns after persist_entries() completes now, so
    // the entry must already be visible — no polling needed.
    assert!(
        storage.log_store().entry(1).await.unwrap().is_some(),
        "entry must be in the storage engine once append_entries() returns"
    );
}
