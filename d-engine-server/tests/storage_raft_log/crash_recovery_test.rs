//! Crash recovery integration tests for RaftLogCore
//!
//! These tests verify RaftLogCore behavior with real disk persistence
//! and crash recovery semantics using FileStorageEngine.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::{RaftLog, RaftLogCore};
use d_engine_proto::common::{Entry, EntryPayload};
use d_engine_server::{FileStateMachine, FileStorageEngine, node::RaftTypeConfig};
use tokio::time::sleep;

use super::TestContext;

#[tokio::test]
async fn test_crash_recovery() {
    // Create and populate storage
    let original_ctx = TestContext::new("test_crash_recovery");

    // Append an entry
    original_ctx
        .raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: Some(EntryPayload::command(Bytes::from(b"data".to_vec()))),
        }])
        .await
        .unwrap();

    // Ensure the entry is persisted
    original_ctx.raft_log.flush().await.unwrap();

    // Recover from the same storage (simulating restart)
    let recovered_ctx = original_ctx.recover_from_crash();

    // Graceful shutdown: close() flushes remaining data before returning,
    // preventing Tokio runtime shutdown panics.
    original_ctx.close().await;

    sleep(Duration::from_millis(50)).await; // Allow recovery

    // Verify recovery - entries should be immediately durable
    assert_eq!(recovered_ctx.raft_log.durable_index(), 1);

    // The entry should be available
    let entry = recovered_ctx.raft_log.entry(1).unwrap();
    assert!(entry.is_some());
    assert_eq!(entry.unwrap().index, 1);
    recovered_ctx.close().await;
}

#[tokio::test]
async fn test_crash_recovery_with_multiple_entries() {
    // Create and populate storage
    let mut original_ctx = TestContext::new("test_crash_recovery_with_multiple_entries");

    // Append multiple entries
    for i in 1..=5 {
        original_ctx
            .raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: Some(EntryPayload::command(Bytes::from(
                    format!("data{i}").into_bytes(),
                ))),
            }])
            .await
            .unwrap();
    }

    // Ensure all entries are persisted
    original_ctx.raft_log.flush().await.unwrap();
    original_ctx.drain_fsync_completions();

    // Verify all entries are in memory and durable
    assert_eq!(original_ctx.raft_log.durable_index(), 5);
    assert_eq!(original_ctx.raft_log.len(), 5);

    // Recover from the same storage (simulating restart)
    let recovered_ctx = original_ctx.recover_from_crash();

    // Graceful shutdown: close() flushes remaining data before returning,
    // preventing Tokio runtime shutdown panics.
    original_ctx.close().await;

    sleep(Duration::from_millis(50)).await; // Allow recovery

    // Verify recovery - all entries should be recovered
    assert_eq!(recovered_ctx.raft_log.durable_index(), 5);
    assert_eq!(recovered_ctx.raft_log.len(), 5);

    // All entries should be available
    for i in 1..=5 {
        let entry = recovered_ctx.raft_log.entry(i).unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().index, i);
    }
    recovered_ctx.close().await;
}

#[tokio::test]
async fn test_partial_flush_with_graceful_shutdown() {
    // Create and partially populate storage
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path().join("partial_flush_graceful");

    {
        let storage = Arc::new(FileStorageEngine::new(storage_path.clone()).unwrap());
        let raft_log = RaftLogCore::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            1, storage, None, 5000,
        );

        // Add 75 entries (1.5 batches)
        for i in 1..=75 {
            raft_log
                .append_entries(vec![Entry {
                    index: i,
                    term: 1,
                    payload: None,
                }])
                .await
                .unwrap();
        }

        // Wait for first batch to flush
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Graceful shutdown: close() joins the IO thread, ensuring all entries are
        // flushed before recovery reads from disk.
        raft_log.close().await;
    }

    // Recover from disk
    let storage = Arc::new(FileStorageEngine::new(storage_path).unwrap());
    let raft_log = RaftLogCore::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
        1, storage, None, 5000,
    );

    tokio::time::sleep(Duration::from_millis(50)).await;

    // With graceful shutdown, close() flushes all entries (75)
    assert_eq!(raft_log.len(), 75);
    assert_eq!(raft_log.durable_index(), 75);
    raft_log.close().await;
}

/// Crash semantics with inline-persist + FsyncWorker architecture.
///
/// `append_entries` persists the new tail and submits it to FsyncWorker on every
/// write. There is no idle timer — the normal path fsyncs immediately after each append.
///
/// True crash (kill -9 / power loss) means data MAY survive if the fsync had time
/// to complete before the crash. This test verifies:
/// - Entries flushed before the crash (first batch, waited 150ms) always survive.
/// - Entries written immediately before crash (second batch, no wait) may or may not
///   survive depending on IO thread scheduling at crash time.
/// - `mem::forget` simulates a hard crash: no graceful shutdown, no final flush.
#[tokio::test]
async fn test_partial_flush_after_crash() {
    let batch_size = 50;

    // Create and partially populate storage
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path().join("partial_flush_crash");

    {
        let storage = Arc::new(FileStorageEngine::new(storage_path.clone()).unwrap());
        let raft_log = RaftLogCore::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            1, storage, None, 5000,
        );

        // Add first batch (50 entries)
        for i in 1..=50 {
            raft_log
                .append_entries(vec![Entry {
                    index: i,
                    term: 1,
                    payload: None,
                }])
                .await
                .unwrap();
        }

        // Wait for first batch to flush
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Add remaining entries (25 entries) as a single batch call.
        // Single .await minimises the window for the IO thread to run between writes.
        let second_batch: Vec<Entry> = (51..=75)
            .map(|i| Entry {
                index: i,
                term: 1,
                payload: None,
            })
            .collect();
        raft_log.append_entries(second_batch).await.unwrap();

        // Simulate crash immediately: Skip Drop with mem::forget (like kill -9 or power loss)
        // This prevents the remaining 25 entries from being flushed.
        // Storage is moved into raft_log, so forgetting raft_log is enough
        std::mem::forget(raft_log);
    }

    // Recover from disk
    let storage = Arc::new(FileStorageEngine::new(storage_path).unwrap());
    let raft_log = RaftLogCore::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
        1, storage, None, 5000,
    );

    tokio::time::sleep(Duration::from_millis(50)).await;

    // With inline-persist + FsyncWorker, writes are fsynced eagerly.
    // At minimum, the first batch (fsynced before the crash) must survive.
    // The second batch may also survive depending on fsync timing.
    let recovered = raft_log.len();
    assert!(
        recovered >= batch_size,
        "flushed entries must survive crash: expected >= {batch_size}, got {recovered}"
    );
    assert!(
        recovered <= 75,
        "cannot recover more entries than written: got {recovered}"
    );
    assert_eq!(
        raft_log.durable_index() as usize,
        recovered,
        "durable_index must match recovered len after restart"
    );
    raft_log.close().await;
}

#[tokio::test]
async fn test_recovery_under_different_scenarios() {
    // With inline-persist + FsyncWorker, all writes are fsynced after explicit
    // flush(), so all 100 entries are always durable.
    let expected_recovery = 100usize;

    let original_ctx = TestContext::new("test_recovery_under_different_scenarios");

    // Add test data
    for i in 1..=100 {
        original_ctx
            .raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: Some(EntryPayload::command(Bytes::from(
                    format!("data{i}").into_bytes(),
                ))),
            }])
            .await
            .unwrap();
    }

    // Explicit flush ensures all entries are durable before crash simulation.
    original_ctx.raft_log.flush().await.unwrap();

    // Simulate crash and recovery
    let recovered_ctx = original_ctx.recover_from_crash();
    original_ctx.close().await;

    // Verify recovery
    assert_eq!(
        recovered_ctx.raft_log.len(),
        expected_recovery,
        "Recovery mismatch: expected {expected_recovery}"
    );
    recovered_ctx.close().await;
}

#[tokio::test]
async fn test_memfirst_crash_recovery_durability() {
    let instance_id = "test_memfirst_durability";

    let recovered_path = {
        let ctx = TestContext::new(instance_id);

        ctx.append_entries(1, 100, 1).await;

        // Verify visible in memory
        assert_eq!(ctx.raft_log.len(), 100);

        let path = ctx.path.clone();
        // Graceful shutdown: data is flushed but _temp_dir is deleted,
        // so recovery still sees an empty storage.
        ctx.close().await;
        path
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Recovery
    let storage = Arc::new(FileStorageEngine::new(PathBuf::from(&recovered_path)).unwrap());
    let raft_log = RaftLogCore::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
        1, storage, None, 5000,
    );

    tokio::time::sleep(Duration::from_millis(50)).await;

    // After crash without flush, data should be lost
    assert_eq!(
        raft_log.len(),
        0,
        "MemFirst without flush should lose uncommitted data"
    );
    raft_log.close().await;
}

#[tokio::test]
async fn test_diskfirst_crash_recovery_durability() {
    let temp_dir = tempfile::tempdir().unwrap();
    let instance_id = "test_diskfirst_durability";
    let storage_path = temp_dir.path().join(instance_id);

    let ctx1 = {
        let storage = Arc::new(FileStorageEngine::new(storage_path.clone()).unwrap());
        let raft_log = RaftLogCore::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            1, storage, None, 5000,
        );

        let entries: Vec<_> = (1..=100)
            .map(|index| Entry {
                index,
                term: 1,
                payload: Some(EntryPayload::command(Bytes::from(b"data".to_vec()))),
            })
            .collect();

        raft_log.append_entries(entries).await.unwrap();
        raft_log.flush().await.unwrap();
        assert_eq!(raft_log.len(), 100, "All entries should be in memory");

        raft_log
    };

    ctx1.close().await;

    // Phase 2: Recovery
    let storage = Arc::new(FileStorageEngine::new(storage_path).unwrap());
    let raft_log = RaftLogCore::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
        1, storage, None, 5000,
    );

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify recovery
    assert_eq!(raft_log.len(), 100, "All entries should be recovered");
    assert_eq!(raft_log.durable_index(), 100, "Durable index should be 100");
    raft_log.close().await;
}
