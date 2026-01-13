//! Crash recovery integration tests for BufferedRaftLog
//!
//! These tests verify BufferedRaftLog behavior with real disk persistence
//! and crash recovery semantics using FileStorageEngine.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::{
    BufferedRaftLog, FlushPolicy, PersistenceConfig, PersistenceStrategy, RaftLog,
};
use d_engine_proto::common::{Entry, EntryPayload};
use d_engine_server::{FileStateMachine, FileStorageEngine, node::RaftTypeConfig};
use tokio::time::sleep;

use super::TestContext;

#[tokio::test]
async fn test_crash_recovery() {
    // Create and populate storage
    let original_ctx = TestContext::new(
        PersistenceStrategy::DiskFirst,
        FlushPolicy::Immediate,
        "test_crash_recovery",
    );

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

    // Ensure the entry is persisted for DiskFirst strategy
    original_ctx.raft_log.flush().await.unwrap();

    // Recover from the same storage (simulating restart)
    let recovered_ctx = original_ctx.recover_from_crash();

    // Simulate crash by dropping the original context
    drop(original_ctx);

    sleep(Duration::from_millis(50)).await; // Allow recovery

    // Verify recovery - for DiskFirst, entries should be immediately durable
    assert_eq!(recovered_ctx.raft_log.durable_index(), 1);

    // The entry should be available
    let entry = recovered_ctx.raft_log.entry(1).unwrap();
    assert!(entry.is_some());
    assert_eq!(entry.unwrap().index, 1);
}

#[tokio::test]
async fn test_crash_recovery_with_multiple_entries() {
    // Create and populate storage
    let original_ctx = TestContext::new(
        PersistenceStrategy::DiskFirst,
        FlushPolicy::Immediate,
        "test_crash_recovery_with_multiple_entries",
    );

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

    // Ensure all entries are persisted for DiskFirst strategy
    original_ctx.raft_log.flush().await.unwrap();

    // Verify all entries are in memory and durable
    assert_eq!(original_ctx.raft_log.durable_index(), 5);
    assert_eq!(original_ctx.raft_log.len(), 5);

    // Recover from the same storage (simulating restart)
    let recovered_ctx = original_ctx.recover_from_crash();

    // Simulate crash by dropping the original context
    drop(original_ctx);

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
}

#[tokio::test]
async fn test_crash_recovery_mem_first() {
    // Create and populate storage with MemFirst strategy
    let original_ctx = TestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 10,
            interval_ms: 1000,
        },
        "test_crash_recovery_mem_first",
    );

    // Append entries but don't flush (simulate data in memory only)
    for i in 1..=3 {
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

    // Verify entries are in memory but not durable
    assert_eq!(original_ctx.raft_log.len(), 3);
    assert_eq!(original_ctx.raft_log.durable_index(), 0);

    // Recover from the same storage (simulating restart)
    let recovered_ctx = original_ctx.recover_from_crash();

    // Simulate crash by dropping the original context without flushing
    drop(original_ctx);

    sleep(Duration::from_millis(50)).await; // Allow recovery

    // Verify recovery - for MemFirst without flush, data should be lost
    assert_eq!(recovered_ctx.raft_log.durable_index(), 0);
    assert_eq!(recovered_ctx.raft_log.len(), 0);

    // No entries should be available
    for i in 1..=3 {
        let entry = recovered_ctx.raft_log.entry(i).unwrap();
        assert!(entry.is_none());
    }
}

#[tokio::test]
async fn test_partial_flush_with_graceful_shutdown() {
    let batch_size = 50;

    // Create and partially populate storage
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path().join("partial_flush_graceful");

    {
        let storage = Arc::new(FileStorageEngine::new(storage_path.clone()).unwrap());
        let (raft_log, receiver) =
            BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
                1,
                PersistenceConfig {
                    strategy: PersistenceStrategy::MemFirst,
                    flush_policy: FlushPolicy::Batch {
                        threshold: batch_size,
                        interval_ms: 100,
                    },
                    max_buffered_entries: 1000,
                    ..Default::default()
                },
                storage,
            );
        let raft_log = raft_log.start(receiver);

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

        // Graceful shutdown: Drop will flush remaining entries
    }

    // Recover from disk
    let storage = Arc::new(FileStorageEngine::new(storage_path).unwrap());
    let (raft_log, receiver) =
        BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            1,
            PersistenceConfig {
                strategy: PersistenceStrategy::MemFirst,
                flush_policy: FlushPolicy::Immediate,
                max_buffered_entries: 1000,
                ..Default::default()
            },
            storage,
        );
    let raft_log = raft_log.start(receiver);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // With graceful shutdown, Drop flushes all entries (75)
    assert_eq!(raft_log.len(), 75);
    assert_eq!(raft_log.durable_index(), 75);
}

#[tokio::test]
async fn test_partial_flush_after_crash() {
    let batch_size = 50;

    // Create and partially populate storage
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path().join("partial_flush_crash");

    {
        let storage = Arc::new(FileStorageEngine::new(storage_path.clone()).unwrap());
        let (raft_log, receiver) =
            BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
                1,
                PersistenceConfig {
                    strategy: PersistenceStrategy::MemFirst,
                    flush_policy: FlushPolicy::Batch {
                        threshold: batch_size,
                        interval_ms: 100,
                    },
                    max_buffered_entries: 1000,
                    ..Default::default()
                },
                storage,
            );
        let raft_log = raft_log.start(receiver);

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

        // Add remaining entries (25 entries) that should NOT be flushed
        for i in 51..=75 {
            raft_log
                .append_entries(vec![Entry {
                    index: i,
                    term: 1,
                    payload: None,
                }])
                .await
                .unwrap();
        }

        // Simulate crash immediately: Skip Drop with mem::forget (like kill -9 or power loss)
        // This prevents the processor from flushing the remaining 25 entries
        // Storage is moved into raft_log, so forgetting raft_log is enough
        std::mem::forget(raft_log);
    }

    // Recover from disk
    let storage = Arc::new(FileStorageEngine::new(storage_path).unwrap());
    let (raft_log, receiver) =
        BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            1,
            PersistenceConfig {
                strategy: PersistenceStrategy::MemFirst,
                flush_policy: FlushPolicy::Immediate,
                max_buffered_entries: 1000,
                ..Default::default()
            },
            storage,
        );
    let raft_log = raft_log.start(receiver);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Only first batch (50 entries) should be recovered after crash
    assert_eq!(raft_log.len(), batch_size);
    assert_eq!(raft_log.durable_index(), batch_size as u64);
}

#[tokio::test]
async fn test_recovery_under_different_scenarios() {
    // Test various recovery scenarios
    let scenarios = vec![
        // MemFirst with Immediate flush - should persist everything
        (PersistenceStrategy::MemFirst, FlushPolicy::Immediate, 100),
        // MemFirst with batch flushing - only persists when threshold met
        (
            PersistenceStrategy::MemFirst,
            FlushPolicy::Batch {
                threshold: 50,
                interval_ms: 10,
            },
            100,
        ),
        (
            PersistenceStrategy::MemFirst,
            FlushPolicy::Batch {
                threshold: 200,
                interval_ms: 10,
            },
            0,
        ),
        // DiskFirst should always persist
        (PersistenceStrategy::DiskFirst, FlushPolicy::Immediate, 100),
    ];

    for (strategy, flush_policy, expected_recovery) in scenarios {
        let instance_id = format!("recovery_test_{strategy:?}_{flush_policy:?}");
        let original_ctx = TestContext::new(strategy.clone(), flush_policy.clone(), &instance_id);

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

        // For DiskFirst and Immediate flush, explicitly flush to ensure persistence
        if matches!(strategy, PersistenceStrategy::DiskFirst)
            || matches!(flush_policy, FlushPolicy::Immediate)
        {
            original_ctx.raft_log.flush().await.unwrap();
        } else if let FlushPolicy::Batch { threshold, .. } = flush_policy {
            // For batch policy, only flush if we reached the threshold
            if 100 >= threshold {
                original_ctx.raft_log.flush().await.unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        // Simulate crash and recovery
        let recovered_ctx = original_ctx.recover_from_crash();
        drop(original_ctx);

        // Verify recovery based on expected behavior
        assert_eq!(
            recovered_ctx.raft_log.len(),
            expected_recovery,
            "Recovery mismatch for strategy {strategy:?} policy {flush_policy:?}"
        );
    }
}

#[tokio::test]
async fn test_memfirst_crash_recovery_durability() {
    let instance_id = "test_memfirst_durability";

    let recovered_path = {
        let ctx = TestContext::new(
            PersistenceStrategy::MemFirst,
            FlushPolicy::Batch {
                threshold: 1000, // High threshold to prevent auto-flush
                interval_ms: 10000,
            },
            instance_id,
        );

        ctx.append_entries(1, 100, 1).await;

        // Verify visible in memory
        assert_eq!(ctx.raft_log.len(), 100);

        let path = ctx.path.clone();
        // Simulate crash WITHOUT flush
        drop(ctx);
        path
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Recovery
    let storage = Arc::new(FileStorageEngine::new(PathBuf::from(&recovered_path)).unwrap());
    let (raft_log, receiver) =
        BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            1,
            PersistenceConfig {
                strategy: PersistenceStrategy::MemFirst,
                flush_policy: FlushPolicy::Immediate,
                max_buffered_entries: 1000,
                ..Default::default()
            },
            storage,
        );
    let raft_log = raft_log.start(receiver);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // After crash without flush, data should be lost
    assert_eq!(
        raft_log.len(),
        0,
        "MemFirst without flush should lose uncommitted data"
    );
}

#[tokio::test]
async fn test_diskfirst_crash_recovery_durability() {
    let temp_dir = tempfile::tempdir().unwrap();
    let instance_id = "test_diskfirst_durability";
    let storage_path = temp_dir.path().join(instance_id);

    let ctx1 = {
        let storage = Arc::new(FileStorageEngine::new(storage_path.clone()).unwrap());
        let (raft_log, receiver) =
            BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
                1,
                PersistenceConfig {
                    strategy: PersistenceStrategy::DiskFirst,
                    flush_policy: FlushPolicy::Immediate,
                    max_buffered_entries: 1000,
                    ..Default::default()
                },
                storage,
            );
        let raft_log = raft_log.start(receiver);

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

    drop(ctx1);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 2: Recovery
    let storage = Arc::new(FileStorageEngine::new(storage_path).unwrap());
    let (raft_log, receiver) =
        BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            1,
            PersistenceConfig {
                strategy: PersistenceStrategy::DiskFirst,
                flush_policy: FlushPolicy::Immediate,
                max_buffered_entries: 1000,
                ..Default::default()
            },
            storage,
        );
    let raft_log = raft_log.start(receiver);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify recovery
    assert_eq!(raft_log.len(), 100, "All entries should be recovered");
    assert_eq!(raft_log.durable_index(), 100, "Durable index should be 100");
}
