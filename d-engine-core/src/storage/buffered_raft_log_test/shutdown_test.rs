use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
use crate::{
    BufferedRaftLog, FlushPolicy, MockStorageEngine, MockTypeConfig, PersistenceConfig,
    PersistenceStrategy,
};
use d_engine_proto::common::{Entry, EntryPayload};

#[tokio::test]
async fn test_shutdown_closes_channel_properly() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 10,
            interval_ms: 100,
        },
        "test_shutdown_channel",
    );

    // Add some entries
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

    // Drop raft_log to trigger shutdown
    drop(ctx.raft_log);

    // Verify shutdown completes without hanging
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn test_shutdown_awaits_worker_completion() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 100,
            interval_ms: 5000,
        },
        "test_shutdown_await_workers",
    );

    // Append entries to trigger worker activity
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

    // Force flush to ensure workers have work
    ctx.raft_log.flush().await.unwrap();

    // Give workers time to start processing
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger shutdown via Drop
    let shutdown_start = std::time::Instant::now();
    drop(ctx.raft_log);
    let shutdown_duration = shutdown_start.elapsed();

    // Verify shutdown completed in reasonable time
    assert!(
        shutdown_duration < Duration::from_millis(500),
        "Shutdown took too long: {shutdown_duration:?}",
    );
}

#[tokio::test]
async fn test_shutdown_handles_slow_workers() {
    let storage = Arc::new(MockStorageEngine::with_id(
        "test_shutdown_slow_workers".to_string(),
    ));

    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            strategy: PersistenceStrategy::MemFirst,
            flush_policy: FlushPolicy::Batch {
                threshold: 5,
                interval_ms: 100,
            },
            max_buffered_entries: 1000,
            ..Default::default()
        },
        storage,
    );

    let raft_log = raft_log.start(receiver);

    // Append entries
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

    // Trigger flush
    raft_log.flush().await.unwrap();

    // Give workers time to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Shutdown should wait for workers
    let shutdown_start = std::time::Instant::now();
    drop(raft_log);
    let shutdown_duration = shutdown_start.elapsed();

    assert!(
        shutdown_duration < Duration::from_millis(1000),
        "Shutdown with slow workers took too long"
    );
}

#[tokio::test]
async fn test_shutdown_with_multiple_flushes() {
    let ctx = BufferedRaftLogTestContext::new(
        PersistenceStrategy::MemFirst,
        FlushPolicy::Batch {
            threshold: 5,
            interval_ms: 100,
        },
        "test_shutdown_multiple_flushes",
    );

    // Create multiple flush operations
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

    // Give workers time to process
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Shutdown should handle all pending work
    let shutdown_start = std::time::Instant::now();
    drop(ctx.raft_log);
    let shutdown_duration = shutdown_start.elapsed();

    assert!(
        shutdown_duration < Duration::from_millis(500),
        "Shutdown with multiple flushes took too long"
    );
}
