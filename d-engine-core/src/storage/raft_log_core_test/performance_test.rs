//! Performance tests for RaftLogCore with controllable delays
//!
//! These tests verify RaftLogCore performance behavior during concurrent
//! operations like flush, using MockStorageEngine with controllable delays.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use tokio::sync::Barrier;
use tokio::time::Instant;

use crate::MockStorageEngine;
use crate::MockTypeConfig;
use crate::RaftLog;
use crate::RaftLogCore;

// Test helper: Creates storage with controllable delay
fn create_delayed_storage(delay_ms: u64) -> Arc<MockStorageEngine> {
    let mut mock = MockStorageEngine::new();
    mock.expect_last_index().returning(|| 0);
    mock.expect_load_purge_boundary().returning(|| Ok(None));
    mock.expect_truncate().returning(|_| Ok(()));
    mock.expect_flush().returning(|| Ok(()));

    // Add controllable delay to persist_entries
    mock.expect_persist_entries().returning(move |_| {
        let delay = Duration::from_millis(delay_ms);
        std::thread::sleep(delay);
        Ok(())
    });

    Arc::new(mock)
}

// Tests filter_out_conflicts performance with active flush
#[tokio::test]
async fn test_filter_conflicts_performance_during_flush() {
    let is_ci = std::env::var("CI").is_ok();
    // Relax time limit in CI environment
    let max_duration_ms = if is_ci { 500 } else { 50 };

    const FLUSH_DELAY_MS: u64 = 300;

    let storage = create_delayed_storage(FLUSH_DELAY_MS);
    let log = RaftLogCore::<MockTypeConfig>::new(1, storage, None, 5000);
    let barrier = Arc::new(Barrier::new(2));

    // Populate with test data
    let mut entries = vec![];
    for i in 1..=1000 {
        entries.push(Entry {
            index: i,
            term: 1,
            payload: Some(EntryPayload::command(Bytes::from(vec![0; 256]))),
        });
    }
    log.append_entries(entries).await.unwrap();

    // Start long flush in background (slow due to persist_entries delay)
    let flush_log = log.clone();
    let flush_barrier = barrier.clone();
    tokio::spawn(async move {
        flush_barrier.wait().await;
        let _ = flush_log.flush().await;
    });

    // Wait for flush to start
    barrier.wait().await;

    // Measure performance during active flush
    let start = Instant::now();
    log.filter_out_conflicts_and_append(
        500,
        1,
        vec![Entry {
            index: 501,
            term: 1,
            payload: Some(EntryPayload::command(Bytes::from(vec![1; 256]))),
        }],
        0,
    )
    .await
    .unwrap();

    let duration = start.elapsed();
    assert!(
        duration.as_millis() < max_duration_ms as u128,
        "Operation took {}ms during flush",
        duration.as_millis(),
    );
}
