//! Performance benchmark integration tests for BufferedRaftLog
//!
//! These tests measure real I/O performance with FileStorageEngine.
//! Most tests are marked with #[ignore] and can be run explicitly or
//! skipped in CI environments.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::{
    BufferedRaftLog, FlushPolicy, PersistenceConfig, PersistenceStrategy, RaftLog,
};
use d_engine_proto::common::{Entry, EntryPayload};
use d_engine_server::{FileStateMachine, FileStorageEngine, node::RaftTypeConfig};
use tempfile::tempdir;
use tokio::time::Instant;

use super::TestContext;

// TODO: Extract 6 real I/O performance tests from legacy_all_tests.rs:
// - test_filter_out_conflicts_performance_consistent_across_flush_intervals_fresh_cluster (✅ migrated)
// - test_filter_out_conflicts_performance_consistent_across_flush_intervals (✅ migrated)
// - test_last_entry_id_performance
// - test_remove_range_performance
// - test_performance_benchmarks
// - test_read_performance_remains_lockfree
//
// Note: The following 3 tests were moved to d-engine-core unit tests (require Mock):
// - test_reset_performance_during_active_flush (moved to core)
// - test_filter_conflicts_performance_during_flush (moved to core)
// - test_fresh_cluster_performance_consistency (moved to core)

mod filter_out_conflicts_and_append_performance_tests {
    use super::*;

    #[tokio::test]
    async fn test_filter_out_conflicts_performance_consistent_across_flush_intervals_fresh_cluster()
    {
        // Test configuration
        let test_cases = vec![
            (10, 50),   // 10ms interval, 50ms max duration
            (100, 50),  // 100ms interval, 50ms max duration
            (1000, 50), // 1000ms interval, 50ms max duration
        ];

        for (interval_ms, max_duration_ms) in test_cases {
            // Create MemFirst storage with batch policy
            let config = PersistenceConfig {
                strategy: PersistenceStrategy::MemFirst,
                flush_policy: FlushPolicy::Batch {
                    threshold: 1000,
                    interval_ms,
                },
                max_buffered_entries: 1000,
                ..Default::default()
            };

            let temp_dir = tempdir().unwrap();
            let path = temp_dir.path().to_path_buf();

            let (log, receiver) = BufferedRaftLog::<
                RaftTypeConfig<FileStorageEngine, FileStateMachine>,
            >::new(
                1, config, Arc::new(FileStorageEngine::new(path).unwrap())
            );
            let log = log.start(receiver);

            // Populate with test data (1000 entries)
            let mut entries = vec![];
            for i in 1..=1000 {
                entries.push(Entry {
                    index: i,
                    term: 1,
                    payload: Some(EntryPayload::command(Bytes::from(vec![0; 256]))), // 256B payload
                });
            }
            log.append_entries(entries.clone()).await.unwrap();

            // Measure conflict resolution performance
            let start = Instant::now();
            log.filter_out_conflicts_and_append(
                0, // prev_log_index
                0, // prev_log_term
                vec![Entry {
                    index: 501,
                    term: 1,
                    payload: Some(EntryPayload::command(Bytes::from(vec![1; 256]))),
                }],
            )
            .await
            .unwrap();

            let duration = start.elapsed().as_millis() as u64;
            println!("Interval {interval_ms}ms: Took {duration}ms");

            // Verify performance consistency
            assert!(
                duration <= max_duration_ms,
                "Duration {duration}ms exceeds max {max_duration_ms}ms for {interval_ms}ms interval"
            );

            // Verify correctness
            assert!(log.entry(500).unwrap().is_none());
            assert!(log.entry(501).unwrap().is_some());
            assert!(log.entry(502).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_filter_out_conflicts_performance_consistent_across_flush_intervals() {
        // Test configuration
        let test_cases = vec![
            (10, 50),   // 10ms interval, 50ms max duration
            (100, 50),  // 100ms interval, 50ms max duration
            (1000, 50), // 1000ms interval, 50ms max duration
        ];

        for (interval_ms, max_duration_ms) in test_cases {
            // Create MemFirst storage with batch policy
            let config = PersistenceConfig {
                strategy: PersistenceStrategy::MemFirst,
                flush_policy: FlushPolicy::Batch {
                    threshold: 1000,
                    interval_ms,
                },
                max_buffered_entries: 1000,
                ..Default::default()
            };

            let temp_dir = tempdir().unwrap();
            let path = temp_dir.path().to_path_buf();

            let (log, receiver) = BufferedRaftLog::<
                RaftTypeConfig<FileStorageEngine, FileStateMachine>,
            >::new(
                1, config, Arc::new(FileStorageEngine::new(path).unwrap())
            );
            let log = log.start(receiver);

            // Populate with test data (1000 entries)
            let mut entries = vec![];
            for i in 1..=1000 {
                entries.push(Entry {
                    index: i,
                    term: 1,
                    payload: Some(EntryPayload::command(Bytes::from(vec![0; 256]))), // 256B payload
                });
            }
            log.append_entries(entries.clone()).await.unwrap();

            // Measure conflict resolution performance
            let start = Instant::now();
            log.filter_out_conflicts_and_append(
                500, // prev_log_index
                1,   // prev_log_term
                vec![Entry {
                    index: 501,
                    term: 1,
                    payload: Some(EntryPayload::command(Bytes::from(vec![1; 256]))),
                }],
            )
            .await
            .unwrap();

            let duration = start.elapsed().as_millis() as u64;
            println!("Interval {interval_ms}ms: Took {duration}ms");

            // Verify performance consistency
            assert!(
                duration <= max_duration_ms,
                "Duration {duration}ms exceeds max {max_duration_ms}ms for {interval_ms}ms interval"
            );

            // Verify correctness
            assert!(log.entry(500).unwrap().is_some());
            assert!(log.entry(501).unwrap().is_some());
            assert!(log.entry(502).unwrap().is_none()); // Conflict removed
        }
    }
}

// TODO: Extract remaining real I/O performance tests from legacy_all_tests.rs:
// - test_last_entry_id_performance
// - test_remove_range_performance
// - test_performance_benchmarks
// - test_read_performance_remains_lockfree
//
// Note: Mock-based performance tests (test_reset_performance_during_active_flush,
// test_filter_conflicts_performance_during_flush, test_fresh_cluster_performance_consistency)
// have been moved to d-engine-core unit tests since they require controllable delays.
