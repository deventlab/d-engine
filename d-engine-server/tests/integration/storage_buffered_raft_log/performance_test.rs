//! Performance benchmark integration tests for BufferedRaftLog
//!
//! These tests measure real I/O performance with FileStorageEngine.
//! Most tests are marked with #[ignore] and can be run explicitly or
//! skipped in CI environments.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::{FlushPolicy, PersistenceStrategy, RaftLog};
use d_engine_proto::common::{Entry, EntryPayload};
use tokio::time::Instant;

use super::TestContext;

// TODO: Extract 9 performance tests from legacy_all_tests.rs:
// - test_filter_out_conflicts_performance_consistent_across_flush_intervals_fresh_cluster
// - test_filter_out_conflicts_performance_consistent_across_flush_intervals
// - test_reset_performance_during_active_flush
// - test_filter_conflicts_performance_during_flush
// - test_fresh_cluster_performance_consistency
// - test_last_entry_id_performance
// - test_remove_range_performance
// - test_performance_benchmarks
// - test_read_performance_remains_lockfree
