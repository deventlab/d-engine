//! Storage-level integration tests for BufferedRaftLog
//!
//! These tests verify BufferedRaftLog integration with FileStorageEngine
//! at the storage layer, including compaction and storage-specific operations.

use d_engine_core::{FlushPolicy, PersistenceStrategy, RaftLog};
use d_engine_proto::common::LogId;

use super::TestContext;

// TODO: Extract 1 storage integration test from legacy_all_tests.rs:
// - test_log_compaction

#[tokio::test]
async fn test_log_compaction() {
    let ctx = TestContext::new(
        PersistenceStrategy::DiskFirst,
        FlushPolicy::Immediate,
        "test_log_compaction",
    );
    ctx.append_entries(1, 100, 1).await;

    // Compact first 50 entries
    ctx.raft_log.purge_logs_up_to(LogId { index: 50, term: 1 }).await.unwrap();

    // Verify compaction
    assert!(ctx.raft_log.entry(25).unwrap().is_none());
    assert_eq!(ctx.raft_log.first_entry_id(), 51);
    assert_eq!(ctx.raft_log.durable_index(), 100);
}
