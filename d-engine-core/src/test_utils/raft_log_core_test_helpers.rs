//! Test helpers for RaftLogCore testing
//!
//! Provides utilities to simplify RaftLogCore unit tests. Entry-generation
//! and log-mutation helpers (`mock_entries`, `simulate_insert_command`, ...)
//! are shared with `buffered_raft_log_test_helpers` — generic over `L:
//! RaftLog`, so they live there once instead of being duplicated per struct.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::{MockStorageEngine, MockTypeConfig, RaftLog, RaftLogCore};

use super::drain_and_apply_fsync_completions;

/// Test context for RaftLogCore tests
pub struct RaftLogCoreTestContext {
    pub raft_log: Arc<RaftLogCore<MockTypeConfig>>,
    pub storage: Arc<MockStorageEngine>,
    pub instance_id: String,
    log_flush_rx: tokio::sync::mpsc::UnboundedReceiver<crate::InternalEvent>,
}

impl RaftLogCoreTestContext {
    /// Create a new test context.
    ///
    /// `RaftLogCore::new()` executes inline, synchronously — there is no
    /// dedicated IO thread to wait on (unlike `BufferedRaftLogTestContext`,
    /// no startup sleep is needed here).
    pub fn new(instance_id: &str) -> Self {
        let storage = Arc::new(MockStorageEngine::with_id(instance_id.to_string()));
        let (log_flush_tx, log_flush_rx) = tokio::sync::mpsc::unbounded_channel();
        let raft_log = RaftLogCore::new(1, storage.clone(), Some(log_flush_tx), 5000);

        Self {
            raft_log,
            storage,
            instance_id: instance_id.to_string(),
            log_flush_rx,
        }
    }

    /// Stands in for `raft.rs`'s `InternalEvent::FsyncCompleted` handler,
    /// which isn't running in these `RaftLogCore`-only unit tests. Call
    /// after any operation that should make `durable_index` advance
    /// (`append_entries`, `flush`, truncation + resync, ...) and before
    /// asserting on `durable_index()`.
    pub fn drain_fsync_completions(&mut self) {
        drain_and_apply_fsync_completions(&self.raft_log, &mut self.log_flush_rx);
    }

    /// Helper to append a batch of entries with specified range and term
    pub async fn append_entries(
        &self,
        start: u64,
        count: u64,
        term: u64,
    ) {
        let entries: Vec<_> = (start..start + count)
            .map(|index| d_engine_proto::common::Entry {
                index,
                term,
                payload: Some(d_engine_proto::common::EntryPayload::command(
                    bytes::Bytes::from(b"data".to_vec()),
                )),
            })
            .collect();

        self.raft_log.append_entries(entries).await.unwrap();
    }

    /// Create a context where `is_write_durable()=false`.
    ///
    /// Returns the context and a counter incremented on every `flush()` call.
    pub fn new_not_durable(instance_id: &str) -> (Self, Arc<AtomicU64>) {
        let (storage, flush_count) = MockStorageEngine::not_durable(instance_id.to_string());
        let storage = Arc::new(storage);
        let (log_flush_tx, log_flush_rx) = tokio::sync::mpsc::unbounded_channel();
        let raft_log = RaftLogCore::new(1, storage.clone(), Some(log_flush_tx), 5000);

        let ctx = Self {
            raft_log,
            storage,
            instance_id: instance_id.to_string(),
            log_flush_rx,
        };
        (ctx, flush_count)
    }

    /// Simulate crash recovery from the same storage instance
    pub fn recover_from_crash(&self) -> Self {
        let storage = Arc::new(MockStorageEngine::with_id(self.instance_id.clone()));
        let (log_flush_tx, log_flush_rx) = tokio::sync::mpsc::unbounded_channel();
        let raft_log = RaftLogCore::new(1, storage.clone(), Some(log_flush_tx), 5000);

        Self {
            raft_log,
            storage,
            instance_id: self.instance_id.clone(),
            log_flush_rx,
        }
    }
}
