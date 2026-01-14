//! Integration tests for BufferedRaftLog with real FileStorageEngine
//!
//! These tests verify BufferedRaftLog behavior with actual disk I/O,
//! crash recovery semantics, and performance characteristics.
//!
//! ## Test Modules
//!
//! - `crash_recovery_test`: Real disk persistence and crash recovery
//! - `performance_test`: I/O performance benchmarks
//! - `stress_test`: High concurrency stress testing
//! - `storage_integration_test`: Storage-level integration

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::{
    BufferedRaftLog, FlushPolicy, PersistenceConfig, PersistenceStrategy, RaftLog, alias::ROF,
};
use d_engine_proto::common::{Entry, EntryPayload};
use d_engine_server::{FileStateMachine, FileStorageEngine, node::RaftTypeConfig};
use tempfile::tempdir;

mod crash_recovery_test;
mod performance_test;
mod storage_integration_test;
mod stress_test;

/// Test context with real FileStorageEngine for integration tests
pub struct TestContext {
    pub raft_log: Arc<ROF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>,
    pub storage: Arc<FileStorageEngine>,
    pub _temp_dir: Option<tempfile::TempDir>,
    pub strategy: PersistenceStrategy,
    pub flush_policy: FlushPolicy,
    pub path: String,
}

impl TestContext {
    /// Create new test context with FileStorageEngine
    pub fn new(
        strategy: PersistenceStrategy,
        flush_policy: FlushPolicy,
        instance_id: &str,
    ) -> Self {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().to_path_buf().join(instance_id);
        let storage = Arc::new(FileStorageEngine::new(path.clone()).unwrap());

        let (raft_log, receiver) = BufferedRaftLog::new(
            1,
            PersistenceConfig {
                strategy: strategy.clone(),
                flush_policy: flush_policy.clone(),
                max_buffered_entries: 1000,
                ..Default::default()
            },
            storage.clone(),
        );
        let raft_log = raft_log.start(receiver);

        // Small delay to ensure processor is ready
        std::thread::sleep(Duration::from_millis(10));

        Self {
            path: path.to_str().unwrap().to_string(),
            raft_log,
            storage,
            strategy,
            flush_policy,
            _temp_dir: Some(temp_dir),
        }
    }

    /// Simulate crash recovery by creating new context from same storage path
    pub fn recover_from_crash(&self) -> Self {
        let temp_dir = tempdir().unwrap();
        let storage = Arc::new(FileStorageEngine::new(PathBuf::from(self.path.clone())).unwrap());

        let (raft_log, receiver) = BufferedRaftLog::new(
            1,
            PersistenceConfig {
                strategy: self.strategy.clone(),
                flush_policy: self.flush_policy.clone(),
                max_buffered_entries: 1000,
                ..Default::default()
            },
            storage.clone(),
        );
        let raft_log = raft_log.start(receiver);

        std::thread::sleep(Duration::from_millis(10));

        Self {
            raft_log,
            storage,
            strategy: self.strategy.clone(),
            flush_policy: self.flush_policy.clone(),
            _temp_dir: Some(temp_dir),
            path: self.path.clone(),
        }
    }

    /// Helper to append a batch of entries
    pub async fn append_entries(
        &self,
        start: u64,
        count: u64,
        term: u64,
    ) {
        let entries: Vec<_> = (start..start + count)
            .map(|index| Entry {
                index,
                term,
                payload: Some(EntryPayload::command(Bytes::from(b"data".to_vec()))),
            })
            .collect();

        self.raft_log.append_entries(entries).await.unwrap();
    }
}
