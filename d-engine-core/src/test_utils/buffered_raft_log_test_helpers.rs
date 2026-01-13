//! Test helpers for BufferedRaftLog testing
//!
//! Provides utilities to simplify BufferedRaftLog unit tests:
//! - Test context management
//! - Mock entry generation
//! - Crash recovery simulation

use std::sync::Arc;

use bytes::Bytes;
use d_engine_proto::common::{Entry, EntryPayload};

use crate::{
    BufferedRaftLog, FlushPolicy, MockStorageEngine, MockTypeConfig, PersistenceConfig,
    PersistenceStrategy, RaftLog,
};

/// Test context for BufferedRaftLog tests
pub struct BufferedRaftLogTestContext {
    pub raft_log: Arc<BufferedRaftLog<MockTypeConfig>>,
    pub storage: Arc<MockStorageEngine>,
    pub strategy: PersistenceStrategy,
    pub flush_policy: FlushPolicy,
    pub instance_id: String,
}

impl BufferedRaftLogTestContext {
    /// Create a new test context with specified strategy and flush policy
    pub fn new(
        strategy: PersistenceStrategy,
        flush_policy: FlushPolicy,
        instance_id: &str,
    ) -> Self {
        let storage = Arc::new(MockStorageEngine::with_id(instance_id.to_string()));

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
        std::thread::sleep(std::time::Duration::from_millis(10));

        Self {
            raft_log,
            storage,
            strategy,
            flush_policy,
            instance_id: instance_id.to_string(),
        }
    }

    /// Helper to append a batch of entries with specified range and term
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

    /// Simulate crash recovery from the same storage instance
    pub fn recover_from_crash(&self) -> Self {
        // Use same instance ID to recover data from thread_local storage
        let storage = Arc::new(MockStorageEngine::with_id(self.instance_id.clone()));

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

        // Small delay to ensure processor is ready
        std::thread::sleep(std::time::Duration::from_millis(10));

        Self {
            raft_log,
            storage,
            strategy: self.strategy.clone(),
            flush_policy: self.flush_policy.clone(),
            instance_id: self.instance_id.clone(),
        }
    }
}

/// Generate mock log entries with sequential indexes
pub fn mock_entries(
    start: u64,
    count: u64,
    term: u64,
) -> Vec<Entry> {
    (start..start + count)
        .map(|index| Entry {
            index,
            term,
            payload: Some(EntryPayload::command(Bytes::from(
                format!("data_{}", index).into_bytes(),
            ))),
        })
        .collect()
}

/// Generate empty mock entries (no payload)
pub fn mock_empty_entries(
    start: u64,
    count: u64,
    term: u64,
) -> Vec<Entry> {
    (start..start + count)
        .map(|index| Entry {
            index,
            term,
            payload: None,
        })
        .collect()
}

/// Insert single entry helper
pub async fn insert_single_entry(
    raft_log: &Arc<BufferedRaftLog<MockTypeConfig>>,
    index: u64,
    term: u64,
) {
    let entry = Entry {
        index,
        term,
        payload: None,
    };
    raft_log.insert_batch(vec![entry]).await.expect("insert should succeed");
}

/// Generate mock insert command payload bytes
fn mock_insert_command_payload(ids: Vec<u64>) -> Bytes {
    let commands: Vec<String> = ids.iter().map(|id| format!("insert_{}", id)).collect();
    Bytes::from(commands.join(","))
}

/// Simulate inserting command entries into the log
///
/// Creates command entries with pre-allocated indexes and appends them to the log.
/// Each ID becomes a command payload with the given term.
pub async fn simulate_insert_command(
    raft_log: &Arc<BufferedRaftLog<MockTypeConfig>>,
    ids: Vec<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in ids {
        let entry = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            payload: Some(EntryPayload::command(mock_insert_command_payload(vec![id]))),
        };
        entries.push(entry);
    }
    raft_log.insert_batch(entries).await.unwrap();
    raft_log.flush().await.unwrap();
}

/// Simulate deleting entries from the log for a range of IDs
///
/// Creates delete command entries for each ID in the specified range and appends
/// them to the log. Each ID in the range becomes a separate delete command entry.
pub async fn simulate_delete_command(
    raft_log: &Arc<BufferedRaftLog<MockTypeConfig>>,
    id_range: std::ops::RangeInclusive<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in id_range {
        let entry = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            payload: Some(EntryPayload::command(Bytes::from(format!("delete_{}", id)))),
        };
        entries.push(entry);
    }
    raft_log.insert_batch(entries).await.unwrap();
    raft_log.flush().await.unwrap();
}
