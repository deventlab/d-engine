//! Test helpers for RaftLogCore testing
//!
//! Provides utilities to simplify RaftLogCore unit tests. Entry-generation
//! and log-mutation helpers (`mock_entries`, `simulate_insert_command`, ...)
//! are generic over `L: RaftLog`, so they live here once instead of being
//! duplicated per struct.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use d_engine_proto::common::{Entry, EntryPayload};

use crate::{MockStorageEngine, MockTypeConfig, RaftLog, RaftLogCore};

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
    /// dedicated IO thread to wait on, so no startup sleep is needed here.
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
                format!("data_{index}").into_bytes(),
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

/// Insert single entry helper. Generic over `L: RaftLog`.
pub async fn insert_single_entry<L: RaftLog>(
    raft_log: &Arc<L>,
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
    let commands: Vec<String> = ids.iter().map(|id| format!("insert_{id}")).collect();
    Bytes::from(commands.join(","))
}

/// Simulate inserting command entries into the log
///
/// Creates command entries with pre-allocated indexes and appends them to the
/// log. Each ID becomes a command payload with the given term.
pub async fn simulate_insert_command<L: RaftLog>(
    raft_log: &Arc<L>,
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
/// Creates delete command entries for each ID in the specified range and
/// appends them to the log. Each ID in the range becomes a separate delete
/// command entry.
pub async fn simulate_delete_command<L: RaftLog>(
    raft_log: &Arc<L>,
    id_range: std::ops::RangeInclusive<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in id_range {
        let entry = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            payload: Some(EntryPayload::command(Bytes::from(format!("delete_{id}")))),
        };
        entries.push(entry);
    }
    raft_log.insert_batch(entries).await.unwrap();
    raft_log.flush().await.unwrap();
}

/// Stands in for `raft.rs`'s `InternalEvent::FsyncCompleted` handler, which
/// `RaftLogCore`-only unit tests don't have running. `durable_index` only
/// advances when something calls `try_advance_durable_index(mark)` in
/// response to that event — `FsyncWorker::notify_fsync_completed` only
/// *sends* the event, it never writes `durable_index` itself. A test that
/// registers a `log_flush_tx` and wants to see `durable_index()` advance
/// must drain that channel through this helper.
pub fn drain_and_apply_fsync_completions<L: RaftLog>(
    raft_log: &Arc<L>,
    log_flush_rx: &mut tokio::sync::mpsc::UnboundedReceiver<crate::InternalEvent>,
) {
    while let Ok(event) = log_flush_rx.try_recv() {
        if let crate::InternalEvent::FsyncCompleted { mark, sent_at: _ } = event {
            raft_log.try_advance_durable_index(mark);
        }
    }
}

/// Like `drain_and_apply_fsync_completions`, but waits: applies `FsyncCompleted`
/// events as they arrive until `durable_index() >= target`. Panics after `timeout`,
/// so a fsync that never completes fails loudly instead of racing a fixed sleep.
pub async fn wait_for_durable_index<L: RaftLog>(
    raft_log: &Arc<L>,
    log_flush_rx: &mut tokio::sync::mpsc::UnboundedReceiver<crate::InternalEvent>,
    target: u64,
    timeout: std::time::Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    while raft_log.durable_index() < target {
        let event = tokio::time::timeout_at(deadline, log_flush_rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "durable_index stuck at {} (< {target}) after {timeout:?}",
                    raft_log.durable_index()
                )
            })
            .expect("log_flush channel closed before durable_index reached target");
        if let crate::InternalEvent::FsyncCompleted { mark, sent_at: _ } = event {
            raft_log.try_advance_durable_index(mark);
        }
    }
}
