use crate::proto::client::write_command::Delete;
use crate::proto::client::write_command::Insert;
use crate::proto::client::write_command::Operation;
use crate::proto::client::WriteCommand;
use crate::proto::common::entry_payload::Payload;
use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::proto::storage::SnapshotMetadata;
use crate::Result;
use crate::StateMachine;
use crate::StorageError;
use parking_lot::RwLock;
use prost::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

#[derive(Default, Debug)]
pub struct MemoryStateMachine {
    // Key-value storage
    data: RwLock<HashMap<Vec<u8>, (Vec<u8>, u64)>>, // (value, term)

    // Raft state
    last_applied_index: AtomicU64,
    last_applied_term: AtomicU64,

    last_snapshot_metadata: RwLock<Option<SnapshotMetadata>>,

    // Operational state
    running: AtomicBool,

    // Node identifier for metrics
    node_id: u32,
}

#[async_trait]
impl StateMachine for MemoryStateMachine {
    fn start(&self) -> Result<()> {
        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let data = self.data.read();
        Ok(data.get(key_buffer).map(|(value, _)| value.clone()))
    }

    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64> {
        let data = self.data.read();
        data.values().find(|(_, index)| *index == entry_id).map(|(_, term)| *term)
    }

    fn apply_chunk(
        &self,
        chunk: Vec<Entry>,
    ) -> Result<()> {
        trace!("Applying chunk: {:?}.", chunk);

        let mut highest_index_entry: Option<LogId> = None;
        let mut data = self.data.write();

        for entry in chunk {
            assert!(entry.payload.is_some(), "Entry payload should not be None!");

            // 确保 entries 是递增的
            if let Some(prev) = &highest_index_entry {
                assert!(
                    entry.index > prev.index,
                    "apply_chunk: received unordered entry at index {} (prev={})",
                    entry.index,
                    prev.index
                );
            }
            highest_index_entry = Some(LogId {
                index: entry.index,
                term: entry.term,
            });

            match entry.payload.unwrap().payload {
                Some(Payload::Noop(_)) => {
                    debug!("Handling NOOP command at index {}", entry.index);
                }
                Some(Payload::Command(bytes)) => match WriteCommand::decode(&bytes[..]) {
                    Ok(write_cmd) => match write_cmd.operation {
                        Some(Operation::Insert(Insert { key, value })) => {
                            debug!(
                                "[Node-{}] Applying INSERT at index {}: {:?}",
                                self.node_id, entry.index, key
                            );
                            data.insert(key, (value, entry.term));
                        }
                        Some(Operation::Delete(Delete { key })) => {
                            debug!(
                                "[Node-{}] Applying DELETE at index {}: {:?}",
                                self.node_id, entry.index, key
                            );
                            data.remove(&key);
                        }
                        None => {
                            warn!(
                                "[Node-{}] WriteCommand without operation at index {}",
                                self.node_id, entry.index
                            );
                        }
                    },
                    Err(e) => {
                        error!(
                            "[Node-{}] Failed to decode WriteCommand at index {}: {:?}",
                            self.node_id, entry.index, e
                        );
                        return Err(StorageError::SerializationError(e.to_string()).into());
                    }
                },
                Some(Payload::Config(_config_change)) => {
                    debug!(
                        "[Node-{}] Ignoring config change at index {}",
                        self.node_id, entry.index
                    );
                }
                None => panic!("Entry payload variant should not be None!"),
            }

            info!("[{}]- COMMITTED_LOG_METRIC: {} ", self.node_id, entry.index);
        }

        if let Some(log_id) = highest_index_entry {
            debug!(
                "[Node-{}] State machine - updated last_applied: {:?}",
                self.node_id, log_id
            );
            self.update_last_applied(log_id);
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.data.read().len()
    }

    fn update_last_applied(
        &self,
        last_applied: LogId,
    ) {
        self.last_applied_index.store(last_applied.index, Ordering::SeqCst);
        self.last_applied_term.store(last_applied.term, Ordering::SeqCst);
    }

    fn last_applied(&self) -> LogId {
        LogId {
            index: self.last_applied_index.load(Ordering::SeqCst),
            term: self.last_applied_term.load(Ordering::SeqCst),
        }
    }

    fn persist_last_applied(
        &self,
        last_applied: LogId,
    ) -> Result<()> {
        self.update_last_applied(last_applied);
        Ok(())
    }

    fn update_last_snapshot_metadata(
        &self,
        snapshot_metadata: &SnapshotMetadata,
    ) -> Result<()> {
        *self.last_snapshot_metadata.write() = Some(snapshot_metadata.clone());
        Ok(())
    }

    fn snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        self.last_snapshot_metadata.read().clone()
    }

    fn persist_last_snapshot_metadata(
        &self,
        snapshot_metadata: &SnapshotMetadata,
    ) -> Result<()> {
        self.update_last_snapshot_metadata(snapshot_metadata)
    }

    async fn apply_snapshot_from_file(
        &self,
        metadata: &SnapshotMetadata,
        _snapshot_path: std::path::PathBuf,
    ) -> Result<()> {
        // For memory-based SM, we can simply reset state and update metadata
        let mut data = self.data.write();
        data.clear();
        *self.last_snapshot_metadata.write() = Some(metadata.clone());
        self.update_last_applied(metadata.last_included.unwrap());
        Ok(())
    }

    async fn generate_snapshot_data(
        &self,
        _new_snapshot_dir: std::path::PathBuf,
        last_included: LogId,
    ) -> Result<[u8; 32]> {
        // For memory SM, return dummy checksum
        let metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: vec![0; 32],
        };
        self.update_last_snapshot_metadata(&metadata)?;
        Ok([0; 32])
    }

    fn save_hard_state(&self) -> Result<()> {
        Ok(()) // No-op for memory-based SM
    }

    fn flush(&self) -> Result<()> {
        Ok(()) // No-op for memory-based SM
    }
}

// Helper implementation
impl MemoryStateMachine {
    pub fn new(node_id: u32) -> Self {
        Self {
            node_id,
            running: AtomicBool::new(true),
            ..Default::default()
        }
    }
}
