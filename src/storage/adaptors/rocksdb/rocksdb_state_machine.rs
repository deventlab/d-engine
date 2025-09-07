use crate::proto::client::write_command::Delete;
use crate::proto::client::write_command::Insert;
use crate::proto::client::write_command::Operation;
use crate::proto::client::WriteCommand;
use crate::proto::common::entry_payload::Payload;
use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::proto::storage::SnapshotMetadata;
use crate::Error;
use crate::StateMachine;
use crate::StorageError;
use parking_lot::RwLock;
use prost::Message;
use rocksdb::Cache;
use rocksdb::{IteratorMode, Options, WriteBatch, DB};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tonic::async_trait;
use tracing::{debug, error, info, instrument, warn};

const STATE_MACHINE_CF: &str = "state_machine";
const STATE_MACHINE_META_CF: &str = "state_machine_meta";
const LAST_APPLIED_INDEX_KEY: &[u8] = b"last_applied_index";
const LAST_APPLIED_TERM_KEY: &[u8] = b"last_applied_term";
const SNAPSHOT_METADATA_KEY: &[u8] = b"snapshot_metadata";

/// RocksDB-based state machine implementation
#[derive(Debug)]
pub struct RocksDBStateMachine {
    db: Arc<DB>,
    is_serving: AtomicBool,
    last_applied_index: AtomicU64,
    last_applied_term: AtomicU64,
    last_snapshot_metadata: RwLock<Option<SnapshotMetadata>>,
}

impl RocksDBStateMachine {
    /// Creates a new RocksDB-based state machine
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        // Configure high-performance RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Memory and write optimization
        opts.set_max_write_buffer_number(6); // Increase the number of write buffers
        opts.set_min_write_buffer_number_to_merge(3); // Increase the merge threshold
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB write buffer

        // Compression optimization
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        opts.set_compression_options(-14, 0, 0, 0); // LZ4 fast compression

        // Performance Tuning
        opts.set_max_background_jobs(4); // Number of background jobs
        opts.set_max_open_files(5000); // Maximum number of open files
        opts.set_use_direct_io_for_flush_and_compaction(true); // Direct I/O
        opts.set_use_direct_reads(true); // Direct reads

        // Leveled Compaction Configuration
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB base file size
        opts.set_max_bytes_for_level_base(256 * 1024 * 1024); // 256MB base level size

        // Block cache configuration (shared)
        let cache = Cache::new_lru_cache(128 * 1024 * 1024); // 128MB block cache
        opts.set_row_cache(&cache);

        let cfs = vec![STATE_MACHINE_CF, STATE_MACHINE_META_CF];

        let db = DB::open_cf(&opts, path, cfs).map_err(|e| StorageError::DbError(e.to_string()))?;
        let db_arc = Arc::new(db);

        // Load metadata
        let (last_applied_index, last_applied_term) = Self::load_state_machine_metadata(&db_arc)?;
        let last_snapshot_metadata = Self::load_snapshot_metadata(&db_arc)?;

        Ok(Self {
            db: db_arc,
            is_serving: AtomicBool::new(true),
            last_applied_index: AtomicU64::new(last_applied_index),
            last_applied_term: AtomicU64::new(last_applied_term),
            last_snapshot_metadata: RwLock::new(last_snapshot_metadata),
        })
    }

    fn load_state_machine_metadata(db: &Arc<DB>) -> Result<(u64, u64), Error> {
        let cf = db
            .cf_handle(STATE_MACHINE_META_CF)
            .ok_or_else(|| StorageError::DbError("State machine meta CF not found".to_string()))?;

        let index = match db
            .get_cf(&cf, LAST_APPLIED_INDEX_KEY)
            .map_err(|e| StorageError::DbError(e.to_string()))?
        {
            Some(bytes) if bytes.len() == 8 => u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            _ => 0,
        };

        let term = match db
            .get_cf(&cf, LAST_APPLIED_TERM_KEY)
            .map_err(|e| StorageError::DbError(e.to_string()))?
        {
            Some(bytes) if bytes.len() == 8 => u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            _ => 0,
        };

        Ok((index, term))
    }

    fn load_snapshot_metadata(db: &Arc<DB>) -> Result<Option<SnapshotMetadata>, Error> {
        let cf = db
            .cf_handle(STATE_MACHINE_META_CF)
            .ok_or_else(|| StorageError::DbError("State machine meta CF not found".to_string()))?;

        match db
            .get_cf(&cf, SNAPSHOT_METADATA_KEY)
            .map_err(|e| StorageError::DbError(e.to_string()))?
        {
            Some(bytes) => {
                let metadata = bincode::deserialize(&bytes).map_err(StorageError::BincodeError)?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    fn persist_state_machine_metadata(&self) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(STATE_MACHINE_META_CF)
            .ok_or_else(|| StorageError::DbError("State machine meta CF not found".to_string()))?;

        let index = self.last_applied_index.load(Ordering::SeqCst);
        let term = self.last_applied_term.load(Ordering::SeqCst);

        self.db
            .put_cf(&cf, LAST_APPLIED_INDEX_KEY, index.to_be_bytes())
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        self.db
            .put_cf(&cf, LAST_APPLIED_TERM_KEY, term.to_be_bytes())
            .map_err(|e| StorageError::DbError(e.to_string()))?;

        Ok(())
    }

    fn persist_snapshot_metadata(&self) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(STATE_MACHINE_META_CF)
            .ok_or_else(|| StorageError::DbError("State machine meta CF not found".to_string()))?;

        if let Some(metadata) = self.last_snapshot_metadata.read().clone() {
            let bytes = bincode::serialize(&metadata).map_err(StorageError::BincodeError)?;
            self.db
                .put_cf(&cf, SNAPSHOT_METADATA_KEY, bytes)
                .map_err(|e| StorageError::DbError(e.to_string()))?;
        }
        Ok(())
    }

    fn apply_batch(
        &self,
        batch: WriteBatch,
    ) -> Result<(), Error> {
        self.db.write(batch).map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl StateMachine for RocksDBStateMachine {
    fn start(&self) -> Result<(), Error> {
        self.is_serving.store(true, Ordering::SeqCst);
        info!("RocksDB state machine started");
        Ok(())
    }

    fn stop(&self) -> Result<(), Error> {
        self.is_serving.store(false, Ordering::SeqCst);
        info!("RocksDB state machine stopped");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.is_serving.load(Ordering::SeqCst)
    }

    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        let cf = self
            .db
            .cf_handle(STATE_MACHINE_CF)
            .ok_or_else(|| StorageError::DbError("State machine CF not found".to_string()))?;

        match self
            .db
            .get_cf(&cf, key_buffer)
            .map_err(|e| StorageError::DbError(e.to_string()))?
        {
            Some(value) => Ok(Some(value.to_vec())),
            None => Ok(None),
        }
    }

    fn entry_term(
        &self,
        _entry_id: u64,
    ) -> Option<u64> {
        // In RocksDB state machine, we don't store term per key. This method is not typically used.
        // If needed, we might need to change the design to store term along with value.
        None
    }

    #[instrument(skip(self, chunk))]
    async fn apply_chunk(
        &self,
        chunk: Vec<Entry>,
    ) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(STATE_MACHINE_CF)
            .ok_or_else(|| StorageError::DbError("State machine CF not found".to_string()))?;

        let mut batch = WriteBatch::default();
        let mut highest_index_entry: Option<LogId> = None;

        for entry in chunk {
            assert!(entry.payload.is_some(), "Entry payload should not be None!");

            if let Some(prev) = highest_index_entry {
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
                Some(Payload::Command(data)) => match WriteCommand::decode(&data[..]) {
                    Ok(write_cmd) => match write_cmd.operation {
                        Some(Operation::Insert(Insert { key, value })) => {
                            batch.put_cf(&cf, &key, value);
                        }
                        Some(Operation::Delete(Delete { key })) => {
                            batch.delete_cf(&cf, &key);
                        }
                        None => {
                            warn!("WriteCommand without operation at index {}", entry.index);
                        }
                    },
                    Err(e) => {
                        error!(
                            "Failed to decode WriteCommand at index {}: {:?}",
                            entry.index, e
                        );
                        return Err(StorageError::SerializationError(e.to_string()).into());
                    }
                },
                Some(Payload::Config(_config_change)) => {
                    debug!("Ignoring config change at index {}", entry.index);
                }
                None => panic!("Entry payload variant should not be None!"),
            }
        }

        self.apply_batch(batch)?;

        if let Some(log_id) = highest_index_entry {
            self.update_last_applied(log_id);
        }

        Ok(())
    }

    fn len(&self) -> usize {
        let cf = match self.db.cf_handle(STATE_MACHINE_CF) {
            Some(cf) => cf,
            None => return 0,
        };

        // Note: This is an expensive operation because it iterates over all keys.
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        iter.count()
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
    ) -> Result<(), Error> {
        self.update_last_applied(last_applied);
        self.persist_state_machine_metadata()
    }

    fn update_last_snapshot_metadata(
        &self,
        snapshot_metadata: &SnapshotMetadata,
    ) -> Result<(), Error> {
        *self.last_snapshot_metadata.write() = Some(snapshot_metadata.clone());
        Ok(())
    }

    fn snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        self.last_snapshot_metadata.read().clone()
    }

    fn persist_last_snapshot_metadata(
        &self,
        snapshot_metadata: &SnapshotMetadata,
    ) -> Result<(), Error> {
        self.update_last_snapshot_metadata(snapshot_metadata)?;
        self.persist_snapshot_metadata()
    }

    #[instrument(skip(self))]
    async fn apply_snapshot_from_file(
        &self,
        metadata: &SnapshotMetadata,
        _snapshot_path: std::path::PathBuf,
    ) -> Result<(), Error> {
        // For RocksDB, applying a snapshot from a file might involve replacing the entire DB.
        // This is a complex operation and might require locking.
        // Here, we'll just log a warning as this is a simplified implementation.
        warn!("Applying snapshot from file is not fully implemented for RocksDBStateMachine");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
        last_included: LogId,
    ) -> Result<[u8; 32], Error> {
        // Create a checkpoint in the new_snapshot_dir
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.db)
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        checkpoint
            .create_checkpoint(&new_snapshot_dir)
            .map_err(|e| StorageError::DbError(e.to_string()))?;

        // Update metadata
        let checksum = [0; 32]; // For now, we return a dummy checksum.
        let snapshot_metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: checksum.to_vec(),
        };
        self.persist_last_snapshot_metadata(&snapshot_metadata)?;

        Ok(checksum)
    }

    fn save_hard_state(&self) -> Result<(), Error> {
        self.persist_state_machine_metadata()?;
        self.persist_snapshot_metadata()?;
        Ok(())
    }

    fn flush(&self) -> Result<(), Error> {
        self.db.flush().map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }

    #[instrument(skip(self))]
    async fn reset(&self) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(STATE_MACHINE_CF)
            .ok_or_else(|| StorageError::DbError("State machine CF not found".to_string()))?;

        // Delete all keys in the state machine
        let mut batch = WriteBatch::default();
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::DbError(e.to_string()))?;
            batch.delete_cf(&cf, &key);
        }

        self.db.write(batch).map_err(|e| StorageError::DbError(e.to_string()))?;

        // Reset metadata
        self.last_applied_index.store(0, Ordering::SeqCst);
        self.last_applied_term.store(0, Ordering::SeqCst);
        *self.last_snapshot_metadata.write() = None;

        self.persist_state_machine_metadata()?;
        self.persist_snapshot_metadata()?;

        Ok(())
    }
}
