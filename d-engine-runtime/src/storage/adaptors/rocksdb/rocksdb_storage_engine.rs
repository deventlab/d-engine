use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use prost::Message;
use rocksdb::Cache;
use rocksdb::DB;
use rocksdb::Direction;
use rocksdb::IteratorMode;
use rocksdb::Options;
use rocksdb::WriteBatch;
use tonic::async_trait;
use tracing::instrument;

use d_engine_core::Error;
use d_engine_core::HardState;
use d_engine_core::LogStore;
use d_engine_core::MetaStore;
use d_engine_core::StorageEngine;
use d_engine_core::StorageError;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;

const LOG_CF: &str = "logs";
const META_CF: &str = "meta";
const HARD_STATE_KEY: &[u8] = b"hard_state";

/// RocksDB-based log store implementation
#[derive(Debug)]
pub struct RocksDBLogStore {
    db: Arc<DB>,
    last_index: AtomicU64,
}

/// RocksDB-based metadata store implementation
#[derive(Debug)]
pub struct RocksDBMetaStore {
    db: Arc<DB>,
}

/// Unified RocksDB-based storage engine
#[derive(Debug)]
pub struct RocksDBStorageEngine {
    log_store: Arc<RocksDBLogStore>,
    meta_store: Arc<RocksDBMetaStore>,
}

impl RocksDBStorageEngine {
    /// Creates new RocksDB-based storage engine with column family separation
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        // Configure high-performance RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Memory and write optimization
        opts.set_max_write_buffer_number(4); // Increase the number of write buffers
        opts.set_min_write_buffer_number_to_merge(2); // Increase the merge threshold
        opts.set_write_buffer_size(128 * 1024 * 1024); // 128MB write buffer

        // Compression optimization
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        opts.set_compression_options(-14, 0, 0, 0); // LZ4 fast compression

        // WAL-related optimizations
        opts.set_wal_bytes_per_sync(1024 * 1024); // 1MB sync
        opts.set_manual_wal_flush(true); // manually control WAL flush

        opts.set_use_fsync(false);

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

        // Define column families
        let cfs = vec![LOG_CF, META_CF];

        let db = DB::open_cf(&opts, path, cfs).map_err(|e| StorageError::DbError(e.to_string()))?;
        let db_arc = Arc::new(db);

        let log_store = Arc::new(RocksDBLogStore::new(Arc::clone(&db_arc))?);
        let meta_store = Arc::new(RocksDBMetaStore::new(Arc::clone(&db_arc))?);

        Ok(Self {
            log_store,
            meta_store,
        })
    }

    /// Helper: convert index to big-endian bytes
    #[allow(dead_code)]
    #[inline]
    fn index_to_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    /// Helper: convert key bytes to index
    #[allow(dead_code)]
    #[inline]
    fn key_to_index(key: &[u8]) -> u64 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&key[0..8]);
        u64::from_be_bytes(bytes)
    }
}

impl StorageEngine for RocksDBStorageEngine {
    type LogStore = RocksDBLogStore;
    type MetaStore = RocksDBMetaStore;

    #[inline]
    fn log_store(&self) -> Arc<Self::LogStore> {
        self.log_store.clone()
    }

    #[inline]
    fn meta_store(&self) -> Arc<Self::MetaStore> {
        self.meta_store.clone()
    }
}

impl RocksDBLogStore {
    fn new(db: Arc<DB>) -> Result<Self, Error> {
        let mut last_index = 0;

        // Find the last index by seeking to the last key
        if let Some(cf) = db.cf_handle(LOG_CF) {
            let iter = db.iterator_cf(&cf, IteratorMode::End);
            if let Some(Ok((key, _))) = iter.last() {
                if key.len() == 8 {
                    last_index = u64::from_be_bytes([
                        key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                    ]);
                }
            }
        }

        Ok(Self {
            db,
            last_index: AtomicU64::new(last_index),
        })
    }

    #[inline]
    fn index_to_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }
}

#[async_trait]
impl LogStore for RocksDBLogStore {
    #[instrument(skip(self, entries))]
    async fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        let mut batch = WriteBatch::default();
        let mut max_index = 0;

        for entry in entries {
            let key = Self::index_to_key(entry.index);
            let value = entry.encode_to_vec();
            batch.put_cf(&cf, key, value);
            max_index = max_index.max(entry.index);
        }

        self.db.write(batch).map_err(|e| StorageError::DbError(e.to_string()))?;

        if max_index > 0 {
            self.last_index.store(max_index, Ordering::SeqCst);
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>, Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        let key = Self::index_to_key(index);
        match self.db.get_cf(&cf, key).map_err(|e| StorageError::DbError(e.to_string()))? {
            Some(bytes) => Entry::decode(&*bytes)
                .map(Some)
                .map_err(|e| StorageError::SerializationError(e.to_string()).into()),
            None => Ok(None),
        }
    }

    #[instrument(skip(self))]
    fn get_entries(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>, Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        let start_key = Self::index_to_key(*range.start());
        let _end_key = Self::index_to_key(*range.end());
        let mut entries = Vec::new();

        let iter = self.db.iterator_cf(&cf, IteratorMode::From(&start_key, Direction::Forward));

        for item in iter {
            let (key, value) = item.map_err(|e| StorageError::DbError(e.to_string()))?;

            // Convert key to u64 for comparison
            if key.len() != 8 {
                continue;
            }

            let key_index = u64::from_be_bytes([
                key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
            ]);

            if key_index > *range.end() {
                break;
            }

            let entry = Entry::decode(&*value)
                .map_err(|e| StorageError::SerializationError(e.to_string()))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    #[instrument(skip(self))]
    async fn purge(
        &self,
        cutoff_index: LogId,
    ) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        let start_key = Self::index_to_key(0);
        let _end_key = Self::index_to_key(cutoff_index.index);
        let mut batch = WriteBatch::default();

        let iter = self.db.iterator_cf(&cf, IteratorMode::From(&start_key, Direction::Forward));

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::DbError(e.to_string()))?;

            if key.len() != 8 {
                continue;
            }

            let key_index = u64::from_be_bytes([
                key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
            ]);

            if key_index > cutoff_index.index {
                break;
            }

            batch.delete_cf(&cf, &key);
        }

        self.db.write(batch).map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn truncate(
        &self,
        from_index: u64,
    ) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        let start_key = Self::index_to_key(from_index);
        let mut batch = WriteBatch::default();

        // Get the last_index before deletion
        let current_last_index = self.last_index.load(Ordering::SeqCst);

        // Collect all keys to be deleted
        let mut keys_to_delete = Vec::new();
        let iter = self.db.iterator_cf(&cf, IteratorMode::From(&start_key, Direction::Forward));

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::DbError(e.to_string()))?;
            keys_to_delete.push(key.clone());

            // Check if it is the last key
            if key.len() == 8 {
                let key_index = u64::from_be_bytes([
                    key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                ]);
                if key_index >= current_last_index {
                    break;
                }
            }
        }

        // Batch delete
        for key in keys_to_delete {
            batch.delete_cf(&cf, &key);
        }

        self.db.write(batch).map_err(|e| StorageError::DbError(e.to_string()))?;

        // Update last_index: The new last_index should be from_index - 1
        // But if from_index is 0 or 1, last_index should be 0
        let new_last_index = from_index.saturating_sub(1);

        self.last_index.store(new_last_index, Ordering::SeqCst);

        Ok(())
    }

    #[instrument(skip(self))]
    fn flush(&self) -> Result<(), Error> {
        // Flush WAL first when manual_wal_flush is enabled
        // This ensures write-ahead log is durably persisted before memtable flush
        self.db
            .flush_wal(true) // true = sync WAL to disk
            .map_err(|e| StorageError::DbError(format!("Failed to flush WAL: {e}")))?;

        // Then flush memtables to SST files
        self.db
            .flush()
            .map_err(|e| StorageError::DbError(format!("Failed to flush memtables: {e}")))?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }

    #[instrument(skip(self))]
    async fn reset(&self) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        // Delete all keys in the log column family
        let mut batch = WriteBatch::default();
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::DbError(e.to_string()))?;
            batch.delete_cf(&cf, &key);
        }

        self.db.write(batch).map_err(|e| StorageError::DbError(e.to_string()))?;
        self.last_index.store(0, Ordering::SeqCst);
        Ok(())
    }

    #[instrument(skip(self))]
    fn last_index(&self) -> u64 {
        self.last_index.load(Ordering::SeqCst)
    }
}

impl RocksDBMetaStore {
    fn new(db: Arc<DB>) -> Result<Self, Error> {
        Ok(Self { db })
    }
}

#[async_trait]
impl MetaStore for RocksDBMetaStore {
    #[instrument(skip(self, state))]
    fn save_hard_state(
        &self,
        state: &HardState,
    ) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(META_CF)
            .ok_or_else(|| StorageError::DbError("Meta column family not found".to_string()))?;

        let serialized = bincode::serialize(state).map_err(StorageError::BincodeError)?;

        self.db
            .put_cf(&cf, HARD_STATE_KEY, serialized)
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }

    #[instrument(skip(self))]
    fn load_hard_state(&self) -> Result<Option<HardState>, Error> {
        let cf = self
            .db
            .cf_handle(META_CF)
            .ok_or_else(|| StorageError::DbError("Meta column family not found".to_string()))?;

        match self
            .db
            .get_cf(&cf, HARD_STATE_KEY)
            .map_err(|e| StorageError::DbError(e.to_string()))?
        {
            Some(bytes) => {
                let state = bincode::deserialize(&bytes).map_err(StorageError::BincodeError)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    #[instrument(skip(self))]
    fn flush(&self) -> Result<(), Error> {
        // Flush WAL first for metadata durability
        // Metadata changes (like HardState) MUST survive crashes
        self.db
            .flush_wal(true) // true = sync WAL to disk
            .map_err(|e| StorageError::DbError(format!("Failed to flush meta WAL: {e}")))?;

        // Then flush meta column family memtables
        self.db
            .flush()
            .map_err(|e| StorageError::DbError(format!("Failed to flush meta memtables: {e}")))?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }
}
