use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use d_engine_core::Error;
use d_engine_core::HardState;
use d_engine_core::StorageEngine;
use d_engine_core::StorageError;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use prost::Message;
use rocksdb::Cache;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::DB;
use rocksdb::Direction;
use rocksdb::IteratorMode;
use rocksdb::WriteBatch;
use rocksdb::WriteOptions;
use tracing::instrument;

use super::LOG_CF;
use super::META_CF;

const HARD_STATE_KEY: &[u8] = b"hard_state";
const PURGE_BOUNDARY_KEY: &[u8] = b"purge_boundary";

/// RocksDB-based Raft log storage
///
/// High-performance storage engine using RocksDB for durability.
/// Recommended for production deployments.
///
/// Open it together with the state machine with [`super::open_rocksdb`], or on its own with
/// [`RocksDBStorageEngine::new`] when the log should live in a directory of your choice.
///
/// # Features
///
/// - ACID transactions
/// - Compression support
/// - Configurable write options
/// - High write throughput
#[derive(Debug)]
pub struct RocksDBStorageEngine {
    db: DB,
    last_index: AtomicU64,
}

impl RocksDBStorageEngine {
    /// Opens (or creates) a dedicated RocksDB instance for log + meta storage.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db_opts = super::base_db_options();

        let cache = Cache::new_lru_cache(128 * 1024 * 1024);
        let log_cf = ColumnFamilyDescriptor::new(LOG_CF, super::log_cf_options(&cache));
        let meta_cf = ColumnFamilyDescriptor::new(META_CF, super::meta_cf_options(&cache));

        let db = DB::open_cf_descriptors(&db_opts, path, vec![log_cf, meta_cf])
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        let mut last_index = 0;

        // Find the last index by seeking to the end and reading the first (largest) key
        // IteratorMode::End positions at the end, next() gives us the largest key
        if let Some(cf) = db.cf_handle(LOG_CF) {
            let mut iter = db.iterator_cf(&cf, IteratorMode::End);
            if let Some(Ok((key, _))) = iter.next()
                && key.len() == 8
            {
                last_index = u64::from_be_bytes([
                    key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                ]);
            }
        }

        Ok(Self {
            db,
            last_index: AtomicU64::new(last_index),
        })
    }

    /// Helper: convert index to big-endian bytes
    #[inline]
    fn index_to_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }
}

impl Drop for RocksDBStorageEngine {
    fn drop(&mut self) {
        // WAL fsync makes LOG_CF and META_CF (hard_state, purge boundary) durable — memtable
        // content survives via WAL replay on next open even without the flushes below. They are an
        // optimization only: they shrink the WAL that must be replayed on the next restart.
        // db.flush() (no CF argument) targets RocksDB's unused "default" CF and would be a no-op
        // here — each CF must be named explicitly.
        if let Err(e) = self.db.flush_wal(true) {
            tracing::error!("Failed to flush WAL on drop: {}", e);
        }
        for cf_name in [LOG_CF, META_CF] {
            match self.db.cf_handle(cf_name) {
                Some(cf) => {
                    if let Err(e) = self.db.flush_cf(&cf) {
                        tracing::error!("Failed to flush {} memtable on drop: {}", cf_name, e);
                    } else {
                        tracing::debug!("RocksDBStorageEngine flushed {} on drop", cf_name);
                    }
                }
                None => tracing::error!("Failed to flush on drop: {} not found", cf_name),
            }
        }
    }
}

#[async_trait]
impl StorageEngine for RocksDBStorageEngine {
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
        let mut value_buf = Vec::new();

        for entry in entries {
            let key = Self::index_to_key(entry.index);
            value_buf.clear();
            entry
                .encode(&mut value_buf)
                .map_err(|e| StorageError::SerializationError(e.to_string()))?;
            batch.put_cf(&cf, key, &value_buf);
            max_index = max_index.max(entry.index);
        }

        let mut write_opts = WriteOptions::default();
        // sync=false: write to OS page cache only. The IO thread calls flush_wal(true)
        // (Method C drain-then-fsync) to make entries crash-safe in a single batched fsync.
        write_opts.set_sync(false);
        self.db
            .write_opt(&batch, &write_opts)
            .map_err(|e| StorageError::DbError(e.to_string()))?;

        if max_index > 0 {
            self.last_index.store(max_index, Ordering::SeqCst);
        }

        Ok(())
    }

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

        self.db.write(&batch).map_err(|e| StorageError::DbError(e.to_string()))?;

        // Persist purge boundary to META_CF for crash recovery.
        // RaftLogCore::new() reads this on restart to restore last_purged_index/term
        // so that entry_term(last_purged_index) returns the correct term after restart.
        if let Some(cf_meta) = self.db.cf_handle(META_CF) {
            let encoded = cutoff_index.encode_to_vec();
            self.db
                .put_cf(&cf_meta, PURGE_BOUNDARY_KEY, encoded)
                .map_err(|e| StorageError::DbError(e.to_string()))?;
        }

        Ok(())
    }

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

        self.db.write(&batch).map_err(|e| StorageError::DbError(e.to_string()))?;

        // Update last_index: The new last_index should be from_index - 1
        // But if from_index is 0 or 1, last_index should be 0
        let new_last_index = from_index.saturating_sub(1);

        self.last_index.store(new_last_index, Ordering::SeqCst);

        Ok(())
    }

    /// Atomically truncate from `from_index` and persist `new_entries` in one WriteBatch.
    ///
    /// Uses `delete_range_cf` (O(1) range tombstone) + puts in a single commit,
    /// eliminating the two-batch window of the default implementation.
    async fn replace_range(
        &self,
        from_index: u64,
        new_entries: Vec<Entry>,
    ) -> Result<u64, Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        let mut batch = WriteBatch::default();

        // delete_range_cf: single range tombstone, compaction reclaims space later
        let start_key = Self::index_to_key(from_index);
        let end_key = Self::index_to_key(u64::MAX);
        batch.delete_range_cf(&cf, start_key, end_key);

        let new_last_index =
            new_entries.last().map(|e| e.index).unwrap_or(from_index.saturating_sub(1));
        for entry in &new_entries {
            batch.put_cf(&cf, Self::index_to_key(entry.index), entry.encode_to_vec());
        }

        self.db.write(&batch).map_err(|e| StorageError::DbError(e.to_string()))?;
        self.last_index.store(new_last_index, Ordering::SeqCst);
        Ok(new_last_index)
    }

    fn load_purge_boundary(&self) -> Result<Option<LogId>, Error> {
        let cf_meta = self
            .db
            .cf_handle(META_CF)
            .ok_or_else(|| StorageError::DbError("Meta column family not found".to_string()))?;
        match self
            .db
            .get_cf(&cf_meta, PURGE_BOUNDARY_KEY)
            .map_err(|e| StorageError::DbError(e.to_string()))?
        {
            Some(bytes) => LogId::decode(&*bytes)
                .map(Some)
                .map_err(|e| StorageError::SerializationError(e.to_string()).into()),
            None => Ok(None),
        }
    }

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

    fn flush(&self) -> Result<(), Error> {
        // WAL fsync is sufficient for crash-safety: data in WAL can be replayed on restart.
        // memtable flush is RocksDB's internal concern — triggered automatically in background.
        let t0 = std::time::Instant::now();
        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::DbError(format!("Failed to flush WAL: {e}")))?;
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        metrics::histogram!("server.storage.rocksdb.wal_flush_ms").record(ms);
        Ok(())
    }

    fn last_index(&self) -> u64 {
        self.last_index.load(Ordering::SeqCst)
    }
}
