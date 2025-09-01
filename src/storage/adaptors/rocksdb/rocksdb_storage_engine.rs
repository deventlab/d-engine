use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::Error;
use crate::HardState;
use crate::LogStore;
use crate::MetaStore;
use crate::StorageEngine;
use crate::StorageError;
use prost::Message;
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, DB};
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tonic::async_trait;
use tracing::instrument;

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
        // Configure RocksDB options for high performance
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_max_write_buffer_number(4);
        opts.set_min_write_buffer_number_to_merge(2);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

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

        let iter = self.db.iterator_cf(&cf, IteratorMode::From(&start_key, Direction::Forward));

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::DbError(e.to_string()))?;
            batch.delete_cf(&cf, &key);
        }

        self.db.write(batch).map_err(|e| StorageError::DbError(e.to_string()))?;

        // Update last index
        if from_index > 0 {
            // We need to find the new last index
            let iter = self.db.iterator_cf(&cf, IteratorMode::End);
            if let Some(Ok((key, _))) = iter.last() {
                if key.len() == 8 {
                    let new_last_index = u64::from_be_bytes([
                        key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                    ]);
                    self.last_index.store(new_last_index, Ordering::SeqCst);
                } else {
                    self.last_index.store(0, Ordering::SeqCst);
                }
            } else {
                self.last_index.store(0, Ordering::SeqCst);
            }
        } else {
            self.last_index.store(0, Ordering::SeqCst);
        }

        Ok(())
    }

    #[instrument(skip(self))]
    fn flush(&self) -> Result<(), Error> {
        self.db.flush().map_err(|e| StorageError::DbError(e.to_string()))?;
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
        self.db.flush().map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }
}
