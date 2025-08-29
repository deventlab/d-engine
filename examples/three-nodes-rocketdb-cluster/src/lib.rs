use async_trait::async_trait;
use bytes::Bytes;
use d_engine::proto::common::{Entry, LogId};
use d_engine::{ConvertError, Error, MetaStore, ProstError, StorageEngine, StorageError};
use prost::Message;
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, DB};
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::error;

const LOG_PREFIX: &[u8] = b"log:";
const STATE_PREFIX: &[u8] = b"state:";
const HARD_STATE_KEY: &[u8] = b"hard_state";
const STATE_MACHINE_PREFIX: &[u8] = b"sm:";

/// RocksDB-based storage engine implementation for d-engine
///
/// Implements the `StorageEngine` trait using RocksDB as the underlying storage.
/// Uses key prefixes to separate different data types:
/// - Log entries: `log:<index>`
/// - State machine data: `sm:<key>`
/// - Hard state: `hard_state`
///
/// # Example
/// ```
/// use d_engine::storage::RocksDBEngine;
///
/// let engine = RocksDBEngine::new("/path/to/db").unwrap();
/// engine.persist_entries(vec![entry1, entry2]).unwrap();
/// ```
pub struct RocksDBEngine {
    log_store: Arc<RocksDBLogStore>,
    meta_store: Arc<RocksDBMetaStore>,
}
/// Dedicated log store implementation
pub struct RocksDBLogStore {}

/// Dedicated metadata store implementation
pub struct RocksDBMetaStore {}

impl StorageEngine for RocksDBEngine {
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

#[async_trait::async_trait]
impl LogStore for RocksDBLogStore {
    /// Persists multiple Raft log entries in a batch write
    async fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        let mut batch = WriteBatch::default();

        for entry in entries {
            let key = Self::make_log_key(entry.index);
            let value = entry.encode_to_vec();
            batch.put(key, value);
        }

        db.write(batch).map_err(|e| StorageError::DbError(e.into_string()).into())
    }

    /// Retrieves a single log entry by index
    async fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>, Error> {
        let db = self.db.lock().unwrap();
        let key = Self::make_log_key(index);

        match db.get(&key) {
            Ok(Some(bytes)) => {
                Entry::decode(&*bytes).map(Some).map_err(|e| ProstError::Decode(e).into())
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::DbError(e.into_string()).into()),
        }
    }

    /// Retrieves log entries within an index range (inclusive)
    fn get_entries(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>, Error> {
        let db = self.db.lock().unwrap();
        let start_key = Self::make_log_key(*range.start());
        let end_key = Self::make_log_key(*range.end());
        let mut entries = Vec::new();

        let iter = db.iterator(IteratorMode::From(&start_key, Direction::Forward));
        for (key, value) in iter {
            // res: Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>
            // let (key, value) = res.map_err(|e| StorageError::DbError(e.into_string()))?;

            // Stop if we've passed the end key
            if key.as_ref() > end_key.as_slice() {
                break;
            }

            let entry = Entry::decode(&*value).map_err(|e| ProstError::Decode(e))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Removes logs up to the specified index (inclusive)
    async fn purge(
        &self,
        cutoff_index: LogId,
    ) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        let start_key = Self::make_log_key(0);
        let end_key = Self::make_log_key(cutoff_index.index);
        let mut batch = WriteBatch::default();

        let iter = db.iterator(IteratorMode::From(&start_key, Direction::Forward));
        for (key, _value) in iter {
            // let (key, _) = item.map_err(|e| StorageError::DbError(e.into_string()))?;

            // Stop if we've passed the end key
            if key.as_ref() > end_key.as_slice() {
                break;
            }

            batch.delete(key);
        }

        db.write(batch).map_err(|e| StorageError::DbError(e.into_string()).into())
    }
    /// Truncates logs from specified index onward (inclusive)
    async fn truncate(
        &self,
        from_index: u64,
    ) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        let start_key = Self::make_log_key(from_index);
        let mut batch = WriteBatch::default();

        let iter = db.iterator(IteratorMode::From(&start_key, Direction::Forward));
        for (key, _) in iter {
            // let (key, _) = item.map_err(|e| StorageError::DbError(e.into_string()))?;
            batch.delete(key);
        }

        db.write(batch).map_err(|e| StorageError::DbError(e.into_string()).into())
    }
    /// Flushes all pending writes to disk
    fn flush(&self) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        db.flush().map_err(|e| StorageError::IoError(e.into()).into())
    }

    /// Flushes all pending writes to disk
    async fn flush_async(&self) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        db.flush().map_err(|e| StorageError::IoError(e.into()).into())
    }

    /// Resets the storage by clearing all log entries
    async fn reset(&self) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        let mut batch = WriteBatch::default();

        // Delete all log entries
        let start_key = LOG_PREFIX.to_vec();
        let mut end_key = LOG_PREFIX.to_vec();
        if let Some(last) = end_key.last_mut() {
            *last = last.wrapping_add(1);
        }

        // Batch delete log range
        batch.delete_range(start_key, end_key);

        // Clear hard state
        batch.delete(HARD_STATE_KEY);

        db.write(batch).map_err(|e| StorageError::DbError(e.into_string()).into())
    }

    /// Gets the last log index from storage
    fn last_index(&self) -> u64 {
        let db = self.db.lock().unwrap();
        let mut iter = db.iterator(IteratorMode::End);

        match iter.next() {
            Some((key, _)) => {
                if key.starts_with(LOG_PREFIX) {
                    Self::parse_log_key(&key).unwrap_or(0)
                } else {
                    0
                }
            }
            _ => 0,
        }
    }
}

#[async_trait]
impl MetaStore for RocksDBMetaStore {
    /// Loads hard state from persistent storage
    fn load_hard_state(&self) -> Result<Option<HardState>, Error> {
        let db = self.db.lock().unwrap();
        match db.get(HARD_STATE_KEY) {
            Ok(Some(bytes)) => bincode::deserialize(&bytes)
                .map(Some)
                .map_err(|e| StorageError::BincodeError(e).into()),
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::DbError(e.into_string()).into()),
        }
    }

    /// Persists hard state to storage
    fn save_hard_state(
        &self,
        hard_state: &HardState,
    ) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        let bytes = bincode::serialize(&hard_state).map_err(|e| StorageError::BincodeError(e))?;

        db.put(HARD_STATE_KEY, bytes)
            .map_err(|e| StorageError::DbError(e.into_string()).into())
    }

    /// Flushes all pending writes to disk
    fn flush(&self) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        db.flush().map_err(|e| StorageError::IoError(e.into()).into())
    }

    /// Flushes all pending writes to disk
    async fn flush_async(&self) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        db.flush().map_err(|e| StorageError::IoError(e.into()).into())
    }
}

impl Drop for RocksDBEngine {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            error!("Failed to flush RocksDB on drop: {}", e);
        }
    }
}

impl From<rocksdb::Error> for std::io::Error {
    fn from(e: rocksdb::Error) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
    }
}

impl RocksDBEngine {
    /// Creates a new RocksDB storage instance
    ///
    /// # Arguments
    /// * `path` - Filesystem path for database storage
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Optimize for Raft workload
        opts.set_write_buffer_size(512 * 1024 * 1024); // 512MB write buffer
        opts.set_max_write_buffer_number(4);
        opts.set_target_file_size_base(128 * 1024 * 1024);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let db =
            DB::open(&opts, path).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }

    /// Generates key for log entries: "log:" + big-endian index
    fn make_log_key(index: u64) -> Vec<u8> {
        let mut key = LOG_PREFIX.to_vec();
        key.extend_from_slice(&index.to_be_bytes());
        key
    }

    /// Generates key for state machine: "sm:" + user key
    fn make_sm_key<K: AsRef<[u8]>>(key: K) -> Vec<u8> {
        let mut k = STATE_MACHINE_PREFIX.to_vec();
        k.extend_from_slice(key.as_ref());
        k
    }

    /// Parses index from log key
    fn parse_log_key(key: &[u8]) -> Result<u64, Error> {
        if key.len() != LOG_PREFIX.len() + 8 {
            return Err(ConvertError::InvalidLength(key.len()).into());
        }

        let index_bytes = &key[LOG_PREFIX.len()..];
        let index = u64::from_be_bytes(
            index_bytes
                .try_into()
                .map_err(|_| ConvertError::ConversionFailure("Invalid log key format".into()))?,
        );
        Ok(index)
    }
}
