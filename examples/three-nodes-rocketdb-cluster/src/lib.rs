use crate::proto::common::{Entry, LogId};
use crate::{HardState, Result};
use bytes::Bytes;
use prost::Message;
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, DB};
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::{Arc, Mutex};

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
    db: Arc<Mutex<DB>>,
}

impl RocksDBEngine {
    /// Creates a new RocksDB storage instance
    ///
    /// # Arguments
    /// * `path` - Filesystem path for database storage
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Optimize for Raft workload
        opts.set_write_buffer_size(512 * 1024 * 1024); // 512MB write buffer
        opts.set_max_write_buffer_number(4);
        opts.set_target_file_size_base(128 * 1024 * 1024);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let db = DB::open(&opts, path)?;
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }

    fn make_log_key(index: u64) -> Vec<u8> {
        let mut key = LOG_PREFIX.to_vec();
        key.extend_from_slice(&index.to_be_bytes());
        key
    }

    fn make_sm_key<K: AsRef<[u8]>>(key: K) -> Vec<u8> {
        let mut k = STATE_MACHINE_PREFIX.to_vec();
        k.extend_from_slice(key.as_ref());
        k
    }
}

impl StorageEngine for RocksDBEngine {
    /// Persists Raft log entries atomically
    ///
    /// Uses RocksDB batch writes to ensure all entries are persisted atomically.
    /// Keys are formatted as `log:<index>` where index is big-endian encoded.
    fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()> {
        let db = self.db.lock().unwrap();
        let mut batch = WriteBatch::default();

        for entry in entries {
            let key = Self::make_log_key(entry.log_id.index);
            let value = entry.encode_to_vec();
            batch.put(&key, &value);
        }

        db.write(batch)?;
        Ok(())
    }

    /// Inserts a key-value pair into the state machine storage
    ///
    /// Keys are prefixed with `sm:` to separate from log entries.
    /// Returns previous value if it existed.
    fn insert<K, V>(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]> + 'static,
        V: AsRef<[u8]> + 'static,
    {
        let db = self.db.lock().unwrap();
        let sm_key = Self::make_sm_key(key);
        let prev = db.get(&sm_key)?;
        db.put(&sm_key, value.as_ref())?;
        Ok(prev)
    }

    /// Retrieves a value from the state machine storage
    fn get<K>(
        &self,
        key: K,
    ) -> Result<Option<Bytes>>
    where
        K: AsRef<[u8]> + Send + 'static,
    {
        let db = self.db.lock().unwrap();
        let sm_key = Self::make_sm_key(key);
        Ok(db.get(&sm_key)?.map(|v| Bytes::copy_from_slice(&v)))
    }

    /// Retrieves a single log entry by index
    fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>> {
        let db = self.db.lock().unwrap();
        let key = Self::make_log_key(index);
        match db.get(&key)? {
            Some(data) => {
                let entry = Entry::decode(&*data)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Retrieves log entries within a range (inclusive)
    ///
    /// Efficiently scans using RocksDB's prefix iterator.
    fn get_entries_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>> {
        let db = self.db.lock().unwrap();
        let mut entries = Vec::new();
        let (start, end) = (*range.start(), *range.end());

        let iter = db.iterator(IteratorMode::From(
            &Self::make_log_key(start),
            Direction::Forward,
        ));

        for item in iter {
            let (key, value) = item?;

            // Stop if we've passed the end of the range
            if !key.starts_with(LOG_PREFIX) || key[LOG_PREFIX.len()..].len() != 8 {
                break;
            }

            let index = u64::from_be_bytes(key[LOG_PREFIX.len()..].try_into().unwrap());
            if index > end {
                break;
            }

            let entry = Entry::decode(&*value)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Purges logs up to a specified index
    ///
    /// Deletes all log entries with indexes <= cutoff_index.
    fn purge_logs(
        &self,
        cutoff_index: LogId,
    ) -> Result<()> {
        let db = self.db.lock().unwrap();
        let mut batch = WriteBatch::default();
        let start_key = Self::make_log_key(0);
        let end_key = Self::make_log_key(cutoff_index.index + 1);

        // Range delete is efficient in RocksDB
        db.delete_range(&start_key, &end_key)?;
        Ok(())
    }

    /// Flushes all pending writes to disk
    fn flush(&self) -> Result<()> {
        let db = self.db.lock().unwrap();
        db.flush()?;
        Ok(())
    }

    /// Resets the storage engine (for testing only)
    fn reset(&self) -> Result<()> {
        let db = self.db.lock().unwrap();

        // Delete all log entries
        let log_start = Self::make_log_key(0);
        let log_end = Self::make_log_key(u64::MAX);
        db.delete_range(&log_start, &log_end)?;

        // Delete all state machine entries
        let sm_start = STATE_MACHINE_PREFIX.to_vec();
        let mut sm_end = sm_start.clone();
        sm_end.push(0xff);
        db.delete_range(&sm_start, &sm_end)?;

        // Delete hard state
        db.delete(HARD_STATE_KEY)?;

        Ok(())
    }

    /// Gets the last log index stored
    fn last_index(&self) -> u64 {
        let db = self.db.lock().unwrap();
        let iter = db.iterator(IteratorMode::End);

        // Find last log entry
        for item in iter {
            if let Ok((key, _)) = item {
                if key.starts_with(LOG_PREFIX) {
                    let index_bytes = &key[LOG_PREFIX.len()..];
                    if index_bytes.len() == 8 {
                        return u64::from_be_bytes(index_bytes.try_into().unwrap());
                    }
                }
            }
            break;
        }

        0
    }

    /// Truncates log from specified index onward
    fn truncate(
        &self,
        from_index: u64,
    ) -> Result<()> {
        let db = self.db.lock().unwrap();
        let start_key = Self::make_log_key(from_index);
        let end_key = Self::make_log_key(u64::MAX);
        db.delete_range(&start_key, &end_key)?;
        Ok(())
    }

    /// Loads persisted hard state
    fn load_hard_state(&self) -> Result<Option<HardState>> {
        let db = self.db.lock().unwrap();
        match db.get(HARD_STATE_KEY)? {
            Some(data) => {
                let state = HardState::decode(&*data)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    /// Saves hard state to persistent storage
    fn save_hard_state(
        &self,
        hard_state: HardState,
    ) -> Result<()> {
        let db = self.db.lock().unwrap();
        let data = hard_state.encode_to_vec();
        db.put(HARD_STATE_KEY, &data)?;
        Ok(())
    }

    #[cfg(test)]
    fn db_size(&self) -> Result<u64> {
        let db = self.db.lock().unwrap();
        let mut size = 0;
        for file in db.live_files() {
            size += file.size;
        }
        Ok(size as u64)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        let db = self.db.lock().unwrap();
        let mut count = 0;
        let iter = db.iterator(IteratorMode::Start);
        for item in iter {
            if let Ok(_) = item {
                count += 1;
            }
        }
        count
    }
}
