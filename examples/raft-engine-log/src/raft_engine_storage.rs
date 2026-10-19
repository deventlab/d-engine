//! raft-engine-backed `StorageEngine` for spike #460.
//!
//! Replaces the RocksDB WAL log store with raft-engine (preallocated,
//! overwrite-based segments) while keeping d-engine's Raft core and state
//! machine untouched. The single raft group lives under a fixed region id.

use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::entry_record::EntryRecord;
use crate::options::RaftEngineOptions;
use async_trait::async_trait;
use d_engine::common::{Entry, LogId};
use d_engine::{Error, HardState, LogStore, MetaStore, StorageEngine, StorageError};
use prost::Message as _;
use raft_engine::{Command, Config, Engine, LogBatch, MessageExt, ReadableSize};

/// Raft log entries.
const LOG_REGION_ID: u64 = 1;
/// Hard state and purge boundary. Kept apart from the entries because `reset`
/// clears every key of a region and must leave these untouched.
const META_REGION_ID: u64 = 2;

const HARD_STATE_KEY: &[u8] = b"hard_state";
const PURGE_BOUNDARY_KEY: &[u8] = b"purge_boundary";

/// Bridges d-engine's prost `Entry` to raft-engine's rust-protobuf entry type.
struct EntryAdapter;

impl MessageExt for EntryAdapter {
    type Entry = EntryRecord;

    fn index(e: &Self::Entry) -> u64 {
        e.entry.index
    }
}

/// Reclaims log files whose entries have all been compacted.
///
/// raft-engine never deletes files by itself: `LogStore::purge` only compacts the
/// in-memory index, and the files are released when `purge_expired_files` runs.
/// That call deletes files and may rewrite older ones, so it runs on a dedicated
/// thread and never on the Raft loop. A failed round is logged and retried on the
/// next tick: the log stays durable, only disk space is delayed.
struct PurgeWorker {
    stop: Option<mpsc::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl PurgeWorker {
    fn spawn(
        engine: Arc<Engine>,
        interval: Duration,
    ) -> Result<Self, Error> {
        let (stop, stop_rx) = mpsc::channel::<()>();
        let handle = std::thread::Builder::new()
            .name("raft-engine-purge".into())
            .spawn(move || {
                // Only a timeout means "run a round"; a message or a closed
                // channel means "stop".
                while let Err(RecvTimeoutError::Timeout) = stop_rx.recv_timeout(interval) {
                    match engine.purge_expired_files() {
                        Ok(regions) => tracing::debug!(?regions, "purged expired log files"),
                        Err(e) => tracing::error!("failed to purge expired log files: {e}"),
                    }
                }
            })
            .map_err(|e| StorageError::LogStorage(format!("failed to spawn purge thread: {e}")))?;
        Ok(Self {
            stop: Some(stop),
            handle: Some(handle),
        })
    }
}

impl Drop for PurgeWorker {
    fn drop(&mut self) {
        // Closing the channel wakes the thread out of its wait immediately.
        self.stop.take();
        if let Some(handle) = self.handle.take()
            && handle.join().is_err()
        {
            tracing::error!("raft-engine purge thread panicked");
        }
    }
}

pub struct RaftEngineLogStore {
    engine: Arc<Engine>,
    _purge_worker: PurgeWorker,
}

pub struct RaftEngineMetaStore {
    engine: Arc<Engine>,
}

pub struct RaftEngineStorage {
    log_store: Arc<RaftEngineLogStore>,
    meta_store: Arc<RaftEngineMetaStore>,
    path: String,
}

impl RaftEngineStorage {
    /// Opens raft-engine at `path` with the default options.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::open_with(path, &RaftEngineOptions::default())
    }

    /// Opens raft-engine at `path` with the given options.
    pub fn open_with(
        path: impl AsRef<Path>,
        options: &RaftEngineOptions,
    ) -> Result<Self, Error> {
        options.validate()?;
        let path = path.as_ref();
        // raft-engine creates only the last path component, so a nested data
        // directory (e.g. `db/1/log`) must exist before it opens.
        std::fs::create_dir_all(path).map_err(StorageError::IoError)?;
        let config = Config {
            dir: path.to_string_lossy().into_owned(),
            target_file_size: ReadableSize::mb(options.target_file_size_mib),
            purge_threshold: ReadableSize::mb(options.purge_threshold_mib),
            prefill_for_recycle: true,
            prefill_limit: Some(ReadableSize::mb(options.prefill_limit_mib)),
            batch_compression_threshold: ReadableSize::kb(0),
            ..Default::default()
        };
        let engine =
            Arc::new(Engine::open(config).map_err(|e| StorageError::LogStorage(e.to_string()))?);
        let purge_worker = PurgeWorker::spawn(
            engine.clone(),
            Duration::from_secs(options.purge_interval_secs),
        )?;
        Ok(Self {
            log_store: Arc::new(RaftEngineLogStore {
                engine: engine.clone(),
                _purge_worker: purge_worker,
            }),
            meta_store: Arc::new(RaftEngineMetaStore { engine }),
            path: path.to_string_lossy().into_owned(),
        })
    }
}

impl std::fmt::Debug for RaftEngineStorage {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RaftEngineStorage").field("path", &self.path).finish()
    }
}

impl StorageEngine for RaftEngineStorage {
    type LogStore = RaftEngineLogStore;
    type MetaStore = RaftEngineMetaStore;

    fn log_store(&self) -> Arc<Self::LogStore> {
        self.log_store.clone()
    }

    fn meta_store(&self) -> Arc<Self::MetaStore> {
        self.meta_store.clone()
    }
}

#[async_trait]
impl LogStore for RaftEngineLogStore {
    async fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<(), Error> {
        let mut batch = LogBatch::default();
        let raft_entries: Vec<EntryRecord> = entries.into_iter().map(EntryRecord::from).collect();

        batch
            .add_entries::<EntryAdapter>(LOG_REGION_ID, &raft_entries)
            .map_err(|e| StorageError::LogStorage(e.to_string()))?;
        // sync=false: lands in raft-engine's pipe buffer; the IO thread's
        // flush() (= engine.sync()) makes it durable, mirroring the RocksDB
        // adapter's drain-then-fsync model.
        self.engine
            .write(&mut batch, false)
            .map_err(|e| StorageError::LogStorage(e.to_string()))?;
        Ok(())
    }

    async fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>, Error> {
        match self
            .engine
            .get_entry::<EntryAdapter>(LOG_REGION_ID, index)
            .map_err(|e| StorageError::LogStorage(e.to_string()))?
        {
            Some(r) => Ok(Some(r.entry)),
            None => Ok(None),
        }
    }

    fn get_entries(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>, Error> {
        let Some(first) = self.engine.first_index(LOG_REGION_ID) else {
            return Ok(Vec::new());
        };
        let last = self.engine.last_index(LOG_REGION_ID).unwrap_or(first);
        let begin = (*range.start()).max(first);
        let end = (*range.end()).min(last);
        if begin > end {
            return Ok(Vec::new());
        }
        // fetch_entries_to is [begin, end); the trait range is inclusive.
        let mut raw = Vec::new();
        self.engine
            .fetch_entries_to::<EntryAdapter>(LOG_REGION_ID, begin, end + 1, None, &mut raw)
            .map_err(|e| StorageError::LogStorage(e.to_string()))?;
        Ok(raw.into_iter().map(|r| r.entry).collect())
    }

    async fn purge(
        &self,
        cutoff_index: LogId,
    ) -> Result<(), Error> {
        // Compact the prefix and record the boundary in one batch, so a reader
        // never sees entries gone before the boundary is persisted (#442).
        let mut batch = LogBatch::default();
        batch.add_command(
            LOG_REGION_ID,
            Command::Compact {
                // raft-engine compacts entries strictly below `index`, while the
                // cutoff is inclusive.
                index: cutoff_index.index + 1,
            },
        );
        batch
            .put(
                META_REGION_ID,
                PURGE_BOUNDARY_KEY.to_vec(),
                cutoff_index.encode_to_vec(),
            )
            .map_err(|e| StorageError::LogStorage(e.to_string()))?;
        self.engine
            .write(&mut batch, false)
            .map_err(|e| StorageError::LogStorage(e.to_string()))?;
        Ok(())
    }

    async fn truncate(
        &self,
        from_index: u64,
    ) -> Result<(), Error> {
        let (Some(first), Some(last)) = (
            self.engine.first_index(LOG_REGION_ID),
            self.engine.last_index(LOG_REGION_ID),
        ) else {
            return Ok(());
        };
        if from_index > last {
            return Ok(());
        }

        let mut batch = LogBatch::default();
        if from_index > first {
            // raft-engine has no suffix-truncate command, but appending an index
            // that is already present drops every entry after it. Re-appending the
            // entry just before `from_index` therefore cuts the tail in one write.
            let keep = self
                .engine
                .get_entry::<EntryAdapter>(LOG_REGION_ID, from_index - 1)
                .map_err(|e| StorageError::LogStorage(e.to_string()))?
                .ok_or_else(|| {
                    StorageError::LogStorage(format!("entry {} missing", from_index - 1))
                })?;
            batch
                .add_entries::<EntryAdapter>(LOG_REGION_ID, &[keep])
                .map_err(|e| StorageError::LogStorage(e.to_string()))?;
        } else {
            // Nothing below `from_index` survives: compact past the last entry.
            batch.add_command(LOG_REGION_ID, Command::Compact { index: last + 1 });
        }
        self.engine
            .write(&mut batch, false)
            .map_err(|e| StorageError::LogStorage(e.to_string()))?;
        Ok(())
    }

    async fn replace_range(
        &self,
        from_index: u64,
        new_entries: Vec<Entry>,
    ) -> Result<u64, Error> {
        if new_entries.is_empty() {
            return Err(StorageError::LogStorage(
                "replace_range with empty entries is unsupported".into(),
            )
            .into());
        }
        let first = self.engine.first_index(LOG_REGION_ID).unwrap_or(from_index);
        let last = self.engine.last_index(LOG_REGION_ID).unwrap_or(0);
        // Guard the two append panic conditions in raft-engine's MemTable.
        if from_index < first {
            return Err(StorageError::LogStorage(
                "replace_range would overwrite compacted history".into(),
            )
            .into());
        }
        if from_index > last + 1 {
            return Err(
                StorageError::LogStorage("replace_range would create a hole".into()).into(),
            );
        }
        let mut batch = LogBatch::default();
        let last = new_entries.last().map(|e| e.index);
        let raft_entries: Vec<EntryRecord> =
            new_entries.into_iter().map(EntryRecord::from).collect();
        batch
            .add_entries::<EntryAdapter>(LOG_REGION_ID, &raft_entries)
            .map_err(|e| StorageError::LogStorage(e.to_string()))?;
        self.engine
            .write(&mut batch, false)
            .map_err(|e| StorageError::LogStorage(e.to_string()))?;
        Ok(last.unwrap_or(from_index.saturating_sub(1)))
    }

    fn is_write_durable(&self) -> bool {
        false
    }

    fn flush(&self) -> Result<(), Error> {
        self.engine.sync().map_err(|e| StorageError::LogStorage(e.to_string()))?;
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }

    async fn reset(&self) -> Result<(), Error> {
        let mut batch = LogBatch::default();
        batch.add_command(LOG_REGION_ID, Command::Clean);
        self.engine
            .write(&mut batch, false)
            .map_err(|e| StorageError::LogStorage(e.to_string()))?;
        Ok(())
    }

    fn last_index(&self) -> u64 {
        self.engine.last_index(LOG_REGION_ID).unwrap_or(0)
    }

    fn load_purge_boundary(&self) -> Result<Option<LogId>, Error> {
        match self.engine.get(META_REGION_ID, PURGE_BOUNDARY_KEY) {
            Some(bytes) => LogId::decode(&*bytes)
                .map(Some)
                .map_err(|e| StorageError::SerializationError(e.to_string()).into()),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl MetaStore for RaftEngineMetaStore {
    fn save_hard_state(
        &self,
        state: &HardState,
    ) -> Result<(), Error> {
        let bytes = bincode::serialize(state).map_err(StorageError::BincodeError)?;
        let mut batch = LogBatch::default();
        batch
            .put(META_REGION_ID, HARD_STATE_KEY.to_vec(), bytes)
            .map_err(|e| StorageError::LogStorage(e.to_string()))?;
        // Not synced here: the caller flushes the meta store right after this call.
        self.engine
            .write(&mut batch, false)
            .map_err(|e| StorageError::LogStorage(e.to_string()))?;
        Ok(())
    }

    fn load_hard_state(&self) -> Result<Option<HardState>, Error> {
        match self.engine.get(META_REGION_ID, HARD_STATE_KEY) {
            Some(bytes) => Ok(Some(
                bincode::deserialize(&bytes).map_err(StorageError::BincodeError)?,
            )),
            None => Ok(None),
        }
    }

    fn flush(&self) -> Result<(), Error> {
        self.engine.sync().map_err(|e| StorageError::LogStorage(e.to_string()))?;
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }
}
