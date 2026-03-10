use std::path::Path;
use std::sync::Arc;

use d_engine_core::Error;
use d_engine_core::StorageError;
use rocksdb::Cache;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::DB;

use super::LOG_CF;
use super::META_CF;
use super::RocksDBStateMachine;
use super::RocksDBStorageEngine;
use super::STATE_MACHINE_CF;
use super::STATE_MACHINE_META_CF;

/// Factory for unified single-DB storage (4 CFs, one `Arc<DB>`).
///
/// Opens a single RocksDB instance with all four column families and returns
/// both a `RocksDBStorageEngine` and a `RocksDBStateMachine` sharing the same
/// underlying `Arc<DB>`. This halves resource usage compared to the dual-instance
/// setup: one 128 MB block cache, one set of background jobs, one file-descriptor
/// budget.
///
/// # Usage
///
/// ```rust,ignore
/// use d_engine_server::storage::RocksDBUnifiedEngine;
///
/// let (storage, sm) = RocksDBUnifiedEngine::open("/data/raft")?;
/// ```
pub struct RocksDBUnifiedEngine;

impl RocksDBUnifiedEngine {
    /// Opens (or creates) a unified RocksDB instance at `path`.
    ///
    /// Returns `(RocksDBStorageEngine, RocksDBStateMachine)` sharing one `Arc<DB>`.
    pub fn open<P: AsRef<Path>>(
        path: P
    ) -> Result<(RocksDBStorageEngine, RocksDBStateMachine), Error> {
        let db = Arc::new(Self::open_db(path.as_ref())?);

        let storage = RocksDBStorageEngine::from_shared_db(Arc::clone(&db))?;
        let sm = RocksDBStateMachine::from_shared_db(Arc::clone(&db))?;

        Ok((storage, sm))
    }

    fn open_db(path: &Path) -> Result<DB, Error> {
        let mut db_opts = super::base_db_options();

        // One DB hosts 4 CFs across two workloads: reduce background job contention.
        db_opts.set_max_background_jobs(2);
        // Disable global write buffer cap so each CF controls its own flush lifecycle.
        // Without this, RocksDB imposes a DB-wide limit that triggers simultaneous flushes
        // across all CFs, causing all CFs to compete for the same background thread pool
        // and producing write stalls under high-throughput embedded workloads.
        db_opts.set_db_write_buffer_size(0);

        // Single shared block cache for all CFs — one 128 MB budget, no per-CF duplication.
        // Block cache sits in front of SST reads; hot data blocks stay in memory.
        let block_cache = Cache::new_lru_cache(128 * 1024 * 1024);

        // Raft log CF: sequential writes, range reads, prefix truncation
        let log_cf = ColumnFamilyDescriptor::new(LOG_CF, super::log_cf_options(&block_cache));
        // Raft meta CF: very-low-frequency point reads/writes (term, vote)
        let meta_cf = ColumnFamilyDescriptor::new(META_CF, super::meta_cf_options(&block_cache));
        // SM CF: high-frequency random reads/writes (user KV data)
        let sm_cf =
            ColumnFamilyDescriptor::new(STATE_MACHINE_CF, super::sm_cf_options(&block_cache));
        // SM meta CF: point reads/writes (applied index, snapshot metadata)
        let sm_meta_cf = ColumnFamilyDescriptor::new(
            STATE_MACHINE_META_CF,
            super::meta_cf_options(&block_cache),
        );

        DB::open_cf_descriptors(&db_opts, path, vec![log_cf, meta_cf, sm_cf, sm_meta_cf])
            .map_err(|e| StorageError::DbError(e.to_string()).into())
    }
}
