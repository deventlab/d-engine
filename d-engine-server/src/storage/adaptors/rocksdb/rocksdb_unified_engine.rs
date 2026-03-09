use std::path::Path;
use std::sync::Arc;

use d_engine_core::Error;
use d_engine_core::StorageError;
use rocksdb::BlockBasedOptions;
use rocksdb::Cache;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::DB;
use rocksdb::DBCompactionStyle;
use rocksdb::Options;

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
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // Disable global write buffer cap so each CF controls its own flush lifecycle.
        // Without this, RocksDB imposes a DB-wide limit that triggers simultaneous flushes
        // across all CFs, causing all CFs to compete for the same background thread pool
        // and producing write stalls under high-throughput embedded workloads.
        db_opts.set_db_write_buffer_size(0);
        db_opts.set_max_background_jobs(2);
        db_opts.set_max_open_files(5000);

        // WAL tuning
        db_opts.set_wal_bytes_per_sync(1024 * 1024);

        // Direct I/O
        db_opts.set_use_direct_io_for_flush_and_compaction(true);
        db_opts.set_use_direct_reads(true);

        // Leveled compaction
        db_opts.set_level_compaction_dynamic_level_bytes(true);
        db_opts.set_target_file_size_base(64 * 1024 * 1024);
        db_opts.set_max_bytes_for_level_base(256 * 1024 * 1024);

        // Single shared block cache for all CFs — one 128 MB budget, no per-CF duplication.
        // Block cache sits in front of SST reads; hot data blocks stay in memory.
        let block_cache = Cache::new_lru_cache(128 * 1024 * 1024);

        // Raft log CF: sequential writes, range reads, prefix truncation
        let log_cf = ColumnFamilyDescriptor::new(LOG_CF, Self::log_cf_options(&block_cache));
        // Raft meta CF: very-low-frequency point reads/writes (term, vote)
        let meta_cf = ColumnFamilyDescriptor::new(META_CF, Self::meta_cf_options(&block_cache));
        // SM CF: high-frequency random reads/writes (user KV data)
        let sm_cf =
            ColumnFamilyDescriptor::new(STATE_MACHINE_CF, Self::sm_cf_options(&block_cache));
        // SM meta CF: point reads/writes (applied index, snapshot metadata)
        let sm_meta_cf =
            ColumnFamilyDescriptor::new(STATE_MACHINE_META_CF, Self::meta_cf_options(&block_cache));

        DB::open_cf_descriptors(&db_opts, path, vec![log_cf, meta_cf, sm_cf, sm_meta_cf])
            .map_err(|e| StorageError::DbError(e.to_string()).into())
    }

    /// Options tuned for sequential-write, range-read Raft log workload.
    ///
    /// Universal Compaction reduces write amplification for append-only + prefix-range-delete
    /// patterns (log truncation / purge), which is the dominant Raft log access pattern.
    fn log_cf_options(block_cache: &Cache) -> Options {
        let mut opts = Options::default();
        opts.set_write_buffer_size(128 * 1024 * 1024);
        opts.set_max_write_buffer_number(4);
        opts.set_min_write_buffer_number_to_merge(2);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        opts.set_compression_options(-14, 0, 0, 0);
        // Universal Compaction: lower write amplification for append-only + bulk-delete workload.
        opts.set_compaction_style(DBCompactionStyle::Universal);

        let mut bb_opts = BlockBasedOptions::default();
        bb_opts.set_block_cache(block_cache);
        opts.set_block_based_table_factory(&bb_opts);

        opts
    }

    /// Options tuned for random read/write KV state machine workload.
    fn sm_cf_options(block_cache: &Cache) -> Options {
        let mut opts = Options::default();
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(4);
        opts.set_min_write_buffer_number_to_merge(2);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);

        let mut bb_opts = BlockBasedOptions::default();
        bb_opts.set_block_cache(block_cache);
        // Bloom filter reduces unnecessary SST reads for missing keys (point lookup heavy).
        bb_opts.set_bloom_filter(10.0, false);
        bb_opts.set_cache_index_and_filter_blocks(true);
        opts.set_block_based_table_factory(&bb_opts);

        opts
    }

    /// Options tuned for low-frequency point read/write metadata workload (term, vote,
    /// applied index, snapshot metadata).
    ///
    /// Bloom filter and shared block cache are cheap to add and eliminate unnecessary
    /// SST reads on point lookups.
    fn meta_cf_options(block_cache: &Cache) -> Options {
        let mut bb_opts = BlockBasedOptions::default();
        bb_opts.set_block_cache(block_cache);
        bb_opts.set_bloom_filter(10.0, false);
        let mut opts = Options::default();
        opts.set_block_based_table_factory(&bb_opts);
        opts
    }
}
