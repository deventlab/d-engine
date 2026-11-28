mod rocksdb_state_machine;
mod rocksdb_storage_engine;

use std::path::Path;

use d_engine_core::Error;
pub use rocksdb_state_machine::*;
pub use rocksdb_storage_engine::*;

#[cfg(test)]
mod rocksdb_state_machine_test;

#[cfg(test)]
mod rocksdb_storage_engine_test;

#[cfg(test)]
mod rocksdb_open_test;

// Column family names — single source of truth for all RocksDB adaptors
pub(super) const LOG_CF: &str = "logs";
pub(super) const META_CF: &str = "meta";
pub(super) const STATE_MACHINE_CF: &str = "state_machine";
pub(super) const STATE_MACHINE_META_CF: &str = "state_machine_meta";

// Directories under a node's data directory. This is the layout a default start has always used.
const STORAGE_DIR: &str = "storage";
const STATE_MACHINE_DIR: &str = "state_machine";

/// Opens the Raft log and the state machine of one node: two RocksDB databases under `data_dir`.
///
/// The log is in `data_dir/storage` and the state machine in `data_dir/state_machine`. They are
/// separate databases so the write load of one cannot stall the other, and this is the layout a
/// node's default start uses, so an existing data directory is found again.
///
/// To put the two databases in directories of your own, open them separately with
/// [`RocksDBStorageEngine::new`] and [`RocksDBStateMachine::new`], which take any path.
///
/// # Errors
///
/// Returns an error if either database cannot be opened, for example because another process
/// holds it.
///
/// # Examples
///
/// ```rust,ignore
/// let (storage, sm) = d_engine_server::open_rocksdb("/data/raft")?;
/// ```
pub fn open_rocksdb<P: AsRef<Path>>(
    data_dir: P
) -> Result<(RocksDBStorageEngine, RocksDBStateMachine), Error> {
    let data_dir = data_dir.as_ref();
    let storage = RocksDBStorageEngine::new(data_dir.join(STORAGE_DIR))?;
    let sm = RocksDBStateMachine::new(data_dir.join(STATE_MACHINE_DIR))?;
    Ok((storage, sm))
}

// ── Shared DB + CF option functions ──────────────────────────────────────────
// The log database and the state machine database call these functions so tuning changes apply
// uniformly.

use rocksdb::BlockBasedOptions;
use rocksdb::Cache;
use rocksdb::DBCompactionStyle;
use rocksdb::Options;

/// Base DB-level options shared by all RocksDB adaptors.
///
/// Each adaptor may override individual settings (e.g. `max_background_jobs`,
/// `set_db_write_buffer_size`) after calling this function.
pub(super) fn base_db_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    // IO-smoothing hint: triggers async sync_file_range every 1 MB of WAL writes.
    // This is NOT fdatasync — it spreads dirty page writeback to avoid IO spikes,
    // but provides no durability guarantee. Pairs well with Level 3 (flush_wal) if added.
    opts.set_wal_bytes_per_sync(1024 * 1024);
    opts.set_manual_wal_flush(true);
    opts.set_max_background_jobs(2);
    opts.set_max_open_files(5000);
    opts.set_use_direct_io_for_flush_and_compaction(true);
    opts.set_use_direct_reads(true);
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.set_target_file_size_base(64 * 1024 * 1024);
    opts.set_max_bytes_for_level_base(256 * 1024 * 1024);
    opts
}

/// Log CF: sequential writes, range reads, prefix truncation.
/// Universal Compaction reduces write amplification for append-only + bulk-delete workload.
pub(super) fn log_cf_options(cache: &Cache) -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(128 * 1024 * 1024);
    opts.set_max_write_buffer_number(4);
    opts.set_min_write_buffer_number_to_merge(2);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
    opts.set_compression_options(-14, 0, 0, 0);
    opts.set_compaction_style(DBCompactionStyle::Universal);

    let mut bb = BlockBasedOptions::default();
    bb.set_block_cache(cache);
    opts.set_block_based_table_factory(&bb);
    opts
}

/// SM CF: high-frequency random reads/writes (user KV data).
pub(super) fn sm_cf_options(cache: &Cache) -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_max_write_buffer_number(4);
    opts.set_min_write_buffer_number_to_merge(1);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);

    let mut bb = BlockBasedOptions::default();
    bb.set_block_cache(cache);
    bb.set_bloom_filter(10.0, false);
    bb.set_cache_index_and_filter_blocks(true);
    opts.set_block_based_table_factory(&bb);
    opts
}

/// Meta CF: low-frequency point reads/writes (term, vote, applied index, snapshot metadata).
pub(super) fn meta_cf_options(cache: &Cache) -> Options {
    let mut bb = BlockBasedOptions::default();
    bb.set_block_cache(cache);
    bb.set_bloom_filter(10.0, false);
    let mut opts = Options::default();
    opts.set_block_based_table_factory(&bb);
    opts
}
