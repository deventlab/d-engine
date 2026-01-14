use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use arc_swap::ArcSwap;
use bytes::Bytes;
use d_engine_core::Error;
use d_engine_core::Lease;
use d_engine_core::StateMachine;
use d_engine_core::StorageError;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::Delete;
use d_engine_proto::client::write_command::Insert;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use d_engine_proto::common::entry_payload::Payload;
use d_engine_proto::server::storage::SnapshotMetadata;
use parking_lot::RwLock;
use prost::Message;
use rocksdb::Cache;
use rocksdb::DB;
use rocksdb::IteratorMode;
use rocksdb::Options;
use rocksdb::WriteBatch;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;

use crate::storage::DefaultLease;

const STATE_MACHINE_CF: &str = "state_machine";
const STATE_MACHINE_META_CF: &str = "state_machine_meta";
const LAST_APPLIED_INDEX_KEY: &[u8] = b"last_applied_index";
const LAST_APPLIED_TERM_KEY: &[u8] = b"last_applied_term";
const SNAPSHOT_METADATA_KEY: &[u8] = b"snapshot_metadata";
const TTL_STATE_KEY: &[u8] = b"ttl_state";

/// RocksDB-based state machine implementation with lease support
#[derive(Debug)]
pub struct RocksDBStateMachine {
    db: Arc<ArcSwap<DB>>,
    db_path: PathBuf,
    is_serving: AtomicBool,
    last_applied_index: AtomicU64,
    last_applied_term: AtomicU64,
    last_snapshot_metadata: RwLock<Option<SnapshotMetadata>>,

    // Lease management for automatic key expiration
    // DefaultLease is thread-safe internally (uses DashMap + Mutex)
    // Injected by NodeBuilder after construction
    lease: Option<Arc<DefaultLease>>,

    /// Whether lease manager is enabled (immutable after init)
    /// Set to true when lease is injected, never changes after that
    ///
    /// Invariant: lease_enabled == true ⟹ lease.is_some()
    /// Performance: Allows safe unwrap_unchecked in hot paths
    lease_enabled: bool,
}

impl RocksDBStateMachine {
    /// Creates a new RocksDB-based state machine
    ///
    /// Lease will be injected by NodeBuilder after construction.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db_path = path.as_ref().to_path_buf();

        // Configure RocksDB options using shared configuration
        let opts = Self::configure_db_options();
        let cfs = vec![STATE_MACHINE_CF, STATE_MACHINE_META_CF];

        let db =
            DB::open_cf(&opts, &db_path, cfs).map_err(|e| StorageError::DbError(e.to_string()))?;
        let db_arc = Arc::new(db);

        // Load metadata
        let (last_applied_index, last_applied_term) = Self::load_state_machine_metadata(&db_arc)?;
        let last_snapshot_metadata = Self::load_snapshot_metadata(&db_arc)?;

        Ok(Self {
            db: Arc::new(ArcSwap::new(db_arc)),
            db_path,
            is_serving: AtomicBool::new(true),
            last_applied_index: AtomicU64::new(last_applied_index),
            last_applied_term: AtomicU64::new(last_applied_term),
            last_snapshot_metadata: RwLock::new(last_snapshot_metadata),
            lease: None,          // Will be injected by NodeBuilder
            lease_enabled: false, // Default: no lease until set
        })
    }

    /// Sets the lease manager for this state machine.
    ///
    /// This is an internal method called by NodeBuilder during initialization.
    /// The lease will also be restored from snapshot during `apply_snapshot_from_file()`.
    /// Also available for testing and benchmarks.
    pub fn set_lease(
        &mut self,
        lease: Arc<DefaultLease>,
    ) {
        // Mark lease as enabled (immutable after this point)
        self.lease_enabled = true;
        self.lease = Some(lease);
    }

    // Injects lease configuration into this state machine.
    //
    // Framework-internal method: called by NodeBuilder::build() during initialization.
    // Opens RocksDB with the standard configuration
    // ========== Private helper methods ==========

    /// Configure high-performance RocksDB options.
    ///
    /// This shared configuration is used by both `new()` and `open_db()` to ensure
    /// consistency between initial DB creation and snapshot restoration.
    ///
    /// # Configuration Details
    ///
    /// - **Memory**: 128MB write buffer, 4 max buffers, merge at 2
    /// - **Compression**: LZ4 (fast), Zstd for bottommost (space-efficient)
    /// - **WAL**: 1MB sync interval, manual flush, no fsync
    /// - **Performance**: 4 background jobs, 5000 max open files, direct I/O
    /// - **Compaction**: Dynamic level bytes, 64MB target file size, 256MB base level
    /// - **Cache**: 128MB LRU block cache
    fn configure_db_options() -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Memory and write optimization
        opts.set_max_write_buffer_number(4);
        opts.set_min_write_buffer_number_to_merge(2);
        opts.set_write_buffer_size(128 * 1024 * 1024); // 128MB

        // Compression optimization
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        opts.set_compression_options(-14, 0, 0, 0); // LZ4 fast compression

        // WAL-related optimizations
        opts.set_wal_bytes_per_sync(1024 * 1024); // 1MB sync
        opts.set_manual_wal_flush(true);
        opts.set_use_fsync(false);

        // Performance Tuning
        opts.set_max_background_jobs(4);
        opts.set_max_open_files(5000);
        opts.set_use_direct_io_for_flush_and_compaction(true);
        opts.set_use_direct_reads(true);

        // Leveled Compaction Configuration
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB
        opts.set_max_bytes_for_level_base(256 * 1024 * 1024); // 256MB

        // Block cache configuration
        let cache = Cache::new_lru_cache(128 * 1024 * 1024); // 128MB
        opts.set_row_cache(&cache);

        opts
    }

    fn open_db<P: AsRef<Path>>(path: P) -> Result<DB, Error> {
        let opts = Self::configure_db_options();
        let cfs = vec![STATE_MACHINE_CF, STATE_MACHINE_META_CF];
        DB::open_cf(&opts, path, cfs).map_err(|e| StorageError::DbError(e.to_string()).into())
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
        let db = self.db.load();
        let cf = db
            .cf_handle(STATE_MACHINE_META_CF)
            .ok_or_else(|| StorageError::DbError("State machine meta CF not found".to_string()))?;

        let index = self.last_applied_index.load(Ordering::SeqCst);
        let term = self.last_applied_term.load(Ordering::SeqCst);

        db.put_cf(&cf, LAST_APPLIED_INDEX_KEY, index.to_be_bytes())
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        db.put_cf(&cf, LAST_APPLIED_TERM_KEY, term.to_be_bytes())
            .map_err(|e| StorageError::DbError(e.to_string()))?;

        Ok(())
    }

    fn persist_snapshot_metadata(&self) -> Result<(), Error> {
        let db = self.db.load();
        let cf = db
            .cf_handle(STATE_MACHINE_META_CF)
            .ok_or_else(|| StorageError::DbError("State machine meta CF not found".to_string()))?;

        if let Some(metadata) = self.last_snapshot_metadata.read().clone() {
            let bytes = bincode::serialize(&metadata).map_err(StorageError::BincodeError)?;
            db.put_cf(&cf, SNAPSHOT_METADATA_KEY, bytes)
                .map_err(|e| StorageError::DbError(e.to_string()))?;
        }
        Ok(())
    }

    fn persist_ttl_metadata(&self) -> Result<(), Error> {
        if let Some(ref lease) = self.lease {
            let db = self.db.load();
            let cf = db.cf_handle(STATE_MACHINE_META_CF).ok_or_else(|| {
                StorageError::DbError("State machine meta CF not found".to_string())
            })?;

            let ttl_snapshot = lease.to_snapshot();

            db.put_cf(&cf, TTL_STATE_KEY, ttl_snapshot)
                .map_err(|e| StorageError::DbError(e.to_string()))?;

            debug!("Persisted TTL state to RocksDB");
        }
        Ok(())
    }

    /// Loads TTL state from RocksDB metadata after lease injection.
    ///
    /// Called after NodeBuilder injects the lease.
    /// Also available for testing and benchmarks.
    pub async fn load_lease_data(&self) -> Result<(), Error> {
        let Some(ref lease) = self.lease else {
            return Ok(()); // No lease configured
        };

        let db = self.db.load();
        let cf = db
            .cf_handle(STATE_MACHINE_META_CF)
            .ok_or_else(|| StorageError::DbError("State machine meta CF not found".to_string()))?;

        match db
            .get_cf(&cf, TTL_STATE_KEY)
            .map_err(|e| StorageError::DbError(e.to_string()))?
        {
            Some(ttl_data) => {
                lease.reload(&ttl_data)?;
                debug!("Loaded TTL state from RocksDB: {} active TTLs", lease.len());
            }
            None => {
                debug!("No TTL state found in RocksDB");
            }
        }

        Ok(())
    }

    /// Piggyback cleanup: Remove expired keys with time budget
    ///
    /// This method is called during apply_chunk to cleanup expired keys
    /// opportunistically (piggyback on existing Raft events).
    ///
    /// # Arguments
    /// * `max_duration_ms` - Maximum time budget for cleanup (milliseconds)
    ///
    /// # Returns
    /// Number of keys deleted
    ///
    /// # Performance
    /// - Fast-path: ~10ns if no TTL keys exist (lazy activation check)
    /// - Cleanup: O(log N + K) where K = expired keys
    /// - Time-bounded: stops after max_duration_ms to avoid blocking Raft
    #[allow(dead_code)]
    fn maybe_cleanup_expired(
        &self,
        max_duration_ms: u64,
    ) -> usize {
        let start = std::time::Instant::now();
        let now = SystemTime::now();
        let mut deleted_count = 0;

        // Fast path: skip if TTL never used (lazy activation)
        if let Some(ref lease) = self.lease {
            if !lease.has_lease_keys() {
                return 0; // No TTL keys, skip cleanup (~10ns overhead)
            }

            // Quick check: any expired keys?
            if !lease.may_have_expired_keys(now) {
                return 0; // No expired keys, skip cleanup (~30ns overhead)
            }
        } else {
            return 0; // No lease configured
        }

        // Get database handle
        let db = self.db.load();
        let cf = match db.cf_handle(STATE_MACHINE_CF) {
            Some(cf) => cf,
            None => {
                error!("State machine CF not found during TTL cleanup");
                return 0;
            }
        };

        // Cleanup expired keys with time budget
        let max_duration = std::time::Duration::from_millis(max_duration_ms);

        loop {
            // Check time budget
            if start.elapsed() >= max_duration {
                debug!(
                    "Piggyback cleanup time budget exceeded: deleted {} keys in {:?}",
                    deleted_count,
                    start.elapsed()
                );
                break;
            }

            // Get next batch of expired keys
            let expired_keys = if let Some(ref lease) = self.lease {
                lease.get_expired_keys(now)
            } else {
                vec![]
            };

            if expired_keys.is_empty() {
                break; // No more expired keys
            }

            // Delete expired keys from RocksDB using batch for efficiency
            let mut batch = WriteBatch::default();
            for key in expired_keys {
                batch.delete_cf(&cf, &key);
                deleted_count += 1;
            }

            // Apply batch delete
            if let Err(e) = db.write(batch) {
                error!("Failed to delete expired keys: {}", e);
                break;
            }
        }

        if deleted_count > 0 {
            debug!(
                "Piggyback cleanup: deleted {} expired keys in {:?}",
                deleted_count,
                start.elapsed()
            );
        }

        deleted_count
    }

    fn apply_batch(
        &self,
        batch: WriteBatch,
    ) -> Result<(), Error> {
        self.db.load().write(batch).map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl StateMachine for RocksDBStateMachine {
    async fn start(&self) -> Result<(), Error> {
        self.is_serving.store(true, Ordering::SeqCst);

        // Load persisted lease data if configured
        if let Some(ref _lease) = self.lease {
            self.load_lease_data().await?;
            debug!("Lease data loaded during state machine initialization");
        }

        info!("RocksDB state machine started");
        Ok(())
    }

    fn stop(&self) -> Result<(), Error> {
        self.is_serving.store(false, Ordering::SeqCst);

        // Graceful shutdown: persist TTL state to disk
        // This ensures lease data survives across restarts
        if let Err(e) = self.persist_ttl_metadata() {
            error!("Failed to persist TTL metadata on shutdown: {:?}", e);
            return Err(e);
        }

        info!("RocksDB state machine stopped");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.is_serving.load(Ordering::SeqCst)
    }

    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Bytes>, Error> {
        // Guard against reads during snapshot restoration
        // During apply_snapshot_from_file(), is_serving is set to false while the database
        // is being replaced. This prevents reads from accessing temporary or inconsistent state.
        if !self.is_serving.load(Ordering::SeqCst) {
            return Err(StorageError::NotServing(
                "State machine is restoring from snapshot".to_string(),
            )
            .into());
        }

        let db = self.db.load();
        let cf = db
            .cf_handle(STATE_MACHINE_CF)
            .ok_or_else(|| StorageError::DbError("State machine CF not found".to_string()))?;

        match db.get_cf(&cf, key_buffer).map_err(|e| StorageError::DbError(e.to_string()))? {
            Some(value) => Ok(Some(Bytes::copy_from_slice(&value))),
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

    /// Thread-safe: called serially by single-task CommitHandler
    #[instrument(skip(self, chunk))]
    async fn apply_chunk(
        &self,
        chunk: Vec<Entry>,
    ) -> Result<(), Error> {
        let db = self.db.load();
        let cf = db
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
                        Some(Operation::Insert(Insert {
                            key,
                            value,
                            ttl_secs,
                        })) => {
                            batch.put_cf(&cf, &key, &value);

                            // Register lease if TTL specified
                            if ttl_secs > 0 {
                                // Validate lease is enabled before accepting TTL requests
                                if !self.lease_enabled {
                                    return Err(StorageError::FeatureNotEnabled(
                                        "TTL feature is not enabled on this server. \
                                         Enable it in config: [raft.state_machine.lease] enabled = true".into()
                                    ).into());
                                }

                                // Safety: lease_enabled invariant ensures lease.is_some()
                                let lease = unsafe { self.lease.as_ref().unwrap_unchecked() };
                                lease.register(key.clone(), ttl_secs);
                            }
                        }
                        Some(Operation::Delete(Delete { key })) => {
                            batch.delete_cf(&cf, &key);

                            // Unregister TTL for deleted key
                            if let Some(ref lease) = self.lease {
                                lease.unregister(&key);
                            }
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

        // Note: Lease cleanup is now handled by:
        // - Lazy strategy: cleanup in get() method
        // - Background strategy: dedicated async task
        // This avoids blocking the Raft apply hot path

        if let Some(log_id) = highest_index_entry {
            self.update_last_applied(log_id);
        }

        Ok(())
    }

    fn len(&self) -> usize {
        let db = self.db.load();
        let cf = match db.cf_handle(STATE_MACHINE_CF) {
            Some(cf) => cf,
            None => return 0,
        };

        // Note: This is an expensive operation because it iterates over all keys.
        let iter = db.iterator_cf(&cf, IteratorMode::Start);
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
        snapshot_dir: std::path::PathBuf,
    ) -> Result<(), Error> {
        info!("Applying snapshot from checkpoint: {:?}", snapshot_dir);

        // PHASE 1: Stop serving requests
        self.is_serving.store(false, Ordering::SeqCst);
        info!("Stopped serving requests for snapshot restoration");

        // PHASE 2: Flush and prepare old DB for replacement
        {
            let old_db = self.db.load();
            old_db.flush().map_err(|e| StorageError::DbError(e.to_string()))?;
            old_db.cancel_all_background_work(true);
            info!("Flushed and stopped background work on old DB");
        }

        // PHASE 2.5: Create temporary DB and swap to release old DB's lock
        // This is critical: we must release the old DB instance completely before moving directories
        // Otherwise, the old DB still holds a lock on rocksdb_sm/LOCK even after directory rename
        let temp_dir = tempfile::TempDir::new()?;
        let temp_db_path = temp_dir.path().join("temp_db");
        let temp_db = Self::open_db(&temp_db_path).map_err(|e| {
            error!("Failed to create temporary DB: {:?}", e);
            e
        })?;

        // Swap temp DB into self.db, which releases the old DB's Arc
        // Now old DB's Arc refcount drops to 0, DB instance is dropped, lock is released
        self.db.store(Arc::new(temp_db));
        info!("Swapped to temporary DB, old DB lock released");

        // PHASE 3: Atomic directory replacement
        let backup_dir = self.db_path.with_extension("backup");

        // Remove old backup if exists
        if backup_dir.exists() {
            tokio::fs::remove_dir_all(&backup_dir).await?;
        }

        // Move current DB to backup
        tokio::fs::rename(&self.db_path, &backup_dir).await?;
        info!("Backed up current DB to: {:?}", backup_dir);

        // Move checkpoint to DB path
        tokio::fs::rename(&snapshot_dir, &self.db_path).await.inspect_err(|_e| {
            // Rollback: restore from backup
            let _ = std::fs::rename(&backup_dir, &self.db_path);
        })?;
        info!("Moved checkpoint to DB path: {:?}", self.db_path);

        // PHASE 4: Open new DB from checkpoint
        let new_db = Self::open_db(&self.db_path).map_err(|e| {
            // Rollback: restore from backup
            let _ = std::fs::rename(&backup_dir, &self.db_path);
            error!("Failed to open new DB, rolled back to backup: {:?}", e);
            e
        })?;

        // Atomically swap DB reference (replacing temp DB with new DB)
        self.db.store(Arc::new(new_db));
        info!("Atomically swapped to new DB instance");

        // PHASE 5: Restore TTL state (if lease is configured)
        if let Some(ref lease) = self.lease {
            let ttl_path = self.db_path.join("ttl_state.bin");
            if ttl_path.exists() {
                let ttl_data = tokio::fs::read(&ttl_path).await?;
                lease.reload(&ttl_data)?;

                // Persist TTL state to metadata CF to ensure consistency after restart
                // Without this, a subsequent restart (non-snapshot) would lose TTL state
                // because load_lease_data() reads from metadata CF, not ttl_state.bin
                self.persist_ttl_metadata()?;

                info!("Lease state restored from snapshot and persisted to metadata CF");
            } else {
                warn!("No lease state found in snapshot");
            }
        }

        // PHASE 6: Update metadata
        *self.last_snapshot_metadata.write() = Some(metadata.clone());
        if let Some(last_included) = &metadata.last_included {
            self.update_last_applied(*last_included);
        }

        // PHASE 7: Resume serving
        self.is_serving.store(true, Ordering::SeqCst);
        info!("Resumed serving requests");

        // PHASE 8: Clean up backup (best effort)
        if let Err(e) = tokio::fs::remove_dir_all(&backup_dir).await {
            warn!("Failed to remove backup directory: {}", e);
        } else {
            info!("Cleaned up backup directory");
        }

        info!("Snapshot applied successfully - full DB restoration complete");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
        last_included: LogId,
    ) -> Result<Bytes, Error> {
        // Create a checkpoint in the new_snapshot_dir
        // Use scope to ensure checkpoint is dropped before await
        {
            let db = self.db.load();
            let checkpoint = rocksdb::checkpoint::Checkpoint::new(db.as_ref())
                .map_err(|e| StorageError::DbError(e.to_string()))?;
            checkpoint
                .create_checkpoint(&new_snapshot_dir)
                .map_err(|e| StorageError::DbError(e.to_string()))?;
        } // checkpoint dropped here, before any await

        // Persist lease state alongside the checkpoint (if configured)
        if let Some(ref lease) = self.lease {
            let ttl_snapshot = lease.to_snapshot();
            let ttl_path = new_snapshot_dir.join("ttl_state.bin");
            tokio::fs::write(&ttl_path, ttl_snapshot).await?;
        }

        // Update metadata
        let checksum = [0; 32]; // For now, we return a dummy checksum.
        let snapshot_metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: Bytes::copy_from_slice(&checksum),
        };
        self.persist_last_snapshot_metadata(&snapshot_metadata)?;

        info!("Snapshot generated at {:?} with TTL data", new_snapshot_dir);
        Ok(Bytes::copy_from_slice(&checksum))
    }

    fn save_hard_state(&self) -> Result<(), Error> {
        self.persist_state_machine_metadata()?;
        self.persist_snapshot_metadata()?;
        Ok(())
    }

    fn flush(&self) -> Result<(), Error> {
        let db = self.db.load();

        // Step 1: Sync WAL to disk (critical!)
        // true = sync to disk
        db.flush_wal(true).map_err(|e| StorageError::DbError(e.to_string()))?;
        // Step 2: Flush memtables to SST files
        db.flush().map_err(|e| StorageError::DbError(e.to_string()))?;

        // Persist state machine metadata (last_applied_index, last_applied_term, snapshot_metadata)
        self.persist_state_machine_metadata()?;

        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }

    #[instrument(skip(self))]
    async fn reset(&self) -> Result<(), Error> {
        let db = self.db.load();
        let cf = db
            .cf_handle(STATE_MACHINE_CF)
            .ok_or_else(|| StorageError::DbError("State machine CF not found".to_string()))?;

        // Delete all keys in the state machine
        let mut batch = WriteBatch::default();
        let iter = db.iterator_cf(&cf, IteratorMode::Start);

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::DbError(e.to_string()))?;
            batch.delete_cf(&cf, &key);
        }

        db.write(batch).map_err(|e| StorageError::DbError(e.to_string()))?;

        // Reset metadata
        self.last_applied_index.store(0, Ordering::SeqCst);
        self.last_applied_term.store(0, Ordering::SeqCst);
        *self.last_snapshot_metadata.write() = None;

        // Note: Lease is managed by NodeBuilder and doesn't need reset

        self.persist_state_machine_metadata()?;
        self.persist_snapshot_metadata()?;

        info!("RocksDB state machine reset completed");
        Ok(())
    }

    async fn lease_background_cleanup(&self) -> Result<Vec<Bytes>, Error> {
        // Fast path: no lease configured
        let Some(ref lease) = self.lease else {
            return Ok(vec![]);
        };

        // Get all expired keys
        let now = SystemTime::now();
        let expired_keys = lease.get_expired_keys(now);

        if expired_keys.is_empty() {
            return Ok(vec![]);
        }

        debug!(
            "Lease background cleanup: found {} expired keys",
            expired_keys.len()
        );

        // Delete expired keys from RocksDB
        let db = self.db.load();
        let cf = db
            .cf_handle(STATE_MACHINE_CF)
            .ok_or_else(|| StorageError::DbError("State machine CF not found".to_string()))?;

        let mut batch = WriteBatch::default();
        for key in &expired_keys {
            batch.delete_cf(&cf, key);
        }

        self.apply_batch(batch)?;

        info!(
            "Lease background cleanup: deleted {} expired keys",
            expired_keys.len()
        );

        Ok(expired_keys)
    }
}
impl Drop for RocksDBStateMachine {
    fn drop(&mut self) {
        // save_hard_state() persists last_applied metadata before flush
        // This is critical to prevent replay of already-applied entries on restart
        if let Err(e) = self.save_hard_state() {
            error!("Failed to save hard state on drop: {}", e);
        }

        // Then flush data to disk
        if let Err(e) = self.flush() {
            error!("Failed to flush on drop: {}", e);
        } else {
            debug!("RocksDBStateMachine flushed successfully on drop");
        }

        // This ensures flush operations are truly finished
        self.db.load().cancel_all_background_work(true); // true = wait for completion
        debug!("RocksDB background work cancelled on drop");
    }
}
