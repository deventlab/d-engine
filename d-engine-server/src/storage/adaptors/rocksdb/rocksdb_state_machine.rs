use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use arc_swap::ArcSwap;
use bytes::Bytes;
use d_engine_core::ApplyResult;
use d_engine_core::Error;
use d_engine_core::Lease;
use d_engine_core::StateMachine;
use d_engine_core::StorageError;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::CompareAndSwap;
use d_engine_proto::client::write_command::Delete;
use d_engine_proto::client::write_command::Insert;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use d_engine_proto::common::entry_payload::Payload;
use d_engine_proto::server::storage::SnapshotMetadata;
use parking_lot::RwLock;
use prost::Message;
use rocksdb::DB;
use rocksdb::ExportImportFilesMetaData;
use rocksdb::ImportColumnFamilyOptions;
use rocksdb::IteratorMode;
use rocksdb::LiveFile;
use rocksdb::Options;
use rocksdb::WriteBatch;
use serde::Deserialize;
use serde::Serialize;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;

use crate::storage::DefaultLease;

use super::LOG_CF;
use super::META_CF;
use super::STATE_MACHINE_CF;
use super::STATE_MACHINE_META_CF;

const LAST_APPLIED_INDEX_KEY: &[u8] = b"last_applied_index";
const LAST_APPLIED_TERM_KEY: &[u8] = b"last_applied_term";
const SNAPSHOT_METADATA_KEY: &[u8] = b"snapshot_metadata";
const TTL_STATE_KEY: &[u8] = b"ttl_state";

/// Persisted representation of `ExportImportFilesMetaData` for cross-node snapshot transfer.
///
/// `directory` is excluded — it is reconstructed from the local snapshot path on restore.
#[derive(Serialize, Deserialize)]
struct CfExportMeta {
    db_comparator_name: String,
    files: Vec<CfExportFile>,
}

#[derive(Serialize, Deserialize)]
struct CfExportFile {
    column_family_name: String,
    name: String,
    size: usize,
    level: i32,
    start_key: Option<Vec<u8>>,
    end_key: Option<Vec<u8>>,
    smallest_seqno: u64,
    largest_seqno: u64,
    num_entries: u64,
    num_deletions: u64,
}

/// RocksDB-based state machine implementation with lease support
#[derive(Debug)]
pub struct RocksDBStateMachine {
    db: Arc<ArcSwap<DB>>,
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
    /// Creates a state machine sharing an existing `Arc<DB>` (unified mode).
    ///
    /// Called by `RocksDBUnifiedEngine::open()`. The DB must already have
    /// `STATE_MACHINE_CF` and `STATE_MACHINE_META_CF` column families open.
    pub(super) fn from_shared_db(db: Arc<DB>) -> Result<Self, Error> {
        let (last_applied_index, last_applied_term) = Self::load_state_machine_metadata(&db)?;
        let last_snapshot_metadata = Self::load_snapshot_metadata(&db)?;

        Ok(Self {
            db: Arc::new(ArcSwap::new(db)),
            is_serving: AtomicBool::new(true),
            last_applied_index: AtomicU64::new(last_applied_index),
            last_applied_term: AtomicU64::new(last_applied_term),
            last_snapshot_metadata: RwLock::new(last_snapshot_metadata),
            lease: None,
            lease_enabled: false,
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
            if let Err(e) = db.write(&batch) {
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
        self.db.load().write(&batch).map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }

    // ===== Snapshot restore helpers =====

    /// Restore SM state from a snapshot.
    ///
    /// Supports two formats:
    /// - New (CF export): `snapshot_dir/sm/` exists — drop+import, O(1), no tombstones.
    /// - Old (full checkpoint): fallback clear+copy for backward compatibility.
    async fn restore_from_snapshot(
        &self,
        metadata: &SnapshotMetadata,
        snapshot_dir: &std::path::Path,
    ) -> Result<(), Error> {
        let db = self.db.load();

        if snapshot_dir.join("sm").is_dir() {
            // New format: CF export via export_column_family
            Self::restore_from_cf_export(&db, snapshot_dir)?;
        } else {
            // Old format: full DB checkpoint — clear + copy (backward compat)
            Self::clear_cf(&db, STATE_MACHINE_CF)?;
            Self::clear_cf(&db, STATE_MACHINE_META_CF)?;
            let snap_db = Self::open_snapshot_readonly(snapshot_dir)?;
            Self::copy_cf(&snap_db, &db, STATE_MACHINE_CF)?;
            Self::copy_cf(&snap_db, &db, STATE_MACHINE_META_CF)?;
        }

        info!("Snapshot restore complete");

        // Restore lease if configured
        if let Some(ref lease) = self.lease {
            let ttl_path = snapshot_dir.join("ttl_state.bin");
            if ttl_path.exists() {
                let ttl_data = tokio::fs::read(&ttl_path).await?;
                lease.reload(&ttl_data)?;
                self.persist_ttl_metadata()?;
            } else {
                warn!("No lease state found in snapshot");
            }
        }

        *self.last_snapshot_metadata.write() = Some(metadata.clone());
        if let Some(last_included) = &metadata.last_included {
            self.update_last_applied(*last_included);
        }

        self.is_serving.store(true, Ordering::SeqCst);
        info!("Snapshot applied successfully");
        Ok(())
    }

    /// Open a checkpoint for read-only access.
    ///
    /// Tries unified (4-CF) first; falls back to standalone (2-CF) for checkpoints
    /// created before the unified engine migration.
    fn open_snapshot_readonly(snapshot_dir: &std::path::Path) -> Result<DB, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(false);
        // Try unified checkpoint (4 CFs)
        match DB::open_cf_for_read_only(
            &opts,
            snapshot_dir,
            [LOG_CF, META_CF, STATE_MACHINE_CF, STATE_MACHINE_META_CF],
            false,
        ) {
            Ok(db) => return Ok(db),
            Err(e) if e.to_string().to_lowercase().contains("column family") => {
                // Expected: snapshot was created before unified engine migration (2 CFs only).
                // Fall through to 2-CF fallback.
            }
            Err(e) => return Err(StorageError::DbError(e.to_string()).into()),
        }
        // Fall back: standalone checkpoint (SM CFs only)
        DB::open_cf_for_read_only(
            &opts,
            snapshot_dir,
            [STATE_MACHINE_CF, STATE_MACHINE_META_CF],
            false,
        )
        .map_err(|e| StorageError::DbError(e.to_string()).into())
    }

    /// Delete all keys in a CF via a single WriteBatch (compatible with `Arc<DB>`).
    fn clear_cf(
        db: &DB,
        cf_name: &str,
    ) -> Result<(), Error> {
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| StorageError::DbError(format!("CF not found: {cf_name}")))?;
        let mut batch = WriteBatch::default();
        for item in db.iterator_cf(&cf, IteratorMode::Start) {
            let (k, _) = item.map_err(|e| StorageError::DbError(e.to_string()))?;
            batch.delete_cf(&cf, k);
        }
        db.write(&batch).map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }

    /// Copy all KV pairs from `cf_name` in `src` into the same CF in `dst`.
    fn copy_cf(
        src: &DB,
        dst: &DB,
        cf_name: &str,
    ) -> Result<(), Error> {
        let cf_src = src
            .cf_handle(cf_name)
            .ok_or_else(|| StorageError::DbError(format!("CF {cf_name} missing in source DB")))?;
        let cf_dst = dst
            .cf_handle(cf_name)
            .ok_or_else(|| StorageError::DbError(format!("CF {cf_name} missing in dest DB")))?;
        let mut batch = WriteBatch::default();
        for item in src.iterator_cf(&cf_src, IteratorMode::Start) {
            let (k, v) = item.map_err(|e| StorageError::DbError(e.to_string()))?;
            batch.put_cf(&cf_dst, k, v);
        }
        dst.write(&batch).map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }

    /// Restore SM CFs from a CF export snapshot (new format).
    ///
    /// drop_cf + create_column_family_with_import: atomic per CF, no tombstones,
    /// O(1) via hard-links when source and DB are on the same filesystem.
    fn restore_from_cf_export(
        db: &DB,
        snapshot_dir: &std::path::Path,
    ) -> Result<(), Error> {
        let sm_meta = Self::load_cf_export_metadata(
            &snapshot_dir.join("sm_metadata.bin"),
            &snapshot_dir.join("sm"),
        )?;
        let sm_meta_meta = Self::load_cf_export_metadata(
            &snapshot_dir.join("sm_meta_metadata.bin"),
            &snapshot_dir.join("sm_meta"),
        )?;

        let import_opts = ImportColumnFamilyOptions::default();
        let cf_opts = Options::default();

        // SAFETY NOTE: drop_cf + create_column_family_with_import is NOT atomic.
        // If the process crashes between the two drop_cf calls and the imports, the CFs
        // will be absent on restart. RocksDB does not support CF rename, so true atomicity
        // is not achievable here. Recovery: the node will restart with missing CFs, return
        // errors on reads, and wait for the leader to re-send the snapshot (issue #308).
        db.drop_cf(STATE_MACHINE_CF).map_err(|e| StorageError::DbError(e.to_string()))?;
        db.drop_cf(STATE_MACHINE_META_CF)
            .map_err(|e| StorageError::DbError(e.to_string()))?;

        // create_column_family_with_import requires at least one SST file.
        // A CF with no files (e.g. sm_meta before its first flush) must use create_cf instead.
        if sm_meta.get_files().is_empty() {
            db.create_cf(STATE_MACHINE_CF, &cf_opts)
                .map_err(|e| StorageError::DbError(e.to_string()))?;
        } else {
            db.create_column_family_with_import(&cf_opts, STATE_MACHINE_CF, &import_opts, &sm_meta)
                .map_err(|e| StorageError::DbError(e.to_string()))?;
        }

        if sm_meta_meta.get_files().is_empty() {
            db.create_cf(STATE_MACHINE_META_CF, &cf_opts)
                .map_err(|e| StorageError::DbError(e.to_string()))?;
        } else {
            db.create_column_family_with_import(
                &cf_opts,
                STATE_MACHINE_META_CF,
                &import_opts,
                &sm_meta_meta,
            )
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        }

        Ok(())
    }

    /// Serialize `ExportImportFilesMetaData` to a bincode file at `path`.
    ///
    /// `directory` is excluded; on restore it is reconstructed from the snapshot path.
    fn save_cf_export_metadata(
        metadata: &ExportImportFilesMetaData,
        path: &std::path::Path,
    ) -> Result<(), Error> {
        let files = metadata
            .get_files()
            .into_iter()
            .map(|f| CfExportFile {
                column_family_name: f.column_family_name,
                name: f.name,
                size: f.size,
                level: f.level,
                start_key: f.start_key,
                end_key: f.end_key,
                smallest_seqno: f.smallest_seqno,
                largest_seqno: f.largest_seqno,
                num_entries: f.num_entries,
                num_deletions: f.num_deletions,
            })
            .collect();

        let meta = CfExportMeta {
            db_comparator_name: metadata.get_db_comparator_name(),
            files,
        };

        let bytes = bincode::serialize(&meta).map_err(StorageError::BincodeError)?;
        std::fs::write(path, bytes).map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }

    /// Deserialize `ExportImportFilesMetaData` from a bincode file, setting `directory` to
    /// `actual_dir` (the local path of the exported SST files after snapshot transfer).
    fn load_cf_export_metadata(
        path: &std::path::Path,
        actual_dir: &std::path::Path,
    ) -> Result<ExportImportFilesMetaData, Error> {
        let bytes = std::fs::read(path).map_err(|e| StorageError::DbError(e.to_string()))?;
        let meta: CfExportMeta =
            bincode::deserialize(&bytes).map_err(StorageError::BincodeError)?;

        let dir_str = actual_dir
            .to_str()
            .ok_or_else(|| StorageError::DbError("Snapshot path is not valid UTF-8".to_string()))?
            .to_string();

        let live_files: Vec<LiveFile> = meta
            .files
            .into_iter()
            .map(|f| LiveFile {
                column_family_name: f.column_family_name,
                name: f.name,
                directory: dir_str.clone(),
                size: f.size,
                level: f.level,
                start_key: f.start_key,
                end_key: f.end_key,
                smallest_seqno: f.smallest_seqno,
                largest_seqno: f.largest_seqno,
                num_entries: f.num_entries,
                num_deletions: f.num_deletions,
            })
            .collect();

        let mut export_metadata = ExportImportFilesMetaData::default();
        export_metadata.set_db_comparator_name(&meta.db_comparator_name);
        export_metadata
            .set_files(&live_files)
            .map_err(|e| StorageError::DbError(e.to_string()))?;

        Ok(export_metadata)
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
    ) -> Result<Vec<ApplyResult>, Error> {
        let db = self.db.load();
        let cf = db
            .cf_handle(STATE_MACHINE_CF)
            .ok_or_else(|| StorageError::DbError("State machine CF not found".to_string()))?;

        let mut batch = WriteBatch::default();
        let mut highest_index_entry: Option<LogId> = None;
        let mut results = Vec::with_capacity(chunk.len());

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
                    // NOOP always succeeds
                    results.push(ApplyResult::success(entry.index));
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

                            // PUT always succeeds (errors returned as Err)
                            results.push(ApplyResult::success(entry.index));
                        }
                        Some(Operation::Delete(Delete { key })) => {
                            batch.delete_cf(&cf, &key);

                            // Unregister TTL for deleted key
                            if let Some(ref lease) = self.lease {
                                lease.unregister(&key);
                            }

                            // DELETE always succeeds (errors returned as Err)
                            results.push(ApplyResult::success(entry.index));
                        }
                        Some(Operation::CompareAndSwap(CompareAndSwap {
                            key,
                            expected_value,
                            new_value,
                        })) => {
                            // RocksDB doesn't have native CAS, implement via read-compare-write
                            // This is safe because apply_chunk is called sequentially per Raft log order
                            let current_value = db.get_cf(&cf, &key).map_err(|e| {
                                StorageError::DbError(format!("CAS read failed: {e}"))
                            })?;

                            let cas_success = match (current_value, &expected_value) {
                                (Some(current), Some(expected)) => current == expected.as_ref(),
                                (None, None) => true,
                                _ => false,
                            };

                            if cas_success {
                                batch.put_cf(&cf, &key, &new_value);
                            }

                            // Store CAS result for client response
                            results.push(if cas_success {
                                ApplyResult::success(entry.index)
                            } else {
                                ApplyResult::failure(entry.index)
                            });

                            debug!(
                                "CAS at index {}: key={:?}, success={}",
                                entry.index,
                                String::from_utf8_lossy(&key),
                                cas_success
                            );
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

        // Update last_applied after successful batch write
        if let Some(highest) = highest_index_entry {
            self.update_last_applied(highest);
        }

        Ok(results)
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
        info!("Applying snapshot from: {:?}", snapshot_dir);

        // Stop serving — prevents SM reads/writes during restore
        self.is_serving.store(false, Ordering::SeqCst);

        let result = self.restore_from_snapshot(metadata, &snapshot_dir).await;

        if let Err(ref e) = result {
            error!(
                "Snapshot restore failed, resuming serving with pre-restore state: {:?}",
                e
            );
            // Restore is_serving so the node stays alive and can accept future snapshots.
            // last_applied_index was NOT updated on failure, so the leader will detect
            // this follower is still behind and re-send the snapshot (see: issue #308).
            self.is_serving.store(true, Ordering::SeqCst);
        }

        result
    }

    #[instrument(skip(self))]
    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
        last_included: LogId,
    ) -> Result<Bytes, Error> {
        // Export only SM CFs: compact SST-only export, no Raft log data.
        // export_column_family() creates only the final subdirectory (e.g. "sm"), so the parent
        // (new_snapshot_dir) must already exist before calling it.
        std::fs::create_dir_all(&new_snapshot_dir)?;
        {
            let db = self.db.load();
            let checkpoint = rocksdb::checkpoint::Checkpoint::new(db.as_ref())
                .map_err(|e| StorageError::DbError(e.to_string()))?;
            let cf_sm = db
                .cf_handle(STATE_MACHINE_CF)
                .ok_or_else(|| StorageError::DbError("SM CF not found".to_string()))?;
            let cf_sm_meta = db
                .cf_handle(STATE_MACHINE_META_CF)
                .ok_or_else(|| StorageError::DbError("SM meta CF not found".to_string()))?;

            // Flush both CFs before export: export_column_family only captures SST files,
            // not MemTable data. Without this, small CFs (e.g. sm_meta with only a few
            // metadata keys) may never have been flushed and would export 0 files.
            // Synchronous flush (FlushOptions::wait = true by default) is intentional:
            // we must wait for flush to complete before export or in-flight writes are lost.
            let flush_opts = rocksdb::FlushOptions::default();
            db.flush_cf_opt(&cf_sm, &flush_opts)
                .map_err(|e| StorageError::DbError(e.to_string()))?;
            db.flush_cf_opt(&cf_sm_meta, &flush_opts)
                .map_err(|e| StorageError::DbError(e.to_string()))?;

            let sm_export = checkpoint
                .export_column_family(&cf_sm, new_snapshot_dir.join("sm"))
                .map_err(|e| StorageError::DbError(e.to_string()))?;
            let sm_meta_export = checkpoint
                .export_column_family(&cf_sm_meta, new_snapshot_dir.join("sm_meta"))
                .map_err(|e| StorageError::DbError(e.to_string()))?;

            Self::save_cf_export_metadata(&sm_export, &new_snapshot_dir.join("sm_metadata.bin"))?;
            Self::save_cf_export_metadata(
                &sm_meta_export,
                &new_snapshot_dir.join("sm_meta_metadata.bin"),
            )?;
        } // checkpoint and CF handles dropped here, before any await

        // Persist lease state alongside the export (if configured)
        if let Some(ref lease) = self.lease {
            let ttl_snapshot = lease.to_snapshot();
            let ttl_path = new_snapshot_dir.join("ttl_state.bin");
            tokio::fs::write(&ttl_path, ttl_snapshot).await?;
        }

        // Update metadata
        let checksum = [0; 32];
        let snapshot_metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: Bytes::copy_from_slice(&checksum),
        };
        self.persist_last_snapshot_metadata(&snapshot_metadata)?;

        info!("Snapshot generated at {:?}", new_snapshot_dir);
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

        db.write(&batch).map_err(|e| StorageError::DbError(e.to_string()))?;

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
