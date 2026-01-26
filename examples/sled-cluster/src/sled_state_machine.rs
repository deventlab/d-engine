//! It works as KV storage for client business CRUDs.

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bincode::config;
use bytes::Bytes;
use d_engine::{
    ApplyResult, Result, SnapshotError, StateMachine, StorageError,
    client::{
        WriteCommand,
        write_command::{CompareAndSwap, Delete, Insert, Operation},
    },
    common::{Entry, LogId, entry_payload::Payload},
    server_storage::SnapshotMetadata,
};
use parking_lot::Mutex;
use prost::Message;
use sled::Batch;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::trace;
use tracing::warn;

use crate::compute_checksum_from_folder_path;
use crate::{safe_kv, safe_vk, safe_vk_ivec};

/// Sled database tree namespaces
pub(crate) const STATE_MACHINE_TREE: &str = "_state_machine_tree";
pub(crate) const STATE_MACHINE_META_NAMESPACE: &str = "_state_machine_metadata";
pub(crate) const STATE_SNAPSHOT_METADATA_TREE: &str = "_snapshot_metadata";

/// Sled entry key namespaces
pub(crate) const STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX: &str = "_raft_last_applied_index";
pub(crate) const STATE_MACHINE_META_KEY_LAST_APPLIED_TERM: &str = "_raft_last_applied_term";

pub(crate) const LAST_SNAPSHOT_METADATA_KEY: &str = "_raft_last_snapshot_metadata";

pub struct SledStateMachine {
    node_id: u32,

    db: ArcSwap<sled::Db>,

    is_serving: Arc<AtomicBool>,

    /// Volatile state on all servers:
    /// index of highest log entry applied to state machine (initialized to 0,
    /// increases monotonically)
    /// The last submitted log index and term (atomic operation ensures lock-free)
    last_applied_index: AtomicU64,
    last_applied_term: AtomicU64,

    /// Snapshot metadata
    last_included_index: AtomicU64, // load from STATE_SNAPSHOT_METADATA_TREE
    last_included_term: AtomicU64,
    last_snapshot_checksum: Mutex<Option<[u8; 32]>>, //SHA-256

    /// Temporary lock when snapshot is generated (to prevent concurrent snapshot generation)
    snapshot_lock: RwLock<()>,
}

impl std::fmt::Debug for SledStateMachine {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("SledStateMachine")
            .field("tree_len", &self.current_tree().len())
            .finish()
    }
}

impl Drop for SledStateMachine {
    fn drop(&mut self) {
        let timer = Instant::now();

        // Save state into local database including flush operation
        match self.save_hard_state() {
            Ok(_) => debug!("StateMachine saved in {:?}", timer.elapsed()),
            Err(e) => error!("Failed to save StateMachine: {}", e),
        }
    }
}

#[async_trait]
impl StateMachine for SledStateMachine {
    async fn start(&self) -> Result<()> {
        debug!("start state machine");
        self.is_serving.store(true, Ordering::Release);
        Ok(())
    }

    fn stop(&self) -> Result<()> {
        debug!("stop state machine");
        self.is_serving.store(false, Ordering::Release);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.is_serving.load(Ordering::Acquire)
    }

    fn update_last_applied(
        &self,
        last_applied: LogId,
    ) {
        debug!(%self.node_id, ?last_applied, "update_last_applied");
        self.last_applied_index.store(last_applied.index, Ordering::SeqCst);
        self.last_applied_term.store(last_applied.term, Ordering::SeqCst);
    }

    fn update_last_snapshot_metadata(
        &self,
        snapshot_metadata: &SnapshotMetadata,
    ) -> Result<()> {
        debug!(%self.node_id, ?snapshot_metadata, "update_last_snapshot_metadata");
        let last_included = snapshot_metadata.last_included.unwrap();
        self.last_included_index.store(last_included.index, Ordering::SeqCst);
        self.last_included_term.store(last_included.term, Ordering::SeqCst);
        let checksum = snapshot_metadata
            .checksum_array()
            .map_err(|e| SnapshotError::OperationFailed(format!("{e:?}")))?;
        *self.last_snapshot_checksum.lock() = Some(checksum);

        Ok(())
    }

    fn last_applied(&self) -> LogId {
        LogId {
            index: self.last_applied_index.load(Ordering::SeqCst),
            term: self.last_applied_term.load(Ordering::SeqCst),
        }
    }

    fn snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        let last_included_index = self.last_included_index.load(Ordering::SeqCst);
        if last_included_index > 0 {
            Some(SnapshotMetadata {
                last_included: Some(LogId {
                    index: self.last_included_index.load(Ordering::SeqCst),
                    term: self.last_included_term.load(Ordering::SeqCst),
                }),
                checksum: self
                    .last_snapshot_checksum
                    .lock()
                    .as_ref()
                    .map(|arr| Bytes::copy_from_slice(arr))?,
            })
        } else {
            None
        }
    }

    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Bytes>> {
        match self.current_tree().get(key_buffer) {
            Ok(Some(v)) => Ok(Some(Bytes::copy_from_slice(&v))),
            Ok(None) => Ok(None),
            Err(e) => {
                error!("state_machine get error: {}", e);
                Err(StorageError::DbError(e.to_string()).into())
            }
        }
    }

    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64> {
        match self.get(&safe_kv(entry_id)) {
            Ok(Some(term_bytes)) => safe_vk(&term_bytes).ok(),
            Ok(None) => None,
            Err(e) => {
                error!("Failed to retrieve term for entry {}: {}", entry_id, e);
                None
            }
        }
    }

    async fn apply_chunk(
        &self,
        chunk: Vec<Entry>,
    ) -> Result<Vec<ApplyResult>> {
        trace!("Applying chunk: {} entries", chunk.len());

        let mut highest_index_entry: Option<LogId> = None;
        let mut batch = Batch::default();
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
                    debug!("NOOP at index {}", entry.index);
                    results.push(ApplyResult::success(entry.index));
                }
                Some(Payload::Command(data)) => {
                    match WriteCommand::decode(&data[..]) {
                        Ok(write_cmd) => match write_cmd.operation {
                            Some(Operation::Insert(Insert {
                                key,
                                value,
                                ttl_secs: _,
                            })) => {
                                batch.insert(key.as_ref(), value.as_ref());
                                results.push(ApplyResult::success(entry.index));
                            }
                            Some(Operation::Delete(Delete { key })) => {
                                batch.remove(key.as_ref());
                                results.push(ApplyResult::success(entry.index));
                            }
                            Some(Operation::CompareAndSwap(CompareAndSwap {
                                key,
                                expected_value,
                                new_value,
                            })) => {
                                // Flush pending batch before CAS (Sled batch doesn't support atomic CAS)
                                self.apply_batch(std::mem::take(&mut batch))?;

                                // Perform native Sled CAS
                                let tree = self.current_tree();
                                let old = expected_value.as_ref().map(|v| v.as_ref());

                                let cas_success = match tree.compare_and_swap(
                                    &key,
                                    old,
                                    Some(new_value.as_ref()),
                                ) {
                                    Ok(Ok(_)) => true,
                                    Ok(Err(_)) => false,
                                    Err(e) => {
                                        error!("CAS error at index {}: {:?}", entry.index, e);
                                        return Err(StorageError::DbError(e.to_string()).into());
                                    }
                                };

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
                            error!("Decode error at index {}: {}", entry.index, e);
                            return Err(StorageError::SerializationError(e.to_string()).into());
                        }
                    }
                }
                Some(Payload::Config(_)) => {
                    debug!("Config change at index {} (ignored)", entry.index);
                }
                None => panic!("Entry payload should not be None!"),
            }
        }

        // Apply final batch
        self.apply_batch(batch)?;

        // Update last applied index
        if let Some(log_id) = highest_index_entry {
            self.update_last_applied(log_id);
        }

        Ok(results)
    }

    fn save_hard_state(&self) -> Result<()> {
        let last_applied = self.last_applied();
        self.persist_last_applied(last_applied)?;

        if let Some(last_snapshot_metadata) = self.snapshot_metadata() {
            self.persist_last_snapshot_metadata(&last_snapshot_metadata)?;
        }

        self.flush()?;
        Ok(())
    }

    fn persist_last_applied(
        &self,
        last_applied: LogId,
    ) -> Result<()> {
        debug!(%self.node_id, ?last_applied, "persist_last_applied");
        let db = self.db.load();
        let tree = db
            .open_tree(STATE_MACHINE_META_NAMESPACE)
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        tree.insert(
            STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX,
            safe_kv(last_applied.index),
        )
        .map_err(|e| StorageError::DbError(e.to_string()))?;
        tree.insert(
            STATE_MACHINE_META_KEY_LAST_APPLIED_TERM,
            safe_kv(last_applied.term),
        )
        .map_err(|e| StorageError::DbError(e.to_string()))?;

        tree.flush().map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }

    fn persist_last_snapshot_metadata(
        &self,
        last_snapshot_metadata: &SnapshotMetadata,
    ) -> Result<()> {
        debug!(%self.node_id, ?last_snapshot_metadata, "persist_last_snapshot_metadata");
        let db = self.db.load();
        let tree = db
            .open_tree(STATE_SNAPSHOT_METADATA_TREE)
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        self.persist_last_snapshot_metadata_with_tree(tree, last_snapshot_metadata)?;

        Ok(())
    }

    fn flush(&self) -> Result<()> {
        let db = self.db.load();
        match db.flush() {
            Ok(bytes) => {
                info!(
                    "Successfully flushed State Machine, bytes flushed: {}",
                    bytes
                );
                println!("Successfully flushed State Machine, bytes flushed: {bytes}");
            }
            Err(e) => {
                error!("Failed to flush State Machine: {}", e);
                eprintln!("Failed to flush State Machine: {e}");
            }
        }
        Ok(())
    }

    async fn flush_async(&self) -> Result<()> {
        self.flush()
    }

    #[instrument(skip(self))]
    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: PathBuf,
        last_included: LogId,
    ) -> Result<Bytes> {
        // 1. Get a lightweight write lock (to prevent concurrent snapshot generation)
        let _guard = self.snapshot_lock.write().await;

        // 2. Create a new state machine database instance
        let new_db = init_sled_state_machine_db(&new_snapshot_dir)
            .map_err(|e| StorageError::DbError(e.to_string()))?;

        let exist_db_tree = self.current_tree();
        let new_state_machine_tree = new_tree(&new_db, STATE_MACHINE_TREE)?;
        let new_snapshot_metadatat_tree = new_tree(&new_db, STATE_SNAPSHOT_METADATA_TREE)?;

        let mut batch = sled::Batch::default();
        let mut counter = 0;

        for item in exist_db_tree.iter() {
            let (k, v) = item.map_err(|e| StorageError::DbError(e.to_string()))?;

            let key_num = match safe_vk(&k) {
                Ok(v) => v,
                Err(e) => {
                    error!(?e, "generate_snapshot_data::safe_vk");
                    return Err(e.into());
                }
            };
            if key_num > last_included.index {
                break; // Stop applying further entries as they will all be greater
            }

            batch.insert(k, v);
            counter += 1;

            // Perform a batch insert every 100 records
            if counter % 100 == 0 {
                new_state_machine_tree
                    .apply_batch(batch)
                    .map_err(|e| StorageError::DbError(e.to_string()))?;
                batch = sled::Batch::default(); // Reset the batch object
            }
        }

        // Process the remaining data (the tail data of less than 100 records)
        if counter % 100 != 0 {
            new_state_machine_tree
                .apply_batch(batch)
                .map_err(|e| StorageError::DbError(e.to_string()))?;
        }

        // Make sure flush into disk
        new_db.flush().map_err(|e| StorageError::DbError(e.to_string()))?;

        // Calculate the checksum after generating snapshot data
        let checksum = compute_checksum_from_folder_path(&new_snapshot_dir).await?;

        println!("checksum = {checksum:?}",);
        // Make sure last included is updated to the new ones

        let last_snapshot_metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: Bytes::copy_from_slice(&checksum),
        };

        self.update_last_snapshot_metadata(&last_snapshot_metadata)?;

        // Make sure last included is persisted into local database
        self.persist_last_snapshot_metadata(&last_snapshot_metadata)?;
        // Make sure last included is persisted into the new database
        self.persist_last_snapshot_metadata_with_tree(
            new_snapshot_metadatat_tree,
            &last_snapshot_metadata,
        )?;

        Ok(Bytes::copy_from_slice(&checksum))
    }

    #[instrument(skip(self))]
    async fn apply_snapshot_from_file(
        &self,
        metadata: &SnapshotMetadata,
        decompressed_snapshot_path: PathBuf,
    ) -> Result<()> {
        if let Some(new_last_included) = metadata.last_included {
            debug!(
                ?new_last_included,
                "1. Acquire write lock to prevent concurrent snapshot generation/application"
            );
            // 1. Acquire write lock to prevent concurrent snapshot generation/application This
            //    ensures atomic snapshot application per Raft requirements
            let _guard = self.snapshot_lock.write().await;

            debug!("2. Validate snapshot version - only apply if newer than current state");
            // 2. Validate snapshot version - only apply if newer than current state
            if let Some(current_metadata) = self.snapshot_metadata() {
                if let Some(current_last_included) = current_metadata.last_included {
                    // Only allow application when the new snapshot index is larger
                    if new_last_included.index <= current_last_included.index {
                        return Err(SnapshotError::Outdated.into());
                    }
                }
            }

            debug!("4. Create temp directory for decompression");
            debug!(
                ?decompressed_snapshot_path,
                "6. CRITICAL SECURITY STEP: Validate checksum"
            );
            // 6. CRITICAL SECURITY STEP: Validate checksum Prevents tampered or corrupted snapshots
            //    from being applied
            let computed_checksum =
                compute_checksum_from_folder_path(&decompressed_snapshot_path).await?;

            if metadata.checksum.as_ref() != computed_checksum.as_ref() {
                error!(
                    "Snapshot checksum mismatch! Computed: {:?}, Expected: {:?}",
                    computed_checksum, metadata.checksum
                );

                metrics::counter!(
                    "snapshot.checksum_failures",
                    &[
                        ("node_id", self.node_id.to_string()),
                        ("snapshot_index", new_last_included.index.to_string()),
                    ]
                )
                .increment(1);

                return Err(SnapshotError::ChecksumMismatch.into());
            }
            debug!(
                ?decompressed_snapshot_path,
                "7. Initialize new state machine database"
            );
            // 7. Initialize new state machine database Maintains ACID properties during state
            //    transition
            let db = init_sled_state_machine_db(&decompressed_snapshot_path)
                .map_err(|e| StorageError::DbError(e.to_string()))?;

            debug!("8. Atomically replace current database");
            // 8. Atomically replace current database Critical for maintaining consistency per Raft
            //    spec
            self.db.store(Arc::new(db));

            debug!(
                ?new_last_included,
                ?metadata,
                "9. Update Raft metadata and indexes"
            );
            // 9. Update Raft metadata and indexes Follows snapshot application procedure from
            self.update_last_applied(new_last_included);
            self.update_last_snapshot_metadata(metadata)?;
        } else {
            error!(
                ?metadata,
                "apply_snapshot_from_file should not be triggered if metadata is none"
            );
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.current_tree().len()
    }

    async fn reset(&self) -> Result<()> {
        let db = self.db.load();
        db.clear().map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }

    async fn lease_background_cleanup(&self) -> Result<Vec<Bytes>> {
        // SledStateMachine example doesn't implement lease support
        // Return empty vector (no cleanup performed)
        Ok(vec![])
    }
}

impl SledStateMachine {
    pub fn new<P: AsRef<Path> + std::fmt::Debug>(
        path: P,
        node_id: u32,
    ) -> Result<Self> {
        let db = Arc::new(
            init_sled_state_machine_db(path).map_err(|e| StorageError::DbError(e.to_string()))?,
        );

        let state_machine_meta_tree = db
            .open_tree(STATE_MACHINE_META_NAMESPACE)
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        let (last_applied_index, last_applied_term) =
            Self::load_state_machine_metadata(&state_machine_meta_tree)?;

        let snapshot_meta_tree = db
            .open_tree(STATE_SNAPSHOT_METADATA_TREE)
            .map_err(|e| StorageError::DbError(e.to_string()))?;

        let sm = SledStateMachine {
            db: ArcSwap::from(db),
            is_serving: Arc::new(AtomicBool::new(true)),
            node_id,

            last_applied_index: AtomicU64::new(0),
            last_applied_term: AtomicU64::new(0),

            last_included_index: AtomicU64::new(0),
            last_included_term: AtomicU64::new(0),
            last_snapshot_checksum: Mutex::new(None),

            snapshot_lock: RwLock::new(()),
        };

        // Important to sync the last applied index into memory
        sm.update_last_applied(LogId {
            index: last_applied_index,
            term: last_applied_term,
        });

        // Update last snapshot metadata
        if let Some(last_snapshot_metadata) = Self::load_snapshot_metadata(&snapshot_meta_tree)? {
            trace!(
                ?last_snapshot_metadata,
                "Updating last snapshot metadata from local database when node starts"
            );
            sm.update_last_snapshot_metadata(&last_snapshot_metadata)?;
        } else {
            info!("No snapshot metadata found in DB");
        }

        Ok(sm)
    }

    fn load_state_machine_metadata(tree: &sled::Tree) -> Result<(u64, u64)> {
        let index = tree
            .get(STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX)
            .map_err(|e| StorageError::DbError(e.to_string()))?
            .map(safe_vk_ivec)
            .unwrap_or(Ok(0))?;

        let term = tree
            .get(STATE_MACHINE_META_KEY_LAST_APPLIED_TERM)
            .map_err(|e| StorageError::DbError(e.to_string()))?
            .map(safe_vk_ivec)
            .unwrap_or(Ok(0))?;

        Ok((index, term))
    }

    pub(super) fn load_snapshot_metadata(tree: &sled::Tree) -> Result<Option<SnapshotMetadata>> {
        if let Ok(Some(v)) = tree.get(LAST_SNAPSHOT_METADATA_KEY) {
            info!(
                "found SnapshotMetadata from DB with key: {}",
                LAST_SNAPSHOT_METADATA_KEY
            );
            let v = v.to_vec();

            let config = config::standard();
            match bincode::serde::decode_from_slice::<SnapshotMetadata, _>(&v, config) {
                Ok((last_snapshot_metadata, _)) => {
                    info!(
                        "load_snapshot_metadata: last_included={:?}",
                        last_snapshot_metadata.last_included
                    );
                    return Ok(Some(last_snapshot_metadata));
                }
                Err(e) => {
                    return Err(SnapshotError::OperationFailed(format!(
                        "state:load_snapshot_metadata deserialize error. {e}"
                    ))
                    .into());
                }
            }
        }
        Ok(None)
    }

    pub(super) fn apply_batch(
        &self,
        batch: Batch,
    ) -> Result<()> {
        if let Err(e) = self.current_tree().apply_batch(batch) {
            error!("state_machine apply_batch failed: {}", e);
            return Err(StorageError::DbError(e.to_string()).into());
        }
        Ok(())
    }

    // Dynamically obtain the current Tree (lock-free cache optimization)
    #[inline]
    fn current_tree(&self) -> sled::Tree {
        // Each Db instance only needs to get the Tree once
        self.db.load().open_tree(STATE_MACHINE_TREE).unwrap()
    }

    pub(super) fn persist_last_snapshot_metadata_with_tree(
        &self,
        tree: sled::Tree,
        last_snapshot_metadata: &SnapshotMetadata,
    ) -> Result<()> {
        let config = config::standard();
        let v = bincode::serde::encode_to_vec(last_snapshot_metadata, config)
            .map_err(|e| StorageError::DbError(e.to_string()))?;

        tree.insert(LAST_SNAPSHOT_METADATA_KEY, v)
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        info!("persist_last_snapshot_metadata_with_tree successfully!");

        tree.flush().map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }
}

/// TODO: how to refactor with `current_tree`
fn new_tree(
    db: &sled::Db,
    key: &str,
) -> Result<sled::Tree> {
    db.open_tree(key)
        .map_err(|e| SnapshotError::OperationFailed(format!("{e:?}")).into())
}

pub fn init_sled_state_machine_db(
    sled_db_root_path: impl AsRef<std::path::Path> + std::fmt::Debug
) -> std::result::Result<sled::Db, sled::Error> {
    tracing::debug!(
        "init_sled_state_machine_db from path: {:?}",
        sled_db_root_path
    );

    let path = sled_db_root_path.as_ref();
    let state_machine_db_path = path.join("state_machine");

    sled::Config::default()
        .path(&state_machine_db_path)
        .cache_capacity(10 * 1024 * 1024) //10MB
        .flush_every_ms(Some(3))
        .use_compression(true)
        .compression_factor(1)
        // .segment_size(256)
        // .print_profile_on_drop(true)
        .open()
}
