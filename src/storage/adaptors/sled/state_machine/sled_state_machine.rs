//! It works as KV storage for client business CRUDs.

use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use async_compression::tokio::bufread::GzipDecoder;
use metrics::counter;
use parking_lot::Mutex;
use prost::Message;
use sled::Batch;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::sync::RwLock;
use tokio_tar::Archive;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::trace;
use tracing::warn;

use crate::constants::LAST_SNAPSHOT_METADATA_KEY;
use crate::constants::STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX;
use crate::constants::STATE_MACHINE_META_KEY_LAST_APPLIED_TERM;
use crate::constants::STATE_MACHINE_META_NAMESPACE;
use crate::constants::STATE_MACHINE_TREE;
use crate::constants::STATE_SNAPSHOT_METADATA_TREE;
use crate::convert::safe_kv;
use crate::convert::safe_vk;
use crate::file_io::compute_checksum_from_folder_path;
use crate::init_sled_state_machine_db;
use crate::proto::client::write_command::Delete;
use crate::proto::client::write_command::Insert;
use crate::proto::client::write_command::Operation;
use crate::proto::client::WriteCommand;
use crate::proto::common::entry_payload::Payload;
use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::proto::storage::SnapshotMetadata;
use crate::Error;
use crate::SnapshotError;
use crate::StateMachine;
use crate::StorageError;

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
    fn start(&self) -> Result<(), Error> {
        debug!("start state machine");
        self.is_serving.store(true, Ordering::Release);
        Ok(())
    }

    fn stop(&self) -> Result<(), Error> {
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
    ) -> Result<(), Error> {
        debug!(%self.node_id, ?snapshot_metadata, "update_last_snapshot_metadata");
        let last_included = snapshot_metadata.last_included.unwrap();
        self.last_included_index.store(last_included.index, Ordering::SeqCst);
        self.last_included_term.store(last_included.term, Ordering::SeqCst);
        *self.last_snapshot_checksum.lock() = Some(snapshot_metadata.checksum_array()?);

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
                checksum: self.last_snapshot_checksum.lock().as_ref().map(|arr| arr.to_vec())?,
            })
        } else {
            None
        }
    }

    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        match self.current_tree().get(key_buffer) {
            Ok(Some(v)) => Ok(Some(v.to_vec())),
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
    ) -> Result<(), Error> {
        trace!("Applying chunk: {:?}.", chunk);

        let mut highest_index_entry: Option<LogId> = None;
        let mut batch = Batch::default();
        for entry in chunk {
            assert!(entry.payload.is_some(), "Entry payload should not be None!");

            if let Some(log_id) = highest_index_entry {
                if entry.index > log_id.index {
                    highest_index_entry = Some(LogId {
                        index: entry.index,
                        term: entry.term,
                    });
                } else {
                    assert!(
                        entry.index > log_id.index,
                        "apply_chunk: received unordered entry at index {}",
                        entry.index
                    );
                }
            } else {
                highest_index_entry = Some(LogId {
                    index: entry.index,
                    term: entry.term,
                });
            }
            match entry.payload.unwrap().payload {
                Some(Payload::Noop(_)) => {
                    debug!("Handling NOOP command at index {}", entry.index);
                }
                Some(Payload::Command(data)) => {
                    // Business write operation - deserialize and apply
                    match WriteCommand::decode(&data[..]) {
                        Ok(write_cmd) => match write_cmd.operation {
                            Some(Operation::Insert(Insert { key, value })) => {
                                debug!(
                                    "Applying INSERT command at index {}: {:?}",
                                    entry.index, key
                                );
                                batch.insert(key, value);
                            }
                            Some(Operation::Delete(Delete { key })) => {
                                debug!(
                                    "Applying DELETE command at index {}: {:?}",
                                    entry.index, key
                                );
                                batch.remove(key);
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
                    }
                }
                Some(Payload::Config(_config_change)) => {
                    debug!(
                        "Ignoring config change in state machine at index {}",
                        entry.index
                    );
                    // Update only the configuration state of the Raft layer, without writing to the
                    // state machine Example: raft.
                    // update_cluster_config(config_change);
                }
                None => panic!("Entry payload variant should not be None!"),
            }

            let msg_id = entry.index.to_string();
            let id = self.node_id.to_string();
            info!("[{}]- COMMITTED_LOG_METRIC: {} ", self.node_id, &msg_id);
            // Increment the "committed_log" counter with labels
            counter!(
                "committed_log",
                "id" => id,
                "msg_id" => msg_id
            )
            .increment(1);
        }

        // Apply batch and update last applied index
        self.apply_batch(batch)?;
        if let Some(log_id) = highest_index_entry {
            debug!(
                "[Node-{}] State machine - updated last_applied: {:?}",
                self.node_id, log_id
            );
            self.update_last_applied(log_id);
        }

        Ok(())
    }

    fn save_hard_state(&self) -> Result<(), Error> {
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
    ) -> Result<(), Error> {
        debug!(%self.node_id, ?last_applied, "persist_last_applied");
        let db = self.db.load();
        let tree = db.open_tree(STATE_MACHINE_META_NAMESPACE)?;
        tree.insert(
            STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX,
            &safe_kv(last_applied.index),
        )?;
        tree.insert(
            STATE_MACHINE_META_KEY_LAST_APPLIED_TERM,
            &safe_kv(last_applied.term),
        )?;

        tree.flush()?;
        Ok(())
    }

    fn persist_last_snapshot_metadata(
        &self,
        last_snapshot_metadata: &SnapshotMetadata,
    ) -> Result<(), Error> {
        debug!(%self.node_id, ?last_snapshot_metadata, "persist_last_snapshot_metadata");
        let db = self.db.load();
        let tree = db.open_tree(STATE_SNAPSHOT_METADATA_TREE)?;
        self.persist_last_snapshot_metadata_with_tree(tree, last_snapshot_metadata)?;

        Ok(())
    }

    fn flush(&self) -> Result<(), Error> {
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

    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }

    #[instrument(skip(self))]
    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: PathBuf,
        last_included: LogId,
    ) -> Result<[u8; 32], Error> {
        // 1. Get a lightweight write lock (to prevent concurrent snapshot generation)
        let _guard = self.snapshot_lock.write().await;

        // 2. Create a new state machine database instance
        let new_db = init_sled_state_machine_db(&new_snapshot_dir)?;

        let exist_db_tree = self.current_tree();
        let new_state_machine_tree = new_tree(&new_db, STATE_MACHINE_TREE)?;
        let new_snapshot_metadatat_tree = new_tree(&new_db, STATE_SNAPSHOT_METADATA_TREE)?;

        let mut batch = sled::Batch::default();
        let mut counter = 0;

        for item in exist_db_tree.iter() {
            let (k, v) = item?;

            let key_num = match safe_vk(&k) {
                Ok(v) => v,
                Err(e) => {
                    error!(?e, "generate_snapshot_data::safe_vk");
                    return Err(e);
                }
            };
            if key_num > last_included.index {
                break; // Stop applying further entries as they will all be greater
            }

            batch.insert(k, v);
            counter += 1;

            // Perform a batch insert every 100 records
            if counter % 100 == 0 {
                new_state_machine_tree.apply_batch(batch)?;
                batch = sled::Batch::default(); // Reset the batch object
            }
        }

        // Process the remaining data (the tail data of less than 100 records)
        if counter % 100 != 0 {
            new_state_machine_tree.apply_batch(batch)?;
        }

        // Make sure flush into disk
        new_db.flush()?;

        // Calculate the checksum after generating snapshot data
        let checksum = compute_checksum_from_folder_path(&new_snapshot_dir).await?;

        println!("checksum = {:?}", checksum);
        // Make sure last included is updated to the new ones

        let last_snapshot_metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: checksum.to_vec(),
        };

        self.update_last_snapshot_metadata(&last_snapshot_metadata)?;

        // Make sure last included is persisted into local database
        self.persist_last_snapshot_metadata(&last_snapshot_metadata)?;
        // Make sure last included is persisted into the new database
        self.persist_last_snapshot_metadata_with_tree(
            new_snapshot_metadatat_tree,
            &last_snapshot_metadata,
        )?;

        Ok(checksum)
    }

    #[instrument(skil(self))]
    async fn apply_snapshot_from_file(
        &self,
        metadata: &SnapshotMetadata,
        decompressed_snapshot_path: PathBuf,
    ) -> Result<(), Error> {
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
            // 4. Create temp directory for decompression Using tempfile crate ensures secure
            //    cleanup
            // let temp_dir = tempfile::tempdir().map_err(StorageError::IoError)?;
            // let temp_dir_path = temp_dir.path().to_path_buf();

            // debug!(
            //     ?decompressed_snapshot_path,
            //     ?temp_dir_path,
            //     "5. Decompress snapshot using proper chunking/validation"
            // );
            // 5. Decompress snapshot using proper chunking/validation Added compression format
            //    validation based on
            // self.decompress_snapshot(&compressed_path, &temp_dir_path).await?;

            debug!(
                ?decompressed_snapshot_path,
                "6. CRITICAL SECURITY STEP: Validate checksum"
            );
            // 6. CRITICAL SECURITY STEP: Validate checksum Prevents tampered or corrupted snapshots
            //    from being applied
            let computed_checksum =
                compute_checksum_from_folder_path(&decompressed_snapshot_path).await?;

            if metadata.checksum != computed_checksum {
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
            let db = init_sled_state_machine_db(&decompressed_snapshot_path)?;

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

    async fn reset(&self) -> Result<(), Error> {
        let db = self.db.load();
        db.clear()?;
        Ok(())
    }
}

impl SledStateMachine {
    pub fn new(
        node_id: u32,
        db: Arc<sled::Db>,
    ) -> Result<Self, Error> {
        let state_machine_meta_tree = db.open_tree(STATE_MACHINE_META_NAMESPACE)?;
        let (last_applied_index, last_applied_term) =
            Self::load_state_machine_metadata(&state_machine_meta_tree)?;

        let snapshot_meta_tree = db.open_tree(STATE_SNAPSHOT_METADATA_TREE)?;

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

    fn load_state_machine_metadata(tree: &sled::Tree) -> Result<(u64, u64), Error> {
        let index = tree
            .get(STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX)?
            .map(safe_vk)
            .unwrap_or(Ok(0))?;

        let term = tree
            .get(STATE_MACHINE_META_KEY_LAST_APPLIED_TERM)?
            .map(safe_vk)
            .unwrap_or(Ok(0))?;

        Ok((index, term))
    }

    pub(super) fn load_snapshot_metadata(
        tree: &sled::Tree
    ) -> Result<Option<SnapshotMetadata>, Error> {
        if let Ok(Some(v)) = tree.get(LAST_SNAPSHOT_METADATA_KEY) {
            info!(
                "found SnapshotMetadata from DB with key: {}",
                LAST_SNAPSHOT_METADATA_KEY
            );
            let v = v.to_vec();
            match bincode::deserialize::<SnapshotMetadata>(&v) {
                Ok(last_snapshot_metadata) => {
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
    ) -> Result<(), Error> {
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
    ) -> Result<(), Error> {
        let v = bincode::serialize(last_snapshot_metadata).map_err(StorageError::BincodeError)?;

        tree.insert(LAST_SNAPSHOT_METADATA_KEY, v)?;
        info!("persist_last_snapshot_metadata_with_tree successfully!");

        tree.flush()?;
        Ok(())
    }

    pub(super) async fn decompress_snapshot(
        &self,
        compressed_path: &Path,
        dest_dir: &Path,
    ) -> Result<(), Error> {
        let file = File::open(compressed_path).await.map_err(StorageError::IoError)?;
        let buf_reader = BufReader::new(file);
        let gzip_decoder = GzipDecoder::new(buf_reader);
        let mut archive = Archive::new(gzip_decoder);

        archive.unpack(dest_dir).await.map_err(StorageError::IoError)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn iter(&self) -> sled::Iter {
        self.current_tree().iter()
    }
}

/// TODO: how to refactor with `current_tree`
fn new_tree(
    db: &sled::Db,
    key: &str,
) -> Result<sled::Tree, Error> {
    db.open_tree(key)
        .map_err(|e| SnapshotError::OperationFailed(format!("{e:?}")).into())
}
