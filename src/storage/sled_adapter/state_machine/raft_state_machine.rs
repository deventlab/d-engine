//! It works as KV storage for client business CRUDs.

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use autometrics::autometrics;
use parking_lot::Mutex;
use prost::Message;
use sled::Batch;
use tokio::sync::RwLock;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::constants::SNAPSHOT_METADATA_KEY_LAST_INCLUDED_INDEX;
use crate::constants::SNAPSHOT_METADATA_KEY_LAST_INCLUDED_TERM;
use crate::constants::SNAPSHOT_METADATA_KEY_LAST_SNAPSHOT_CHECKSUM;
use crate::constants::STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX;
use crate::constants::STATE_MACHINE_META_KEY_LAST_APPLIED_TERM;
use crate::constants::STATE_MACHINE_META_NAMESPACE;
use crate::constants::STATE_MACHINE_TREE;
use crate::constants::STATE_SNAPSHOT_METADATA_TREE;
use crate::convert::safe_kv;
use crate::convert::safe_vk;
use crate::file_io::compute_checksum_from_path;
use crate::init_sled_state_machine_db;
use crate::proto::client::client_command::Command;
use crate::proto::client::client_command::Insert;
use crate::proto::client::ClientCommand;
use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::proto::storage::SnapshotMetadata;
use crate::Result;
use crate::StateMachine;
use crate::StateMachineIter;
use crate::StorageError;
use crate::API_SLO;
use crate::COMMITTED_LOG_METRIC;

pub struct RaftStateMachine {
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

impl std::fmt::Debug for RaftStateMachine {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RaftStateMachine")
            .field("tree_len", &self.current_tree().len())
            .finish()
    }
}

impl Drop for RaftStateMachine {
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
impl StateMachine for RaftStateMachine {
    fn start(&self) -> Result<()> {
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
        debug!(?last_applied, "update_last_applied");
        self.last_applied_index.store(last_applied.index, Ordering::SeqCst);
        self.last_applied_term.store(last_applied.term, Ordering::SeqCst);
    }

    fn update_last_included(
        &self,
        last_included: LogId,
        new_checksum: Option<[u8; 32]>,
    ) {
        debug!(?last_included, "update_last_included");
        self.last_included_index.store(last_included.index, Ordering::SeqCst);
        self.last_included_term.store(last_included.term, Ordering::SeqCst);
        *self.last_snapshot_checksum.lock() = new_checksum;
    }

    fn last_applied(&self) -> LogId {
        LogId {
            index: self.last_applied_index.load(Ordering::SeqCst),
            term: self.last_applied_term.load(Ordering::SeqCst),
        }
    }

    fn last_included(&self) -> (LogId, Option<[u8; 32]>) {
        (
            LogId {
                index: self.last_included_index.load(Ordering::SeqCst),
                term: self.last_included_term.load(Ordering::SeqCst),
            },
            *self.last_snapshot_checksum.lock(),
        )
    }

    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        match self.current_tree().get(key_buffer) {
            Ok(Some(v)) => Ok(Some(v.to_vec())),
            Ok(None) => Ok(None),
            Err(e) => {
                error!("state_machine get error: {}", e);
                Err(StorageError::DbError(e.to_string()).into())
            }
        }
    }

    fn iter(&self) -> StateMachineIter {
        self.current_tree().iter()
    }

    fn apply_chunk(
        &self,
        chunk: Vec<Entry>,
    ) -> Result<()> {
        let mut highest_index_entry: Option<LogId> = None;
        let mut batch = Batch::default();
        for entry in chunk {
            if entry.command.is_empty() {
                warn!("why entry command is empty?");
                continue;
            }
            if let Some(log_id) = highest_index_entry {
                if entry.index > log_id.index {
                    highest_index_entry = Some(LogId {
                        index: entry.index,
                        term: entry.term,
                    });
                } else {
                    panic!("apply_chunk: receive entry not in order, TBF")
                }
            } else {
                highest_index_entry = Some(LogId {
                    index: entry.index,
                    term: entry.term,
                });
            }

            debug!("[ConverterEngine] prepare to insert entry({:?})", entry);
            let req = match ClientCommand::decode(entry.command.as_slice()) {
                Ok(r) => r,
                Err(e) => {
                    error!("ClientCommand::decode failed: {:?}", e);
                    continue;
                }
            };
            match req.command {
                Some(Command::Insert(Insert { key, value })) => {
                    debug!("Handling INSERT command: {:?}", key);
                    batch.insert(key, value);
                }
                Some(Command::Delete(key)) => {
                    // Handle DELETE command
                    debug!("Handling DELETE command: {:?}", key);
                    batch.remove(key);
                }
                Some(Command::NoOp(true)) => {
                    // Handle NOOP command
                    info!("Handling NOOP command. Do Nothing.");
                }
                _ => {
                    // Handle the case where no command is set
                    warn!("Can not identify which command it is.");
                }
            }

            let msg_id = entry.index.to_string();
            let id = self.node_id.to_string();
            COMMITTED_LOG_METRIC.with_label_values(&[&id, &msg_id]).inc();
            info!("[{}]- COMMITTED_LOG_METRIC: {} ", self.node_id, &msg_id);
        }

        if let Err(e) = self.apply_batch(batch) {
            error!("local insert commit entry into kv store failed: {:?}", e);
            Err(e)
        } else {
            debug!("[ConverterEngine] convert bath successfully! ");
            if let Some(log_id) = highest_index_entry {
                self.update_last_applied(log_id);
            }
            Ok(())
        }
    }

    fn save_hard_state(&self) -> Result<()> {
        let last_applied = self.last_applied();
        self.persist_last_applied(last_applied)?;

        let (last_included, last_checksum) = self.last_included();
        self.persist_last_included(last_included, last_checksum)?;

        self.flush()?;
        Ok(())
    }

    fn persist_last_applied(
        &self,
        last_applied: LogId,
    ) -> Result<()> {
        let db = self.db.load();
        let tree = db.open_tree(STATE_MACHINE_META_NAMESPACE)?;
        tree.insert(STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX, &safe_kv(last_applied.index))?;
        tree.insert(STATE_MACHINE_META_KEY_LAST_APPLIED_TERM, &safe_kv(last_applied.term))?;

        tree.flush()?;
        Ok(())
    }

    fn persist_last_included(
        &self,
        last_included: LogId,
        last_checksum: Option<[u8; 32]>,
    ) -> Result<()> {
        let db = self.db.load();
        let tree = db.open_tree(STATE_SNAPSHOT_METADATA_TREE)?;
        self.persist_last_included_with_tree(tree, last_included, last_checksum)?;

        Ok(())
    }

    fn flush(&self) -> Result<()> {
        let db = self.db.load();
        match db.flush() {
            Ok(bytes) => {
                info!("Successfully flushed State Machine, bytes flushed: {}", bytes);
                println!("Successfully flushed State Machine, bytes flushed: {}", bytes);
            }
            Err(e) => {
                error!("Failed to flush State Machine: {}", e);
                eprintln!("Failed to flush State Machine: {}", e);
            }
        }
        Ok(())
    }

    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: PathBuf,
        last_included: LogId,
    ) -> Result<[u8; 32]> {
        // 1. Get a lightweight write lock (to prevent concurrent snapshot generation)
        let _guard = self.snapshot_lock.write().await;

        // 2. Create a new state machine database instance
        let new_db = init_sled_state_machine_db(&new_snapshot_dir).map_err(StorageError::IoError)?;

        let exist_db_tree = self.current_tree();
        let new_state_machine_tree = new_tree(&new_db, STATE_MACHINE_TREE)?;
        let new_snapshot_metadatat_tree = new_tree(&new_db, STATE_SNAPSHOT_METADATA_TREE)?;

        let mut batch = sled::Batch::default();
        let mut counter = 0;

        for item in exist_db_tree.iter() {
            let (k, v) = item?;

            let key_num = safe_vk(&k)?;
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
        let checksum = compute_checksum_from_path(&new_snapshot_dir).await?;

        // Make sure last included is updated to the new ones
        self.update_last_included(last_included, Some(checksum));

        // Make sure last included is persisted into local database
        self.persist_last_included(last_included, Some(checksum))?;
        // Make sure last included is persisted into the new database
        self.persist_last_included_with_tree(new_snapshot_metadatat_tree, last_included, Some(checksum))?;

        Ok(checksum)
    }

    async fn apply_snapshot_from_file(
        &self,
        metadata: &SnapshotMetadata,
        snapshot_path: PathBuf,
    ) -> Result<()> {
        if let Some(last_included) = metadata.last_included {
            // 1. Get a lightweight write lock (to prevent concurrent snapshot generation)
            let _guard = self.snapshot_lock.write().await;

            // 2. Create a new state machine database instance
            let db = init_sled_state_machine_db(&snapshot_path).map_err(|e| StorageError::PathError {
                path: snapshot_path,
                source: e,
            })?;

            // 3. Atomically replace the current database
            self.db.store(Arc::new(db));

            // 4. Update the last applied and last included index and term
            self.update_last_applied(last_included);
            self.update_last_included(last_included, Some(metadata.checksum_array()?));
        } else {
            error!(
                ?metadata,
                "apply_snapshot_from_file should not be triggered if metadata is none"
            );
        }

        Ok(())
    }

    #[cfg(test)]
    fn clean(&self) -> Result<()> {
        self.current_tree().clear()?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.current_tree().len()
    }
}

impl RaftStateMachine {
    pub fn new(
        node_id: u32,
        db: Arc<sled::Db>,
    ) -> Result<Self> {
        let state_machine_meta_tree = db.open_tree(STATE_MACHINE_META_NAMESPACE)?;
        let (last_applied_index, last_applied_term) = Self::load_state_machine_metadata(&state_machine_meta_tree)?;

        let snapshot_meta_tree = db.open_tree(STATE_SNAPSHOT_METADATA_TREE)?;
        let (last_included_index, last_included_term, last_snapshot_checksum) =
            Self::load_snapshot_metadata(&snapshot_meta_tree)?;

        let sm = RaftStateMachine {
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
        sm.update_last_included(
            LogId {
                term: last_included_term,
                index: last_included_index,
            },
            last_snapshot_checksum,
        );
        Ok(sm)
    }

    fn load_state_machine_metadata(tree: &sled::Tree) -> Result<(u64, u64)> {
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

    fn load_snapshot_metadata(tree: &sled::Tree) -> Result<(u64, u64, Option<[u8; 32]>)> {
        let index = tree
            .get(SNAPSHOT_METADATA_KEY_LAST_INCLUDED_INDEX)?
            .map(safe_vk)
            .unwrap_or(Ok(0))?;

        let term = tree
            .get(SNAPSHOT_METADATA_KEY_LAST_INCLUDED_TERM)?
            .map(safe_vk)
            .unwrap_or(Ok(0))?;

        let checksum = tree
            .get(SNAPSHOT_METADATA_KEY_LAST_SNAPSHOT_CHECKSUM)?
            .and_then(|ivec| {
                let bytes: Vec<u8> = ivec.to_vec();
                bytes.try_into().ok()
            });

        Ok((index, term, checksum))
    }

    #[autometrics(objective = API_SLO)]
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

    fn persist_last_included_with_tree(
        &self,
        tree: sled::Tree,
        last_included: LogId,
        last_checksum: Option<[u8; 32]>,
    ) -> Result<()> {
        tree.insert(SNAPSHOT_METADATA_KEY_LAST_INCLUDED_INDEX, &safe_kv(last_included.index))?;
        tree.insert(SNAPSHOT_METADATA_KEY_LAST_INCLUDED_TERM, &safe_kv(last_included.term))?;

        if let Some(checksum) = last_checksum {
            tree.insert(SNAPSHOT_METADATA_KEY_LAST_SNAPSHOT_CHECKSUM, &checksum)?;
        } else {
            // delete old checksum, if exists
            tree.remove(SNAPSHOT_METADATA_KEY_LAST_SNAPSHOT_CHECKSUM)?;
        }

        tree.flush()?;
        Ok(())
    }
}

/// TODO: how to refactor with `current_tree`
fn new_tree(
    db: &sled::Db,
    key: &str,
) -> Result<sled::Tree> {
    db.open_tree(key)
        .map_err(|e| StorageError::Snapshot(format!("{:?}", e)).into())
}
