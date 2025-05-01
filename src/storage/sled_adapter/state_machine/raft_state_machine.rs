//! It works as KV storage for client business CRUDs.

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arc_swap::ArcSwap;
use autometrics::autometrics;
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
use crate::constants::STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX;
use crate::constants::STATE_MACHINE_META_KEY_LAST_APPLIED_TERM;
use crate::constants::STATE_MACHINE_META_NAMESPACE;
use crate::constants::STATE_MACHINE_TREE;
use crate::constants::STATE_SNAPSHOT_METADATA_TREE;
use crate::convert::safe_kv;
use crate::convert::safe_vk;
use crate::init_sled_state_machine_db;
use crate::proto;
use crate::proto::client_command::Command;
use crate::proto::client_command::Insert;
use crate::proto::ClientCommand;
use crate::proto::Entry;
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
        index: u64,
        term: u64,
    ) {
        debug!(%index, %term, "update_last_applied");
        self.last_applied_index.store(index, Ordering::SeqCst);
        self.last_applied_term.store(term, Ordering::SeqCst);
    }

    fn last_applied(&self) -> (u64, u64) {
        (
            self.last_applied_index.load(Ordering::SeqCst),
            self.last_applied_term.load(Ordering::SeqCst),
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
        let mut highest_index_entry = None;
        let mut batch = Batch::default();
        for entry in chunk {
            if entry.command.is_empty() {
                warn!("why entry command is empty?");
                continue;
            }
            if let Some((index, _term)) = highest_index_entry {
                if entry.index > index {
                    highest_index_entry = Some((entry.index, entry.term));
                } else {
                    panic!("apply_chunk: receive entry not in order, TBF")
                }
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
            if let Some((index, term)) = highest_index_entry {
                self.update_last_applied(index, term);
            }
            Ok(())
        }
    }

    fn save_hard_state(&self) -> Result<()> {
        let db = self.db.load();
        let tree = db.open_tree(STATE_MACHINE_META_NAMESPACE)?;
        let (last_applied_index, last_applied_term) = self.last_applied();
        tree.insert(STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX, &safe_kv(last_applied_index))?;
        tree.insert(STATE_MACHINE_META_KEY_LAST_APPLIED_TERM, &safe_kv(last_applied_term))?;

        tree.flush()?;
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
        temp_snapshot_dir: &PathBuf,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<()> {
        // 1. Get a lightweight write lock (to prevent concurrent snapshot generation)
        let _guard = self.snapshot_lock.write().await;
        // 2. Create a new state machine database instance
        let new_db = init_sled_state_machine_db(temp_snapshot_dir).map_err(|e| StorageError::IoError(e))?;

        let exist_db_tree = self.current_tree();
        let new_state_machine_tree = new_tree(&new_db, STATE_MACHINE_TREE)?;
        let new_snapshot_metadata_tree = new_tree(&new_db, STATE_SNAPSHOT_METADATA_TREE)?;

        let mut batch = sled::Batch::default();
        let mut counter = 0;

        for item in exist_db_tree.iter() {
            let (k, v) = item?;

            let key_num = safe_vk(&k)?;
            if key_num > last_included_index {
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
        new_snapshot_metadata_tree.insert(SNAPSHOT_METADATA_KEY_LAST_INCLUDED_INDEX, &safe_kv(last_included_index))?;
        new_snapshot_metadata_tree.insert(SNAPSHOT_METADATA_KEY_LAST_INCLUDED_TERM, &safe_kv(last_included_term))?;

        new_db.flush()?;
        Ok(())
    }

    async fn apply_snapshot_from_file(
        &self,
        metadata: proto::SnapshotMetadata,
        snapshot_path: PathBuf,
    ) -> Result<()> {
        // 1. Get a lightweight write lock (to prevent concurrent snapshot generation)
        let _guard = self.snapshot_lock.write().await;

        // 2. Create a new state machine database instance
        let db = init_sled_state_machine_db(&snapshot_path).map_err(|e| StorageError::PathError {
            path: snapshot_path,
            source: e,
        })?;

        // 3. Atomically replace the current database
        self.db.store(Arc::new(db));

        // 4. Update the last applied log index and term
        self.last_applied_index
            .store(metadata.last_included_index, Ordering::SeqCst);
        self.last_applied_term
            .store(metadata.last_included_term, Ordering::SeqCst);

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
        let (index, term) = Self::load_metadata(&state_machine_meta_tree)?;

        let sm = RaftStateMachine {
            db: ArcSwap::from(db),
            is_serving: Arc::new(AtomicBool::new(true)),
            node_id,

            last_applied_index: AtomicU64::new(0),
            last_applied_term: AtomicU64::new(0),

            snapshot_lock: RwLock::new(()),
        };

        //very important to sync the last applied index into memory
        sm.update_last_applied(index, term);
        Ok(sm)
    }

    fn load_metadata(tree: &sled::Tree) -> Result<(u64, u64)> {
        let index = tree
            .get(STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX)?
            .map(|v| safe_vk(v))
            .unwrap_or(Ok(0))?;

        let term = tree
            .get(STATE_MACHINE_META_KEY_LAST_APPLIED_TERM)?
            .map(|v| safe_vk(v))
            .unwrap_or(Ok(0))?;

        Ok((index, term))
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
}

/// TODO: how to refactor with `current_tree`
fn new_tree(
    db: &sled::Db,
    key: &str,
) -> Result<sled::Tree> {
    db.open_tree(key)
        .map_err(|e| StorageError::Snapshot(format!("{:?}", e)).into())
}
