//! It works as KV storage for client business CRUDs.
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use autometrics::autometrics;
use log::debug;
use log::error;
use log::info;
use log::warn;
use prost::Message;
use sled::Batch;
use tonic::async_trait;

use crate::convert::vk;
use crate::grpc::rpc_service::client_command::Command;
use crate::grpc::rpc_service::client_command::Insert;
use crate::grpc::rpc_service::ClientCommand;
use crate::grpc::rpc_service::Entry;
use crate::grpc::rpc_service::SnapshotEntry;
use crate::storage::sled_adapter::STATE_MACHINE_NAMESPACE;
use crate::Error;
use crate::Result;
use crate::StateMachine;
use crate::StateMachineIter;
use crate::API_SLO;
use crate::COMMITTED_LOG_METRIC;

pub struct RaftStateMachine {
    node_id: u32,

    /// Volatile state on all servers:
    /// index of highest log entry applied to state machine (initialized to 0,
    /// increases monotonically)
    last_applied: AtomicU64,

    db: Arc<sled::Db>,
    tree: Arc<sled::Tree>,
    is_serving: Arc<AtomicBool>,
}

impl std::fmt::Debug for RaftStateMachine {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RaftStateMachine")
            .field("tree_len", &self.tree.len())
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

    fn iter(&self) -> StateMachineIter {
        self.tree.iter()
    }

    fn get(
        &self,
        key_buffer: &Vec<u8>,
    ) -> Result<Option<Vec<u8>>> {
        match self.tree.get(key_buffer) {
            Ok(Some(v)) => Ok(Some(v.to_vec())),
            Ok(None) => Ok(None),
            Err(e) => {
                error!("state_machine get error: {}", e);
                Err(Error::SledError(e))
            }
        }
    }

    #[autometrics(objective = API_SLO)]
    fn apply_snapshot(
        &self,
        entry: SnapshotEntry,
    ) -> Result<()> {
        if self.is_running() {
            return Err(Error::StateMachinneError(
                "state machine is still running while applying snapshot".to_string(),
            ));
        }
        let key = entry.key;
        if let Err(e) = self.tree.insert(key.clone(), entry.value) {
            error!("apply_snapshot insert error: {}", e);
            return Err(Error::SledError(e));
        } else {
            debug!("state machine insert snapshot entry (index: {:?}) successfully!", key);
        }
        Ok(())
    }

    /// return the last entry index from state machine
    #[autometrics(objective = API_SLO)]
    fn last_entry_index(&self) -> Option<u64> {
        debug!("getting last entry from state machine");
        if let Ok(Some(v)) = self.tree.last() {
            let key: u64 = vk(&v.0.to_vec());
            debug!("last entry, key: {:?}.", key);

            Some(key)
        } else {
            debug!("no entry found from State Machine.");
            None
        }
    }

    fn flush(&self) -> Result<()> {
        match self.db.flush() {
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
    #[cfg(test)]
    fn clean(&self) -> Result<()> {
        self.tree.clear()?;
        Ok(())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.tree.len()
    }

    fn last_applied(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }

    fn update_last_applied(
        &self,
        new_id: u64,
    ) {
        debug!("update_last_applied_id: {:?}", new_id);
        self.last_applied.store(new_id, Ordering::SeqCst);
    }

    fn apply_chunk(
        &self,
        chunk: Vec<Entry>,
    ) -> Result<()> {
        let mut highest_index = None;
        let mut batch = Batch::default();
        for entry in chunk {
            if entry.command.is_empty() {
                warn!("why entry command is empty?");
                continue;
            }
            highest_index = Some(entry.index);

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
                    // Handle DELETE command
                    debug!("Handling NOOP command. Do Nothing.");
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
            Err(Error::MessageIOError)
        } else {
            debug!("[ConverterEngine] convert bath successfully! ");
            if highest_index.is_some() {
                self.update_last_applied(highest_index.unwrap());
            }
            Ok(())
        }
    }
}

impl RaftStateMachine {
    #[autometrics(objective = API_SLO)]
    pub fn new(
        node_id: u32,
        db: Arc<sled::Db>,
    ) -> Self {
        match db.open_tree(STATE_MACHINE_NAMESPACE) {
            Ok(state_machine_tree) => {
                let sm = RaftStateMachine {
                    db,
                    tree: Arc::new(state_machine_tree),
                    is_serving: Arc::new(AtomicBool::new(true)),

                    last_applied: AtomicU64::new(0),
                    node_id,
                };

                //very important to sync the last applied index into memory
                sm.last_applied
                    .store(sm.last_entry_index().unwrap_or(0), Ordering::SeqCst);
                sm
            }
            Err(e) => {
                error!("Failed to open state machine db tree: {}", e);
                panic!("failed to open sled tree: {}", e);
            }
        }
    }

    #[autometrics(objective = API_SLO)]
    pub(super) fn apply_batch(
        &self,
        batch: Batch,
    ) -> Result<()> {
        if let Err(e) = self.tree.apply_batch(batch) {
            error!("state_machine apply_batch failed: {}", e);
            return Err(Error::SledError(e));
        }
        Ok(())
    }
}
