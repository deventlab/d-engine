//! It works as KV storage for client business CRUDs.
//!
//!
use crate::{
    grpc::rpc_service::{
        client_command::{Command, Insert},
        ClientCommand, Entry, SnapshotEntry,
    },
    util::vk,
    Error, Result, StateMachine, API_SLO, COMMITTED_LOG_METRIC,
};
use crate::{storage::sled_adapter::STATE_MACHINE_NAMESPACE, StateMachineIter};
use autometrics::autometrics;
use log::{debug, error, info, warn};
use prost::Message;
use sled::Batch;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tonic::async_trait;

#[derive(Debug)]
pub struct RaftStateMachine {
    node_id: u32,
    /// Volatile state on all servers:
    /// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    last_applied: AtomicU64,

    db: Arc<sled::Db>,
    tree: Arc<sled::Tree>,
    is_serving: Arc<AtomicBool>,
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

    fn get(&self, key_buffer: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.tree.get(key_buffer) {
            Ok(Some(v)) => {
                return Ok(Some(v.to_vec()));
            }
            Ok(None) => return Ok(None),
            Err(e) => {
                error!("state_machine get error: {}", e);
                return Err(Error::SledError(e));
            }
        }
    }

    // fn handle_commit(&self, new_commit_index: u64) -> Result<()> {
    //     info!(
    //         "CommitSuccessHandler received new commit: {:?}",
    //         new_commit_index
    //     );

    //     let last_applied = self.last_applied();
    //     info!(
    //         "last_applied: {} - new_commit_index: {}",
    //         last_applied, new_commit_index
    //     );

    //     if new_commit_index <= last_applied {
    //         debug!("new_commit_index <= last_applied, skipping apply action.");
    //         return Ok(());
    //     }

    //     let start_index = last_applied + 1;
    //     let start_time = Instant::now();

    //     match self.apply_to_state_machine_up_to_commit_index(start_index..=new_commit_index) {
    //         Ok(result) => {
    //             if let Some(last_applied) = result.last() {
    //                 self.update_last_applied(*last_applied);
    //             }

    //             info!(
    //                 "Applied to state machine from {} to {} in [{:?}] ms",
    //                 start_index,
    //                 new_commit_index,
    //                 start_time.elapsed().as_millis()
    //             );
    //         }
    //         Err(e) => {
    //             error!("Failed to apply to state machine: {:?}", e);
    //             return Err(e);
    //         }
    //     }

    //     Ok(())
    // }

    // #[autometrics(objective = API_SLO)]
    // fn apply_to_state_machine_up_to_commit_index(
    //     &self,
    //     range: RangeInclusive<u64>,
    // ) -> Result<Vector<u64>> {
    //     // Start the timer
    //     let start_time = Instant::now();
    //     debug!(
    //         "apply_to_state_machine_up_to_commit_index, range: {:?}",
    //         &range,
    //     );

    //     let mut result = Vector::<u64>::new();
    //     let mut batch = Batch::default();
    //     for entry in self.raft_log.get_entries_between(range.clone()) {
    //         if entry.command.len() < 1 {
    //             warn!("why entry command is empty?");
    //             continue;
    //         }

    //         debug!("[ConverterEngine] prepare to insert entry({:?})", entry);
    //         let req = match ClientCommand::decode(entry.command.as_slice()) {
    //             Ok(r) => r,
    //             Err(e) => {
    //                 error!("ClientCommand::decode failed: {:?}", e);
    //                 continue;
    //             }
    //         };
    //         match req.command {
    //             Some(Command::Insert(Insert { key, value })) => {
    //                 debug!("Handling INSERT command: {:?}", key);
    //                 batch.insert(key, value);
    //             }
    //             Some(Command::Delete(key)) => {
    //                 // Handle DELETE command
    //                 debug!("Handling DELETE command: {:?}", key);
    //                 batch.remove(key);
    //             }
    //             Some(Command::NoOp(true)) => {
    //                 // Handle DELETE command
    //                 debug!("Handling NOOP command. Do Nothing.");
    //             }
    //             _ => {
    //                 // Handle the case where no command is set
    //                 warn!("Can not identify which command it is.");
    //             }
    //         }
    //         //note: entry.index might be different than req.key
    //         result.push_back(entry.index);

    //         let msg_id = entry.index.to_string();
    //         let id = self.node_id.to_string();
    //         COMMITTED_LOG_METRIC
    //             .with_label_values(&[&id, &msg_id])
    //             .inc();
    //         info!("COMMITTED_LOG_METRIC: {} ", &msg_id);
    //     }

    //     // Calculate the duration
    //     info!(
    //         "get_entries_between loop range ({:?}), takes: [{:?}] ms",
    //         &range,
    //         start_time.elapsed().as_millis()
    //     );

    //     if let Err(e) = self.apply_batch(batch) {
    //         error!("local insert commit entry into kv store failed: {:?}", e);
    //         return Err(Error::MessageIOError);
    //     } else {
    //         debug!(
    //             "[ConverterEngine] convert bath successfully! range:{:?}",
    //             &range
    //         );
    //     }

    //     Ok(result)
    // }

    #[autometrics(objective = API_SLO)]
    fn apply_batch(&self, batch: Batch) -> Result<()> {
        if let Err(e) = self.tree.apply_batch(batch) {
            error!("state_machine apply_batch failed: {}", e);
            return Err(Error::SledError(e));
        }
        Ok(())
    }

    #[autometrics(objective = API_SLO)]
    fn apply_snapshot(&self, entry: SnapshotEntry) -> Result<()> {
        let key = entry.key;
        if let Err(e) = self.tree.insert(key.clone(), entry.value) {
            error!("apply_snapshot insert error: {}", e);
            return Err(Error::SledError(e));
        } else {
            debug!(
                "state machine insert snapshot entry (index: {:?}) successfully!",
                key
            );
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
                info!(
                    "Successfully flushed State Machine, bytes flushed: {}",
                    bytes
                );
                println!(
                    "Successfully flushed State Machine, bytes flushed: {}",
                    bytes
                );
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

    fn update_last_applied(&self, new_id: u64) {
        debug!("update_last_applied_id: {:?}", new_id);
        self.last_applied.store(new_id, Ordering::SeqCst);
    }

    async fn apply_chunk(&self, chunk: Vec<Entry>) -> Result<()> {
        let mut batch = Batch::default();
        for entry in chunk {
            if entry.command.len() < 1 {
                warn!("why entry command is empty?");
                continue;
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
            COMMITTED_LOG_METRIC
                .with_label_values(&[&id, &msg_id])
                .inc();
            info!("COMMITTED_LOG_METRIC: {} ", &msg_id);
        }

        // Calculate the duration
        // info!(
        //     "get_entries_between loop range ({:?}), takes: [{:?}] ms",
        //     &range,
        //     start_time.elapsed().as_millis()
        // );

        if let Err(e) = self.apply_batch(batch) {
            error!("local insert commit entry into kv store failed: {:?}", e);
            return Err(Error::MessageIOError);
        } else {
            debug!("[ConverterEngine] convert bath successfully! ");
            Ok(())
        }
    }
}

impl RaftStateMachine {
    #[autometrics(objective = API_SLO)]
    pub(crate) fn new(node_id: u32, db: Arc<sled::Db>) -> Self {
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
    pub(crate) fn before_shutdown(&self) -> crate::Result<()> {
        info!("state machine:: before_shutdown...");
        self.flush()?;
        Ok(())
    }
}
