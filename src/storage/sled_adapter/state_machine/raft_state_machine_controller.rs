use crate::alias::ROF;
use crate::alias::SMOF;
use crate::grpc::rpc_service::client_command::Command;
use crate::grpc::rpc_service::client_command::Insert;
use crate::grpc::rpc_service::ClientCommand;
use crate::RaftLog;
use crate::StateMachine;
use crate::TypeConfig;
use crate::API_SLO;
use crate::COMMITTED_LOG_METRIC;
use crate::{Error, Result};
use autometrics::autometrics;
use im::Vector;
use log::warn;
use log::{debug, error, info};
use prost::Message;
use sled::Batch;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;
use tonic::async_trait;

#[cfg(test)]
use mockall::automock;

pub(crate) struct RaftStateMachineController<T>
where
    T: TypeConfig,
{
    my_id: u32,

    raft_log: Arc<ROF<T>>,
    state_machine: Arc<SMOF<T>>,
    running: Arc<(Mutex<bool>, Notify)>,
}

#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait RaftStateMachineControllerApis: Send + Sync + 'static {
    /// Convert local log command to KV Store event.
    /// local log will be picked up to commit_index.
    /// This function will be triggerd when commit index been updated.
    ///
    fn apply_to_state_machine_up_to_commit_index(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vector<u64>>;

    fn handle_commit(&self, new_commit_index: u64) -> Result<()>;

    async fn start(
        &self,
        commit_success_receiver: mpsc::UnboundedReceiver<u64>,
        shutdown_signal: watch::Receiver<()>,
    ) -> Result<()>;

    async fn turn_on(&self);

    async fn turn_off(&self);

    async fn is_running(&self) -> bool;
}

impl<T> RaftStateMachineController<T>
where
    T: TypeConfig,
{
    pub(crate) fn new(my_id: u32, raft_log: Arc<ROF<T>>, state_machine: Arc<SMOF<T>>) -> Self {
        let running = Arc::new((Mutex::new(false), Notify::new()));
        RaftStateMachineController {
            my_id,
            raft_log,
            state_machine,
            running,
        }
    }

    async fn block_thread_until(&self, mut shutdown_signal: watch::Receiver<()>, my_id: u32) {
        let (lock, notify) = &*self.running;
        loop {
            // if the node is leader, here it always return true.
            if *lock.lock().await {
                break;
            }
            tokio::select! {
                // _ = time::sleep(Duration::from_millis(10)) => {
                //     if !server_is_ready.load(Ordering::Acquire) {
                //         info!("[AppendEntriesController] cluster is not ready yet");
                //     } else {
                //         return;
                //     }
                // }
                _ = shutdown_signal.changed() => {
                    warn!("[AppendEntriesController::block_thread_until | {}] shutdown signal received.", my_id);
                    break;
                }
                _ = notify.notified() => {
                    if *lock.lock().await {
                        break;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl<T> RaftStateMachineControllerApis for RaftStateMachineController<T>
where
    T: TypeConfig,
{
    #[autometrics(objective = API_SLO)]
    async fn start(
        &self,
        mut commit_success_receiver: mpsc::UnboundedReceiver<u64>,
        mut shutdown_signal: watch::Receiver<()>,
    ) -> Result<()> {
        info!("Start RaftStateMachineController.");

        self.block_thread_until(shutdown_signal.clone(), self.my_id)
            .await;

        loop {
            tokio::select! {
                _ = shutdown_signal.changed() => {
                    // Shutdown signal received
                    warn!("[CommitSuccessHandler::start_commit_success_listener | {}] shutdown signal received. Exiting...", self.my_id);
                    return Err(Error::Exit);
                }
                msg = commit_success_receiver.recv() => {
                    if let Some(new_commit_index) = msg {
                         if let Err(e) = self.handle_commit(new_commit_index) {
                            error!("handle_commit: {:?}", e);
                         }
                    } else {
                        error!("Receiver stream closed. Exiting listener...");
                        return Ok(());
                    }
                }
            }
        }
    }

    fn handle_commit(&self, new_commit_index: u64) -> Result<()> {
        info!(
            "CommitSuccessHandler received new commit: {:?}",
            new_commit_index
        );

        let last_applied = self.state_machine.last_applied();
        info!(
            "last_applied: {} - new_commit_index: {}",
            last_applied, new_commit_index
        );

        if new_commit_index <= last_applied {
            debug!("new_commit_index <= last_applied, skipping apply action.");
            return Ok(());
        }

        let start_index = last_applied + 1;
        let start_time = Instant::now();

        match self.apply_to_state_machine_up_to_commit_index(start_index..=new_commit_index) {
            Ok(result) => {
                if let Some(last_applied) = result.last() {
                    self.state_machine.update_last_applied(*last_applied);
                }

                info!(
                    "Applied to state machine from {} to {} in [{:?}] ms",
                    start_index,
                    new_commit_index,
                    start_time.elapsed().as_millis()
                );
            }
            Err(e) => {
                error!("Failed to apply to state machine: {:?}", e);
                return Err(e);
            }
        }

        Ok(())
    }

    #[autometrics(objective = API_SLO)]
    fn apply_to_state_machine_up_to_commit_index(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vector<u64>> {
        // Start the timer
        let start_time = Instant::now();
        debug!(
            "apply_to_state_machine_up_to_commit_index, range: {:?}",
            &range,
        );

        let mut result = Vector::<u64>::new();
        let mut batch = Batch::default();
        for entry in self.raft_log.get_entries_between(range.clone()) {
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
            //note: entry.index might be different than req.key
            result.push_back(entry.index);

            let msg_id = entry.index.to_string();
            let id = self.my_id.to_string();
            COMMITTED_LOG_METRIC
                .with_label_values(&[&id, &msg_id])
                .inc();
            info!("COMMITTED_LOG_METRIC: {} ", &msg_id);
        }

        // Calculate the duration
        info!(
            "get_entries_between loop range ({:?}), takes: [{:?}] ms",
            &range,
            start_time.elapsed().as_millis()
        );

        if let Err(e) = self.state_machine.apply_batch(batch) {
            error!("local insert commit entry into kv store failed: {:?}", e);
            return Err(Error::MessageIOError);
        } else {
            debug!(
                "[ConverterEngine] convert bath successfully! range:{:?}",
                &range
            );
        }

        Ok(result)
    }

    async fn turn_on(&self) {
        info!("turn on AppendEntriesController");
        let (lock, notify) = &*self.running;
        notify.notify_waiters(); // Notify all waiting tasks
        {
            let mut running = lock.lock().await;
            *running = true;
        }
        notify.notify_waiters(); // Notify all waiting tasks
    }

    async fn turn_off(&self) {
        info!("turn off AppendEntriesController");
        let (lock, _) = &*self.running;
        let mut running = lock.lock().await;

        *running = false;
    }

    async fn is_running(&self) -> bool {
        let (lock, _notify) = &*self.running;
        *lock.lock().await
    }
}

/// e.g.
/// let a = vector![1,2,3,4,6,5,7,8,9]; - true
/// let b = vector![1,2,3,4,6,7,8]; - false
/// let c = vector![2,3,4,5,6,7,8]; - true
///
/// also returns (start, end) range
#[autometrics(objective = API_SLO)]
fn is_legal_list(list: &Vector<u64>) -> (bool, (usize, usize)) {
    let emprty_range = (0, 0);
    if list.is_empty() {
        return (true, emprty_range);
    }

    let front = list.front().unwrap().clone() as usize;
    let back = list.back().unwrap().clone() as usize;

    if (back - front) == (list.len() - 1) {
        return (true, (front, (back + 1)));
    } else {
        return (false, emprty_range);
    }
}

// #[cfg(test)]
// mod tests {

//     use crate::{storage::raft_log, test_utils, util::kv, MockStateMachine};

//     use super::*;

//     #[test]
//     fn test_is_leagal_list() {
//         let a = vector![1, 2, 3, 4, 6, 5, 7, 8, 9];
//         let b = vector![1, 2, 3, 4, 6, 7, 8];
//         let c = vector![2, 3, 4, 5, 6, 7, 8];
//         let d = vector![];
//         let e = vector![7];
//         let f = vector![1, 2, 4];
//         let r = is_legal_list(&a);
//         assert!(r.0);
//         assert_eq!((1, 10), r.1);

//         let r = is_legal_list(&b);
//         assert!(!r.0);
//         assert_eq!((0, 0), r.1);

//         let r = is_legal_list(&c);
//         assert!(r.0);
//         assert_eq!((2, 9), r.1);

//         let r = is_legal_list(&d);
//         assert!(r.0);
//         assert_eq!((0, 0), r.1);

//         let r = is_legal_list(&e);
//         assert!(r.0);
//         assert_eq!((7, 8), r.1);

//         let r = is_legal_list(&f);
//         assert!(!r.0);
//         assert_eq!((0, 0), r.1);
//     }

//     /// # Case 1: success case
//     ///
//     #[tokio::test]
//     async fn test_apply_to_state_machine_up_to_commit_index_case1() {
//         let context = test_utils::setup(
//             "/tmp/test_apply_to_state_machine_up_to_commit_index_case1",
//             None,
//         )
//         .await;
//         let server = context.node.clone();
//         let raft = server.raft.clone();
//         let state = raft.state();

//         let key: u64 = 1;

//         let key_buffer = kv(key);
//         let value_buffer = kv(key);
//         test_utils::simulate_insert_proposal(raft.raft_log(), vector![1], 1);

//         raft.state_machine_controller()
//             .apply_to_state_machine_up_to_commit_index(1..=1)
//             .expect("failed!");
//         if let Ok(Some(target_buffer)) = raft.state_machine().get(&key_buffer) {
//             // let target_buffer = v.to_vec();
//             assert_eq!(value_buffer, target_buffer);
//         } else {
//             assert!(false);
//         }
//     }

//     /// # Case 2: failed case
//     ///
//     /// ## Setup:
//     /// 1. simulate state_machine io error
//     ///
//     /// ## Criterias:
//     /// 1. validate receive error
//     ///
//     #[tokio::test]
//     async fn test_apply_to_state_machine_up_to_commit_index_case2() {
//         let context = test_utils::setup(
//             "/tmp/test_apply_to_state_machine_up_to_commit_index_case2",
//             None,
//         )
//         .await;
//         let server = context.node.clone();
//         let raft = server.raft_core.clone();
//         let raft_log = raft.raft_log();
//         let state = raft.state();

//         let key: u64 = 1;

//         test_utils::simulate_insert_proposal(raft.raft_log(), vector![1], 1);

//         let mut mock_state_machine = MockStateMachine::new();
//         mock_state_machine
//             .expect_apply_batch()
//             .returning(|_| Err(Error::MessageIOError));

//         let state_machine_controller = RaftStateMachineController::new(
//             raft.id(),
//             state.clone(),
//             raft_log.clone(),
//             Arc::new(mock_state_machine),
//         );

//         if let Err(_) = state_machine_controller.apply_to_state_machine_up_to_commit_index(1..=1) {
//             assert!(true);
//         } else {
//             assert!(false);
//         }
//     }

//     /// # Case 3: test delete command
//     ///
//     /// ## Setup:
//     /// 1. simulate 2 entries in local logs
//     ///    entry(key=1) is insert command while second entry(key=1) entry is delete command
//     ///
//     /// ## Criterias:
//     /// 1. get key=1 from state machine, returns none
//     ///
//     #[tokio::test]
//     async fn test_apply_to_state_machine_up_to_commit_index_case3() {
//         let context = test_utils::setup(
//             "/tmp/test_apply_to_state_machine_up_to_commit_index_case3",
//             None,
//         )
//         .await;
//         let server = context.node.clone();
//         let raft = server.raft.clone();
//         let raft_log = raft.raft_log();
//         let state = raft.state();
//         let key: u64 = 1;
//         test_utils::simulate_insert_proposal(raft_log.clone(), vector![key], 1);
//         test_utils::simulate_delete_proposal(raft_log.clone(), key..=key, 1);

//         let state_machine_controller = RaftStateMachineController::new(
//             raft.id(),
//             state.clone(),
//             raft_log.clone(),
//             raft.state_machine(),
//         );

//         if let Err(_) = state_machine_controller.apply_to_state_machine_up_to_commit_index(1..=2) {
//             assert!(false);
//         } else {
//             assert!(true);
//         }

//         match raft.state_machine().get(&kv(key)) {
//             Ok(Some(v)) => {
//                 println!("{:?}", v);
//                 assert!(false);
//             }
//             Ok(None) => {
//                 assert!(true);
//             }
//             Err(e) => {
//                 eprintln!("{:?}", e);
//                 assert!(false);
//             }
//         }
//     }
//     /// # Case 4: test client proposal(insert and delete command), validate last entry id in both raft log and state machine
//     ///
//     /// ## Setup:
//     /// 1. simulate client proposals: insert(1..=10) => delete(1..3) => insert(11..=20)
//     /// 2. After apply action
//     ///
//     /// ## Criterias:
//     /// 1. state machine's last entry index = 20
//     /// 2. state machine's length =17
//     ///
//     #[tokio::test]
//     async fn test_apply_to_state_machine_up_to_commit_index_case4() {
//         let context = test_utils::setup(
//             "/tmp/test_apply_to_state_machine_up_to_commit_index_case4",
//             None,
//         )
//         .await;
//         let server = context.node.clone();
//         let raft = server.raft.clone();
//         let raft_log = raft.raft_log();
//         let state = raft.state();

//         test_utils::simulate_insert_proposal(raft_log.clone(), (1..=10).collect(), 1);
//         test_utils::simulate_delete_proposal(raft_log.clone(), 1..=3, 1);
//         test_utils::simulate_insert_proposal(raft_log.clone(), (11..=20).collect(), 1);

//         let state_machine_controller = RaftStateMachineController::new(
//             raft.id(),
//             state,
//             raft_log.clone(),
//             raft.state_machine(),
//         );

//         if let Err(_) = state_machine_controller.apply_to_state_machine_up_to_commit_index(1..=24) {
//             assert!(false);
//         } else {
//             assert!(true);
//         }

//         let state_machine = raft.state_machine();

//         assert_eq!(state_machine.len(), 17);
//         match state_machine.get(&kv(20)) {
//             Ok(Some(_v)) => assert!(true),
//             Ok(None) => assert!(false),
//             Err(_) => assert!(false),
//         }

//         match state_machine.get(&kv(21)) {
//             Ok(Some(_v)) => assert!(false),
//             Ok(None) => assert!(true),
//             Err(_) => assert!(false),
//         }
//     }

//     /// # Case 1: last_applied = commit_index, no change to state machine.
//     ///
//     /// ## Criteria:
//     /// 1. last_applied not changed
//     ///
//     #[tokio::test]
//     async fn test_handle_commit_case1() {
//         let context = test_utils::setup("/tmp/test_handle_commit_case1", None).await;
//         let server = context.node.clone();
//         let raft = server.raft.clone();
//         let raft_log = raft.raft_log();
//         let state = raft.state();
//         let last_applied = 10;
//         let commit_index = last_applied;
//         state.update_last_applied_id(last_applied);

//         raft.state_machine_controller()
//             .handle_commit(commit_index)
//             .expect("failed!");

//         assert_eq!(state.last_applied(), last_applied);
//     }

//     /// # Case 2: last_applied < commit_index
//     ///
//     /// ## Criteria:
//     /// 1. last_applied updated
//     ///
//     #[tokio::test]
//     async fn test_handle_commit_case2() {
//         let context = test_utils::setup("/tmp/test_handle_commit_case2", None).await;
//         let server = context.node.clone();
//         let raft = server.raft.clone();
//         let raft_log = raft.raft_log();
//         let state = raft.state();
//         let last_applied = 10;
//         let commit_index = last_applied + 1;
//         state.update_last_applied_id(last_applied);

//         test_utils::simulate_insert_proposal(raft_log.clone(), (1..=11).collect(), 1);

//         raft.state_machine_controller()
//             .handle_commit(commit_index)
//             .expect("failed!");

//         assert_eq!(state.last_applied(), commit_index);
//     }
// }
