use bytes::Bytes;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::NodeRole::Leader;
use d_engine_proto::common::membership_change::Change;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{self};

use super::CommitHandler;
use super::CommitHandlerDependencies;
use super::DefaultCommitHandler;
use crate::Error;
use crate::InternalEvent;
use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockStateMachineHandler;
use crate::MockTypeConfig;
use crate::NewCommitData;
use crate::Result;

const TEST_TERM: u64 = 1;

pub enum CommandType {
    Command(Bytes),
    Configuration(Change),
    Noop,
}

pub fn build_entries(
    commands: Vec<CommandType>,
    term: u64,
) -> Vec<Entry> {
    let mut r = vec![];
    let mut index = 1;
    for c in commands {
        let entry = match c {
            CommandType::Command(data) => Entry {
                index,
                term,
                payload: Some(EntryPayload::command(data)),
            },
            CommandType::Configuration(change) => Entry {
                index,
                term,
                payload: Some(EntryPayload::config(change)),
            },
            CommandType::Noop => Entry {
                index,
                term,
                payload: Some(EntryPayload::noop()),
            },
        };
        r.push(entry);
        index += 1;
    }
    r
}

pub struct TestHarness {
    role: i32,
    term: u64,
    mock_smh: Arc<MockStateMachineHandler<MockTypeConfig>>,
    mock_log: Arc<MockRaftLog>,
    mock_membership: Arc<MockMembership<MockTypeConfig>>,
    commit_tx: mpsc::UnboundedSender<NewCommitData>,
    commit_rx: Option<mpsc::UnboundedReceiver<NewCommitData>>,
    internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
    internal_event_rx: mpsc::UnboundedReceiver<InternalEvent>,
    shutdown_tx: watch::Sender<()>,
    shutdown_rx: Option<watch::Receiver<()>>,
    handle: Option<JoinHandle<()>>,
}
#[allow(clippy::too_many_arguments)]
fn setup_harness<F>(
    role: i32,
    term: u64,
    entries: Vec<Entry>,
    last_applied: u64,
    config_hook: F,
    snapshot_condition: Option<u64>,
) -> TestHarness
where
    F: Fn() -> bool + 'static + Send + Sync,
{
    let (commit_tx, commit_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let (internal_event_tx, internal_event_rx) = mpsc::unbounded_channel(); // Large capacity to prevent blocking in tests

    // Mock state machine
    let mut mock_smh = MockStateMachineHandler::new();
    let cloned_entries = entries.clone();
    mock_smh
        .expect_pending_range()
        .returning(move || Some(1..=cloned_entries.last().map(|e| e.index).unwrap_or(1)));
    mock_smh.expect_update_pending().returning(|_| {});
    mock_smh.expect_last_applied().returning(move || last_applied);
    mock_smh
        .expect_should_snapshot()
        .returning(move |data| snapshot_condition.is_some_and(|idx| data.new_commit_index >= idx));

    // Mock raft log
    let mut mock_log = MockRaftLog::new();
    mock_log.expect_get_entries_range().returning(move |_| Ok(entries.clone()));

    // Mock membership
    let mut mock_membership = MockMembership::new();
    mock_membership.expect_notify_config_applied().returning(|_| {});
    mock_membership.expect_apply_config_change().returning(move |_| {
        if config_hook() {
            Err(Error::Fatal("Command execution failed".to_string()))
        } else {
            Ok(())
        }
    });

    TestHarness {
        role,
        term,
        mock_smh: Arc::new(mock_smh),
        mock_log: Arc::new(mock_log),
        mock_membership: Arc::new(mock_membership),
        commit_rx: Some(commit_rx),
        commit_tx,
        internal_event_tx,
        internal_event_rx,
        shutdown_tx,
        shutdown_rx: Some(shutdown_rx),
        handle: None,
    }
}

impl TestHarness {
    async fn run_handler(&mut self) {
        let (sm_apply_tx, mut sm_apply_rx) = tokio::sync::mpsc::unbounded_channel();
        let sm_apply_tx = crate::StateMachineCommandSender::new(sm_apply_tx);

        let deps = CommitHandlerDependencies {
            state_machine_handler: self.mock_smh.clone(),
            raft_log: self.mock_log.clone(),
            membership: self.mock_membership.clone(),
            internal_event_tx: self.internal_event_tx.clone(),
            sm_apply_tx,
            shutdown_signal: self.shutdown_rx.take().unwrap(),
            max_batch_size: 10,
        };

        let mut handler = DefaultCommitHandler::<MockTypeConfig>::new(
            1,
            self.role,
            self.term,
            deps,
            self.commit_rx.take().unwrap(),
        );

        // Spawn SM Worker to process entries
        let _sm_worker_handle = tokio::spawn(async move {
            while let Some(_entries) = sm_apply_rx.recv().await {
                // SM Worker simply consumes entries from the channel
                // In a real system, this would apply them to the state machine
                // For testing, we just drain the channel to keep it open
            }
        });

        self.handle = Some(tokio::spawn(async move {
            let _ = handler.run().await;
        }));
    }

    async fn process_batch_handler(&mut self) -> Result<()> {
        let (sm_apply_tx, mut sm_apply_rx) = tokio::sync::mpsc::unbounded_channel();
        let sm_apply_tx = crate::StateMachineCommandSender::new(sm_apply_tx);

        let deps = CommitHandlerDependencies {
            state_machine_handler: self.mock_smh.clone(),
            raft_log: self.mock_log.clone(),
            membership: self.mock_membership.clone(),
            internal_event_tx: self.internal_event_tx.clone(),
            sm_apply_tx,
            shutdown_signal: self.shutdown_rx.take().unwrap(),
            max_batch_size: 10,
        };

        let handler = DefaultCommitHandler::<MockTypeConfig>::new(
            1,
            self.role,
            self.term,
            deps,
            self.commit_rx.take().unwrap(),
        );

        // Spawn SM Worker to process entries
        let _sm_worker_handle = tokio::spawn(async move {
            while let Some(_entries) = sm_apply_rx.recv().await {
                // SM Worker simply consumes entries from the channel
            }
        });

        handler.process_batch().await
    }

    /// Same as `process_batch_handler`, but the SM Worker's receiving end is dropped
    /// immediately instead of being drained — simulates the Worker task having
    /// already exited, so `send_to_sm_worker`'s send must fail.
    async fn process_batch_handler_with_closed_worker_channel(&mut self) -> Result<()> {
        let (sm_apply_tx, sm_apply_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(sm_apply_rx);
        let sm_apply_tx = crate::StateMachineCommandSender::new(sm_apply_tx);

        let deps = CommitHandlerDependencies {
            state_machine_handler: self.mock_smh.clone(),
            raft_log: self.mock_log.clone(),
            membership: self.mock_membership.clone(),
            internal_event_tx: self.internal_event_tx.clone(),
            sm_apply_tx,
            shutdown_signal: self.shutdown_rx.take().unwrap(),
            max_batch_size: 10,
        };

        let handler = DefaultCommitHandler::<MockTypeConfig>::new(
            1,
            self.role,
            self.term,
            deps,
            self.commit_rx.take().unwrap(),
        );

        handler.process_batch().await
    }

    async fn send_commit(
        &self,
        index: u64,
        role: i32,
    ) {
        self.commit_tx
            .send(NewCommitData {
                new_commit_index: index,
                role,
                current_term: TEST_TERM,
            })
            .unwrap();
    }

    async fn expect_snapshot_trigger(&mut self) -> bool {
        match time::timeout(Duration::from_millis(50), self.internal_event_rx.recv()).await {
            Ok(Some(_)) => true, // Event received normally
            Ok(None) => false,   // Channel closed
            Err(_) => false,     // Timeout
        }
    }
}

#[cfg(test)]
mod run_test {
    use d_engine_proto::common::AddNode;
    use d_engine_proto::common::NodeRole::Follower;
    use d_engine_proto::common::NodeRole::Leader;
    use d_engine_proto::common::RemoveNode;
    use d_engine_proto::common::membership_change::Change;
    use tokio::time;

    use super::*;

    /// 1. Test happy path with all entry types
    #[tokio::test]
    async fn test_full_processing_flow() {
        let entries = build_entries(
            vec![
                CommandType::Command(Bytes::from(b"cmd1".to_vec())),
                CommandType::Configuration(Change::AddNode(AddNode {
                    node_id: 1,
                    address: "addr".into(),
                    status: d_engine_proto::common::NodeStatus::Promotable as i32,
                })),
                CommandType::Noop,
                CommandType::Command(Bytes::from(b"cmd2".to_vec())),
            ],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            Some(4),
        );
        harness.run_handler().await;

        // Send commits to trigger processing
        for i in 1..=4 {
            harness.send_commit(i, Leader as i32).await;
        }

        // Verify snapshot triggered
        assert!(harness.expect_snapshot_trigger().await);

        // Clean shutdown
        harness.shutdown_tx.send(()).unwrap();
        harness.handle.unwrap().await.unwrap();
    }

    /// 2. Test leadership change during batch processing
    #[tokio::test]
    async fn test_leadership_loss_during_batch() {
        let entries = build_entries(
            vec![
                CommandType::Command(Bytes::from(b"cmd1".to_vec())),
                CommandType::Command(Bytes::from(b"cmd2".to_vec())),
            ],
            1,
        );
        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            None,
        );

        harness.run_handler().await;
        harness.send_commit(1, Leader as i32).await;
        harness.send_commit(2, Follower as i32).await;

        // Should not process second command
        time::sleep(Duration::from_millis(50)).await;
        harness.shutdown_tx.send(()).unwrap();
        harness.handle.unwrap().await.unwrap();
    }

    /// 3. Test config change with node removal (non-self)
    #[tokio::test]
    async fn test_config_remove_node() {
        let entries = build_entries(
            vec![CommandType::Configuration(Change::RemoveNode(RemoveNode {
                node_id: 2, // Remove different node (not self)
            }))],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            Some(4),
        );

        harness.run_handler().await;
        harness.send_commit(1, Leader as i32).await;
        time::sleep(Duration::from_millis(50)).await;
        harness.shutdown_tx.send(()).unwrap();
        harness.handle.unwrap().await.unwrap();
    }

    /// 4. Test self-removal detection logic
    ///
    /// Lightweight unit test verifying is_self_removal_config() correctly
    /// identifies when a node is removing itself from cluster.
    ///
    /// Related: Issue #200
    #[test]
    fn test_is_self_removal_config() {
        use d_engine_proto::common::AddNode;
        use d_engine_proto::common::MembershipChange;
        use d_engine_proto::common::RemoveNode;
        use d_engine_proto::common::membership_change::Change;

        // Case 1: Self-removal (my_id matches remove node_id)
        let self_removal = MembershipChange {
            change: Some(Change::RemoveNode(RemoveNode { node_id: 1 })),
        };
        assert!(
            DefaultCommitHandler::<MockTypeConfig>::is_self_removal_config(1, &self_removal),
            "Should detect self-removal when my_id == removed node_id"
        );

        // Case 2: Removing other node (my_id != remove node_id)
        let other_removal = MembershipChange {
            change: Some(Change::RemoveNode(RemoveNode { node_id: 2 })),
        };
        assert!(
            !DefaultCommitHandler::<MockTypeConfig>::is_self_removal_config(1, &other_removal),
            "Should NOT detect self-removal when removing different node"
        );

        // Case 3: AddNode (not a removal)
        let add_node = MembershipChange {
            change: Some(Change::AddNode(AddNode {
                node_id: 1,
                address: "127.0.0.1:8080".to_string(),
                status: 1,
            })),
        };
        assert!(
            !DefaultCommitHandler::<MockTypeConfig>::is_self_removal_config(1, &add_node),
            "Should NOT detect self-removal for AddNode changes"
        );

        // Case 4: No change
        let no_change = MembershipChange { change: None };
        assert!(
            !DefaultCommitHandler::<MockTypeConfig>::is_self_removal_config(1, &no_change),
            "Should NOT detect self-removal when change is None"
        );
    }

    /// 5. Test batch processing under load
    #[tokio::test]
    async fn test_high_throughput_processing() {
        let mut entries = Vec::new();
        for i in 1..=1000 {
            entries.push(CommandType::Command(Bytes::from(format!("cmd{i}"))));
        }
        let entries = build_entries(entries, 1);

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            None,
        );
        harness.run_handler().await;

        // Send all commits at once
        for i in 1..=1000 {
            harness.send_commit(i, Leader as i32).await;
        }

        println!("Sent all commits");
        // Verify snapshot at end
        assert!(!harness.expect_snapshot_trigger().await);
        harness.shutdown_tx.send(()).unwrap();
        println!("Shutdown sent");
        println!("Waiting for handle to finish");
        harness.handle.unwrap().await.unwrap();
        println!("Handle finished");
    }

    // Case 1: test if process_batch been triggered if
    // interval ticks
    //
    // ## Setup:
    // - process_idle_flush_interval_ms  = 1ms

    // ## Criterias:
    // - apply_batch been triggered twice
    //
    #[tokio::test]
    async fn test_run_case1() {
        tokio::time::pause();

        let entries = build_entries(
            vec![
                CommandType::Command(Bytes::from(b"cmd1".to_vec())),
                CommandType::Command(Bytes::from(b"cmd2".to_vec())),
            ],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            Some(4),
        );
        harness.run_handler().await;

        // Send commits to trigger processing
        for i in 1..=2 {
            harness.send_commit(i, Leader as i32).await;
        }
        tokio::time::advance(Duration::from_millis(3)).await;
        // Clean shutdown
        harness.shutdown_tx.send(()).unwrap();
        // Ensure the task completes
        harness.handle.unwrap().await.unwrap();
    }

    // Case 2: test if process_batch been triggered if
    // batch exceeds threshold
    //
    // ## Setup:
    // - commit_handle_interval_in_ms  = 1s
    // - send 10 new commit ids
    // - batch threshold = 2
    //
    // ## Criterias:
    // - apply_batch been triggered twice
    //
    #[tokio::test]
    async fn test_run_case2() {
        tokio::time::pause();

        // prepare commit channel
        // let (new_commit_tx, new_commit_rx) = mpsc::unbounded_channel::<NewCommitData>();
        // let (_graceful_tx, graceful_rx) = watch::channel(());
        // let mut handler = setup(batch_thresold, 1000, 2, new_commit_rx, graceful_rx);
        let batch_thresold = 10;
        let entries = build_entries(
            vec![
                CommandType::Command(Bytes::from(b"cmd1".to_vec())),
                CommandType::Command(Bytes::from(b"cmd2".to_vec())),
            ],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            None,
        );
        harness.run_handler().await;

        for i in 1..=batch_thresold {
            harness.send_commit(i, Leader as i32).await;
        }
        tokio::time::advance(Duration::from_millis(2)).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        // Clean shutdown
        harness.shutdown_tx.send(()).unwrap();
        // Ensure the task completes
        harness.handle.unwrap().await.unwrap();
    }

    // Case 3: test if process_batch will not be triggered if
    // batch not exceeds threshold, duration not exceeds interval
    //
    // ## Setup:
    // - commit_handle_interval_in_ms  = 1s
    // - send 10 new commit ids
    // - batch threshold = 1000
    //
    // ## Criterias:
    // - apply_batch been triggered only 1 time
    //
    #[tokio::test]
    async fn test_run_case3() {
        tokio::time::pause();

        // prepare commit channel
        // let (new_commit_tx, new_commit_rx) = mpsc::unbounded_channel::<NewCommitData>();
        // let (_graceful_tx, graceful_rx) = watch::channel(());
        // let mut handler = setup(batch_thresold, 1000, 1, new_commit_rx, graceful_rx);
        let batch_thresold = 1000;
        let entries = build_entries(
            vec![
                CommandType::Command(Bytes::from(b"cmd1".to_vec())),
                CommandType::Command(Bytes::from(b"cmd2".to_vec())),
            ],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            None,
        );
        harness.run_handler().await;

        for i in 1..=(batch_thresold - 10) {
            harness.send_commit(i, Leader as i32).await;
        }
        tokio::time::advance(Duration::from_millis(2)).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        // Clean shutdown
        harness.shutdown_tx.send(()).unwrap();
        // Ensure the task completes
        harness.handle.unwrap().await.unwrap();
    }

    // Case 4: test if process_batch will be triggered if both
    // batch exceeds threshold and duration exceeds interval
    //
    // ## Setup:
    // - commit_handle_interval_in_ms  = 2ms
    // - run for 3ms
    // - send 10 new commit ids
    // - batch threshold = 10
    //
    // ## Criterias:
    // - apply_batch been triggered only 3 time
    //
    #[tokio::test]
    async fn test_run_case4() {
        tokio::time::pause();

        // prepare commit channel
        // let (new_commit_tx, new_commit_rx) = mpsc::unbounded_channel::<NewCommitData>();
        // let (_graceful_tx, graceful_rx) = watch::channel(());
        let batch_thresold = 10;
        let entries = build_entries(
            vec![
                CommandType::Command(Bytes::from(b"cmd1".to_vec())),
                CommandType::Command(Bytes::from(b"cmd2".to_vec())),
            ],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            None,
        );
        harness.run_handler().await;

        for i in 1..=batch_thresold {
            harness.send_commit(i, Leader as i32).await;
        }
        tokio::time::advance(Duration::from_millis(2)).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        // Clean shutdown
        harness.shutdown_tx.send(()).unwrap();
        // Ensure the task completes
        harness.handle.unwrap().await.unwrap();
    }
}

#[cfg(test)]
mod process_batch_test {
    use d_engine_proto::common::AddNode;
    use d_engine_proto::common::RemoveNode;
    use d_engine_proto::common::membership_change::Change;
    use parking_lot::Mutex;

    use super::*;

    // Test helper setup with configurable mocks
    // fn setup_test_handler<F, G>(
    //     role: i32,
    //     term: u64,
    //     entries: Vec<Entry>,
    //     config_hook: F,
    //     command_hook: G,
    //     snapshot_condition: Option<u64>,
    // ) -> (
    //     DefaultCommitHandler<MockTypeConfig>,
    //     mpsc::Receiver<InternalEvent>,
    //     watch::Sender<()>,
    // ) where
    //     F: Fn() -> bool + 'static + Send + Sync,
    //     G: Fn() -> bool + 'static + Send + Sync
    // {
    //     let (_new_commit_tx, new_commit_rx) = mpsc::unbounded_channel();
    //     let (shutdown_tx, shutdown_rx) = watch::channel(());
    //     let (event_tx, event_rx) = mpsc::channel(10);

    //     // Mock state machine
    //     let mut mock_smh = MockStateMachineHandler::new();
    //     let cloned_entries = entries.clone();
    //     mock_smh.expect_pending_range()
    //         .returning(move || Some(1..=cloned_entries.last().map(|e| e.index).unwrap_or(1)));
    //     mock_smh.expect_apply_chunk()
    //         .returning(move |_| {
    //             if command_hook() {
    //                 Err(Error::Fatal("Command execution failed".to_string()))
    //             } else {
    //                 Ok(())
    //             }
    //         });
    //     mock_smh.expect_should_snapshot()
    //         .returning(move |data| snapshot_condition.map_or(false, |idx| data.new_commit_index
    // >= idx));

    //     // Mock raft log
    //     let mut mock_log = MockRaftLog::new();
    //     mock_log.expect_get_entries_range()
    //         .return_once(move |_| entries.clone());

    //     // Mock membership
    //     let mut mock_membership = MockMembership::new();
    //     mock_membership.expect_apply_config_change()
    //         .returning(move |_| if config_hook() {
    //             Err(Error::Fatal("Command execution failed".to_string()))
    //         } else {
    //             Ok(())
    //         });

    //     let handler = DefaultCommitHandler::new(
    //         role,
    //         term,
    //         Arc::new(mock_smh),
    //         Arc::new(mock_log),
    //         Arc::new(mock_membership),
    //         new_commit_rx,
    //         event_tx.clone(),
    //         100, // batch_threshold
    //         100, // process_interval
    //         shutdown_rx,
    //     );

    //     (handler, event_rx, shutdown_tx)
    // }

    // Generic entry creation helpers
    #[tokio::test]
    async fn processes_empty_batch_successfully() {
        let last_applied = 0;
        let mut harness = setup_harness(
            Leader as i32,
            1,
            vec![],
            last_applied as u64,
            move || false,
            None,
        );
        let result = harness.process_batch_handler().await;
        assert!(result.is_ok());
    }

    /// `CommitHandler` sends `ApplyEntries` on its own raw channel handle rather than
    /// through `StateMachineCommandSender` (the wrapper every other caller of
    /// `StateMachineWorker` goes through) — so a closed-channel failure here is
    /// currently classified as `Error::Fatal`, not `SnapshotError::WorkerUnavailable`
    /// like every other Worker-unavailable error in the codebase.
    ///
    /// `spawn_state_machine_commit_listener` (builder.rs) doesn't branch on
    /// `is_fatal()` — any `Err` from `run()` just logs and lets the task exit either
    /// way — so this discrepancy has no runtime consequence today. It's still the
    /// wrong classification: a `StateMachineWorker unavailable` should carry the same
    /// error type everywhere, not be Fatal only when CommitHandler happens to hit it.
    #[tokio::test]
    async fn test_process_batch_reports_worker_unavailable_not_fatal_when_channel_closed() {
        let entries = build_entries(vec![CommandType::Command(Bytes::from(b"cmd1".to_vec()))], 1);
        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            None,
        );

        let result = harness.process_batch_handler_with_closed_worker_channel().await;

        let err = result.expect_err("process_batch must fail when the SM Worker channel is closed");
        assert!(
            !err.is_fatal(),
            "a closed SM Worker channel must be reported the same way every other \
             Worker-unavailable error is (non-fatal) — not as Error::Fatal, which is \
             reserved for conditions that require node shutdown"
        );
    }

    #[tokio::test]
    async fn batches_consecutive_commands() {
        let entries = build_entries(
            vec![
                CommandType::Command(Bytes::from(b"cmd1".to_vec())),
                CommandType::Command(Bytes::from(b"cmd2".to_vec())),
                CommandType::Command(Bytes::from(b"cmd3".to_vec())),
            ],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            None,
        );

        // Expect single apply_chunk call with all 3 commands
        let result = harness.process_batch_handler().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn flushes_commands_at_config_change() {
        let entries = build_entries(
            vec![
                CommandType::Command(Bytes::from(b"cmd1".to_vec())),
                CommandType::Configuration(Change::AddNode(AddNode {
                    node_id: 1,
                    address: "addr".into(),
                    status: d_engine_proto::common::NodeStatus::Promotable as i32,
                })),
                CommandType::Command(Bytes::from(b"cmd2".to_vec())),
            ],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            None,
        );

        // Expect single apply_chunk call with all 3 commands
        let result = harness.process_batch_handler().await;
        assert!(result.is_ok());

        // Verification:
        // - apply_chunk called twice: [cmd1] and [cmd2]
        // - apply_config_change called once for entry 2
    }

    #[tokio::test]
    async fn flushes_commands_at_noop() {
        let entries = build_entries(
            vec![
                CommandType::Command(Bytes::from(b"cmd1".to_vec())),
                CommandType::Noop,
                CommandType::Command(Bytes::from(b"cmd2".to_vec())),
            ],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            None,
        );

        // Expect single apply_chunk call with all 3 commands
        let result = harness.process_batch_handler().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handles_config_failure_properly() {
        let entries = build_entries(
            vec![
                CommandType::Configuration(Change::AddNode(AddNode {
                    node_id: 1,
                    address: "addr".into(),
                    status: d_engine_proto::common::NodeStatus::Promotable as i32,
                })),
                CommandType::Command(Bytes::from(b"cmd1".to_vec())),
            ],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || true, // Config will fail
            None,
        );
        let result = harness.process_batch_handler().await;
        assert!(result.is_err());

        // Verify command was NOT applied
    }

    #[tokio::test]
    async fn processes_mixed_entries_correctly() {
        let entries = build_entries(
            vec![
                CommandType::Command(Bytes::from(b"cmd1".to_vec())),
                CommandType::Configuration(Change::AddNode(AddNode {
                    node_id: 1,
                    address: "addr".into(),
                    status: d_engine_proto::common::NodeStatus::Promotable as i32,
                })),
                CommandType::Noop,
                CommandType::Command(Bytes::from(b"cmd2".to_vec())),
                CommandType::Command(Bytes::from(b"cmd3".to_vec())),
            ],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            None,
        );
        let result = harness.process_batch_handler().await;
        assert!(result.is_ok());
        // Verification:
        // - apply_config_change called once for entry 2
        // - apply_chunk called twice: [cmd1] and [cmd2, cmd3]
    }

    #[tokio::test]
    async fn handles_large_command_batches() {
        let mut r = vec![];
        for _i in 1..=1000 {
            r.push(CommandType::Command(Bytes::from(b"cmd".to_vec())))
        }
        let entries = build_entries(r, 1);
        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            None,
        );
        let result = harness.process_batch_handler().await;
        assert!(result.is_ok());

        // Verification: apply_chunk called once with 1000 commands
    }

    /// Test 1: MembershipApplied event MUST be sent after successful config change
    ///
    /// Verifies that when apply_config_change() succeeds, the commit handler
    /// sends InternalEvent::MembershipApplied to notify Leader to refresh cache.
    ///
    /// Related: Bug fix #209 - cluster metadata cache timing issue
    #[tokio::test]
    async fn membership_applied_event_sent_on_success() {
        let entries = build_entries(
            vec![CommandType::Configuration(Change::AddNode(AddNode {
                node_id: 2,
                address: "127.0.0.1:8080".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            }))],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            || false, // Config succeeds
            None,
        );

        let result = harness.process_batch_handler().await;
        assert!(result.is_ok(), "Config change should succeed");

        // Verify MembershipApplied event was sent
        match tokio::time::timeout(Duration::from_millis(50), harness.internal_event_rx.recv())
            .await
        {
            Ok(Some(InternalEvent::MembershipApplied)) => {
                // Success - event received
            }
            Ok(Some(other)) => panic!("Expected MembershipApplied, got {other:?}"),
            Ok(None) => panic!("Event channel closed unexpectedly"),
            Err(_) => panic!("Timeout waiting for MembershipApplied event"),
        }
    }

    /// Test 2: Event order - MembershipApplied sent AFTER apply_config_change
    ///
    /// Verifies the critical ordering guarantee:
    /// 1. apply_config_change() updates membership state
    /// 2. notify_config_applied() barrier
    /// 3. MembershipApplied event sent
    ///
    /// This ensures Leader refreshes cache with up-to-date membership.
    ///
    /// Related: Bug fix #209
    #[tokio::test]
    async fn membership_applied_event_sent_after_apply() {
        let apply_order = Arc::new(Mutex::new(Vec::new()));
        let order_clone = apply_order.clone();

        let entries = build_entries(
            vec![CommandType::Configuration(Change::AddNode(AddNode {
                node_id: 2,
                address: "127.0.0.1:8080".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            }))],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            {
                let order = order_clone.clone();
                move || {
                    order.lock().push("apply_config_change");
                    false
                }
            },
            None,
        );

        harness.process_batch_handler().await.unwrap();

        // Verify event received
        match tokio::time::timeout(Duration::from_millis(50), harness.internal_event_rx.recv())
            .await
        {
            Ok(Some(InternalEvent::MembershipApplied)) => {
                // Record event reception
                apply_order.lock().push("MembershipApplied_event");
            }
            _ => panic!("Expected MembershipApplied event"),
        }

        // Verify ordering: apply first, then event
        let order = apply_order.lock();
        assert_eq!(
            *order,
            vec!["apply_config_change", "MembershipApplied_event"],
            "Event must be sent AFTER apply_config_change"
        );
    }

    /// Test 3: NO event sent when config change fails
    ///
    /// Verifies that when apply_config_change() returns an error,
    /// MembershipApplied event is NOT sent (membership state is unchanged).
    ///
    /// Related: Bug fix #209
    #[tokio::test]
    async fn no_membership_applied_event_on_config_failure() {
        let entries = build_entries(
            vec![CommandType::Configuration(Change::AddNode(AddNode {
                node_id: 2,
                address: "127.0.0.1:8080".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            }))],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            || true, // Config fails
            None,
        );

        let result = harness.process_batch_handler().await;
        assert!(result.is_err(), "Config change should fail");

        // The membership change failed: MembershipApplied must NOT be announced (#209).
        // A FatalError is allowed and expected (#455: a committed membership change that cannot
        // be applied cannot self-heal). Every event is checked, not only the first one.
        while let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(50), harness.internal_event_rx.recv()).await
        {
            assert!(
                !matches!(event, InternalEvent::MembershipApplied),
                "MembershipApplied sent although the membership change failed"
            );
        }
    }

    /// Test 5: All config change types trigger MembershipApplied event
    ///
    /// Verifies that AddNode, RemoveNode, and Promote all send the event.
    ///
    /// Related: Bug fix #209
    #[tokio::test]
    async fn all_config_change_types_send_membership_applied() {
        use d_engine_proto::common::NodeStatus;
        use d_engine_proto::common::PromoteLearner;

        // Test AddNode
        let entries = build_entries(
            vec![CommandType::Configuration(Change::AddNode(AddNode {
                node_id: 2,
                address: "127.0.0.1:8080".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            }))],
            1,
        );
        let mut harness = setup_harness(Leader as i32, 1, entries, 1, || false, None);
        harness.process_batch_handler().await.unwrap();
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_millis(50), harness.internal_event_rx.recv())
                    .await,
                Ok(Some(InternalEvent::MembershipApplied))
            ),
            "AddNode should send MembershipApplied"
        );

        // Test RemoveNode
        let entries = build_entries(
            vec![CommandType::Configuration(Change::RemoveNode(RemoveNode {
                node_id: 2,
            }))],
            1,
        );
        let mut harness = setup_harness(Leader as i32, 1, entries, 1, || false, None);
        harness.process_batch_handler().await.unwrap();
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_millis(50), harness.internal_event_rx.recv())
                    .await,
                Ok(Some(InternalEvent::MembershipApplied))
            ),
            "RemoveNode should send MembershipApplied"
        );

        // Test PromoteLearner
        let entries = build_entries(
            vec![CommandType::Configuration(Change::Promote(
                PromoteLearner {
                    node_id: 2,
                    status: NodeStatus::Active as i32,
                },
            ))],
            1,
        );
        let mut harness = setup_harness(Leader as i32, 1, entries, 1, || false, None);
        harness.process_batch_handler().await.unwrap();
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_millis(50), harness.internal_event_rx.recv())
                    .await,
                Ok(Some(InternalEvent::MembershipApplied))
            ),
            "PromoteLearner should send MembershipApplied"
        );
    }

    /// Dropping the event receiver makes event_tx.send() fail.
    /// The MembershipApplied warn path must be executed without panicking or returning Err.
    #[tokio::test]
    async fn membership_applied_send_fails_gracefully_when_receiver_dropped() {
        let entries = build_entries(
            vec![CommandType::Configuration(Change::AddNode(AddNode {
                node_id: 2,
                address: "127.0.0.1:8080".into(),
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
            }))],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            || false,
            None,
        );

        // Replace the receiver with a dummy so the original event_tx sends will fail.
        // Swap old_rx out of the harness so harness is still usable, then drop old_rx.
        let (_dummy_tx, dummy_rx) = mpsc::unbounded_channel::<InternalEvent>();
        let old_rx = std::mem::replace(&mut harness.internal_event_rx, dummy_rx);
        drop(old_rx); // Now harness.event_tx's peer receiver is gone → sends fail.

        let result = harness.process_batch_handler().await;
        // Config change itself succeeds; send failure is non-fatal (warn only).
        assert!(
            result.is_ok(),
            "send failure on MembershipApplied must not propagate as Err"
        );
    }

    /// Dropping the event receiver makes StepDownSelfRemoved send fail.
    /// The error! path inside the self-removal branch must be executed without panicking.
    #[tokio::test]
    async fn step_down_send_fails_gracefully_when_receiver_dropped() {
        let entries = build_entries(
            vec![CommandType::Configuration(Change::RemoveNode(RemoveNode {
                node_id: 1, // Self-removal (node_id = 1 matches my_id = 1)
            }))],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            || false,
            None,
        );

        // Replace receiver with a dummy so both sends fail.
        let (_dummy_tx, dummy_rx) = mpsc::unbounded_channel::<InternalEvent>();
        let old_rx = std::mem::replace(&mut harness.internal_event_rx, dummy_rx);
        drop(old_rx);

        let result = harness.process_batch_handler().await;
        // Self-removal is committed; send failure is non-fatal (error! logged, not returned).
        assert!(
            result.is_ok(),
            "send failure on StepDownSelfRemoved must not propagate as Err"
        );
    }

    /// Test 6: Self-removal sends both MembershipApplied AND StepDownSelfRemoved
    ///
    /// Verifies that when Leader removes itself:
    /// 1. MembershipApplied is sent (to refresh cache)
    /// 2. StepDownSelfRemoved is sent (to trigger step down)
    ///
    /// Order: MembershipApplied should be sent before StepDownSelfRemoved
    ///
    /// Related: Bug fix #209, Issue #200 (self-removal)
    #[tokio::test]
    async fn self_removal_sends_both_events() {
        let entries = build_entries(
            vec![CommandType::Configuration(Change::RemoveNode(RemoveNode {
                node_id: 1, // Remove self (node_id = 1)
            }))],
            1,
        );

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            || false,
            None,
        );

        harness.process_batch_handler().await.unwrap();

        // First event should be MembershipApplied
        match tokio::time::timeout(Duration::from_millis(50), harness.internal_event_rx.recv())
            .await
        {
            Ok(Some(InternalEvent::MembershipApplied)) => {
                // Correct first event
            }
            Ok(Some(other)) => panic!("Expected MembershipApplied first, got {other:?}"),
            _ => panic!("Expected MembershipApplied event"),
        }

        // Second event should be StepDownSelfRemoved
        match tokio::time::timeout(Duration::from_millis(50), harness.internal_event_rx.recv())
            .await
        {
            Ok(Some(InternalEvent::StepDownSelfRemoved)) => {
                // Correct second event
            }
            Ok(Some(other)) => panic!("Expected StepDownSelfRemoved second, got {other:?}"),
            _ => panic!("Expected StepDownSelfRemoved event"),
        }
    }
}

// ================================================================================================
// Exactly-once delivery contract (#455)
// ================================================================================================
//
// CONTRACT: every committed log entry is handed to the state machine worker EXACTLY ONCE, in log
// order, with no gaps — no matter how the commit notifications arrive (back to back, repeated,
// stale) or how far behind the state machine is.
//
// WHY: today the range to deliver is `applied+1 ..= pending_commit`, and `applied` only advances
// when the state machine has FINISHED applying. A second notification arriving while the first
// batch is still queued/being applied therefore re-reads and re-delivers entries that were already
// delivered. Measured on a 3-node run of 100k writes: the leader handed 147,402 entries to the
// state machine for 100,001 log entries (47,401 re-applied); followers ~2,900. The RocksDB state
// machine does not filter them. Re-delivery also replays committed membership changes.
//
// HOW THE RIG WORKS: the real `DefaultStateMachineHandler` cannot be plugged into
// `DefaultCommitHandler<MockTypeConfig>` (its state machine handler type is the mock), and a
// mock with a canned `pending_range()` can never show the bug. So `Progress` re-implements the two
// real methods the commit path depends on, and `emulated_progress_matches_real_handler` proves it
// stays identical to the real one. The test acts as the state machine worker: it records every
// batch it receives and decides when "apply finished" (`complete_apply`).
//
// RETARGETING: when the commit handler is merged into the Raft loop, keep every test body and
// assertion; only `Rig::start` / `Rig::notify` change (feed `NotifyNewCommitIndex` to the loop
// instead of the commit channel). The assertions describe the contract, not the implementation.
mod exactly_once_delivery {
    use super::*;
    use crate::StateMachineCommand;
    use crate::StateMachineHandler;
    use crate::test_utils::snapshot_config;
    use d_engine_proto::common::{AddNode, NodeStatus};
    use std::ops::RangeInclusive;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

    const LOG_LEN: usize = 200;

    /// Same arithmetic as `DefaultStateMachineHandler::{pending_range, update_pending}`.
    struct Progress {
        /// Highest index the state machine has FINISHED applying.
        applied: AtomicU64,
        /// Highest commit index announced so far (only ever raised).
        pending: AtomicU64,
    }

    impl Progress {
        fn new(initial_applied: u64) -> Self {
            Self {
                applied: AtomicU64::new(initial_applied),
                pending: AtomicU64::new(0),
            }
        }
        fn range(&self) -> Option<RangeInclusive<u64>> {
            let applied = self.applied.load(Ordering::Acquire);
            let pending = self.pending.load(Ordering::Acquire);
            (pending > applied).then(|| (applied + 1)..=pending)
        }
        fn raise_pending(
            &self,
            new_commit: u64,
        ) {
            self.pending.fetch_max(new_commit, Ordering::AcqRel);
        }
    }

    /// `LOG_LEN` plain commands, optionally with `config_at` replaced by an AddNode change.
    fn log_entries(config_at: Option<u64>) -> Vec<Entry> {
        let commands = (1..=LOG_LEN as u64)
            .map(|i| {
                if Some(i) == config_at {
                    CommandType::Configuration(Change::AddNode(AddNode {
                        node_id: 9,
                        address: "addr".into(),
                        status: NodeStatus::Promotable as i32,
                    }))
                } else {
                    CommandType::Command(Bytes::from(format!("cmd{i}")))
                }
            })
            .collect();
        build_entries(commands, TEST_TERM)
    }

    struct Rig {
        progress: Arc<Progress>,
        commit_tx: mpsc::UnboundedSender<NewCommitData>,
        /// One element per `ApplyEntries` batch the (fake) state machine worker received.
        delivered: Arc<Mutex<Vec<Vec<u64>>>>,
        config_applied: Arc<AtomicUsize>,
        internal_event_rx: mpsc::UnboundedReceiver<InternalEvent>,
        handle: JoinHandle<()>,
        shutdown_tx: watch::Sender<()>,
    }

    impl Rig {
        async fn start(
            entries: Vec<Entry>,
            initial_applied: u64,
            first_config_apply_fails: bool,
        ) -> Self {
            let progress = Arc::new(Progress::new(initial_applied));

            let mut smh = MockStateMachineHandler::<MockTypeConfig>::new();
            let p = progress.clone();
            smh.expect_pending_range().returning(move || p.range());
            let p = progress.clone();
            smh.expect_update_pending().returning(move |n| p.raise_pending(n));
            let p = progress.clone();
            smh.expect_last_applied().returning(move || p.applied.load(Ordering::Acquire));

            let mut log = MockRaftLog::new();
            log.expect_get_entries_range().returning(move |range| {
                Ok(entries.iter().filter(|e| range.contains(&e.index)).cloned().collect())
            });

            let config_applied = Arc::new(AtomicUsize::new(0));
            let calls = config_applied.clone();
            let mut membership = MockMembership::new();
            membership.expect_notify_config_applied().returning(|_| {});
            membership.expect_apply_config_change().returning(move |_| {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if first_config_apply_fails && n == 1 {
                    Err(Error::Fatal("config change could not be applied".to_string()))
                } else {
                    Ok(())
                }
            });

            let (commit_tx, commit_rx) = mpsc::unbounded_channel();
            let (shutdown_tx, shutdown_rx) = watch::channel(());
            let (internal_event_tx, internal_event_rx) = mpsc::unbounded_channel();
            let (sm_tx, mut sm_rx) = mpsc::unbounded_channel::<StateMachineCommand>();

            // The fake state machine worker: records what it is handed; never applies by itself.
            let delivered = Arc::new(Mutex::new(Vec::new()));
            let sink = delivered.clone();
            tokio::spawn(async move {
                while let Some(cmd) = sm_rx.recv().await {
                    if let StateMachineCommand::ApplyEntries { entries, .. } = cmd {
                        sink.lock().unwrap().push(entries.iter().map(|e| e.index).collect());
                    }
                }
            });

            let deps = CommitHandlerDependencies {
                state_machine_handler: Arc::new(smh),
                raft_log: Arc::new(log),
                membership: Arc::new(membership),
                internal_event_tx,
                sm_apply_tx: crate::StateMachineCommandSender::new(sm_tx),
                shutdown_signal: shutdown_rx,
                max_batch_size: 10,
            };
            let mut handler = DefaultCommitHandler::<MockTypeConfig>::new(
                1,
                Leader as i32,
                TEST_TERM,
                deps,
                commit_rx,
            );
            let handle = tokio::spawn(async move {
                let _ = handler.run().await;
            });

            Self {
                progress,
                commit_tx,
                delivered,
                config_applied,
                internal_event_rx,
                handle,
                shutdown_tx,
            }
        }

        /// Announce a new commit index and give the commit path a chance to run.
        async fn notify(
            &self,
            commit_index: u64,
        ) {
            self.commit_tx
                .send(NewCommitData {
                    new_commit_index: commit_index,
                    role: Leader as i32,
                    current_term: TEST_TERM,
                })
                .unwrap();
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
        }

        /// Wait until nothing more is being delivered.
        async fn settle(&self) {
            let mut stable = 0;
            let mut last = self.delivered_flat().len();
            for _ in 0..200 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let now = self.delivered_flat().len();
                stable = if now == last { stable + 1 } else { 0 };
                last = now;
                if stable >= 3 {
                    return;
                }
            }
        }

        /// The state machine finished applying everything up to `index`.
        fn complete_apply(
            &self,
            index: u64,
        ) {
            self.progress.applied.fetch_max(index, Ordering::AcqRel);
        }

        /// A snapshot install moved the state machine to `index` without going through entries.
        fn jump_applied_to(
            &self,
            index: u64,
        ) {
            self.progress.applied.store(index, Ordering::Release);
        }

        fn delivered_flat(&self) -> Vec<u64> {
            self.delivered.lock().unwrap().iter().flatten().copied().collect()
        }

        fn assert_delivered(
            &self,
            expected: Vec<u64>,
            why: &str,
        ) {
            let batches = self.delivered.lock().unwrap().clone();
            assert_eq!(
                self.delivered_flat(),
                expected,
                "{why}\n  batches handed to the state machine, in order: {batches:?}"
            );
        }

        fn drain_events(&mut self) -> Vec<InternalEvent> {
            let mut out = Vec::new();
            while let Ok(ev) = self.internal_event_rx.try_recv() {
                out.push(ev);
            }
            out
        }
    }

    fn range(r: RangeInclusive<u64>) -> Vec<u64> {
        r.collect()
    }

    // ── the contract ────────────────────────────────────────────────────────────────────────

    /// A second commit notification arrives while the state machine has not applied the first
    /// batch yet (the normal case under load). Entries 1..=5 must not be delivered again.
    #[tokio::test]
    async fn test_commit_notifications_while_apply_is_pending_deliver_each_entry_once() {
        let rig = Rig::start(log_entries(None), 0, false).await;
        rig.notify(5).await;
        rig.settle().await;
        rig.notify(10).await; // state machine still has not finished anything
        rig.settle().await;
        rig.notify(15).await;
        rig.settle().await;
        rig.assert_delivered(
            range(1..=15),
            "each committed entry must be handed to the state machine exactly once",
        );
    }

    /// The same notification repeated (leader re-announces the same commit index, e.g. on every
    /// ack) must not make the commit path re-read and re-send anything.
    #[tokio::test]
    async fn test_repeated_identical_notification_does_not_redeliver() {
        let rig = Rig::start(log_entries(None), 0, false).await;
        for _ in 0..3 {
            rig.notify(8).await;
            rig.settle().await;
        }
        rig.assert_delivered(range(1..=8), "a repeated commit index must deliver nothing new");
    }

    /// A stale (lower) notification arriving after a higher one must be ignored.
    #[tokio::test]
    async fn test_stale_lower_notification_does_not_redeliver() {
        let rig = Rig::start(log_entries(None), 0, false).await;
        rig.notify(10).await;
        rig.settle().await;
        rig.notify(4).await;
        rig.settle().await;
        rig.assert_delivered(range(1..=10), "a lower commit index must deliver nothing");
    }

    /// 100 single-step notifications in a row while nothing is ever reported as applied: the
    /// state machine must still see 1..=100 once each, contiguous and ordered.
    #[tokio::test]
    async fn test_burst_of_single_step_notifications_delivers_each_entry_once() {
        let rig = Rig::start(log_entries(None), 0, false).await;
        for i in 1..=100 {
            rig.notify(i).await;
        }
        rig.settle().await;
        rig.assert_delivered(
            range(1..=100),
            "a burst of notifications must neither duplicate nor skip entries",
        );
    }

    /// Guard (green today, must stay green): when the state machine does report progress between
    /// notifications, only the NEW entries are delivered.
    #[tokio::test]
    async fn test_apply_progress_between_notifications_delivers_only_new_entries() {
        let rig = Rig::start(log_entries(None), 0, false).await;
        rig.notify(5).await;
        rig.settle().await;
        rig.complete_apply(5);
        rig.notify(9).await;
        rig.settle().await;
        rig.assert_delivered(range(1..=9), "entries 1..=5 were applied; only 6..=9 are new");
    }

    /// Guard: announcing a commit index the state machine already reached delivers nothing.
    #[tokio::test]
    async fn test_notification_for_already_applied_index_delivers_nothing() {
        let rig = Rig::start(log_entries(None), 0, false).await;
        rig.notify(5).await;
        rig.settle().await;
        rig.complete_apply(5);
        rig.notify(5).await;
        rig.notify(3).await;
        rig.settle().await;
        rig.assert_delivered(range(1..=5), "nothing above index 5 was committed");
    }

    /// Guard: after a restart the state machine is already at index 20. Entries up to 20 are
    /// never delivered, whatever commit index is announced first.
    #[tokio::test]
    async fn test_entries_at_or_below_initial_applied_index_are_never_delivered() {
        let rig = Rig::start(log_entries(None), 20, false).await;
        rig.notify(10).await;
        rig.settle().await;
        rig.assert_delivered(vec![], "commit index 10 is below what is already applied (20)");
        rig.notify(25).await;
        rig.settle().await;
        rig.assert_delivered(range(21..=25), "only 21..=25 are new");
    }

    /// Guard: a snapshot install moves the state machine past entries it never saw. Delivery
    /// resumes after the snapshot point and never goes back for the covered entries.
    #[tokio::test]
    async fn test_snapshot_install_jump_skips_covered_entries() {
        let rig = Rig::start(log_entries(None), 0, false).await;
        rig.notify(10).await;
        rig.settle().await;
        rig.complete_apply(10);
        rig.notify(30).await;
        rig.settle().await; // 11..=30 delivered, not yet reported applied
        rig.jump_applied_to(50); // snapshot at index 50 installed
        rig.notify(60).await;
        rig.settle().await;
        let mut expected = range(1..=30);
        expected.extend(range(51..=60));
        rig.assert_delivered(expected, "after the snapshot at 50 only 51..=60 are new");
    }

    // ── side effects of delivery ─────────────────────────────────────────────────────────────

    /// A committed membership change is applied to the membership exactly once, even when the
    /// commit path is notified again before the state machine reports the config entry applied.
    #[tokio::test]
    async fn test_config_change_is_applied_exactly_once_when_commits_are_announced_again() {
        let mut rig = Rig::start(log_entries(Some(3)), 0, false).await;
        rig.notify(3).await; // contains the AddNode at index 3
        rig.settle().await;
        rig.notify(5).await; // state machine has not reported anything applied
        rig.settle().await;
        assert_eq!(
            rig.config_applied.load(Ordering::SeqCst),
            1,
            "the AddNode at index 3 must change the membership once"
        );
        let applied_events = rig
            .drain_events()
            .into_iter()
            .filter(|e| matches!(e, InternalEvent::MembershipApplied))
            .count();
        assert_eq!(applied_events, 1, "one committed change => one MembershipApplied event");
        rig.assert_delivered(range(1..=5), "entries are still delivered exactly once");
    }

    /// A committed membership change that cannot be applied is FATAL for the node (owner
    /// decision, #455): the cluster has already agreed on it, so a node that skips it holds a
    /// membership view that no longer matches its own log, and nothing can repair that. Today the
    /// failing entry has already been handed to the state machine, is never read again, and only
    /// an error log remains.
    ///
    /// Also keeps the intent of #209: no `MembershipApplied` is announced for a change that failed.
    #[tokio::test]
    async fn test_failed_config_change_raises_fatal_error_and_announces_nothing() {
        let mut rig = Rig::start(log_entries(Some(3)), 0, true).await;
        rig.notify(3).await; // the AddNode at index 3 fails to apply
        rig.settle().await;
        rig.complete_apply(3); // the state machine has applied entries 1..=3
        // A later commit may be announced, but the commit path has stopped after the fatal
        // error, so the channel can already be closed: that is the intended behaviour.
        let _ = rig.commit_tx.send(NewCommitData {
            new_commit_index: 5,
            role: Leader as i32,
            current_term: TEST_TERM,
        });
        rig.settle().await;

        let events = rig.drain_events();
        assert!(
            events.iter().any(|e| matches!(e, InternalEvent::FatalError { .. })),
            "a committed membership change failed to apply but no FatalError was raised \
             (applied {} time(s)); the node silently diverges from its log",
            rig.config_applied.load(Ordering::SeqCst)
        );
        assert!(
            !events.iter().any(|e| matches!(e, InternalEvent::MembershipApplied)),
            "MembershipApplied must not be announced for a change that failed"
        );
    }

    /// An entry whose payload kind is missing (corrupt / hostile peer) must never panic the
    /// commit path (ADR-018: no panics in embedded core paths). Either it is skipped/treated as
    /// a no-op and later entries are still delivered, or a FatalError is raised — but the task
    /// must stay alive and the node must not silently stop applying.
    #[tokio::test]
    async fn test_entry_with_missing_payload_kind_does_not_panic_the_commit_path() {
        let mut entries = log_entries(None);
        entries[1].payload = Some(EntryPayload { payload: None }); // index 2: oneof not set
        let mut rig = Rig::start(entries, 0, false).await;
        rig.notify(3).await;
        rig.settle().await;

        assert!(
            !rig.handle.is_finished(),
            "the commit path terminated (panicked) on an entry with an empty payload kind"
        );
        let fatal = rig
            .drain_events()
            .iter()
            .any(|e| matches!(e, InternalEvent::FatalError { .. }));
        assert!(
            fatal || rig.delivered_flat().contains(&3),
            "after the malformed entry, later entries were not delivered and no FatalError was \
             raised (delivered: {:?})",
            rig.delivered_flat()
        );
    }

    // ── graceful shutdown ────────────────────────────────────────────────────────────────────

    /// CONTRACT (owner decision, #455): on graceful shutdown, every commit the node has already
    /// been told about is handed to the state machine worker BEFORE the commit path exits. (The
    /// worker then drains its queue and `Node::stop` waits for it, so the state machine is
    /// current when `stop()` returns.) Nothing is lost either way — the log is durable and
    /// restart re-delivers — this is about the state being current at stop.
    ///
    /// Today the commit path picks at random between "shutdown" and "new commit" when both are
    /// ready, so a single run would be flaky; 30 rounds make the current behaviour fail reliably
    /// (probability of passing by luck is 2^-30) and the fixed one pass reliably.
    #[tokio::test]
    async fn test_graceful_shutdown_hands_all_announced_commits_to_the_state_machine() {
        for round in 0..30 {
            let mut rig = Rig::start(log_entries(None), 0, false).await;
            // Announce the commit and the shutdown before the commit path gets to run at all.
            rig.commit_tx
                .send(NewCommitData {
                    new_commit_index: 10,
                    role: Leader as i32,
                    current_term: TEST_TERM,
                })
                .unwrap();
            rig.shutdown_tx.send(()).unwrap();

            tokio::time::timeout(Duration::from_secs(2), &mut rig.handle)
                .await
                .expect("the commit path must exit after shutdown")
                .expect("the commit path must not panic");
            rig.settle().await;
            rig.assert_delivered(
                range(1..=10),
                &format!("round {round}: shutdown dropped an announced commit"),
            );
        }
    }

    /// Several commit notifications still queued at shutdown are all honoured, and the entries
    /// are still delivered exactly once.
    #[tokio::test]
    async fn test_graceful_shutdown_with_queued_notifications_delivers_everything_once() {
        for round in 0..30 {
            let mut rig = Rig::start(log_entries(None), 0, false).await;
            for commit_index in [2, 4, 6, 8, 10] {
                rig.commit_tx
                    .send(NewCommitData {
                        new_commit_index: commit_index,
                        role: Leader as i32,
                        current_term: TEST_TERM,
                    })
                    .unwrap();
            }
            rig.shutdown_tx.send(()).unwrap();

            tokio::time::timeout(Duration::from_secs(2), &mut rig.handle)
                .await
                .expect("the commit path must exit after shutdown")
                .expect("the commit path must not panic");
            rig.settle().await;
            rig.assert_delivered(
                range(1..=10),
                &format!("round {round}: queued commits were lost or duplicated at shutdown"),
            );
        }
    }

    /// Guard: shutdown with nothing pending exits promptly and delivers nothing.
    #[tokio::test]
    async fn test_graceful_shutdown_with_no_pending_commit_exits_cleanly() {
        let mut rig = Rig::start(log_entries(None), 0, false).await;
        rig.shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut rig.handle)
            .await
            .expect("the commit path must exit after shutdown")
            .expect("the commit path must not panic");
        rig.assert_delivered(vec![], "nothing was committed");
    }

    // ── the rig itself ───────────────────────────────────────────────────────────────────────

    /// The rig's `Progress` must behave exactly like the real handler's, otherwise the tests above
    /// would prove nothing about production. Same inputs => same `pending_range()`.
    #[test]
    fn test_emulated_progress_matches_real_state_machine_handler() {
        for initial_applied in [0u64, 5, 40] {
            let dir = PathBuf::from(format!("/tmp/test_emulated_progress_{initial_applied}"));
            let real = crate::DefaultStateMachineHandler::<MockTypeConfig>::new(
                1,
                initial_applied,
                Arc::new(crate::MockStateMachine::new()),
                dir.clone(),
                snapshot_config(dir),
                crate::MockSnapshotPolicy::new(),
            );
            let emulated = Progress::new(initial_applied);
            assert_eq!(real.pending_range(), emulated.range(), "before any commit");
            for commit in [3, 7, 7, 2, 12, 12, 50, 49] {
                real.update_pending(commit);
                emulated.raise_pending(commit);
                assert_eq!(
                    real.pending_range(),
                    emulated.range(),
                    "after update_pending({commit}) with last_applied={initial_applied}"
                );
            }
        }
    }

    /// A failed membership change raises FatalError, which makes the node shut down; the
    /// shutdown hand-over must not deliver the same entries a second time.
    #[tokio::test]
    async fn test_shutdown_after_failed_config_change_does_not_redeliver_entries() {
        let mut rig = Rig::start(log_entries(Some(3)), 0, true).await;
        rig.notify(5).await; // AddNode at index 3 fails to apply
        rig.settle().await;
        // The commit path may already have stopped after the fatal error: nobody listens then.
        let _ = rig.shutdown_tx.send(());
        tokio::time::timeout(Duration::from_secs(2), &mut rig.handle)
            .await
            .expect("the commit path must exit after shutdown")
            .expect("the commit path must not panic");
        rig.settle().await;

        let flat = rig.delivered_flat();
        let mut unique = flat.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(
            flat.len(),
            unique.len(),
            "entries delivered twice around fatal shutdown: {:?}",
            rig.delivered.lock().unwrap().clone()
        );
    }

    // Keep the atomics import used even if a test above is edited away.
    #[allow(dead_code)]
    fn _unused(_: AtomicBool) {}
}
