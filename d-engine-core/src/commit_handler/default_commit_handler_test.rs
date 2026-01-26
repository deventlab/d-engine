use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Leader;
use d_engine_proto::common::membership_change::Change;
use d_engine_proto::server::storage::SnapshotMetadata;
use tokio::sync::mpsc::{self};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::time::{self};

use super::CommitHandler;
use super::CommitHandlerDependencies;
use super::DefaultCommitHandler;
use crate::CommitHandlerConfig;
use crate::Error;
use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockStateMachineHandler;
use crate::MockTypeConfig;
use crate::NewCommitData;
use crate::RaftConfig;
use crate::RaftEvent;
use crate::RaftNodeConfig;
use crate::Result;
use crate::test_utils::generate_insert_commands;

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
    event_tx: mpsc::Sender<RaftEvent>,
    event_rx: mpsc::Receiver<RaftEvent>,
    shutdown_tx: watch::Sender<()>,
    shutdown_rx: Option<watch::Receiver<()>>,
    batch_size_threshold: u64,
    process_interval_ms: u64,
    handle: Option<JoinHandle<()>>,
}
#[allow(clippy::too_many_arguments)]
fn setup_harness<F, G>(
    role: i32,
    term: u64,
    entries: Vec<Entry>,
    last_applied: u64,
    config_hook: F,
    command_hook: G,
    snapshot_condition: Option<u64>,
    batch_size_threshold: u64,
    process_interval_ms: u64,
) -> TestHarness
where
    F: Fn() -> bool + 'static + Send + Sync,
    G: Fn() -> bool + 'static + Send + Sync,
{
    let (commit_tx, commit_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let (event_tx, event_rx) = mpsc::channel(1000); // Large capacity to prevent blocking in tests

    // Mock state machine
    let mut mock_smh = MockStateMachineHandler::new();
    let cloned_entries = entries.clone();
    mock_smh
        .expect_pending_range()
        .returning(move || Some(1..=cloned_entries.last().map(|e| e.index).unwrap_or(1)));
    mock_smh.expect_apply_chunk().returning(move |_| {
        if command_hook() {
            Err(Error::Fatal("Command execution failed".to_string()))
        } else {
            Ok(vec![])
        }
    });
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
        event_tx,
        event_rx,
        shutdown_tx,
        shutdown_rx: Some(shutdown_rx),
        batch_size_threshold,
        process_interval_ms,
        handle: None,
    }
}

impl TestHarness {
    async fn run_handler(&mut self) {
        let deps = CommitHandlerDependencies {
            state_machine_handler: self.mock_smh.clone(),
            raft_log: self.mock_log.clone(),
            membership: self.mock_membership.clone(),
            event_tx: self.event_tx.clone(),
            shutdown_signal: self.shutdown_rx.take().unwrap(),
        };

        let commit_handler_config = CommitHandlerConfig {
            batch_size_threshold: self.batch_size_threshold, // batch_threshold
            process_interval_ms: self.process_interval_ms,   // process_interval
            max_entries_per_chunk: 1,
        };

        let config = RaftNodeConfig {
            raft: RaftConfig {
                commit_handler: commit_handler_config,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut handler = DefaultCommitHandler::<MockTypeConfig>::new(
            1,
            self.role,
            self.term,
            deps,
            Arc::new(config),
            self.commit_rx.take().unwrap(),
        );
        self.handle = Some(tokio::spawn(async move {
            let _ = handler.run().await;
        }));
    }

    async fn process_batch_handler(&mut self) -> Result<()> {
        let deps = CommitHandlerDependencies {
            state_machine_handler: self.mock_smh.clone(),
            raft_log: self.mock_log.clone(),
            membership: self.mock_membership.clone(),
            event_tx: self.event_tx.clone(),
            shutdown_signal: self.shutdown_rx.take().unwrap(),
        };

        let commit_handler_config = CommitHandlerConfig {
            batch_size_threshold: self.batch_size_threshold, // batch_threshold
            process_interval_ms: self.process_interval_ms,   // process_interval
            max_entries_per_chunk: 1,
        };

        let config = RaftNodeConfig {
            raft: RaftConfig {
                commit_handler: commit_handler_config,
                ..Default::default()
            },
            ..Default::default()
        };
        let handler = DefaultCommitHandler::<MockTypeConfig>::new(
            1,
            self.role,
            self.term,
            deps,
            Arc::new(config),
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
        match time::timeout(Duration::from_millis(50), self.event_rx.recv()).await {
            Ok(Some(_)) => true, // Event received normally
            Ok(None) => false,   // Channel closed
            Err(_) => false,     // Timeout
        }
    }
}

fn setup(
    batch_size_threshold: u64,
    process_interval_ms: u64,
    apply_batch_expected_execution_times: usize,
    new_commit_rx: mpsc::UnboundedReceiver<NewCommitData>,
    shutdown_signal: watch::Receiver<()>,
) -> DefaultCommitHandler<MockTypeConfig> {
    // prepare commit channel

    // Mock Applier
    let mut mock_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    mock_handler
        .expect_apply_chunk()
        .times(apply_batch_expected_execution_times)
        .returning(|_| Ok(vec![]));
    mock_handler.expect_update_pending().returning(|_| {});
    mock_handler.expect_create_snapshot().returning(|| {
        Ok((
            SnapshotMetadata {
                last_included: Some(LogId { index: 1, term: 1 }),
                checksum: Bytes::new(),
            },
            PathBuf::from("/tmp/value"),
        ))
    });
    mock_handler.expect_pending_range().returning(|| Some(1..=2));
    mock_handler.expect_should_snapshot().returning(|_| true);

    // Mock Raft Log
    let mut mock_raft_log = MockRaftLog::new();
    mock_raft_log.expect_purge_logs_up_to().returning(|_| Ok(()));
    mock_raft_log.expect_get_entries_range().returning(|_| {
        Ok(vec![Entry {
            index: 1,
            term: 1,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
        }])
    });
    let mock_membership = MockMembership::new();

    // Init handler
    let (event_tx, _event_rx) = mpsc::channel(1);
    let deps = CommitHandlerDependencies {
        state_machine_handler: Arc::new(mock_handler),
        raft_log: Arc::new(mock_raft_log),
        membership: Arc::new(mock_membership),
        event_tx,
        shutdown_signal,
    };

    let commit_handler_config = CommitHandlerConfig {
        batch_size_threshold,
        process_interval_ms,
        max_entries_per_chunk: 1,
    };

    let config = RaftNodeConfig {
        raft: RaftConfig {
            commit_handler: commit_handler_config,
            ..Default::default()
        },
        ..Default::default()
    };
    DefaultCommitHandler::<MockTypeConfig>::new(
        1,
        Leader as i32,
        1,
        deps,
        Arc::new(config),
        new_commit_rx,
    )
}

/// # Case 1: interval_uses_correct_duration
#[tokio::test(start_paused = true)]
async fn test_dynamic_interval_case1() {
    // Prpeare interval
    let interval_ms = 100;

    // Setup handler
    let (_new_commit_tx, new_commit_rx) = mpsc::unbounded_channel::<NewCommitData>();
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let batch_thresold = 0;
    let apply_batch_expected_execution_times = 0; // we will not trigger `run`
    let handler = setup(
        batch_thresold,
        interval_ms,
        apply_batch_expected_execution_times,
        new_commit_rx,
        graceful_rx,
    );
    let mut interval = handler.dynamic_interval();

    // First tick is immediate
    interval.tick().await;

    // Advance time by the interval duration
    // tokio::time::advance(Duration::from_millis(interval_ms)).await;

    // Second tick should be ready immediately after advancing
    let start = Instant::now();
    interval.tick().await;
    let elapsed = start.elapsed();

    // Allow a small margin for timing approximations
    assert!(
        elapsed >= Duration::from_millis(interval_ms),
        "Expected interval to wait at least {}ms, but got {}ms",
        interval_ms,
        elapsed.as_millis()
    );
}

/// # Case 2: missed_ticks_delay_to_next_interval
#[tokio::test(start_paused = true)]
async fn test_dynamic_interval_case2() {
    // Prpeare interval
    let interval_ms = 100;

    // Setup handler
    let (_new_commit_tx, new_commit_rx) = mpsc::unbounded_channel::<NewCommitData>();
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let batch_thresold = 0;
    let apply_batch_expected_execution_times = 0; // we will not trigger `run`
    let handler = setup(
        batch_thresold,
        interval_ms,
        apply_batch_expected_execution_times,
        new_commit_rx,
        graceful_rx,
    );
    let mut interval = handler.dynamic_interval();

    // First tick is immediate
    interval.tick().await;

    // Simulate a delay longer than one interval
    let delay = interval_ms * 3;
    tokio::time::advance(Duration::from_millis(delay)).await;

    // Second tick should be ready immediately after advancing
    let start = Instant::now();
    interval.tick().await;
    let elapsed = start.elapsed();

    // Expect the tick to complete after remaining time to the next interval
    let expected_wait = delay % interval_ms;
    assert!(
        elapsed <= Duration::from_millis(expected_wait),
        "Expected to wait up to {}ms, but waited {}ms",
        expected_wait,
        elapsed.as_millis()
    );
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
            move || false,
            Some(4),
            3,
            1,
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
            move || false,
            None,
            3,
            1,
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
            move || false,
            Some(4),
            3,
            1,
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
            move || false,
            None,
            3,
            1,
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
    // - process_interval_ms  = 1ms

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
            move || false,
            Some(4),
            3,
            1,
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
            move || false,
            None,
            batch_thresold,
            1000,
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
            move || false,
            None,
            batch_thresold,
            1000,
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
            move || false,
            None,
            batch_thresold,
            2,
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
    use crate::test_utils::*;

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
    //     mpsc::Receiver<RaftEvent>,
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
            move || false,
            None,
            100,
            100,
        );
        let result = harness.process_batch_handler().await;
        assert!(result.is_ok());
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
            move || false,
            None,
            100,
            100,
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
            move || false,
            None,
            100,
            100,
        );

        // Expect single apply_chunk call with all 3 commands
        let result = harness.process_batch_handler().await;
        assert!(result.is_ok());

        // Verification:
        // - apply_chunk called twice: [cmd1] and [cmd2]
        // - apply_config_change called once for entry 2
    }

    #[allow(dead_code)]
    pub fn build_entries_with_noop(term: u64) -> Vec<Entry> {
        let (builder, cmd1) = EntryBuilder::new(1, term).command(b"cmd1");
        let (builder, noop) = builder.noop();
        let (_, cmd2) = builder.command(b"cmd2");
        vec![cmd1, noop, cmd2]
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
            move || false,
            None,
            100,
            100,
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
            move || false,
            None,
            100,
            100,
        );
        let result = harness.process_batch_handler().await;
        assert!(result.is_err());

        // Verify command was NOT applied
    }

    #[tokio::test]
    async fn handles_command_failure_properly() {
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
            move || true,
            None,
            100,
            100,
        );
        let result = harness.process_batch_handler().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn triggers_snapshot_when_condition_met() {
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
            move || false,
            Some(2), // Snapshot condition: last index >= 2
            100,
            100,
        );
        assert!(harness.process_batch_handler().await.is_ok());

        // Verify snapshot triggered
        assert!(harness.expect_snapshot_trigger().await);
    }

    #[tokio::test]
    async fn does_not_trigger_snapshot_when_condition_not_met() {
        let entries = build_entries(vec![CommandType::Command(Bytes::from(b"cmd1".to_vec()))], 1);

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            move || false,
            move || false,
            Some(2), // Requires index >=2
            100,
            100,
        );
        let result = harness.process_batch_handler().await;
        assert!(result.is_ok());

        // Verify no snapshot event
        assert!(!harness.expect_snapshot_trigger().await);
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
            move || false,
            None,
            100,
            100,
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
            move || false,
            None,
            100,
            100,
        );
        let result = harness.process_batch_handler().await;
        assert!(result.is_ok());

        // Verification: apply_chunk called once with 1000 commands
    }

    #[tokio::test]
    async fn maintains_ordering_across_entry_types() {
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

        let process_order = Arc::new(Mutex::new(Vec::new()));
        let order_capture = process_order.clone();
        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            {
                let order_capture = order_capture.clone();
                move || {
                    order_capture.lock().push("config");
                    false // Returns bool to indicate no simulation error
                }
            },
            {
                let order_capture = process_order.clone();
                move || {
                    order_capture.lock().push("command");
                    false
                }
            },
            None,
            100,
            100,
        );
        let result = harness.process_batch_handler().await;
        assert!(result.is_ok());

        // Verify processing order: cmd1 (command), then config, then cmd2 (command)
        let order = process_order.lock();
        assert_eq!(*order, vec!["command", "config", "command"]);
    }

    #[tokio::test]
    async fn config_failure_prevents_subsequent_processing() {
        let entries = build_entries(
            vec![
                CommandType::Configuration(Change::AddNode(AddNode {
                    node_id: 1,
                    address: "addr".into(),
                    status: d_engine_proto::common::NodeStatus::Promotable as i32,
                })),
                CommandType::Command(Bytes::from(b"cmd1".to_vec())),
                CommandType::Configuration(Change::RemoveNode(RemoveNode { node_id: 1 })),
            ],
            1,
        );

        let process_order = Arc::new(Mutex::new(Vec::new()));
        let order_capture = process_order.clone();

        let last_applied = entries.len();
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            last_applied as u64,
            {
                let order_capture = order_capture.clone();
                // Create a closure that returns bool (true means simulated error)
                move || {
                    order_capture.lock().push("config");
                    false // Returns bool to indicate no simulation error
                }
            },
            {
                let order_capture = process_order.clone();
                move || {
                    order_capture.lock().push("command");
                    true
                }
            },
            None,
            100,
            100,
        );
        let result = harness.process_batch_handler().await;
        assert!(result.is_err());

        // Verify only first config was processed
        let order = process_order.lock();
        println!("Order: {order:?}",);
        assert_eq!(order.len(), 1);
        assert!(order[0].starts_with("command"));
    }

    /// Test 1: MembershipApplied event MUST be sent after successful config change
    ///
    /// Verifies that when apply_config_change() succeeds, the commit handler
    /// sends RaftEvent::MembershipApplied to notify Leader to refresh cache.
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
            || false,
            None,
            100,
            100,
        );

        let result = harness.process_batch_handler().await;
        assert!(result.is_ok(), "Config change should succeed");

        // Verify MembershipApplied event was sent
        match tokio::time::timeout(Duration::from_millis(50), harness.event_rx.recv()).await {
            Ok(Some(RaftEvent::MembershipApplied)) => {
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
            || false,
            None,
            100,
            100,
        );

        harness.process_batch_handler().await.unwrap();

        // Verify event received
        match tokio::time::timeout(Duration::from_millis(50), harness.event_rx.recv()).await {
            Ok(Some(RaftEvent::MembershipApplied)) => {
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
            || false,
            None,
            100,
            100,
        );

        let result = harness.process_batch_handler().await;
        assert!(result.is_err(), "Config change should fail");

        // Verify NO event was sent
        match tokio::time::timeout(Duration::from_millis(50), harness.event_rx.recv()).await {
            Err(_) => {
                // Timeout is expected - no event sent
            }
            Ok(Some(event)) => panic!("Expected no event, but received {event:?}"),
            Ok(None) => {
                // Channel closed is also acceptable
            }
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
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            1,
            || false,
            || false,
            None,
            100,
            100,
        );
        harness.process_batch_handler().await.unwrap();
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_millis(50), harness.event_rx.recv()).await,
                Ok(Some(RaftEvent::MembershipApplied))
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
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            1,
            || false,
            || false,
            None,
            100,
            100,
        );
        harness.process_batch_handler().await.unwrap();
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_millis(50), harness.event_rx.recv()).await,
                Ok(Some(RaftEvent::MembershipApplied))
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
        let mut harness = setup_harness(
            Leader as i32,
            1,
            entries,
            1,
            || false,
            || false,
            None,
            100,
            100,
        );
        harness.process_batch_handler().await.unwrap();
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_millis(50), harness.event_rx.recv()).await,
                Ok(Some(RaftEvent::MembershipApplied))
            ),
            "PromoteLearner should send MembershipApplied"
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
            || false,
            None,
            100,
            100,
        );

        harness.process_batch_handler().await.unwrap();

        // First event should be MembershipApplied
        match tokio::time::timeout(Duration::from_millis(50), harness.event_rx.recv()).await {
            Ok(Some(RaftEvent::MembershipApplied)) => {
                // Correct first event
            }
            Ok(Some(other)) => panic!("Expected MembershipApplied first, got {other:?}"),
            _ => panic!("Expected MembershipApplied event"),
        }

        // Second event should be StepDownSelfRemoved
        match tokio::time::timeout(Duration::from_millis(50), harness.event_rx.recv()).await {
            Ok(Some(RaftEvent::StepDownSelfRemoved)) => {
                // Correct second event
            }
            Ok(Some(other)) => panic!("Expected StepDownSelfRemoved second, got {other:?}"),
            _ => panic!("Expected StepDownSelfRemoved event"),
        }
    }
}
