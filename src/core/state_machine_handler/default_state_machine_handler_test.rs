use super::{DefaultStateMachineHandler, StateMachineHandler};
use crate::{grpc::rpc_service::Entry, test_utils::MockTypeConfig, MockRaftLog, MockStateMachine};
use std::sync::Arc;

// Case 1: normal update
#[test]
fn test_update_pending_case1() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let applier =
        DefaultStateMachineHandler::<MockTypeConfig>::new(None, 1, Arc::new(state_machine_mock));
    applier.update_pending(1);
    assert_eq!(applier.pending_commit(), 1);
    applier.update_pending(10);
    assert_eq!(applier.pending_commit(), 10);
}

// Case 2: new commit < existing commit
#[test]
fn test_update_pending_case2() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let applier =
        DefaultStateMachineHandler::<MockTypeConfig>::new(None, 1, Arc::new(state_machine_mock));
    applier.update_pending(10);
    assert_eq!(applier.pending_commit(), 10);

    applier.update_pending(7);
    assert_eq!(applier.pending_commit(), 10);
}
// Case 3: multi thread update
#[tokio::test]
async fn test_update_pending_case3() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let applier = Arc::new(DefaultStateMachineHandler::<MockTypeConfig>::new(
        None,
        1,
        Arc::new(state_machine_mock),
    ));

    let mut tasks = vec![];
    for i in 1..=10 {
        let applier = applier.clone();
        tasks.push(tokio::spawn(async move {
            applier.update_pending(i);
        }));
    }
    futures::future::join_all(tasks).await;
    assert_eq!(applier.pending_commit(), 10);
}

// Case 1: pending commit is zero
#[test]
fn test_pending_range_case1() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(
        Some(10),
        1,
        Arc::new(state_machine_mock),
    );
    assert_eq!(applier.pending_range(), None);
}

// Case 2: pending commit <= last_applied
#[test]
fn test_pending_range_case2() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(
        Some(10),
        1,
        Arc::new(state_machine_mock),
    );
    applier.update_pending(7);
    applier.update_pending(10);
    assert_eq!(applier.pending_range(), None);
}

// Case 3: pending commit > last_applied
#[test]
fn test_pending_range_case3() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(
        Some(10),
        1,
        Arc::new(state_machine_mock),
    );
    applier.update_pending(7);
    applier.update_pending(10);
    applier.update_pending(11);
    assert_eq!(applier.pending_range(), Some(11..=11));
}

// Case1: No pending commit
#[tokio::test]
async fn test_apply_batch_case1() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let mut raft_log_mock = MockRaftLog::new();
    raft_log_mock
        .expect_get_entries_between()
        .times(0)
        .returning(|_| vec![]);
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(
        Some(10),
        1,
        Arc::new(state_machine_mock),
    );
    assert!(applier.apply_batch(Arc::new(raft_log_mock)).await.is_ok());
}

// Case2: There is pending commit
//
// Criteria:
// - last_applied should be updated
#[tokio::test]
async fn test_apply_batch_case2() {
    // Init Applier
    let mut state_machine_mock = MockStateMachine::new();
    state_machine_mock
        .expect_apply_chunk()
        .times(1)
        .returning(|_| Ok(()));
    let mut raft_log_mock = MockRaftLog::new();
    raft_log_mock
        .expect_get_entries_between()
        .times(1)
        .returning(|_| {
            vec![Entry {
                index: 1,
                term: 1,
                command: vec![1; 8],
            }]
        });

    let max_entries_per_chunk = 1;
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(
        Some(10),
        max_entries_per_chunk,
        Arc::new(state_machine_mock),
    );

    // Update pending commit
    applier.update_pending(11);

    assert!(applier.apply_batch(Arc::new(raft_log_mock)).await.is_ok());
    assert_eq!(applier.last_applied(), 11);
}

// Case 3: There is pending commit
//
// Criteria:
// - test the pending commit should be split into 10 chunks
#[tokio::test]
async fn test_apply_batch_case3() {
    // Init Applier
    let mut state_machine_mock = MockStateMachine::new();
    state_machine_mock
        .expect_apply_chunk()
        .times(10)
        .returning(|_| Ok(()));
    let mut raft_log_mock = MockRaftLog::new();
    raft_log_mock
        .expect_get_entries_between()
        .times(1)
        .returning(move |_| {
            let mut entries = vec![];
            for i in 1..=10 {
                entries.push(Entry {
                    index: i,
                    term: 1,
                    command: vec![1; 8],
                });
            }
            entries
        });

    let max_entries_per_chunk = 1;
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(
        Some(10),
        max_entries_per_chunk,
        Arc::new(state_machine_mock),
    );

    // Update pending commit
    applier.update_pending(21);

    assert!(applier.apply_batch(Arc::new(raft_log_mock)).await.is_ok());
    assert_eq!(applier.last_applied(), 21);
}
