use std::{sync::Arc, time::Duration};

use tokio::{
    sync::{mpsc, watch},
    time,
};

use super::DefaultCommitHandler;
use crate::{
    test_utils::{self, MockTypeConfig},
    CommitHandler, MockRaftLog, MockStateMachineHandler,
};

// Case 1: test if process_batch been triggered if
// interval ticks
//
// ## Setup:
// - commit_handle_interval_in_ms  = 1ms

// ## Criterias:
// - apply_batch been triggered twice
//
#[tokio::test]
async fn test_run_case1() {
    tokio::time::pause();
    test_utils::enable_logger();

    // prepare commit channel
    let (new_commit_tx, new_commit_rx) = mpsc::unbounded_channel::<u64>();

    // Mock Applier
    let mut mock_applier = MockStateMachineHandler::<MockTypeConfig>::new();
    mock_applier
        .expect_apply_batch()
        .times(2)
        .returning(|_| Ok(()));
    mock_applier.expect_update_pending().returning(|_| {});

    // Mock Raft Log
    let mock_raft_log = MockRaftLog::new();

    // Init handler
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut handler = DefaultCommitHandler::<MockTypeConfig>::new(
        Arc::new(mock_applier),
        Arc::new(mock_raft_log),
        new_commit_rx,
        1000,
        2, //1ms
        graceful_rx,
    );

    new_commit_tx
        .send(1)
        .expect("Should succeed to send new commit");
    new_commit_tx
        .send(2)
        .expect("Should succeed to send new commit");
    // Start the handler loop
    let handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(3), handler.run()).await; // 3 ms is key for this unit test
    });
    tokio::time::advance(Duration::from_millis(3)).await;
    // tokio::time::sleep(Duration::from_millis(10)).await;
    // Ensure the task completes
    handle.await.expect("should succeed");
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
    let (new_commit_tx, new_commit_rx) = mpsc::unbounded_channel::<u64>();

    // Mock Applier
    let mut mock_applier = MockStateMachineHandler::<MockTypeConfig>::new();
    mock_applier
        .expect_apply_batch()
        .times(2)
        .returning(|_| Ok(()));
    mock_applier.expect_update_pending().returning(|_| {});

    // Mock Raft Log
    let mock_raft_log = MockRaftLog::new();

    // Init handler
    let batch_thresold = 10;
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut handler = DefaultCommitHandler::<MockTypeConfig>::new(
        Arc::new(mock_applier),
        Arc::new(mock_raft_log),
        new_commit_rx,
        batch_thresold,
        1000, //1s
        graceful_rx,
    );

    for i in 1..=batch_thresold {
        new_commit_tx
            .send(i)
            .expect("Should succeed to send new commit");
    }
    // Start the handler loop
    let handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(10), handler.run()).await;
    });
    tokio::time::advance(Duration::from_millis(2)).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    // Ensure the task completes
    handle.await.expect("should succeed");
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
    let (new_commit_tx, new_commit_rx) = mpsc::unbounded_channel::<u64>();

    // Mock Applier
    let mut mock_applier = MockStateMachineHandler::<MockTypeConfig>::new();
    mock_applier
        .expect_apply_batch()
        .times(1)
        .returning(|_| Ok(()));
    mock_applier.expect_update_pending().returning(|_| {});

    // Mock Raft Log
    let mock_raft_log = MockRaftLog::new();

    // Init handler
    let batch_thresold = 1000;
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut handler = DefaultCommitHandler::<MockTypeConfig>::new(
        Arc::new(mock_applier),
        Arc::new(mock_raft_log),
        new_commit_rx,
        batch_thresold,
        1000, //1s
        graceful_rx,
    );

    for i in 1..=(batch_thresold - 10) {
        new_commit_tx
            .send(i)
            .expect("Should succeed to send new commit");
    }
    // Start the handler loop
    let handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(10), handler.run()).await;
    });
    tokio::time::advance(Duration::from_millis(2)).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    // Ensure the task completes
    handle.await.expect("should succeed");
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
    let (new_commit_tx, new_commit_rx) = mpsc::unbounded_channel::<u64>();

    // Mock Applier
    let mut mock_applier = MockStateMachineHandler::<MockTypeConfig>::new();
    mock_applier
        .expect_apply_batch()
        .times(3)
        .returning(|_| Ok(()));
    mock_applier.expect_update_pending().returning(|_| {});

    // Mock Raft Log
    let mock_raft_log = MockRaftLog::new();

    // Init handler
    let batch_thresold = 10;
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut handler = DefaultCommitHandler::<MockTypeConfig>::new(
        Arc::new(mock_applier),
        Arc::new(mock_raft_log),
        new_commit_rx,
        batch_thresold,
        2, //1s
        graceful_rx,
    );

    for i in 1..=batch_thresold {
        new_commit_tx
            .send(i)
            .expect("Should succeed to send new commit");
    }
    // Start the handler loop
    let handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(3), handler.run()).await;
    });
    tokio::time::advance(Duration::from_millis(2)).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    // Ensure the task completes
    handle.await.expect("should succeed");
}
