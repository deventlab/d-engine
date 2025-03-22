use std::{sync::Arc, time::Duration};

use tokio::{
    sync::{
        mpsc::{self, UnboundedReceiver},
        watch,
    },
    time::{self, Instant},
};

use super::DefaultCommitHandler;
use crate::{
    test_utils::{self, MockTypeConfig},
    CommitHandler, MockRaftLog, MockStateMachineHandler,
};

fn setup(
    batch_size_threshold: u64,
    commit_handle_interval_in_ms: u64,
    apply_batch_expected_execution_times: usize,
    new_commit_rx: mpsc::UnboundedReceiver<u64>,
    shutdown_signal: watch::Receiver<()>,
) -> DefaultCommitHandler<MockTypeConfig> {
    // prepare commit channel

    // Mock Applier
    let mut mock_applier = MockStateMachineHandler::<MockTypeConfig>::new();
    mock_applier
        .expect_apply_batch()
        .times(apply_batch_expected_execution_times)
        .returning(|_| Ok(()));
    mock_applier.expect_update_pending().returning(|_| {});

    // Mock Raft Log
    let mock_raft_log = MockRaftLog::new();

    // Init handler
    DefaultCommitHandler::<MockTypeConfig>::new(
        Arc::new(mock_applier),
        Arc::new(mock_raft_log),
        new_commit_rx,
        batch_size_threshold,
        commit_handle_interval_in_ms,
        shutdown_signal,
    )
}

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
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut handler = setup(1000, 2, 2, new_commit_rx, graceful_rx);

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
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let batch_thresold = 10;
    let mut handler = setup(batch_thresold, 1000, 2, new_commit_rx, graceful_rx);

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
    let batch_thresold = 1000;
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut handler = setup(batch_thresold, 1000, 1, new_commit_rx, graceful_rx);

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
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let batch_thresold = 10;
    let mut handler = setup(batch_thresold, 2, 3, new_commit_rx, graceful_rx);

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

/// # Case 1: interval_uses_correct_duration
#[tokio::test(start_paused = true)]
async fn test_dynamic_interval_case1() {
    // Prpeare interval
    let interval_ms = 100;

    // Setup handler
    let (_new_commit_tx, new_commit_rx) = mpsc::unbounded_channel::<u64>();
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
    let (_new_commit_tx, new_commit_rx) = mpsc::unbounded_channel::<u64>();
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
