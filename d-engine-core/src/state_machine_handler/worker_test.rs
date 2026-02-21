//! Unit tests for StateMachineWorker
//!
//! Tests verify SM Worker's core responsibilities:
//! - Apply entries asynchronously via apply_chunk
//! - Send ApplyCompleted events on success
//! - Send FatalError events on apply failures
//! - Gracefully drain remaining entries on shutdown

use std::sync::Arc;

use d_engine_proto::common::Entry;
use tokio::sync::{mpsc, watch};

use super::StateMachineWorker;
use crate::{Error, MockStateMachineHandler, MockTypeConfig, RaftEvent};

/// Helper: Create test entry
fn create_test_entry(
    index: u64,
    term: u64,
) -> Entry {
    Entry {
        index,
        term,
        payload: Some(d_engine_proto::common::EntryPayload::command(
            bytes::Bytes::from("test_data"),
        )),
    }
}

/// Test: Apply Success - Sends ApplyCompleted Event
///
/// # Test Objective
/// Verify that when apply_chunk succeeds, SM Worker sends
/// RaftEvent::ApplyCompleted with correct results.
///
/// # Test Scenario
/// SM Worker receives entries via sm_apply_rx, apply_chunk succeeds,
/// and ApplyCompleted event is sent to event_tx.
///
/// # Given
/// - Mock StateMachineHandler with successful apply_chunk
/// - SM Worker running in background
/// - Entries sent to sm_apply_rx
///
/// # When
/// - apply_chunk returns Ok(results)
///
/// # Then
/// - RaftEvent::ApplyCompleted is sent to event_tx
/// - Event contains correct last_index and results
///
/// # Success Criteria
/// - Event received within timeout
/// - last_index matches last entry index
/// - results match mock return value
#[tokio::test]
async fn test_apply_success_sends_apply_completed() {
    // Given: Mock StateMachineHandler with successful apply
    let mut mock_smh = MockStateMachineHandler::new();
    mock_smh.expect_apply_chunk().times(1).returning(|entries| {
        let results: Vec<crate::ApplyResult> =
            entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect();
        Ok(results)
    });

    // Setup channels
    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = mpsc::channel(10);
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    // Create and spawn worker
    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smh),
        sm_apply_rx,
        event_tx,
        shutdown_rx,
    );

    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    // When: Send entries to apply
    let entries = vec![create_test_entry(1, 1), create_test_entry(2, 1)];
    sm_apply_tx.send(entries).unwrap();

    // Then: Verify ApplyCompleted event received
    match tokio::time::timeout(std::time::Duration::from_millis(100), event_rx.recv()).await {
        Ok(Some(RaftEvent::ApplyCompleted {
            last_index,
            results,
        })) => {
            assert_eq!(last_index, 2, "last_index should match last entry");
            assert_eq!(results.len(), 2, "results should contain 2 entries");
            assert_eq!(results[0].index, 1);
            assert_eq!(results[1].index, 2);
        }
        Ok(Some(other)) => panic!("Expected ApplyCompleted, got {other:?}"),
        Ok(None) => panic!("Event channel closed unexpectedly"),
        Err(_) => panic!("Timeout waiting for ApplyCompleted event"),
    }
}

/// Test: Apply Failure - Sends FatalError Event
///
/// # Test Objective
/// Verify that when apply_chunk fails, SM Worker sends
/// RaftEvent::FatalError with error details.
///
/// # Test Scenario
/// SM Worker receives entries, apply_chunk returns Error::Fatal,
/// and FatalError event is propagated to Raft main loop.
///
/// # Given
/// - Mock StateMachineHandler with failing apply_chunk
/// - SM Worker running in background
/// - Entries sent to sm_apply_rx
///
/// # When
/// - apply_chunk returns Err(Error::Fatal)
///
/// # Then
/// - RaftEvent::FatalError is sent to event_tx
/// - Event contains source="StateMachine" and error message
/// - Worker returns error (exits run loop)
///
/// # Success Criteria
/// - FatalError event received within timeout
/// - Event source is "StateMachine"
/// - Error message matches mock error
#[tokio::test]
async fn test_apply_failure_sends_fatal_error() {
    // Given: Mock StateMachineHandler with failing apply
    let mut mock_smh = MockStateMachineHandler::new();
    mock_smh.expect_apply_chunk().times(1).returning(|_| {
        Err(Error::Fatal(
            "Disk failure - cannot write to storage".to_string(),
        ))
    });

    // Setup channels
    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = mpsc::channel(10);
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    // Create and spawn worker
    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smh),
        sm_apply_rx,
        event_tx,
        shutdown_rx,
    );

    let worker_handle = tokio::spawn(async move { worker.run().await });

    // When: Send entries to apply
    let entries = vec![create_test_entry(1, 1)];
    sm_apply_tx.send(entries).unwrap();

    // Then: Verify FatalError event received
    match tokio::time::timeout(std::time::Duration::from_millis(100), event_rx.recv()).await {
        Ok(Some(RaftEvent::FatalError { source, error })) => {
            assert_eq!(source, "StateMachine", "source should be StateMachine");
            assert!(
                error.contains("Disk failure") || error.contains("storage"),
                "error should contain failure details, got: {error}"
            );
        }
        Ok(Some(other)) => panic!("Expected FatalError, got {other:?}"),
        Ok(None) => panic!("Event channel closed unexpectedly"),
        Err(_) => panic!("Timeout waiting for FatalError event"),
    }

    // Verify worker exits with error
    let result = worker_handle.await.unwrap();
    assert!(
        result.is_err(),
        "Worker should return error after fatal failure"
    );
}

/// Test: Shutdown - Drains Remaining Entries
///
/// # Test Objective
/// Verify that when shutdown signal is received, SM Worker:
/// 1. Stops accepting new entries
/// 2. Drains all pending entries from sm_apply_rx
/// 3. Applies all pending entries before exit
/// 4. Exits gracefully
///
/// # Test Scenario
/// Shutdown signal sent while entries are buffered in channel.
/// Worker should process all buffered entries before shutdown.
///
/// # Given
/// - SM Worker running in background
/// - Multiple entries in sm_apply_rx channel
/// - Shutdown signal sent
///
/// # When
/// - shutdown_signal.send(())
///
/// # Then
/// - All buffered entries are applied
/// - ApplyCompleted events sent for all entries
/// - Worker exits cleanly (Ok result)
///
/// # Success Criteria
/// - All entries applied (verified via ApplyCompleted count)
/// - Worker run() returns Ok(())
/// - No data loss
#[tokio::test]
async fn test_shutdown_drains_remaining_entries() {
    // Given: Mock StateMachineHandler that tracks apply count
    let mut mock_smh = MockStateMachineHandler::new();
    mock_smh.expect_apply_chunk().times(3).returning(|entries| {
        let results: Vec<crate::ApplyResult> =
            entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect();
        Ok(results)
    });

    // Setup channels
    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = mpsc::channel(10);
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    // Create and spawn worker
    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smh),
        sm_apply_rx,
        event_tx,
        shutdown_rx,
    );

    let worker_handle = tokio::spawn(async move { worker.run().await });

    // When: Send multiple entries, then shutdown
    for i in 1..=3 {
        sm_apply_tx.send(vec![create_test_entry(i, 1)]).unwrap();
    }

    // Small delay to ensure entries are in channel
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Send shutdown signal
    shutdown_tx.send(()).unwrap();

    // Then: Verify all entries are applied
    let mut apply_count = 0;
    while apply_count < 3 {
        match tokio::time::timeout(std::time::Duration::from_millis(100), event_rx.recv()).await {
            Ok(Some(RaftEvent::ApplyCompleted { .. })) => {
                apply_count += 1;
            }
            Ok(Some(other)) => panic!("Expected ApplyCompleted, got {other:?}"),
            Ok(None) => break,
            Err(_) => panic!("Timeout waiting for ApplyCompleted events"),
        }
    }

    assert_eq!(
        apply_count, 3,
        "All 3 entries should be applied during shutdown drain"
    );

    // Verify worker exits cleanly
    let result = worker_handle.await.unwrap();
    assert!(result.is_ok(), "Worker should exit cleanly after shutdown");
}

/// Test: Channel Closed - Worker Exits
///
/// # Test Objective
/// Verify that when sm_apply_rx channel is closed (sender dropped),
/// SM Worker detects channel closure and exits gracefully.
///
/// # Test Scenario
/// sm_apply_tx is dropped, causing sm_apply_rx.recv() to return None.
/// Worker should exit the run loop cleanly.
///
/// # Given
/// - SM Worker running in background
/// - sm_apply_tx dropped (channel closed)
///
/// # When
/// - sm_apply_rx.recv() returns None
///
/// # Then
/// - Worker detects channel closure
/// - Worker exits run() with Ok(())
/// - No panic or error
///
/// # Success Criteria
/// - Worker run() returns Ok(())
/// - Worker exits within timeout
#[tokio::test]
async fn test_channel_closed_worker_exits() {
    // Given: Mock StateMachineHandler (no apply expected)
    let mock_smh = MockStateMachineHandler::new();

    // Setup channels
    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(10);
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    // Create and spawn worker
    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smh),
        sm_apply_rx,
        event_tx,
        shutdown_rx,
    );

    let worker_handle = tokio::spawn(async move { worker.run().await });

    // When: Drop sender to close channel
    drop(sm_apply_tx);

    // Then: Verify worker exits cleanly
    let result = tokio::time::timeout(std::time::Duration::from_millis(100), worker_handle).await;

    match result {
        Ok(Ok(Ok(()))) => {
            // Worker exited cleanly
        }
        Ok(Ok(Err(e))) => panic!("Worker returned error: {e:?}"),
        Ok(Err(e)) => panic!("Worker task panicked: {e:?}"),
        Err(_) => panic!("Worker did not exit within timeout"),
    }
}

/// Test: Multiple Batches - Sequential Processing
///
/// # Test Objective
/// Verify that SM Worker processes multiple batches sequentially,
/// sending ApplyCompleted events in correct order.
///
/// # Test Scenario
/// Multiple batches sent to sm_apply_rx in sequence.
/// Worker should process each batch and send events in order.
///
/// # Given
/// - SM Worker running in background
/// - Multiple batches sent sequentially
///
/// # When
/// - Batches processed one by one
///
/// # Then
/// - ApplyCompleted events received in order
/// - last_index increases monotonically
/// - All batches processed successfully
///
/// # Success Criteria
/// - 3 ApplyCompleted events received
/// - last_index values: 2, 4, 6 (sequential batches)
#[tokio::test]
async fn test_multiple_batches_sequential_processing() {
    // Given: Mock StateMachineHandler
    let mut mock_smh = MockStateMachineHandler::new();
    mock_smh.expect_apply_chunk().times(3).returning(|entries| {
        let results: Vec<crate::ApplyResult> =
            entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect();
        Ok(results)
    });

    // Setup channels
    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = mpsc::channel(10);
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    // Create and spawn worker
    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smh),
        sm_apply_rx,
        event_tx,
        shutdown_rx,
    );

    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    // When: Send 3 batches
    sm_apply_tx
        .send(vec![create_test_entry(1, 1), create_test_entry(2, 1)])
        .unwrap();
    sm_apply_tx
        .send(vec![create_test_entry(3, 1), create_test_entry(4, 1)])
        .unwrap();
    sm_apply_tx
        .send(vec![create_test_entry(5, 1), create_test_entry(6, 1)])
        .unwrap();

    // Then: Verify events received in order
    let expected_last_indices = vec![2, 4, 6];
    for expected_index in expected_last_indices {
        match tokio::time::timeout(std::time::Duration::from_millis(100), event_rx.recv()).await {
            Ok(Some(RaftEvent::ApplyCompleted { last_index, .. })) => {
                assert_eq!(
                    last_index, expected_index,
                    "last_index should match expected value"
                );
            }
            Ok(Some(other)) => panic!("Expected ApplyCompleted, got {other:?}"),
            Ok(None) => panic!("Event channel closed unexpectedly"),
            Err(_) => panic!("Timeout waiting for ApplyCompleted event"),
        }
    }
}
