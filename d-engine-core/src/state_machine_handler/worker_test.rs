//! Unit tests for StateMachineWorker
//!
//! Tests verify SM Worker's core responsibilities:
//! - Apply entries asynchronously via apply_chunk
//! - Send ApplyCompleted events on success via role_tx (P2)
//! - Send FatalError events on apply failures via role_tx (P2)
//! - Gracefully drain remaining entries on shutdown

use std::sync::Arc;

use d_engine_proto::common::Entry;
use tokio::sync::{mpsc, watch};

use super::StateMachineWorker;
use crate::{Error, MockStateMachineHandler, MockTypeConfig, RoleEvent};

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
/// RoleEvent::ApplyCompleted with correct results via role_tx (P2, unbounded).
///
/// # Given
/// - Mock StateMachineHandler with successful apply_chunk
/// - SM Worker running in background
///
/// # When
/// - apply_chunk returns Ok(results)
///
/// # Then
/// - RoleEvent::ApplyCompleted is sent to role_tx
/// - Event contains correct last_index and results
#[tokio::test]
async fn test_apply_success_sends_apply_completed() {
    let mut mock_smh = MockStateMachineHandler::new();
    mock_smh.expect_apply_chunk().times(1).returning(|entries| {
        let results: Vec<crate::ApplyResult> =
            entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect();
        Ok(results)
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smh),
        sm_apply_rx,
        role_tx,
        shutdown_rx,
    );

    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    sm_apply_tx
        .send(vec![create_test_entry(1, 1), create_test_entry(2, 1)])
        .unwrap();

    match tokio::time::timeout(std::time::Duration::from_millis(100), role_rx.recv()).await {
        Ok(Some(RoleEvent::ApplyCompleted {
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
/// RoleEvent::FatalError via role_tx (P2, unbounded) and exits with error.
///
/// # Given
/// - Mock StateMachineHandler with failing apply_chunk
///
/// # When
/// - apply_chunk returns Err(Error::Fatal)
///
/// # Then
/// - RoleEvent::FatalError is sent to role_tx
/// - Worker returns error (exits run loop)
#[tokio::test]
async fn test_apply_failure_sends_fatal_error() {
    let mut mock_smh = MockStateMachineHandler::new();
    mock_smh.expect_apply_chunk().times(1).returning(|_| {
        Err(Error::Fatal(
            "Disk failure - cannot write to storage".to_string(),
        ))
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smh),
        sm_apply_rx,
        role_tx,
        shutdown_rx,
    );

    let worker_handle = tokio::spawn(async move { worker.run().await });

    sm_apply_tx.send(vec![create_test_entry(1, 1)]).unwrap();

    match tokio::time::timeout(std::time::Duration::from_millis(100), role_rx.recv()).await {
        Ok(Some(RoleEvent::FatalError { source, error })) => {
            assert_eq!(source, "StateMachine");
            assert!(
                error.contains("Disk failure") || error.contains("storage"),
                "error should contain failure details, got: {error}"
            );
        }
        Ok(Some(other)) => panic!("Expected FatalError, got {other:?}"),
        Ok(None) => panic!("Event channel closed unexpectedly"),
        Err(_) => panic!("Timeout waiting for FatalError event"),
    }

    let result = worker_handle.await.unwrap();
    assert!(
        result.is_err(),
        "Worker should return error after fatal failure"
    );
}

/// Test: Shutdown - Drains Remaining Entries
///
/// # Test Objective
/// Verify that when shutdown signal is received, SM Worker drains all
/// pending entries before exit (no data loss).
///
/// # Success Criteria
/// - All 3 entries applied (verified via ApplyCompleted count on role_rx)
/// - Worker run() returns Ok(())
#[tokio::test]
async fn test_shutdown_drains_remaining_entries() {
    let mut mock_smh = MockStateMachineHandler::new();
    mock_smh.expect_apply_chunk().times(3).returning(|entries| {
        let results: Vec<crate::ApplyResult> =
            entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect();
        Ok(results)
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smh),
        sm_apply_rx,
        role_tx,
        shutdown_rx,
    );

    let worker_handle = tokio::spawn(async move { worker.run().await });

    for i in 1..=3 {
        sm_apply_tx.send(vec![create_test_entry(i, 1)]).unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    shutdown_tx.send(()).unwrap();

    let mut apply_count = 0;
    while apply_count < 3 {
        match tokio::time::timeout(std::time::Duration::from_millis(100), role_rx.recv()).await {
            Ok(Some(RoleEvent::ApplyCompleted { .. })) => apply_count += 1,
            Ok(Some(other)) => panic!("Expected ApplyCompleted, got {other:?}"),
            Ok(None) => break,
            Err(_) => panic!("Timeout waiting for ApplyCompleted events"),
        }
    }

    assert_eq!(
        apply_count, 3,
        "All 3 entries should be applied during shutdown drain"
    );

    let result = worker_handle.await.unwrap();
    assert!(result.is_ok(), "Worker should exit cleanly after shutdown");
}

/// Test: Channel Closed - Worker Exits
///
/// Verify that when sm_apply_rx channel is closed, worker exits cleanly.
#[tokio::test]
async fn test_channel_closed_worker_exits() {
    let mock_smh = MockStateMachineHandler::new();

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (role_tx, _role_rx) = mpsc::unbounded_channel::<RoleEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smh),
        sm_apply_rx,
        role_tx,
        shutdown_rx,
    );

    let worker_handle = tokio::spawn(async move { worker.run().await });

    drop(sm_apply_tx);

    match tokio::time::timeout(std::time::Duration::from_millis(100), worker_handle).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => panic!("Worker returned error: {e:?}"),
        Ok(Err(e)) => panic!("Worker task panicked: {e:?}"),
        Err(_) => panic!("Worker did not exit within timeout"),
    }
}

/// Test: Multiple Batches - Sequential Processing
///
/// Verify SM Worker processes batches in order, last_index increases monotonically.
///
/// # Success Criteria
/// - 3 ApplyCompleted events received on role_rx with last_index: 2, 4, 6
#[tokio::test]
async fn test_multiple_batches_sequential_processing() {
    let mut mock_smh = MockStateMachineHandler::new();
    mock_smh.expect_apply_chunk().times(3).returning(|entries| {
        let results: Vec<crate::ApplyResult> =
            entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect();
        Ok(results)
    });

    let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let worker = StateMachineWorker::<MockTypeConfig>::new(
        1,
        Arc::new(mock_smh),
        sm_apply_rx,
        role_tx,
        shutdown_rx,
    );

    tokio::spawn(async move {
        let _ = worker.run().await;
    });

    sm_apply_tx
        .send(vec![create_test_entry(1, 1), create_test_entry(2, 1)])
        .unwrap();
    sm_apply_tx
        .send(vec![create_test_entry(3, 1), create_test_entry(4, 1)])
        .unwrap();
    sm_apply_tx
        .send(vec![create_test_entry(5, 1), create_test_entry(6, 1)])
        .unwrap();

    for expected_index in [2u64, 4, 6] {
        match tokio::time::timeout(std::time::Duration::from_millis(100), role_rx.recv()).await {
            Ok(Some(RoleEvent::ApplyCompleted { last_index, .. })) => {
                assert_eq!(
                    last_index, expected_index,
                    "last_index should match expected"
                );
            }
            Ok(Some(other)) => panic!("Expected ApplyCompleted, got {other:?}"),
            Ok(None) => panic!("Event channel closed unexpectedly"),
            Err(_) => panic!("Timeout waiting for ApplyCompleted event"),
        }
    }
}
