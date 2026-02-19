//! Tests for leader state snapshot creation and management
//!
//! This module tests snapshot creation, snapshot completion handling,
//! log purging, and peer purge progress tracking.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::SnapshotError;
use crate::config::RaftNodeConfig as CoreRaftNodeConfig;
use crate::event::RaftEvent;
use crate::raft_role::leader_state::LeaderState;
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::{MockBuilder, MockTypeConfig};
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;
use tokio::sync::{mpsc, watch};

// ============================================================================
// CreateSnapshot Event Tests
// ============================================================================

/// Test that snapshot creation is started, and duplicate requests are ignored while in progress
///
/// # Test Scenario
/// Leader receives CreateSnapshotEvent and starts snapshot creation.
/// While snapshot is in progress, duplicate requests should be ignored.
///
/// # Given
/// - Leader with no snapshot in progress
///
/// # When
/// - First CreateSnapshotEvent is received
/// - Second CreateSnapshotEvent is received (while first is in progress)
///
/// # Then
/// - First event sets snapshot_in_progress flag to true
/// - Second event is ignored (no panic, no duplicate task)
/// - Flag remains true throughout
#[tokio::test]
async fn test_create_snapshot_event_starts_and_ignores_duplicates() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx).build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());

    // Initially, no snapshot in progress
    assert!(!state.snapshot_in_progress.load(Ordering::SeqCst));

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // First event should set the flag and spawn the task
    state
        .handle_raft_event(RaftEvent::CreateSnapshotEvent, &context, role_tx.clone())
        .await
        .expect("Should start snapshot creation");
    assert!(state.snapshot_in_progress.load(Ordering::SeqCst));

    // Second event should be ignored (flag still set)
    state
        .handle_raft_event(RaftEvent::CreateSnapshotEvent, &context, role_tx)
        .await
        .expect("Should ignore duplicate snapshot creation");
    // Still true, no panic, no duplicate task
    assert!(state.snapshot_in_progress.load(Ordering::SeqCst));
}

/// Test that snapshot_in_progress is reset after SnapshotCreated event
///
/// # Test Scenario
/// Leader completes snapshot creation (success or error).
/// The snapshot_in_progress flag should be reset in both cases.
///
/// # Given
/// - Leader with snapshot_in_progress = true
///
/// # When
/// - SnapshotCreated event is received (success case)
/// - SnapshotCreated event is received (error case)
///
/// # Then
/// - Flag is reset to false in success case
/// - Flag is reset to false in error case
#[tokio::test]
async fn test_snapshot_in_progress_flag_reset_on_created_event() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx).build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    state.snapshot_in_progress.store(true, Ordering::SeqCst);

    // Success case
    let raft_event = RaftEvent::SnapshotCreated(Ok((
        SnapshotMetadata {
            last_included: Some(LogId { term: 1, index: 10 }),
            checksum: "abc".into(),
        },
        PathBuf::from("/tmp/fake"),
    )));
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let _ = state.handle_raft_event(raft_event, &context, role_tx).await;
    assert!(!state.snapshot_in_progress.load(Ordering::SeqCst));

    // Error case
    state.snapshot_in_progress.store(true, Ordering::SeqCst);
    let raft_event =
        RaftEvent::SnapshotCreated(Err(SnapshotError::OperationFailed("fail".into()).into()));
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let _ = state.handle_raft_event(raft_event, &context, role_tx).await;
    assert!(!state.snapshot_in_progress.load(Ordering::SeqCst));
}

/// Test that CreateSnapshotEvent returns immediately and does not block
///
/// # Test Scenario
/// Leader receives CreateSnapshotEvent and should return quickly
/// without waiting for the actual snapshot creation to complete.
///
/// # Given
/// - Leader ready to create snapshot
///
/// # When
/// - CreateSnapshotEvent is handled
///
/// # Then
/// - Event handling returns in < 100ms (non-blocking)
#[tokio::test]
async fn test_create_snapshot_event_is_non_blocking() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx).build_context();
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    let start = std::time::Instant::now();
    let _ = state.handle_raft_event(RaftEvent::CreateSnapshotEvent, &context, role_tx).await;
    let elapsed = start.elapsed();

    // Should return quickly (not wait for snapshot)
    assert!(elapsed < std::time::Duration::from_millis(100));
}

// ============================================================================
// SnapshotCreated Event Tests
// ============================================================================

/// Test handling SnapshotCreated with transport error
///
/// # Test Scenario
/// Leader completes snapshot creation but encounters transport error
/// when sending purge requests to peers.
///
/// # Given
/// - Leader with completed snapshot
/// - Mock transport returns error
///
/// # When
/// - SnapshotCreated event is handled
///
/// # Then
/// - Event handling returns error
/// - No role transition events sent
/// - snapshot_in_progress flag is reset
#[tokio::test]
async fn test_handle_log_purge_completed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_log_purge_completed");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, Arc::new(CoreRaftNodeConfig::default()));
    state.last_purged_index = Some(LogId {
        term: 1,
        index: 100,
    });

    // Test updating to a higher index
    let event = RaftEvent::LogPurgeCompleted(LogId {
        term: 1,
        index: 150,
    });
    state
        .handle_raft_event(event, &context, mpsc::unbounded_channel().0)
        .await
        .unwrap();
    assert_eq!(
        state.last_purged_index,
        Some(LogId {
            term: 1,
            index: 150
        })
    );

    // Test updating to a lower index (should be ignored)
    let event = RaftEvent::LogPurgeCompleted(LogId {
        term: 1,
        index: 120,
    });
    state
        .handle_raft_event(event, &context, mpsc::unbounded_channel().0)
        .await
        .unwrap();
    assert_eq!(
        state.last_purged_index,
        Some(LogId {
            term: 1,
            index: 150
        })
    );

    // Test first purge
    state.last_purged_index = None;
    let event = RaftEvent::LogPurgeCompleted(LogId { term: 1, index: 50 });
    state
        .handle_raft_event(event, &context, mpsc::unbounded_channel().0)
        .await
        .unwrap();
    assert_eq!(state.last_purged_index, Some(LogId { term: 1, index: 50 }));
}
