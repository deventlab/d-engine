//! Tests for leader state snapshot creation and management
//!
//! This module tests snapshot creation, snapshot completion handling,
//! log purging, and peer purge progress tracking.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::SnapshotError;
use crate::config::RaftNodeConfig as CoreRaftNodeConfig;
use crate::event::InboundEvent;
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

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // First event should set the flag and spawn the task
    state
        .handle_create_snapshot(&context, &internal_event_tx)
        .await
        .expect("Should start snapshot creation");
    assert!(state.snapshot_in_progress.load(Ordering::SeqCst));

    // Second event should be ignored (flag still set)
    state
        .handle_create_snapshot(&context, &internal_event_tx)
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

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // Success case
    let _ = state
        .handle_snapshot_created(
            Ok((
                SnapshotMetadata {
                    last_included: Some(LogId { term: 1, index: 10 }),
                    checksum: "abc".into(),
                },
                PathBuf::from("/tmp/fake"),
            )),
            &context,
            &internal_event_tx,
        )
        .await;
    assert!(!state.snapshot_in_progress.load(Ordering::SeqCst));

    // Error case
    state.snapshot_in_progress.store(true, Ordering::SeqCst);
    let _ = state
        .handle_snapshot_created(
            Err(SnapshotError::OperationFailed("fail".into()).into()),
            &context,
            &internal_event_tx,
        )
        .await;
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

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    let start = std::time::Instant::now();
    let _ = state.handle_create_snapshot(&context, &internal_event_tx).await;
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
#[test]
fn test_handle_log_purge_completed() {
    let mut state = LeaderState::<MockTypeConfig>::new(1, Arc::new(CoreRaftNodeConfig::default()));
    state.last_purged_index = Some(LogId {
        term: 1,
        index: 100,
    });

    // Test updating to a higher index
    state
        .handle_log_purge_completed(LogId {
            term: 1,
            index: 150,
        })
        .unwrap();
    assert_eq!(
        state.last_purged_index,
        Some(LogId {
            term: 1,
            index: 150
        })
    );

    // Test updating to a lower index (should be ignored)
    state
        .handle_log_purge_completed(LogId {
            term: 1,
            index: 120,
        })
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
    state.handle_log_purge_completed(LogId { term: 1, index: 50 }).unwrap();
    assert_eq!(state.last_purged_index, Some(LogId { term: 1, index: 50 }));
}

// ============================================================================
// StreamSnapshot Startup Rejection Tests
// ============================================================================

/// Leader rejects StreamSnapshot when no snapshot metadata is available.
///
/// # Given
/// - Leader with no snapshot created yet (snapshot_metadata() → None)
///
/// # When
/// - StreamSnapshot event is received
///
/// # Then
/// - startup_rx receives Err(Status::not_found)
/// - chunk_rx is closed immediately (no chunks sent)
#[tokio::test]
async fn test_stream_snapshot_rejects_when_no_metadata() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    // Default mock: snapshot_metadata() returns None
    let context = MockBuilder::new(graceful_rx).build_context();
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());

    let (ack_tx, ack_rx) = mpsc::channel::<d_engine_proto::server::storage::SnapshotAck>(4);
    let (chunk_tx, mut chunk_rx) =
        mpsc::channel::<std::sync::Arc<d_engine_proto::server::storage::SnapshotChunk>>(4);
    let (startup_tx, startup_rx) = tokio::sync::oneshot::channel::<Result<(), tonic::Status>>();

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    state
        .handle_inbound_event(
            InboundEvent::StreamSnapshot(ack_rx, chunk_tx, startup_tx),
            &context,
            internal_event_tx,
        )
        .await
        .expect("handler must not return Err");

    // startup_rx must carry a rejection, not Ok
    let result = startup_rx.await.expect("startup_tx must be sent");
    assert!(result.is_err(), "expected Err from startup channel, got Ok");
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);

    // chunk channel must be closed — no chunks were sent
    drop(ack_tx); // silence unused warning
    assert!(
        chunk_rx.recv().await.is_none(),
        "expected chunk_rx to be closed"
    );
}

/// Leader confirms startup and begins transfer when snapshot metadata exists.
///
/// # Given
/// - Leader with valid snapshot metadata
/// - State machine handler returns a single-chunk data stream
///
/// # When
/// - StreamSnapshot event is received
///
/// # Then
/// - startup_rx receives Ok(())
/// - chunk_rx eventually delivers the chunk (background task runs)
#[tokio::test]
async fn test_stream_snapshot_confirms_startup_when_metadata_exists() {
    use bytes::Bytes;
    use futures::StreamExt;
    use futures::stream;

    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Build state machine from scratch so snapshot_metadata returns Some.
    // (Can't override expectations added by mock_state_machine() — mockall
    // uses the first matching expectation, so the None default would win.)
    let mut sm = crate::MockStateMachine::new();
    sm.expect_start().returning(|| Ok(()));
    sm.expect_stop().returning(|| Ok(()));
    sm.expect_is_running().returning(|| true);
    sm.expect_get().returning(|_| Ok(None));
    sm.expect_entry_term().returning(|_| None);
    sm.expect_apply_chunk().returning(|_| Ok(vec![]));
    sm.expect_len().returning(|| 0);
    sm.expect_update_last_applied().returning(|_| ());
    sm.expect_last_applied().return_const(LogId::default());
    sm.expect_persist_last_applied().returning(|_| Ok(()));
    sm.expect_update_last_snapshot_metadata().returning(|_| Ok(()));
    sm.expect_snapshot_metadata().returning(|| {
        Some(SnapshotMetadata {
            last_included: Some(LogId { term: 1, index: 10 }),
            ..Default::default()
        })
    });
    sm.expect_persist_last_snapshot_metadata().returning(|_| Ok(()));
    sm.expect_apply_snapshot_from_file().returning(|_, _| Ok(()));
    sm.expect_generate_snapshot_data()
        .returning(|_, _| Ok(bytes::Bytes::copy_from_slice(&[0u8; 32])));
    sm.expect_save_hard_state().returning(|| Ok(()));
    sm.expect_flush().returning(|| Ok(()));

    // State machine handler returns a minimal 1-chunk stream
    let mut smh = crate::test_utils::mock::mock_state_machine_handler();
    smh.expect_load_snapshot_data().returning(|_| {
        let chunk = d_engine_proto::server::storage::SnapshotChunk {
            seq: 0,
            total_chunks: 1,
            data: Bytes::from_static(b"hello"),
            metadata: Some(SnapshotMetadata::default()),
            ..Default::default()
        };
        Ok(stream::iter(vec![Ok(chunk)]).boxed())
    });

    let context = MockBuilder::new(graceful_rx)
        .with_state_machine(sm)
        .with_state_machine_handler(smh)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());

    let (_ack_tx, ack_rx) = mpsc::channel::<d_engine_proto::server::storage::SnapshotAck>(4);
    let (chunk_tx, mut chunk_rx) =
        mpsc::channel::<std::sync::Arc<d_engine_proto::server::storage::SnapshotChunk>>(4);
    let (startup_tx, startup_rx) = tokio::sync::oneshot::channel::<Result<(), tonic::Status>>();

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    state
        .handle_inbound_event(
            InboundEvent::StreamSnapshot(ack_rx, chunk_tx, startup_tx),
            &context,
            internal_event_tx,
        )
        .await
        .expect("handler must not return Err");

    // startup_rx must carry Ok — transfer has started
    let result = startup_rx.await.expect("startup_tx must be sent");
    assert!(
        result.is_ok(),
        "expected Ok from startup channel, got {:?}",
        result
    );

    // Background task delivers the chunk; give it a moment to run
    let chunk = tokio::time::timeout(std::time::Duration::from_millis(200), chunk_rx.recv())
        .await
        .expect("timed out waiting for chunk")
        .expect("chunk_rx closed before first chunk");

    assert_eq!(chunk.seq, 0);
}
