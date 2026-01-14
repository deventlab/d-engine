//! Tests for leader state snapshot creation and management
//!
//! This module tests snapshot creation, snapshot completion handling,
//! log purging, and peer purge progress tracking.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::{mpsc, watch};

use crate::MockPurgeExecutor;
use crate::MockTransport;
use crate::SnapshotError;
use crate::config::RaftNodeConfig as CoreRaftNodeConfig;
use crate::event::RaftEvent;
use crate::event::RoleEvent;
use crate::raft_role::leader_state::LeaderState;
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::{MockBuilder, MockTypeConfig};
use d_engine_proto::common::{LogId, NodeRole::Follower, NodeStatus};
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::storage::{PurgeLogResponse, SnapshotMetadata};
use mockall::predicate::eq;

// ============================================================================
// Test Helper Functions
// ============================================================================

/// Create a mock membership for testing (default: multi-node cluster)
fn create_mock_membership() -> crate::MockMembership<MockTypeConfig> {
    let mut membership = crate::MockMembership::new();
    membership.expect_is_single_node_cluster().returning(|| false);
    membership
}

/// Helper function to create a mock membership with replication peers for multi-node tests.
fn create_mock_membership_with_peers(
    peers: Vec<NodeMeta>
) -> crate::MockMembership<MockTypeConfig> {
    let mut membership = crate::MockMembership::new();
    membership.expect_is_single_node_cluster().returning(|| false);
    membership.expect_can_rejoin().returning(|_, _| Ok(()));

    let peers_for_voters = peers.clone();
    membership.expect_voters().returning(move || peers_for_voters.clone());

    let peers_for_replication = peers.clone();
    membership
        .expect_replication_peers()
        .returning(move || peers_for_replication.clone());

    membership
}

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
async fn test_handle_snapshot_created_transport_error() {
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_snapshot_created_transport_error");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Prepare AppendResults
    let peers = vec![NodeMeta {
        id: 2,
        address: "http://127.0.0.1:55001".to_string(),
        role: Follower.into(),
        status: NodeStatus::Active.into(),
    }];
    let membership = create_mock_membership_with_peers(peers);
    context.membership = Arc::new(membership);

    let mut transport = MockTransport::new();
    transport
        .expect_send_purge_requests()
        .times(1)
        .returning(|_, _, _| Err(crate::Error::Fatal("Mock transport error".to_string())));
    context.transport = Arc::new(transport);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    state.snapshot_in_progress.store(true, Ordering::SeqCst);

    // Initialize cluster_metadata with replication targets
    state
        .init_cluster_metadata(&context.membership)
        .await
        .expect("Should initialize cluster metadata");

    let raft_event = RaftEvent::SnapshotCreated(Ok((
        SnapshotMetadata {
            last_included: Some(LogId {
                term: 3,
                index: 100,
            }),
            checksum: "checksum".into(),
        },
        case_path.to_path_buf(),
    )));

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_err());

    // Validation criteria 2: No role event should be triggered
    assert!(role_rx.try_recv().is_err());
    assert!(!state.snapshot_in_progress.load(Ordering::SeqCst));
}

/// Test handling SnapshotCreated successfully
///
/// # Test Scenario
/// Leader completes snapshot creation and successfully sends purge requests to peers.
/// Local log purge is also executed successfully.
///
/// # Given
/// - Leader with completed snapshot
/// - Mock transport returns successful responses
/// - Mock purge executor succeeds
/// - Commit index > snapshot index (purge precondition met)
///
/// # When
/// - SnapshotCreated event is handled
///
/// # Then
/// - scheduled_purge_upto is set to snapshot index
/// - peer_purge_progress is updated
/// - LogPurgeCompleted event is sent for reprocessing
/// - snapshot_in_progress flag is reset
#[tokio::test]
async fn test_handle_snapshot_created_successful() {
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_snapshot_created_successful");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock peer configuration
    let peers = vec![NodeMeta {
        id: 2,
        address: "".to_string(),
        role: Follower.into(),
        status: NodeStatus::Active.into(),
    }];
    let membership = create_mock_membership_with_peers(peers);
    context.membership = Arc::new(membership);

    // Mock transport with successful response
    let mut transport = MockTransport::new();
    transport.expect_send_purge_requests().times(1).returning(|_, _, _| {
        Ok(vec![Ok(PurgeLogResponse {
            node_id: 2,
            term: 3,
            success: true,
            last_purged: Some(LogId {
                term: 3,
                index: 100,
            }),
        })])
    });
    context.transport = Arc::new(transport);

    // Mock purge executor
    let mut purge_executor = MockPurgeExecutor::new();
    purge_executor
        .expect_execute_purge()
        .with(eq(LogId {
            term: 3,
            index: 100,
        }))
        .times(1)
        .returning(|_| Ok(()));
    context.handlers.purge_executor = Arc::new(purge_executor);

    // Prepare leader state
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    state.update_current_term(3);
    state.shared_state.commit_index = 150; // Commit index > snapshot index
    state.last_purged_index = Some(LogId { term: 2, index: 80 });
    state.peer_purge_progress.insert(2, 100); // Peer has progressed beyond snapshot
    state.snapshot_in_progress.store(true, Ordering::SeqCst);

    // Initialize cluster_metadata with replication targets
    state
        .init_cluster_metadata(&context.membership)
        .await
        .expect("Should initialize cluster metadata");

    // Trigger event
    let raft_event = RaftEvent::SnapshotCreated(Ok((
        SnapshotMetadata {
            last_included: Some(LogId {
                term: 3,
                index: 100,
            }),
            checksum: "checksum".into(),
        },
        case_path.to_path_buf(),
    )));

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should succeed");

    // Validate state updates
    assert_eq!(
        state.scheduled_purge_upto,
        Some(LogId {
            term: 3,
            index: 100
        })
    );
    assert_eq!(state.peer_purge_progress.get(&2), Some(&100));
    assert!(!state.snapshot_in_progress.load(Ordering::SeqCst));

    // Check LogPurgeCompleted event was sent
    let event = role_rx.try_recv().expect("Should receive LogPurgeCompleted event");
    if let RoleEvent::ReprocessEvent(inner) = event {
        if let RaftEvent::LogPurgeCompleted(purged_id) = *inner {
            assert_eq!(
                purged_id,
                LogId {
                    term: 3,
                    index: 100
                }
            );
        } else {
            panic!("Expected LogPurgeCompleted event");
        }
    } else {
        panic!("Expected ReprocessEvent");
    }
}

/// Test snapshot creation failure scenario
///
/// # Test Scenario
/// Snapshot creation fails with an error.
/// Leader should handle the error gracefully without crashing.
///
/// # Given
/// - Leader with snapshot_in_progress = true
///
/// # When
/// - SnapshotCreated event is received with error
///
/// # Then
/// - Event is handled without error
/// - No state changes occur
/// - snapshot_in_progress flag is reset
#[tokio::test]
async fn test_handle_snapshot_created_error() {
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_snapshot_created_error");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, Arc::new(CoreRaftNodeConfig::default()));
    state.snapshot_in_progress.store(true, Ordering::SeqCst);

    // Trigger event with error
    let raft_event = RaftEvent::SnapshotCreated(Err(SnapshotError::OperationFailed(
        "Test failure".to_string(),
    )
    .into()));

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Execute and validate error is handled gracefully
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should handle error without crashing");

    // Validate no state changes occurred and flag is reset
    assert!(state.scheduled_purge_upto.is_none());
    assert_eq!(state.last_purged_index, None);
    assert!(state.peer_purge_progress.is_empty());
    assert!(!state.snapshot_in_progress.load(Ordering::SeqCst));
}

/// Test higher term response triggering leader step-down
///
/// # Test Scenario
/// Leader completes snapshot and sends purge requests to peers.
/// A peer responds with higher term, triggering step-down to follower.
///
/// # Given
/// - Leader with current_term = 3
/// - Mock peer response with term = 4 (higher)
///
/// # When
/// - SnapshotCreated event is handled
///
/// # Then
/// - Leader steps down to Follower (BecomeFollower event sent)
/// - Leader's term is updated to 4
/// - snapshot_in_progress flag is reset
#[tokio::test]
async fn test_handle_snapshot_created_higher_term() {
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_snapshot_created_higher_term");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock peer configuration
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(move || {
        vec![NodeMeta {
            id: 2,
            address: "http://127.0.0.1:55001".to_string(),
            role: Follower.into(),
            status: NodeStatus::Active.into(),
        }]
    });
    membership.expect_replication_peers().returning(move || {
        vec![NodeMeta {
            id: 2,
            address: "http://127.0.0.1:55001".to_string(),
            role: Follower.into(),
            status: NodeStatus::Active.into(),
        }]
    });
    context.membership = Arc::new(membership);

    // Mock transport with higher term response
    let mut transport = MockTransport::new();
    transport.expect_send_purge_requests().times(1).returning(|_, _, _| {
        Ok(vec![Ok(PurgeLogResponse {
            node_id: 2,
            term: 4, // Higher than current term (3)
            success: true,
            last_purged: Some(LogId {
                term: 3,
                index: 100,
            }),
        })])
    });
    context.transport = Arc::new(transport);

    // Prepare leader state
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    state.update_current_term(3);
    state.shared_state.commit_index = 150;
    state.last_purged_index = Some(LogId { term: 2, index: 80 });
    state.peer_purge_progress.insert(2, 100);
    state.snapshot_in_progress.store(true, Ordering::SeqCst);

    // Initialize cluster_metadata with replication targets
    state
        .init_cluster_metadata(&context.membership)
        .await
        .expect("Should initialize cluster metadata");

    // Trigger event
    let raft_event = RaftEvent::SnapshotCreated(Ok((
        SnapshotMetadata {
            last_included: Some(LogId {
                term: 3,
                index: 100,
            }),
            checksum: "checksum".into(),
        },
        case_path.to_path_buf(),
    )));

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should handle higher term response");

    // Validate leader stepped down
    let event = role_rx.try_recv().expect("Should receive step down event");
    assert!(matches!(event, RoleEvent::BecomeFollower(None)));

    // Validate term was updated
    assert_eq!(state.current_term(), 4);
    assert!(!state.snapshot_in_progress.load(Ordering::SeqCst));
}

/// Test purge preconditions not met (commit index < snapshot index)
///
/// # Test Scenario
/// Leader completes snapshot but commit index is less than snapshot index.
/// Log purge should be skipped as precondition is not met.
///
/// # Given
/// - Leader with commit_index = 90
/// - Snapshot with index = 100 (commit_index < snapshot_index)
///
/// # When
/// - SnapshotCreated event is handled
///
/// # Then
/// - scheduled_purge_upto is NOT set (purge skipped)
/// - snapshot_in_progress flag is reset
#[tokio::test]
async fn test_handle_snapshot_created_purge_conditions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_snapshot_created_purge_conditions");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    let peers = vec![NodeMeta {
        id: 2,
        address: "http://127.0.0.1:55001".to_string(),
        role: Follower.into(),
        status: NodeStatus::Active.into(),
    }];
    let membership = create_mock_membership_with_peers(peers);
    context.membership = Arc::new(membership);

    // Mock transport with successful response
    let mut transport = MockTransport::new();
    transport.expect_send_purge_requests().times(1).returning(|_, _, _| {
        Ok(vec![Ok(PurgeLogResponse {
            node_id: 2,
            term: 3,
            success: true,
            last_purged: Some(LogId {
                term: 3,
                index: 100,
            }),
        })])
    });
    context.transport = Arc::new(transport);

    // Prepare leader state where commit index < snapshot index
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    state.update_current_term(3);
    state.shared_state.commit_index = 90; // Commit index < snapshot index (100)
    state.last_purged_index = Some(LogId { term: 2, index: 80 });
    state.peer_purge_progress.insert(2, 100);
    state.snapshot_in_progress.store(true, Ordering::SeqCst);

    // Initialize cluster_metadata with replication targets
    state
        .init_cluster_metadata(&context.membership)
        .await
        .expect("Should initialize cluster metadata");

    // Trigger event
    let raft_event = RaftEvent::SnapshotCreated(Ok((
        SnapshotMetadata {
            last_included: Some(LogId {
                term: 3,
                index: 100,
            }),
            checksum: "checksum".into(),
        },
        case_path.to_path_buf(),
    )));

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should skip purge");

    // Validate scheduled_purge_upto was NOT set
    assert!(state.scheduled_purge_upto.is_none());
    assert!(!state.snapshot_in_progress.load(Ordering::SeqCst));
}

/// Test peer purge progress tracking
///
/// # Test Scenario
/// Leader completes snapshot and sends purge requests to multiple peers.
/// Each peer responds with different purge progress.
/// Leader should track each peer's progress correctly.
///
/// # Given
/// - Leader with multiple peers (ids: 2, 3)
/// - Mock responses with different purge indices (100, 95)
///
/// # When
/// - SnapshotCreated event is handled
///
/// # Then
/// - peer_purge_progress[2] = 100
/// - peer_purge_progress[3] = 95
/// - snapshot_in_progress flag is reset
#[tokio::test]
async fn test_handle_snapshot_created_peer_progress() {
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_snapshot_created_peer_progress");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock peer configuration (multiple peers)
    let peers = vec![
        NodeMeta {
            id: 2,
            address: "http://127.0.0.1:55001".to_string(),
            role: Follower.into(),
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 3,
            address: "http://127.0.0.1:55002".to_string(),
            role: Follower.into(),
            status: NodeStatus::Active.into(),
        },
    ];
    let membership = create_mock_membership_with_peers(peers);
    context.membership = Arc::new(membership);

    // Mock transport with mixed responses
    let mut transport = MockTransport::new();
    transport.expect_send_purge_requests().times(1).returning(|_, _, _| {
        Ok(vec![
            Ok(PurgeLogResponse {
                node_id: 2,
                term: 3,
                success: true,
                last_purged: Some(LogId {
                    term: 3,
                    index: 100,
                }),
            }),
            Ok(PurgeLogResponse {
                node_id: 3,
                term: 3,
                success: true,
                last_purged: Some(LogId { term: 3, index: 95 }), // Different purge index
            }),
        ])
    });
    context.transport = Arc::new(transport);

    // Prepare leader state
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    state.update_current_term(3);
    state.shared_state.commit_index = 150;
    state.last_purged_index = Some(LogId { term: 2, index: 80 });
    state.snapshot_in_progress.store(true, Ordering::SeqCst);

    // Initialize cluster_metadata with replication targets
    state
        .init_cluster_metadata(&context.membership)
        .await
        .expect("Should initialize cluster metadata");

    // Trigger event
    let raft_event = RaftEvent::SnapshotCreated(Ok((
        SnapshotMetadata {
            last_included: Some(LogId {
                term: 3,
                index: 100,
            }),
            checksum: "checksum".into(),
        },
        case_path.to_path_buf(),
    )));

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should succeed");

    // Validate peer purge progress tracking
    assert_eq!(state.peer_purge_progress.get(&2), Some(&100));
    assert_eq!(state.peer_purge_progress.get(&3), Some(&95));
    assert!(!state.snapshot_in_progress.load(Ordering::SeqCst));
}

// ============================================================================
// LogPurgeCompleted Event Tests
// ============================================================================

/// Test handling LogPurgeCompleted event
///
/// # Test Scenario
/// Leader receives LogPurgeCompleted event and updates last_purged_index.
/// The index should only be updated if the new value is higher.
///
/// # Given
/// - Leader with last_purged_index = Some(LogId { term: 1, index: 100 })
///
/// # When
/// - LogPurgeCompleted(150) is received (higher index)
/// - LogPurgeCompleted(120) is received (lower index)
/// - LogPurgeCompleted(50) is received when last_purged_index = None (first purge)
///
/// # Then
/// - Index is updated to 150 (higher)
/// - Index remains 150 (lower value ignored)
/// - Index is set to 50 (first purge)
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
