//! Tests for leader state management utilities and helper functions
//!
//! This module tests various state management functions including:
//! - Log purge validation (can_purge_logs)
//! - Leader discovery event handling
//! - State size tracking

use std::mem::size_of;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tracing_test::traced_test;

use crate::event::RaftEvent;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_role::leader_state::LeaderState;
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use crate::test_utils::node_config;
use d_engine_proto::common::{LogId, NodeRole::Leader, NodeStatus};
use d_engine_proto::server::cluster::{LeaderDiscoveryRequest, NodeMeta};

// ============================================================================
// Test Helper Functions
// ============================================================================

/// Create a mock membership for testing (default: multi-node cluster)
fn create_mock_membership() -> crate::MockMembership<MockTypeConfig> {
    let mut membership = crate::MockMembership::new();
    membership.expect_is_single_node_cluster().returning(|| false);
    membership
}

// ============================================================================
// State Size Tests
// ============================================================================

/// Document LeaderState memory footprint for awareness, not enforcement
///
/// # Test Scenario
/// Track LeaderState size evolution to catch unexpected memory bloat.
///
/// # Given
/// - LeaderState struct
///
/// # Then
/// - Size is documented
/// - Warning issued if > 512 bytes (cache-friendly guideline)
/// - Cache line usage calculated
#[test]
fn test_state_size() {
    let size = size_of::<LeaderState<MockTypeConfig>>();
    println!("LeaderState size: {size} bytes");

    // Size evolution history (for documentation):
    // - 376 bytes: Initial version
    // - 392 bytes: Added cluster_metadata (ClusterMetadata)
    // - 432 bytes: Added read_buffer + read_buffer_start_time (#236)

    // Soft guideline: Keep under 512 bytes (cache-friendly)
    // If significantly larger, consider moving large fields to heap
    if size > 512 {
        eprintln!("⚠️  WARNING: LeaderState is now {size} bytes (exceeds 512 byte guideline)");
        eprintln!("Consider moving large fields to heap allocation (Box/Arc)");
        eprintln!("This is a warning, not a failure - review if growth is justified");
    }

    // For reference: Modern CPU cache line is 64 bytes
    println!("Cache lines occupied: {}", size.div_ceil(64));
}

// ============================================================================
// Log Purge Validation Tests
// ============================================================================

/// Test can_purge_logs with valid purge conditions
///
/// # Test Scenario
/// Validates proper purge window according to Raft paper §7.2 requirements.
///
/// # Given
/// - commit_index = 100
/// - peer_purge_progress: all peers at 100
/// - last_purge_index = 90
///
/// # When
/// - Attempt to purge up to index 99
///
/// # Then
/// - Purge allowed (90 < 99 < 100)
/// - Boundary case: 99 == commit_index - 1 (valid gap)
/// - Invalid case: 100 not < 100 (violates gap rule)
#[test]
fn test_can_purge_logs_valid_conditions() {
    let node_config = Arc::new(node_config("/tmp/test_can_purge_logs_valid_conditions"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, node_config);

    // Setup per Raft paper §7.2 requirements
    state.shared_state.commit_index = 100;
    state.peer_purge_progress.insert(2, 100); // Follower 2
    state.peer_purge_progress.insert(3, 100); // Follower 3

    // Valid purge window (last_purge=90 < snapshot=99 < commit=100)
    assert!(state.can_purge_logs(
        Some(LogId { index: 90, term: 1 }), // last_purge_index
        LogId { index: 99, term: 1 }        // last_included_in_snapshot
    ));

    // Boundary check: 99 == commit_index - 1 (valid gap)
    assert!(state.can_purge_logs(
        Some(LogId { index: 90, term: 1 }),
        LogId { index: 99, term: 1 }
    ));

    // Violate gap rule: 100 not < 100
    assert!(!state.can_purge_logs(
        Some(LogId { index: 90, term: 1 }),
        LogId {
            index: 100,
            term: 1
        }
    ));
}

/// Test can_purge_logs rejects uncommitted purge
///
/// # Test Scenario
/// Reject purge attempts beyond commit index (Raft §5.4.2).
///
/// # Given
/// - commit_index = 50
/// - peer_purge_progress: peer 2 at 100
///
/// # When
/// - Attempt to purge beyond commit index
///
/// # Then
/// - Purge rejected when snapshot_index > commit_index
/// - Boundary: snapshot_index == commit_index also rejected
#[test]
fn test_can_purge_logs_reject_uncommitted() {
    let node_config = Arc::new(node_config("/tmp/test_can_purge_logs_reject_uncommitted"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, node_config);

    state.shared_state.commit_index = 50;
    state.peer_purge_progress.insert(2, 100);

    // Attempt to purge beyond commit index
    assert!(!state.can_purge_logs(
        Some(LogId { index: 40, term: 1 }),
        LogId { index: 51, term: 1 } // 51 > commit_index(50)
    ));

    // Boundary violation: 50 == commit_index (requires <)
    assert!(!state.can_purge_logs(
        Some(LogId { index: 40, term: 1 }),
        LogId { index: 50, term: 1 }
    ));
}

/// Test can_purge_logs enforces monotonicity
///
/// # Test Scenario
/// Enforce purge sequence monotonicity (Raft §7.2).
/// Prevent backward or duplicate purges.
///
/// # Given
/// - commit_index = 200
/// - last_purge_index = 150
///
/// # When
/// - Attempt various purge sequences
///
/// # Then
/// - Forward purge allowed (150 -> 199)
/// - Backward purge rejected (150 -> 120)
/// - Duplicate purge rejected (150 -> 150)
#[test]
fn test_can_purge_logs_enforce_monotonicity() {
    let node_config = Arc::new(node_config("/tmp/test_can_purge_logs_enforce_monotonicity"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, node_config);

    state.shared_state.commit_index = 200;
    state.peer_purge_progress.insert(2, 200);
    state.peer_purge_progress.insert(3, 200);

    // Valid sequence: 100 → 150 → 199
    assert!(state.can_purge_logs(
        Some(LogId {
            index: 150,
            term: 1
        }),
        LogId {
            index: 199,
            term: 1
        }
    ));

    // Invalid backward purge (150 → 120)
    assert!(!state.can_purge_logs(
        Some(LogId {
            index: 150,
            term: 1
        }),
        LogId {
            index: 120,
            term: 1
        }
    ));

    // Same index purge attempt
    assert!(!state.can_purge_logs(
        Some(LogId {
            index: 150,
            term: 1
        }),
        LogId {
            index: 150,
            term: 1
        }
    ));
}

/// Test can_purge_logs verifies cluster progress
///
/// # Test Scenario
/// Enhanced durability check - ensure peers have progressed sufficiently.
///
/// # Given
/// - commit_index = 100
/// - Various peer progress states
///
/// # When
/// - Check purge eligibility
///
/// # Then
/// - Purge allowed when peers meet requirements
/// - Single lagging peer handled correctly
#[test]
fn test_can_purge_logs_cluster_progress() {
    let node_config = Arc::new(node_config("/tmp/test_can_purge_logs_cluster_progress"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, node_config);

    state.shared_state.commit_index = 100;

    // Single lagging peer (index 99 < 100)
    state.peer_purge_progress.insert(2, 99);
    state.peer_purge_progress.insert(3, 100);
    assert!(state.can_purge_logs(
        Some(LogId { index: 90, term: 1 }),
        LogId { index: 99, term: 1 }
    ));

    // All peers at required index
    state.peer_purge_progress.insert(2, 100);
    assert!(state.can_purge_logs(
        Some(LogId { index: 90, term: 1 }),
        LogId { index: 99, term: 1 }
    ));
}

/// Test can_purge_logs initial purge state
///
/// # Test Scenario
/// Validate first purge when last_purge_index = None.
///
/// # Given
/// - commit_index = 100
/// - last_purge_index = None (first purge)
///
/// # When
/// - Attempt first purge
///
/// # Then
/// - First purge allowed (None -> 99)
/// - Must still respect commit_index gap
#[test]
fn test_can_purge_logs_initial_purge() {
    let node_config = Arc::new(node_config("/tmp/test_can_purge_logs_initial_purge"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, node_config);

    state.shared_state.commit_index = 100;
    state.peer_purge_progress.insert(2, 100);
    state.peer_purge_progress.insert(3, 100);

    // First purge (last_purge_index = None)
    assert!(state.can_purge_logs(None, LogId { index: 99, term: 1 }));

    // Must still respect commit_index gap
    assert!(!state.can_purge_logs(
        None,
        LogId {
            index: 100,
            term: 1
        } // 100 not < 100
    ));
}

// ============================================================================
// Leader Discovery Tests
// ============================================================================

/// Test handling DiscoverLeader request successfully
///
/// # Test Scenario
/// Leader receives discovery request and returns its metadata.
///
/// # Given
/// - Leader with id=1, address="127.0.0.1:50051", term=1
/// - Mock membership returns leader metadata
///
/// # When
/// - DiscoverLeader event is handled
///
/// # Then
/// - Response contains leader_id=1
/// - Response contains correct address
/// - Response contains current term
#[tokio::test]
async fn test_handle_discover_leader_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_discover_leader_success",
        graceful_rx,
        None,
    );

    // Mock membership to return leader metadata
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_retrieve_node_meta().returning(|_| {
        Some(NodeMeta {
            id: 1,
            address: "127.0.0.1:50051".to_string(),
            role: Leader.into(),
            status: NodeStatus::Active.into(),
        })
    });
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();

    let request = LeaderDiscoveryRequest {
        node_id: 100,
        requester_address: "127.0.0.1:8080".to_string(),
    };
    let raft_event = RaftEvent::DiscoverLeader(request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should handle successfully");

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(response.leader_id, 1);
    assert_eq!(response.leader_address, "127.0.0.1:50051");
    assert_eq!(response.term, state.current_term());
}

/// Test handling DiscoverLeader when metadata not found
///
/// # Test Scenario
/// Leader cannot find its own metadata (should panic - indicates bug).
///
/// # Given
/// - Mock membership returns None for leader metadata
///
/// # When
/// - DiscoverLeader event is handled
///
/// # Then
/// - Should panic with message about bug
#[tokio::test]
#[should_panic(expected = "Leader can not find its address? It must be a bug.")]
async fn test_handle_discover_leader_metadata_not_found() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_discover_leader_metadata_not_found",
        graceful_rx,
        None,
    );

    // Mock membership to return no metadata
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_retrieve_node_meta().returning(|_| None);
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, _) = <MaybeCloneOneshot as RaftOneshot<_>>::new();

    let request = LeaderDiscoveryRequest {
        node_id: 100,
        requester_address: "127.0.0.1:8080".to_string(),
    };
    let raft_event = RaftEvent::DiscoverLeader(request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should panic during handling");
}

/// Test handling DiscoverLeader with different leader terms
///
/// # Test Scenario
/// Leader discovery returns current term (which may have changed).
///
/// # Given
/// - Leader with term=5 (updated from initial term)
///
/// # When
/// - DiscoverLeader event is handled
///
/// # Then
/// - Response term matches current term (5)
#[tokio::test]
#[traced_test]
async fn test_handle_discover_leader_different_terms() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_discover_leader_different_terms",
        graceful_rx,
        None,
    );

    // Mock membership to return leader metadata
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_retrieve_node_meta().returning(|_| {
        Some(NodeMeta {
            id: 1,
            address: "127.0.0.1:50051".to_string(),
            role: Leader.into(),
            status: NodeStatus::Active.into(),
        })
    });
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();

    // Set different terms
    state.update_current_term(5);
    let request = LeaderDiscoveryRequest {
        node_id: 100,
        requester_address: "127.0.0.1:8080".to_string(),
    };
    let raft_event = RaftEvent::DiscoverLeader(request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should handle successfully");

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(response.term, 5);
}

/// Test handling DiscoverLeader with invalid node ID
///
/// # Test Scenario
/// Leader handles discovery request with invalid node ID (0).
/// Should still return valid response.
///
/// # Given
/// - Discovery request with node_id=0 (invalid)
///
/// # When
/// - DiscoverLeader event is handled
///
/// # Then
/// - Response still contains valid leader_id
#[tokio::test]
#[traced_test]
async fn test_handle_discover_leader_invalid_node_id() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_discover_leader_invalid_node_id",
        graceful_rx,
        None,
    );

    // Mock membership to return leader metadata
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_retrieve_node_meta().returning(|_| {
        Some(NodeMeta {
            id: 1,
            address: "127.0.0.1:50051".to_string(),
            role: Leader.into(),
            status: NodeStatus::Active.into(),
        })
    });
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();

    // Use invalid node ID (0)
    let request = LeaderDiscoveryRequest {
        node_id: 0,
        requester_address: "127.0.0.1:8080".to_string(),
    };
    let raft_event = RaftEvent::DiscoverLeader(request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should handle successfully");

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(response.leader_id, 1);
}
