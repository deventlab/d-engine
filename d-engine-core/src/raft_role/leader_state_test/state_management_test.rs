//! Tests for leader state management utilities and helper functions
//!
//! This module tests various state management functions including:
//! - Leader discovery event handling
//! - State size tracking

use crate::candidate_state::CandidateState;
use crate::event::InboundEvent;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::node_config;
use crate::raft_role::leader_state::LeaderState;
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use d_engine_proto::common::LogId;
use d_engine_proto::common::{NodeRole::Leader, NodeStatus};
use d_engine_proto::server::cluster::{LeaderDiscoveryRequest, NodeMeta};
use std::mem::size_of;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing_test::traced_test;

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
    let inbound_event = InboundEvent::DiscoverLeader(request, resp_tx);

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    state
        .handle_inbound_event(inbound_event, &context, internal_event_tx)
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
    let inbound_event = InboundEvent::DiscoverLeader(request, resp_tx);

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    state
        .handle_inbound_event(inbound_event, &context, internal_event_tx)
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
    state.shared_state_mut().update_current_term(5);
    let request = LeaderDiscoveryRequest {
        node_id: 100,
        requester_address: "127.0.0.1:8080".to_string(),
    };
    let inbound_event = InboundEvent::DiscoverLeader(request, resp_tx);

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    state
        .handle_inbound_event(inbound_event, &context, internal_event_tx)
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
    let inbound_event = InboundEvent::DiscoverLeader(request, resp_tx);

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    state
        .handle_inbound_event(inbound_event, &context, internal_event_tx)
        .await
        .expect("Should handle successfully");

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(response.leader_id, 1);
}

// ============================================================================
// Role Transition Tests — scheduled_purge_upto / last_purged_index
// ============================================================================

/// From<&CandidateState>: last_purged_index preserved, scheduled_purge_upto always None.
///
/// Leaders start each term with a clean purge schedule to avoid applying a stale purge
/// boundary computed in a prior term under a different last_applied index.
#[test]
fn test_leader_from_candidate_preserves_last_purged_index_resets_scheduled() {
    let cfg = Arc::new(node_config("/tmp/test_leader_from_candidate_purge"));
    let mut candidate = CandidateState::<MockTypeConfig>::new(1, cfg);
    candidate.last_purged_index = Some(LogId { term: 2, index: 8 });

    let leader = LeaderState::from(&candidate);

    assert_eq!(leader.last_purged_index, Some(LogId { term: 2, index: 8 }));
    assert_eq!(leader.scheduled_purge_upto, None);
}
