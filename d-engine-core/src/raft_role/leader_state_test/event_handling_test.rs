//! Tests for leader state event handling
//!
//! This module tests the `handle_raft_event` method for various Raft events
//! including vote requests, append entries, client operations, and more.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tonic::Code;
use tracing_test::traced_test;

use crate::AppendResults;
use crate::ConsensusError;
use crate::Error;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::MockStateMachineHandler;
use crate::PeerUpdate;
use crate::ReplicationError;
use crate::config::RaftNodeConfig;
use crate::event::RaftEvent;
use crate::event::{NewCommitData, RoleEvent};
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_role::leader_state::LeaderState;
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::mock_raft_context;
use crate::test_utils::mock::{MockBuilder, MockTypeConfig};
use crate::test_utils::{create_test_chunk, create_test_snapshot_stream};
use crate::utils::convert::safe_kv_bytes;
use d_engine_proto::client::ReadConsistencyPolicy;
use d_engine_proto::client::{ClientReadRequest, ClientWriteRequest};
use d_engine_proto::server::cluster::{
    ClusterConfChangeRequest, ClusterMembership, MetadataRequest,
};
use d_engine_proto::server::election::{VoteRequest, VoteResponse};
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::storage::PurgeLogRequest;
use tonic::Status;

// ============================================================================
// Test Helper Functions
// ============================================================================

/// Create a mock membership for testing (default: multi-node cluster)
fn create_mock_membership() -> crate::MockMembership<MockTypeConfig> {
    let mut membership = crate::MockMembership::new();
    membership.expect_is_single_node_cluster().returning(|| false);
    membership
}

/// Helper to create VoteRequest event
fn setup_handle_raft_event_case1_params(
    resp_tx: crate::MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>,
    term: u64,
) -> RaftEvent {
    RaftEvent::ReceiveVoteRequest(
        VoteRequest {
            term,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        },
        resp_tx,
    )
}

// ============================================================================
// Vote Request Event Tests
// ============================================================================

/// Test handling VoteRequest when leader's term >= request term
///
/// # Test Scenario
/// Leader receives VoteRequest with term equal to or less than its own term.
/// The leader should reject the vote and remain as leader.
///
/// # Given
/// - Leader with current_term = T
/// - VoteRequest with term = T (same term)
///
/// # When
/// - Leader handles VoteRequest event
///
/// # Then
/// - VoteResponse returned with vote_granted = false
/// - No role transition event sent
/// - Leader's term remains unchanged
#[tokio::test]
#[traced_test]
async fn test_handle_vote_request_reject_same_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context(
        "/tmp/test_handle_vote_request_reject_same_term",
        graceful_rx,
        None,
    );

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let term_before = state.current_term();
    let request_term = term_before;

    // Prepare function params
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = setup_handle_raft_event_case1_params(resp_tx, request_term);

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    // Receive response with vote_granted = false
    assert!(!resp_rx.recv().await.unwrap().unwrap().vote_granted);

    // No role event receives
    assert!(role_rx.try_recv().is_err());

    // Term should not be updated
    assert_eq!(term_before, state.current_term());
}

/// Test handling VoteRequest when request term > leader's term
///
/// # Test Scenario
/// Leader receives VoteRequest with higher term, triggering step-down to follower.
///
/// # Given
/// - Leader with current_term = T
/// - VoteRequest with term = T + 1 (higher term)
///
/// # When
/// - Leader handles VoteRequest event
///
/// # Then
/// - Leader steps down to Follower (BecomeFollower event sent)
/// - Event is reprocessed (ReprocessEvent sent)
/// - Leader's term is updated to the higher term
/// - No response sent on original channel (will be handled after step-down)
#[tokio::test]
#[traced_test]
async fn test_handle_vote_request_step_down_on_higher_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context(
        "/tmp/test_handle_vote_request_step_down_on_higher_term",
        graceful_rx,
        None,
    );

    let updated_term = 100;

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare function params
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = setup_handle_raft_event_case1_params(resp_tx, updated_term);

    let r = state.handle_raft_event(raft_event, &context, role_tx).await;
    assert!(r.is_ok());

    // Step to Follower
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::BecomeFollower(_))
    ));
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::ReprocessEvent(_))
    ));

    // Term should be updated
    assert_eq!(state.current_term(), updated_term);

    // Make sure this assert is at the end of the test function.
    // Because we should wait handle_raft_event fun finish running after the role
    // events been consumed above.
    assert!(resp_rx.recv().await.is_err());
}

// ============================================================================
// Cluster Configuration Event Tests
// ============================================================================

/// Test handling ClusterConf (metadata) request
///
/// # Test Scenario
/// Leader receives request for cluster membership configuration.
///
/// # Given
/// - Leader with configured membership
/// - Mock membership returns cluster config
///
/// # When
/// - Leader handles ClusterConf event
///
/// # Then
/// - ClusterMembership response is sent successfully
/// - Membership data matches mock configuration
#[tokio::test]
#[traced_test]
async fn test_handle_cluster_conf_metadata_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_cluster_conf_metadata_request",
        graceful_rx,
        None,
    );
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_retrieve_cluster_membership_config().times(1).returning(
        |_current_leader_id| ClusterMembership {
            version: 1,
            nodes: vec![],
            current_leader_id: None,
        },
    );
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare function params
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConf(MetadataRequest {}, resp_tx);

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let m = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(m.nodes, vec![]);
}

/// Test rejecting ClusterConfUpdate with stale term
///
/// # Test Scenario
/// Leader receives ClusterConfChangeRequest with term lower than its own.
///
/// # Given
/// - Leader with current_term = 5
/// - ClusterConfChangeRequest with term = 3 (stale)
///
/// # When
/// - Leader handles ClusterConfUpdate event
///
/// # Then
/// - Response with success = false
/// - Response indicates higher term exists (is_higher_term = true)
/// - Response contains leader's current term
#[tokio::test]
#[traced_test]
async fn test_handle_cluster_conf_update_reject_stale_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_cluster_conf_update_reject_stale_term",
        graceful_rx,
        None,
    );
    // Mock membership to return success
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_get_cluster_conf_version().returning(|| 1);
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(5);

    let request = ClusterConfChangeRequest {
        id: 2,
        term: 3, // Lower than leader's term (5)
        version: 1,
        change: None,
    };

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let raft_event = RaftEvent::ClusterConfUpdate(request, resp_tx);

    state
        .handle_raft_event(raft_event, &context, mpsc::unbounded_channel().0)
        .await
        .unwrap();

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);
    assert!(response.is_higher_term());
    assert_eq!(response.term, 5);
}

/// Test stepping down on ClusterConfUpdate with higher term
///
/// # Test Scenario
/// Leader receives ClusterConfChangeRequest with higher term, triggering step-down.
///
/// # Given
/// - Leader with current_term = 3
/// - ClusterConfChangeRequest with term = 5 (higher)
///
/// # When
/// - Leader handles ClusterConfUpdate event
///
/// # Then
/// - Leader steps down to Follower (BecomeFollower event with node_id)
/// - Event is reprocessed (ReprocessEvent sent)
/// - No response sent on original channel
#[tokio::test]
#[traced_test]
async fn test_handle_cluster_conf_update_step_down_on_higher_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_cluster_conf_update_step_down_on_higher_term",
        graceful_rx,
        None,
    );
    // Mock membership to return success
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_get_cluster_conf_version().returning(|| 1);
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(3);

    let request = ClusterConfChangeRequest {
        id: 2,
        term: 5, // Higher than leader's term (3)
        version: 1,
        change: None,
    };

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConfUpdate(request, resp_tx);

    state.handle_raft_event(raft_event, &context, role_tx).await.unwrap();

    // Verify step down to follower
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::BecomeFollower(Some(2)))
    ));
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::ReprocessEvent(_))
    ));
    assert!(resp_rx.recv().await.is_err()); // Original sender should not get response
}

// ============================================================================
// AppendEntries Event Tests
// ============================================================================

/// Test rejecting AppendEntries when leader's term >= request term
///
/// # Test Scenario
/// Leader receives AppendEntries from another node with same or lower term.
/// This should be rejected as invalid (two leaders in same term).
///
/// # Given
/// - Leader with current_term = T
/// - AppendEntries request with term = T
///
/// # When
/// - Leader handles AppendEntries event
///
/// # Then
/// - Response with success = false and is_higher_term = true
/// - Leader remains in leader role
#[tokio::test]
#[traced_test]
async fn test_handle_append_entries_reject_same_term() {
    // Prepare Leader State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context(
        "/tmp/test_handle_append_entries_reject_same_term",
        graceful_rx,
        None,
    );
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Update my term higher than request one
    let my_term = 10;
    let request_term = my_term;
    state.update_current_term(my_term);

    // Prepare request
    let append_entries_request = AppendEntriesRequest {
        term: request_term,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let raft_event = RaftEvent::AppendEntries(append_entries_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Execute fun
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("should succeed");

    // Validate request should receive AppendEntriesResponse with success = false
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.is_higher_term());
}

/// Test stepping down on AppendEntries with higher term
///
/// # Test Scenario
/// Leader receives AppendEntries from another node with higher term.
/// This indicates a new leader has been elected, so step down to follower.
///
/// # Given
/// - Leader with current_term = T
/// - AppendEntries request with term = T + 1 from new leader
///
/// # When
/// - Leader handles AppendEntries event
///
/// # Then
/// - Leader steps down to Follower (BecomeFollower event sent)
/// - Event is reprocessed to handle as follower
/// - Leader's term is updated to request term
/// - No response sent on original channel
#[tokio::test]
#[traced_test]
async fn test_handle_append_entries_step_down_on_higher_term() {
    // Prepare Leader State

    // Prepare leader term smaller than request one
    let my_term = 10;
    let request_term = my_term + 1;

    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_append_entries_step_down_on_higher_term")
        .build_context();
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Update my term higher than request one
    state.update_current_term(my_term);

    // Prepare request
    let new_leader_id = 7;
    let append_entries_request = AppendEntriesRequest {
        term: request_term,
        leader_id: new_leader_id,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let raft_event = RaftEvent::AppendEntries(append_entries_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Test fun
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    // Validate criterias: step down as Follower
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::BecomeFollower(_))
    ));
    assert!(matches!(
        role_rx.try_recv().unwrap(),
        RoleEvent::ReprocessEvent(_)
    ));

    // Validate no response received
    assert!(resp_rx.recv().await.is_err());
}

// ============================================================================
// Client Request Event Tests
// ============================================================================

/// Test handling ClientPropose (write) request successfully
///
/// # Test Scenario
/// Leader receives client write request and processes it successfully.
///
/// # Given
/// - Leader state ready to process requests
/// - Mock replication handler returns successful quorum
///
/// # When
/// - Leader handles ClientPropose event
///
/// # Then
/// - Request is processed successfully (Ok result)
/// - Replication handler is called once
#[tokio::test]
#[traced_test]
async fn test_handle_client_propose_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        mock_raft_context("/tmp/test_handle_client_propose_success", graceful_rx, None);
    // Setup replication handler to return success
    context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([(2, PeerUpdate::success(5, 6))]),
            })
        });
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
    context.storage.raft_log = Arc::new(raft_log);

    // New state
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Handle raft event
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, _resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let raft_event = RaftEvent::ClientPropose(
        ClientWriteRequest {
            client_id: 1,
            commands: vec![],
        },
        resp_tx,
    );

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());
}

/// Test handling ClientReadRequest with linearizable read failure
///
/// # Test Scenario
/// Leader receives linearizable read request but replication fails.
///
/// # Given
/// - Leader with read batching threshold = 1 (immediate flush)
/// - Mock replication handler returns Fatal error
///
/// # When
/// - Leader handles ClientReadRequest with LinearizableRead policy
///
/// # Then
/// - Event handling succeeds (error sent to client via channel)
/// - Client receives error response with FailedPrecondition code
#[tokio::test]
#[traced_test]
async fn test_handle_client_read_linearizable_failure() {
    // Prepare Leader State
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::Fatal("".to_string())));

    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.read_batching.size_threshold = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_client_read_linearizable_failure")
        .with_replication_handler(replication_handler)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare request
    let keys = vec![safe_kv_bytes(1)];

    let client_read_request = ClientReadRequest {
        client_id: 1,
        keys,
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
    };
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    // Event handling should succeed (error is sent to client via resp_rx)
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Event should be handled successfully");

    // Client receives error via response channel
    let e = resp_rx.recv().await.unwrap().unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition);
}

/// Test handling ClientReadRequest with linearizable read success
///
/// # Test Scenario
/// Leader receives linearizable read request and successfully confirms with quorum.
///
/// # Given
/// - Leader with commit_index = 1
/// - Mock replication handler returns successful quorum with new commit index = 3
/// - Mock state machine handler ready to serve reads
///
/// # When
/// - Leader handles ClientReadRequest with LinearizableRead policy
///
/// # Then
/// - Leader's commit_index is updated to 3
/// - NotifyNewCommitIndex event is sent
/// - Client receives successful read response
#[tokio::test]
#[traced_test]
async fn test_handle_client_read_linearizable_success() {
    let expect_new_commit_index = 3;
    // Prepare Leader State
    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_raft_request_in_batch().times(1).returning(
        |_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: Some(3),
                            next_index: 4,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: Some(4),
                            next_index: 5,
                            success: true,
                        },
                    ),
                ]),
                learner_progress: HashMap::new(),
            })
        },
    );

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(move |_, _, _| Some(expect_new_commit_index));

    // Mock state machine handler for linearizable read
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_update_pending().times(1).returning(|_| ());
    state_machine_handler.expect_wait_applied().times(1).returning(|_, _| Ok(()));
    state_machine_handler
        .expect_read_from_state_machine()
        .returning(|_| Some(vec![]));

    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.read_batching.size_threshold = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_client_read_linearizable_success")
        .with_raft_log(raft_log)
        .with_replication_handler(replication_handler)
        .with_state_machine_handler(state_machine_handler)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare request
    let keys = vec![safe_kv_bytes(1)];
    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        keys,
    };
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("should succeed");

    // Validation criteria 1: Leader commit should be updated to: 3(new commit index)
    assert_eq!(state.shared_state().commit_index, expect_new_commit_index);

    // Validation criteria 2: resp_rx receives Ok()
    assert!(resp_rx.recv().await.unwrap().is_ok());

    // Validation criteria 3: event "RoleEvent::NotifyNewCommitIndex" should be received
    let event = role_rx.try_recv().unwrap();
    assert!(matches!(
        event,
        RoleEvent::NotifyNewCommitIndex(NewCommitData {
            new_commit_index: _expect_new_commit_index,
            role: _,
            current_term: _
        })
    ));
}

/// Test handling ClientReadRequest encountering higher term during replication
///
/// # Test Scenario
/// Leader receives linearizable read request but discovers higher term during replication.
/// This triggers step-down to follower.
///
/// # Given
/// - Leader with commit_index = 1
/// - Mock replication handler returns HigherTerm error
///
/// # When
/// - Leader handles ClientReadRequest with LinearizableRead policy
///
/// # Then
/// - Leader's commit_index remains at 1 (operation aborted)
/// - BecomeFollower event is sent
/// - Client receives error response
#[tokio::test]
#[traced_test]
async fn test_handle_client_read_encounters_higher_term() {
    // Prepare Leader State
    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_raft_request_in_batch().times(1).returning(
        move |_, _, _, _, _| {
            Err(Error::Consensus(ConsensusError::Replication(
                ReplicationError::HigherTerm(1),
            )))
        },
    );

    let expect_new_commit_index = 3;
    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(move |_, _, _| Some(expect_new_commit_index));

    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.read_batching.size_threshold = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_client_read_encounters_higher_term")
        .with_replication_handler(replication_handler)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_commit_index(1).expect("should succeed");

    // Prepare request
    let keys = vec![safe_kv_bytes(1)];
    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        keys,
    };
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    // Event handling should succeed (HigherTerm is handled, client gets error via resp_rx)
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Event should be handled successfully");

    // Validation criteria 1: Leader commit should remain unchanged (HigherTerm aborted the operation)
    assert_eq!(state.shared_state().commit_index, 1);

    // Validation criteria 2: event "RoleEvent::BecomeFollower" should be received
    let event = role_rx.try_recv().unwrap();
    assert!(matches!(event, RoleEvent::BecomeFollower(None)));

    // Validation criteria 3: Client receives error via response channel
    assert!(resp_rx.recv().await.unwrap().is_err());
}

// ============================================================================
// Snapshot Event Tests
// ============================================================================

/// Test rejecting InstallSnapshotChunk event (leader should not receive this)
///
/// # Test Scenario
/// Leader receives InstallSnapshotChunk event, which is invalid for leader role.
///
/// # Given
/// - Leader state
/// - InstallSnapshotChunk event with test snapshot stream
///
/// # When
/// - Leader handles InstallSnapshotChunk event
///
/// # Then
/// - Event handling returns error
/// - Client receives PermissionDenied error
/// - No role transition events sent
#[tokio::test]
#[traced_test]
async fn test_handle_install_snapshot_returns_permission_denied() {
    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_install_snapshot_returns_permission_denied")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();

    let stream = create_test_snapshot_stream(vec![create_test_chunk(0, b"chunk0", 1, 1, 2)]);
    let raft_event = RaftEvent::InstallSnapshotChunk(Box::new(stream), resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_err());

    // Validation criteria 1: The response should return an error
    // Assert that resp_rx receives permission_denied
    let e = resp_rx.recv().await.unwrap().unwrap_err();
    assert!(matches!(e.code(), Code::PermissionDenied));

    // Validation criteria 2: No role event should be triggered
    assert!(role_rx.try_recv().is_err());
}

// ============================================================================
// Log Cleanup Event Tests
// ============================================================================

/// Test rejecting RaftLogCleanUp event (leader should not receive this)
///
/// # Test Scenario
/// Leader receives RaftLogCleanUp event, which is invalid for leader role.
///
/// # Given
/// - Leader state
/// - RaftLogCleanUp (PurgeLogRequest) event
///
/// # When
/// - Leader handles RaftLogCleanUp event
///
/// # Then
/// - Event handling returns error
/// - Client receives PermissionDenied error
/// - No role transition events sent
#[tokio::test]
#[traced_test]
async fn test_handle_raft_log_cleanup_returns_permission_denied() {
    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_raft_log_cleanup_returns_permission_denied")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();

    let raft_event = RaftEvent::RaftLogCleanUp(
        PurgeLogRequest {
            term: 1,
            leader_id: 1,
            leader_commit: 1,
            last_included: None,
            snapshot_checksum: Bytes::new(),
        },
        resp_tx,
    );

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_err());

    // Validation criteria 1: The response should return an error
    let e = resp_rx.recv().await.unwrap().unwrap_err();
    assert!(matches!(e.code(), Code::PermissionDenied));

    // Validation criteria 2: No role event should be triggered
    assert!(role_rx.try_recv().is_err());
}
