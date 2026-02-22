use crate::ClientCmd;
use crate::Error;
use crate::MaybeCloneOneshot;
use crate::MockBuilder;
use crate::MockMembership;
use crate::MockStateMachineHandler;
use crate::NewCommitData;
use crate::RaftEvent;
use crate::RaftOneshot;
use crate::RoleEvent;
use crate::raft_role::learner_state::LearnerState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use crate::test_utils::mock::mock_raft_context_with_temp;
use crate::test_utils::node_config;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::cluster::cluster_conf_update_response;
use d_engine_proto::server::election::VoteRequest;
use mockall::predicate::eq;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tonic::Code;

/// Test: LearnerState drain_read_buffer returns NotLeader error
///
/// Scenario: Call drain_read_buffer() on Learner
/// Expected: Returns NotLeader error (Learner doesn't buffer reads)
#[tokio::test]
async fn test_learner_drain_read_buffer_returns_error() {
    let mut state =
        LearnerState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_learner_drain")));

    // Action: Call drain_read_buffer()
    let result = state.drain_read_buffer();

    // Verify: Returns NotLeader error
    assert!(
        result.is_err(),
        "Learner drain_read_buffer should return error"
    );

    if let Err(e) = result {
        let error_str = format!("{e:?}");
        assert!(
            error_str.contains("NotLeader"),
            "Error should be NotLeader, got: {error_str}"
        );
    }
}

// ============================================================================
// Basic Operations Tests
// ============================================================================

/// Test: LearnerState tick behavior
///
/// Scenario:
/// - Learner receives tick event
/// - Learner doesn't participate in elections (no election timeout)
///
/// Expected:
/// - tick() returns Ok()
/// - No role change events
/// - Learner remains passive
///
/// This validates that learners don't initiate elections.
///
/// Original: test_tick
#[tokio::test]
async fn test_learner_tick_succeeds() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);

    // Action: Tick
    assert!(
        state.tick(&role_tx, &event_tx, &context).await.is_ok(),
        "Learner tick should succeed"
    );
}

/// Test: LearnerState rejects VoteRequest but updates term
///
/// Scenario:
/// - Learner (term=1) receives VoteRequest from candidate (term=11)
/// - Learners cannot vote (not voters)
/// - But must update term to latest
///
/// Expected:
/// - Returns response with vote_granted=false
/// - Updates current_term to request term (11)
/// - handle_raft_event returns Ok()
///
/// This validates Raft rule: all nodes update term when seeing higher term,
/// but learners never grant votes.
///
/// Original: test_handle_raft_event_case1
#[tokio::test]
async fn test_learner_rejects_vote_request_updates_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    let term_before = state.current_term();
    let request_term = term_before + 10;

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ReceiveVoteRequest(
        VoteRequest {
            term: request_term,
            candidate_id: 2,
            last_log_index: 11,
            last_log_term: 0,
        },
        resp_tx,
    );

    // Action: Handle VoteRequest
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: Term updated
    assert_eq!(
        state.current_term(),
        request_term,
        "Should update to request term"
    );

    // Verify: Vote rejected
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.vote_granted, "Learner should never grant votes");
}

/// Test: LearnerState rejects ClusterConf metadata request
///
/// Scenario:
/// - Learner receives ClusterConf (metadata request)
/// - Learners cannot serve cluster metadata (not full members)
///
/// Expected:
/// - Returns Status error with Code::PermissionDenied
/// - handle_raft_event returns Ok()
///
/// This validates that learners redirect metadata requests to leader.
///
/// Original: test_handle_raft_event_case2
#[tokio::test]
async fn test_learner_rejects_cluster_conf_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConf(MetadataRequest {}, resp_tx);

    // Action: Handle ClusterConf
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: PermissionDenied
    let status = resp_rx.recv().await.unwrap().unwrap_err();
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "Should return PermissionDenied"
    );
}

/// Test: LearnerState handles ClusterConfUpdate successfully
///
/// Scenario:
/// - Learner receives ClusterConfUpdate from leader
/// - Membership update succeeds
///
/// Expected:
/// - Returns response with success=true
/// - error_code = Unspecified
/// - handle_raft_event returns Ok()
///
/// This validates that learners can receive and apply cluster
/// configuration updates from leader.
///
/// Original: test_handle_raft_event_case3
#[tokio::test]
async fn test_learner_handles_cluster_conf_update_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Mock membership to accept update
    let mut membership = MockMembership::new();
    membership
        .expect_update_cluster_conf_from_leader()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(ClusterConfUpdateResponse {
                id: 1,
                term: 1,
                version: 1,
                success: true,
                error_code: cluster_conf_update_response::ErrorCode::Unspecified.into(),
            })
        });
    membership.expect_get_cluster_conf_version().returning(|| 1);
    context.membership = Arc::new(membership);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2, // Leader ID
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    // Action: Handle ClusterConfUpdate
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: Success response
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.success, "Update should succeed");
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::Unspecified as i32
    );
}

// ============================================================================
// AppendEntries Tests
// ============================================================================

/// Test: LearnerState successfully handles AppendEntries from leader
///
/// Scenario:
/// - Learner (term=1) receives AppendEntries from leader (term=2)
/// - replication_handler.handle_append_entries returns success
///
/// Expected:
/// - Sends LeaderDiscovered event
/// - Sends NotifyNewCommitIndex event
/// - Updates current_term to leader's term
/// - Updates commit_index
/// - Returns AppendEntriesResponse with success=true
///
/// Original: test_handle_raft_event_case4_1
#[tokio::test]
async fn test_learner_handles_append_entries_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let learner_term = 1;
    let leader_term = learner_term + 1;
    let expected_commit = 2;

    // Mock replication handler
    let mut replication_handler = crate::MockReplicationCore::new();
    replication_handler.expect_handle_append_entries().returning(move |_, _, _| {
        Ok(crate::AppendResponseWithUpdates {
            response: d_engine_proto::server::replication::AppendEntriesResponse::success(
                1,
                leader_term,
                Some(LogId {
                    term: leader_term,
                    index: 1,
                }),
            ),
            commit_index_update: Some(expected_commit),
        })
    });

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(learner_term);

    let append_request = d_engine_proto::server::replication::AppendEntriesRequest {
        term: leader_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_request, resp_tx);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: LeaderDiscovered event
    assert!(matches!(
        role_rx.try_recv().unwrap(),
        crate::RoleEvent::LeaderDiscovered(5, _)
    ));

    // Verify: NotifyNewCommitIndex event
    assert!(matches!(
        role_rx.try_recv().unwrap(),
        crate::RoleEvent::NotifyNewCommitIndex(_)
    ));

    assert_eq!(state.current_term(), leader_term);
    assert_eq!(state.commit_index(), expected_commit);

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.is_success());
}

/// Test: LearnerState rejects AppendEntries with stale term
///
/// Scenario:
/// - Learner (term=2) receives AppendEntries with stale term (term=1)
///
/// Expected:
/// - No events sent
/// - Term unchanged
/// - Returns AppendEntriesResponse with is_higher_term=true
///
/// Original: test_handle_raft_event_case4_2
#[tokio::test]
async fn test_learner_rejects_append_entries_stale_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let learner_term = 2;
    let stale_term = learner_term - 1;

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(learner_term);

    let append_request = d_engine_proto::server::replication::AppendEntriesRequest {
        term: stale_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_request, resp_tx);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    assert!(role_rx.try_recv().is_err(), "No events should be sent");
    assert_eq!(state.current_term(), learner_term);

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.is_higher_term());
}

/// Test: LearnerState handles AppendEntries failure
///
/// Scenario:
/// - Learner receives AppendEntries
/// - replication_handler returns error
///
/// Expected:
/// - Sends LeaderDiscovered event (leader is valid)
/// - Updates term
/// - Returns AppendEntriesResponse with success=false
/// - handle_raft_event returns Err()
///
/// Original: test_handle_raft_event_case4_3
#[tokio::test]
async fn test_learner_handles_append_entries_handler_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let learner_term = 1;
    let leader_term = learner_term + 1;

    let mut replication_handler = crate::MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .returning(|_, _, _| Err(crate::Error::Fatal("test".to_string())));

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(learner_term);

    let append_request = d_engine_proto::server::replication::AppendEntriesRequest {
        term: leader_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_request, resp_tx);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_err(),
        "handle_raft_event should return error"
    );

    assert!(matches!(
        role_rx.try_recv().unwrap(),
        crate::RoleEvent::LeaderDiscovered(5, _)
    ));
    assert!(role_rx.try_recv().is_err());

    assert_eq!(state.current_term(), leader_term);

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.is_success());
}

// ============================================================================
// Client Request Tests
// ============================================================================

/// Test: LearnerState rejects ClientWriteRequest
///
/// Scenario:
/// - Learner receives ClientWriteRequest
/// - Learners cannot process writes
///
/// Expected:
/// - Returns response with error_code = NOT_LEADER
///
/// Original: test_handle_raft_event_case5
#[tokio::test]
async fn test_learner_rejects_client_write_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Propose(
        d_engine_proto::client::ClientWriteRequest {
            client_id: 1,
            command: Some(WriteCommand::default()),
        },
        resp_tx,
    );

    // Non-leader: push_client_cmd will immediately reject
    state.push_client_cmd(cmd, &context);

    let result = resp_rx.recv().await.expect("channel should not be closed");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("Not leader"));
}

/// Test: LearnerState rejects ClientReadRequest
///
/// Scenario:
/// - Learner receives ClientReadRequest
/// - Learners cannot serve reads directly
///
/// Expected:
/// - Returns response with error_code = NOT_LEADER
///
/// Original: test_handle_raft_event_case6
#[tokio::test]
async fn test_learner_rejects_client_read_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let client_read_request = d_engine_proto::client::ClientReadRequest {
        client_id: 1,
        consistency_policy: None,
        keys: vec![],
    };

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    // Non-leader: push_client_cmd will immediately reject
    state.push_client_cmd(cmd, &context);

    let result = resp_rx.recv().await.expect("channel should not be closed");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("Not leader"));
}
///
/// Original: test_handle_raft_event_case10
#[tokio::test]
async fn test_learner_rejects_join_cluster() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let request = d_engine_proto::server::cluster::JoinRequest {
        status: d_engine_proto::common::NodeStatus::Promotable as i32,
        node_id: 2,
        node_role: d_engine_proto::common::NodeRole::Learner.into(),
        address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::JoinCluster(request, resp_tx);
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_err(),
        "handle_raft_event should return error"
    );

    let response = resp_rx.recv().await.expect("should receive response");
    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied);
}

/// Test: LearnerState rejects LeaderDiscovery
///
/// Scenario:
/// - Learner receives LeaderDiscoveryRequest
/// - Learner cannot serve discovery requests
///
/// Expected:
/// - Returns Status error with Code::PermissionDenied
/// - handle_raft_event returns Ok()
///
/// Original: test_handle_raft_event_case11
#[tokio::test]
async fn test_learner_rejects_leader_discovery() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let request = d_engine_proto::server::cluster::LeaderDiscoveryRequest {
        node_id: 2,
        requester_address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::DiscoverLeader(request, resp_tx);
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    let response = resp_rx.recv().await.expect("should receive response");
    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied);
}

// ============================================================================
// Discovery & Selection Tests
// ============================================================================

/// Test: LearnerState broadcast_discovery succeeds on first attempt
///
/// Scenario:
/// - Learner broadcasts discovery to find leader
/// - Transport returns valid LeaderDiscoveryResponse
///
/// Expected:
/// - Returns Ok with leader info
///
/// Original: test_broadcast_discovery_case1_success
#[tokio::test]
async fn test_broadcast_discovery_succeeds_first_attempt() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut transport = crate::MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| {
        Ok(vec![
            d_engine_proto::server::cluster::LeaderDiscoveryResponse {
                leader_id: 5,
                leader_address: "127.0.0.1:5005".to_string(),
                term: 3,
            },
        ])
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    let result = state.broadcast_discovery(context.membership.clone(), &context).await;

    assert!(result.is_ok(), "Should return leader info");
}

/// Test: LearnerState broadcast_discovery fails after retry exhaustion
///
/// Scenario:
/// - Learner broadcasts discovery
/// - Transport always returns empty responses
///
/// Expected:
/// - Returns NetworkError::RetryTimeoutError
///
/// Original: test_broadcast_discovery_case2_retry_exhaustion
#[tokio::test]
async fn test_broadcast_discovery_fails_after_retries() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut transport = crate::MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| Ok(vec![]));
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    let result = state.broadcast_discovery(context.membership.clone(), &context).await;

    assert!(result.is_err(), "Should error after retries");
    assert!(matches!(
        result.unwrap_err(),
        crate::Error::System(crate::SystemError::Network(
            crate::NetworkError::RetryTimeoutError(_)
        ))
    ));
}

/// Test: LearnerState select_valid_leader chooses highest term
///
/// Scenario:
/// - Multiple valid responses with different terms
/// - Leader with highest term should be selected
///
/// Expected:
/// - Returns leader_id with highest term
///
/// Original: test_select_valid_leader_case1_priority
#[tokio::test]
async fn test_select_valid_leader_prioritizes_highest_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let responses = vec![
        d_engine_proto::server::cluster::LeaderDiscoveryResponse {
            leader_id: 3,
            term: 5,
            leader_address: "127.0.0.1:5003".to_string(),
        },
        d_engine_proto::server::cluster::LeaderDiscoveryResponse {
            leader_id: 5,
            term: 7,
            leader_address: "127.0.0.1:5005".to_string(),
        }, // Highest term
        d_engine_proto::server::cluster::LeaderDiscoveryResponse {
            leader_id: 4,
            term: 7,
            leader_address: "127.0.0.1:5004".to_string(),
        }, // Same term
    ];

    let result = state.select_valid_leader(responses).await;

    assert!(result.is_some());
    assert_eq!(result.unwrap(), 5, "Should select highest term");
}

/// Test: LearnerState select_valid_leader filters invalid responses
///
/// Scenario:
/// - Responses with invalid leader_id (0) or invalid term (0)
///
/// Expected:
/// - Returns None (all responses filtered)
///
/// Original: test_select_valid_leader_case2_invalid_responses
#[tokio::test]
async fn test_select_valid_leader_filters_invalid_responses() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let responses = vec![
        d_engine_proto::server::cluster::LeaderDiscoveryResponse {
            leader_id: 0,
            term: 5,
            leader_address: "127.0.0.1:5003".to_string(),
        }, // Invalid ID
        d_engine_proto::server::cluster::LeaderDiscoveryResponse {
            leader_id: 3,
            term: 0,
            leader_address: "127.0.0.1:5003".to_string(),
        }, // Invalid term
    ];

    let result = state.select_valid_leader(responses).await;

    assert!(result.is_none(), "Should filter invalid responses");
}

// ============================================================================
// Join Cluster Tests
// ============================================================================

/// Test: LearnerState join_cluster succeeds with known leader
///
/// Scenario:
/// - Learner has known leader in shared_state
/// - Transport join_cluster succeeds
///
/// Expected:
/// - Returns Ok
///
/// Original: test_join_cluster_case1_success_known_leader
#[tokio::test]
async fn test_join_cluster_succeeds_with_known_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(d_engine_proto::server::cluster::JoinResponse {
            success: true,
            error: "".to_string(),
            config: None,
            config_version: 1,
            snapshot_metadata: None,
            leader_id: 3,
        })
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
    state.shared_state.set_current_leader(5);

    let result = state.join_cluster(&context).await;

    assert!(result.is_ok(), "Join should succeed with known leader");
}

/// Test: LearnerState join_cluster succeeds after discovery
///
/// Scenario:
/// - No known leader initially
/// - Discovery succeeds
/// - Join succeeds
///
/// Expected:
/// - Returns Ok
///
/// Original: test_join_cluster_case2_success_after_discovery
#[tokio::test]
async fn test_join_cluster_succeeds_after_discovery() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| {
        Ok(vec![
            d_engine_proto::server::cluster::LeaderDiscoveryResponse {
                leader_id: 5,
                leader_address: "127.0.0.1:5005".to_string(),
                term: 3,
            },
        ])
    });
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(d_engine_proto::server::cluster::JoinResponse {
            success: true,
            error: "".to_string(),
            config: None,
            config_version: 0,
            snapshot_metadata: None,
            leader_id: 2,
        })
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
    let result = state.join_cluster(&context).await;

    assert!(result.is_ok(), "Join should succeed after discovery");
}

/// Test: LearnerState join_cluster fails on discovery timeout
///
/// Scenario:
/// - No known leader
/// - Discovery returns empty responses (timeout)
///
/// Expected:
/// - Returns RetryTimeoutError
///
/// Original: test_join_cluster_case3_discovery_timeout
#[tokio::test]
async fn test_join_cluster_fails_discovery_timeout() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| Ok(vec![]));
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
    let result = state.join_cluster(&context).await;

    assert!(result.is_err(), "Should timeout during discovery");
    assert!(matches!(
        result.unwrap_err(),
        crate::Error::System(crate::SystemError::Network(
            crate::NetworkError::RetryTimeoutError(_)
        ))
    ));
}

/// Test: LearnerState join_cluster fails on RPC failure
///
/// Scenario:
/// - Known leader exists
/// - Join RPC fails with ServiceUnavailable
///
/// Expected:
/// - Returns ServiceUnavailable error
///
/// Original: test_join_cluster_case4_join_rpc_failure
#[tokio::test]
async fn test_join_cluster_fails_on_rpc_failure() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Err(crate::NetworkError::ServiceUnavailable("Service unavailable".to_string()).into())
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
    state.shared_state.set_current_leader(5);

    let result = state.join_cluster(&context).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        crate::Error::System(crate::SystemError::Network(
            crate::NetworkError::ServiceUnavailable(_)
        ))
    ));
}

/// Test: LearnerState join_cluster fails on invalid response
///
/// Scenario:
/// - Known leader exists
/// - Join response has success=false
///
/// Expected:
/// - Returns JoinClusterFailed error
///
/// Original: test_join_cluster_case5_invalid_join_response
#[tokio::test]
async fn test_join_cluster_fails_invalid_response() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(d_engine_proto::server::cluster::JoinResponse {
            success: false,
            error: "Node rejected".to_string(),
            config: None,
            config_version: 0,
            snapshot_metadata: None,
            leader_id: 0,
        })
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
    state.shared_state.set_current_leader(5);

    let result = state.join_cluster(&context).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        crate::Error::Consensus(crate::ConsensusError::Membership(
            crate::MembershipError::JoinClusterFailed(_)
        ))
    ));
}

/// Test: LearnerState join_cluster handles redirect
///
/// Scenario:
/// - Known leader exists
/// - First join attempt fails (ServiceUnavailable)
/// - Retry mechanism should handle redirect
///
/// Expected:
/// - Returns error (single attempt fails)
///
/// Original: test_join_cluster_case6_leader_redirect
#[tokio::test]
async fn test_join_cluster_handles_redirect() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Err(crate::NetworkError::ServiceUnavailable("Not leader".to_string()).into())
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
    state.shared_state.set_current_leader(5);

    let result = state.join_cluster(&context).await;

    assert!(result.is_err(), "Should handle redirect scenario");
}

/// Test: LearnerState join_cluster succeeds in large cluster
///
/// Scenario:
/// - Large cluster (simulated with 100 node_id)
/// - Discovery and join succeed
///
/// Expected:
/// - Returns Ok
///
/// Original: test_join_cluster_case7_large_cluster
#[tokio::test]
async fn test_join_cluster_succeeds_large_cluster() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| {
        Ok(vec![
            d_engine_proto::server::cluster::LeaderDiscoveryResponse {
                leader_id: 5,
                leader_address: "127.0.0.1:5005".to_string(),
                term: 3,
            },
        ])
    });
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(d_engine_proto::server::cluster::JoinResponse {
            success: true,
            error: "".to_string(),
            config: None,
            config_version: 1,
            snapshot_metadata: None,
            leader_id: 3,
        })
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());

    let result = state.join_cluster(&context).await;

    assert!(result.is_ok(), "Should handle large cluster");
}

// ============================================================================
// Membership Applied Tests
// ============================================================================

/// Test: LearnerState detects promotion to Follower on MembershipApplied
///
/// Scenario:
/// - Learner receives MembershipApplied event
/// - Membership shows node has been promoted to Follower role
///
/// Expected:
/// - Sends BecomeFollower event
/// - handle_raft_event returns Ok()
///
/// This validates learner promotion mechanism.
///
/// Original: test_learner_promotion_on_membership_applied
#[tokio::test]
async fn test_learner_promotion_on_membership_applied() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut mock_membership = MockMembership::<MockTypeConfig>::new();

    let promoted_node = NodeMeta {
        id: 3,
        address: "127.0.0.1:8003".to_string(),
        role: NodeRole::Follower as i32,
        status: NodeStatus::Active as i32,
    };

    mock_membership
        .expect_retrieve_node_meta()
        .with(eq(3))
        .times(1)
        .return_once(move |_| Some(promoted_node));

    context.membership = Arc::new(mock_membership);

    let mut state = LearnerState::<MockTypeConfig>::new(3, context.node_config.clone());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::MembershipApplied;

    let result = state.handle_raft_event(raft_event, &context, role_tx).await;
    assert!(result.is_ok(), "MembershipApplied should succeed");

    let role_event = tokio::time::timeout(std::time::Duration::from_millis(100), role_rx.recv())
        .await
        .expect("Should receive event within timeout")
        .expect("Channel should not be closed");

    match role_event {
        crate::RoleEvent::BecomeFollower(leader_id) => {
            assert_eq!(leader_id, None, "Should not specify leader on promotion");
        }
        other => panic!("Expected BecomeFollower event, got: {other:?}"),
    }
}

/// Test: LearnerState remains Learner on MembershipApplied
///
/// Scenario:
/// - Learner receives MembershipApplied event
/// - Membership still shows node as Learner
///
/// Expected:
/// - No role transition event
/// - handle_raft_event returns Ok()
///
/// Original: test_learner_stays_learner_on_membership_applied
#[tokio::test]
async fn test_learner_stays_learner_on_membership_applied() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut mock_membership = MockMembership::<MockTypeConfig>::new();

    let learner_node = NodeMeta {
        id: 3,
        address: "127.0.0.1:8003".to_string(),
        role: NodeRole::Learner as i32,
        status: NodeStatus::Promotable as i32,
    };

    mock_membership
        .expect_retrieve_node_meta()
        .with(eq(3))
        .times(1)
        .return_once(move |_| Some(learner_node));

    context.membership = Arc::new(mock_membership);

    let mut state = LearnerState::<MockTypeConfig>::new(3, context.node_config.clone());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::MembershipApplied;

    let result = state.handle_raft_event(raft_event, &context, role_tx).await;
    assert!(result.is_ok(), "MembershipApplied should succeed");

    let timeout_result =
        tokio::time::timeout(std::time::Duration::from_millis(100), role_rx.recv()).await;

    if let Ok(Some(event)) = timeout_result {
        panic!("Should not send role transition when still Learner, got: {event:?}");
    }
}

/// Test: LearnerState handles node not found in membership
///
/// Scenario:
/// - Learner receives MembershipApplied event
/// - Node not found in membership (edge case)
///
/// Expected:
/// - No role transition event
/// - handle_raft_event returns Ok()
///
/// Original: test_learner_node_not_found_on_membership_applied
#[tokio::test]
async fn test_learner_node_not_found_on_membership_applied() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut mock_membership = MockMembership::<MockTypeConfig>::new();

    mock_membership
        .expect_retrieve_node_meta()
        .with(eq(3))
        .times(1)
        .return_once(move |_| None);

    context.membership = Arc::new(mock_membership);

    let mut state = LearnerState::<MockTypeConfig>::new(3, context.node_config.clone());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::MembershipApplied;

    let result = state.handle_raft_event(raft_event, &context, role_tx).await;
    assert!(
        result.is_ok(),
        "MembershipApplied should succeed even when node not found"
    );

    let timeout_result =
        tokio::time::timeout(std::time::Duration::from_millis(100), role_rx.recv()).await;

    if let Ok(Some(event)) = timeout_result {
        panic!("Should not send role transition when node not found, got: {event:?}");
    }
}

// ============================================================================
// Role Violation Tests Module
// ============================================================================

mod role_violation_tests {
    use super::*;

    /// Test: LearnerState rejects leader-only events
    ///
    /// Scenario:
    /// - Learner receives events only Leader can handle:
    ///   - CreateSnapshotEvent
    ///   - SnapshotCreated
    ///   - LogPurgeCompleted
    ///
    /// Expected:
    /// - All return RoleViolation error
    ///
    /// Original: test_role_violation_events (in module)
    #[tokio::test]
    async fn test_learner_rejects_leader_only_events() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

        // [Test LogPurgeCompleted]
        // Learner now handles CreateSnapshotEvent and SnapshotCreated independently (Raft §7)
        // but should NOT receive LogPurgeCompleted from external sources
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::LogPurgeCompleted(LogId { term: 1, index: 1 });
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        assert!(
            matches!(
                e,
                crate::Error::Consensus(crate::ConsensusError::RoleViolation { .. })
            ),
            "LogPurgeCompleted should return RoleViolation"
        );
    }

    /// Test: Learner ignores duplicate CreateSnapshotEvent while snapshot is in progress
    ///
    /// Purpose:
    /// Validates that the snapshot_in_progress flag prevents concurrent snapshot creation,
    /// ensuring snapshot consistency and avoiding resource waste from duplicate operations.
    ///
    /// Scenario:
    /// - First CreateSnapshotEvent is received and sets snapshot_in_progress=true
    /// - Second CreateSnapshotEvent arrives before first completes
    ///
    /// Expected:
    /// - First event: Returns Ok(), starts async snapshot creation
    /// - Second event: Returns Ok() immediately without starting new snapshot (logged as skipped)
    /// - snapshot_in_progress flag protects against concurrent creation
    #[tokio::test]
    async fn test_learner_ignores_duplicate_create_snapshot_event() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

        // First CreateSnapshotEvent - should succeed
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let result1 = state
            .handle_raft_event(RaftEvent::CreateSnapshotEvent, &context, role_tx.clone())
            .await;
        assert!(result1.is_ok(), "First CreateSnapshotEvent should succeed");

        // Verify flag is set
        assert!(
            state.snapshot_in_progress.load(std::sync::atomic::Ordering::SeqCst),
            "snapshot_in_progress should be true after first event"
        );

        // Second CreateSnapshotEvent - should be ignored
        let result2 =
            state.handle_raft_event(RaftEvent::CreateSnapshotEvent, &context, role_tx).await;
        assert!(
            result2.is_ok(),
            "Second CreateSnapshotEvent should return Ok (ignored)"
        );

        // Flag should still be true (first snapshot still in progress)
        assert!(
            state.snapshot_in_progress.load(std::sync::atomic::Ordering::SeqCst),
            "snapshot_in_progress should remain true"
        );
    }

    /// Test: Learner resets snapshot_in_progress flag after SnapshotCreated (success case)
    ///
    /// Purpose:
    /// Validates that the snapshot_in_progress flag is correctly reset after snapshot completion,
    /// allowing subsequent snapshots to be created when needed.
    ///
    /// Scenario:
    /// - snapshot_in_progress is manually set to true (simulating ongoing snapshot)
    /// - SnapshotCreated event with successful result is received
    ///
    /// Expected:
    /// - Event handler returns Ok()
    /// - snapshot_in_progress flag is reset to false
    /// - System is ready to accept new CreateSnapshotEvent
    ///
    /// This ensures the flag lifecycle is: false → true (on create) → false (on complete)
    #[tokio::test]
    async fn test_learner_resets_snapshot_flag_on_success() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

        // Simulate snapshot in progress
        state.snapshot_in_progress.store(true, std::sync::atomic::Ordering::SeqCst);

        // Create successful snapshot result
        let metadata = d_engine_proto::server::storage::SnapshotMetadata {
            last_included: Some(LogId { term: 1, index: 50 }),
            checksum: bytes::Bytes::new(),
        };
        let snapshot_result = Ok((metadata, std::path::PathBuf::from("/tmp/test_snapshot.bin")));

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let result = state
            .handle_raft_event(
                RaftEvent::SnapshotCreated(snapshot_result),
                &context,
                role_tx,
            )
            .await;

        assert!(result.is_ok(), "SnapshotCreated should succeed");

        // Verify flag is reset
        assert!(
            !state.snapshot_in_progress.load(std::sync::atomic::Ordering::SeqCst),
            "snapshot_in_progress should be false after SnapshotCreated"
        );
    }

    /// Test: Learner resets snapshot_in_progress flag after SnapshotCreated (failure case)
    ///
    /// Purpose:
    /// Validates that the snapshot_in_progress flag is reset even when snapshot creation fails,
    /// allowing the system to retry snapshot creation later without being permanently blocked.
    ///
    /// Scenario:
    /// - snapshot_in_progress is set to true
    /// - SnapshotCreated event with error result is received
    ///
    /// Expected:
    /// - Event handler returns Ok() (error is logged but not propagated)
    /// - snapshot_in_progress flag is reset to false
    /// - System can retry snapshot creation on next ApplyCompleted trigger
    ///
    /// This ensures failure recovery: the flag doesn't stay locked after an error
    #[tokio::test]
    async fn test_learner_resets_snapshot_flag_on_failure() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

        // Simulate snapshot in progress
        state.snapshot_in_progress.store(true, std::sync::atomic::Ordering::SeqCst);

        // Create failed snapshot result
        let snapshot_result = Err(crate::Error::Fatal("Snapshot creation failed".to_string()));

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let result = state
            .handle_raft_event(
                RaftEvent::SnapshotCreated(snapshot_result),
                &context,
                role_tx,
            )
            .await;

        assert!(
            result.is_ok(),
            "SnapshotCreated with error should return Ok"
        );

        // Verify flag is reset even on failure
        assert!(
            !state.snapshot_in_progress.load(std::sync::atomic::Ordering::SeqCst),
            "snapshot_in_progress should be false after failed SnapshotCreated"
        );
    }
}

/// Test: Learner handles FatalError and returns error
///
/// Verifies that when Learner receives FatalError from any component,
/// it returns Error::Fatal and stops further processing.
///
/// # Test Scenario
/// Learner receives FatalError event from state machine while in learner role.
/// Learner should recognize the fatal error and return Error::Fatal.
///
/// # Given
/// - Learner in normal state (catching up on logs)
/// - FatalError event from StateMachine component
///
/// # When
/// - Learner handles FatalError event via handle_raft_event()
///
/// # Then
/// - handle_raft_event() returns Error::Fatal
/// - Error message contains source and error details
/// - No role transition events are sent (log replication is aborted)
#[tokio::test]
async fn test_learner_handles_fatal_error_returns_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context(
        "/tmp/test_learner_handles_fatal_error_returns_error",
        graceful_rx,
        None,
    );

    let mut learner = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Create FatalError event
    let fatal_error = RaftEvent::FatalError {
        source: "StateMachine".to_string(),
        error: "Disk failure".to_string(),
    };

    // Create role event channel
    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();

    // Handle the FatalError event
    let result = learner.handle_raft_event(fatal_error, &context, role_tx).await;

    // VERIFY 1: handle_raft_event() returns Error::Fatal
    assert!(
        result.is_err(),
        "Expected handle_raft_event to return Err, got: {result:?}"
    );

    // VERIFY 2: Error is Fatal and contains source information
    match result.unwrap_err() {
        Error::Fatal(msg) => {
            assert!(
                msg.contains("StateMachine"),
                "Error message should mention source, got: {msg}"
            );
        }
        other => panic!("Expected Error::Fatal, got: {other:?}"),
    }

    // VERIFY 3: No role events sent
    assert!(
        role_rx.try_recv().is_err(),
        "No role transition events should be sent during FatalError handling"
    );
}

// ============================================================================
// Role-Specific Behavior Tests
// ============================================================================

/// Learner - Eventual Read Support
///
/// **Objective**: Verify Learner can serve EventualConsistency reads locally
///
/// **Scenario**:
/// - Learner node receives EventualConsistency read request
/// - State machine returns valid data
///
/// **Expected**:
/// - Read processed immediately in push_client_cmd()
/// - No NOT_LEADER error
/// - Response latency < 10ms
/// - Data served from local state machine
#[tokio::test]
async fn test_learner_serves_eventual_read_locally() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Setup state machine mock for eventual read
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_read_from_state_machine()
        .times(1)
        .withf(|keys| keys.len() == 1 && keys[0] == "eventual_key")
        .returning(|_| {
            Some(vec![d_engine_proto::client::ClientResult {
                key: bytes::Bytes::from("eventual_key"),
                value: bytes::Bytes::from("eventual_value"),
            }])
        });
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Send EventualConsistency read request
    let (response_tx, mut response_rx) = MaybeCloneOneshot::new();
    let read_req = d_engine_proto::client::ClientReadRequest {
        client_id: 1,
        keys: vec![bytes::Bytes::from("eventual_key")],
        consistency_policy: Some(
            d_engine_proto::client::ReadConsistencyPolicy::EventualConsistency as i32,
        ),
    };

    let start = tokio::time::Instant::now();

    // Push read command (should process immediately)
    state.push_client_cmd(ClientCmd::Read(read_req, response_tx), &context);

    // Verify: Response ready immediately
    let result = response_rx.recv().await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Eventual read should return response");

    // Verify: Latency < 10ms
    assert!(
        elapsed.as_millis() < 10,
        "Eventual read latency should be <10ms, got {:?}ms",
        elapsed.as_millis()
    );

    // Verify: Response is success (not NOT_LEADER error)
    if let Ok(response) = result {
        match response {
            Ok(read_response) => {
                // Check success_result for ReadResults
                match read_response.success_result {
                    Some(d_engine_proto::client::client_response::SuccessResult::ReadData(
                        read_data,
                    )) => {
                        assert!(!read_data.results.is_empty(), "Should have read results");
                        assert_eq!(
                            read_data.results[0].value,
                            bytes::Bytes::from("eventual_value")
                        );
                    }
                    other => panic!("Expected ReadData variant, got: {other:?}"),
                }
            }
            Err(e) => {
                panic!("Eventual read should succeed on Learner, got error: {e:?}");
            }
        }
    }
}

/// Test: Learner ApplyCompleted triggers snapshot when condition is met
///
/// Purpose: Verify that learners independently create snapshots per Raft §7.
/// This ensures learner snapshot progress allows leader to advance its purge_safe_index
/// and prevent unbounded log growth.
///
/// Scenario:
/// - Learner receives ApplyCompleted event after state machine apply
/// - Snapshot is enabled in config
/// - State machine handler indicates snapshot should be taken
///
/// Expected:
/// - CreateSnapshotEvent is sent back to role event loop for processing
/// - Event is reprocessed as RoleEvent::ReprocessEvent
#[tokio::test]
async fn test_apply_completed_triggers_snapshot_when_condition_met() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Create a mock state machine handler that returns true for should_snapshot
    let mut mock_sm_handler = MockStateMachineHandler::new();
    mock_sm_handler
        .expect_should_snapshot()
        .with(eq(NewCommitData {
            new_commit_index: 100,
            role: NodeRole::Learner as i32,
            current_term: 1,
        }))
        .times(1)
        .returning(|_| true);

    // Build context with mock state machine handler before context creation
    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(mock_sm_handler)
        .build_context();

    let mut learner = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();

    // ACTION: Handle ApplyCompleted event
    let apply_completed_event = RaftEvent::ApplyCompleted {
        last_index: 100,
        results: vec![],
    };

    let result = learner.handle_raft_event(apply_completed_event, &context, role_tx).await;

    // VERIFY 1: Event handling succeeds
    assert!(
        result.is_ok(),
        "ApplyCompleted should be handled successfully, got: {result:?}"
    );

    // VERIFY 2: CreateSnapshotEvent is sent as RoleEvent::ReprocessEvent
    let event = role_rx.try_recv().expect("Should receive snapshot event");
    match event {
        RoleEvent::ReprocessEvent(boxed_event) => {
            match *boxed_event {
                RaftEvent::CreateSnapshotEvent => {
                    // Success! Event is correctly wrapped
                }
                other => panic!("Expected CreateSnapshotEvent, got: {other:?}"),
            }
        }
        other => panic!("Expected RoleEvent::ReprocessEvent, got: {other:?}"),
    }

    // VERIFY 3: No additional events queued
    assert!(
        role_rx.try_recv().is_err(),
        "Should only send one snapshot event"
    );
}

/// Test: Learner ApplyCompleted does NOT trigger snapshot when condition is not met
///
/// Purpose: Verify that learners respect snapshot conditions and don't create unnecessary snapshots.
///
/// Scenario:
/// - Learner receives ApplyCompleted event
/// - Snapshot is enabled in config
/// - State machine handler indicates snapshot should NOT be taken (returns false)
///
/// Expected:
/// - No CreateSnapshotEvent is sent
/// - ApplyCompleted is processed normally without side effects
#[tokio::test]
async fn test_apply_completed_does_not_trigger_snapshot_when_condition_not_met() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Create a mock state machine handler that returns false for should_snapshot
    let mut mock_sm_handler = MockStateMachineHandler::new();
    mock_sm_handler
        .expect_should_snapshot()
        .with(eq(NewCommitData {
            new_commit_index: 50,
            role: NodeRole::Learner as i32,
            current_term: 1,
        }))
        .times(1)
        .returning(|_| false);

    // Build context with mock state machine handler before context creation
    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(mock_sm_handler)
        .build_context();

    let mut learner = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();

    // ACTION: Handle ApplyCompleted event
    let apply_completed_event = RaftEvent::ApplyCompleted {
        last_index: 50,
        results: vec![],
    };

    let result = learner.handle_raft_event(apply_completed_event, &context, role_tx).await;

    // VERIFY 1: Event handling succeeds
    assert!(
        result.is_ok(),
        "ApplyCompleted should be handled successfully"
    );

    // VERIFY 2: No snapshot event is sent
    assert!(
        role_rx.try_recv().is_err(),
        "Should not send snapshot event when condition is not met"
    );
}

/// Test: Learner ApplyCompleted respects snapshot config disabled state
///
/// Purpose: Verify that snapshots are not triggered when snapshot feature is disabled.
///
/// Scenario:
/// - Learner receives ApplyCompleted event
/// - Snapshot is DISABLED in config (enable = false)
/// - State machine handler would indicate snapshot (returns true)
///
/// Expected:
/// - No CreateSnapshotEvent is sent (config takes precedence)
/// - ApplyCompleted is processed without attempting snapshot
#[tokio::test]
async fn test_apply_completed_respects_snapshot_disabled_config() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Create a mock state machine handler
    let mock_sm_handler = MockStateMachineHandler::new();

    // Build context with snapshot disabled and mock handler
    let mut node_config = node_config("/tmp/test_learner_snapshot_disabled");
    node_config.raft.snapshot.enable = false;

    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(mock_sm_handler)
        .with_node_config(node_config)
        .build_context();

    let mut learner = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();

    // ACTION: Handle ApplyCompleted event
    let apply_completed_event = RaftEvent::ApplyCompleted {
        last_index: 100,
        results: vec![],
    };

    let result = learner.handle_raft_event(apply_completed_event, &context, role_tx).await;

    // VERIFY 1: Event handling succeeds
    assert!(
        result.is_ok(),
        "ApplyCompleted should be handled successfully"
    );

    // VERIFY 2: No snapshot event is sent (snapshot disabled in config)
    assert!(
        role_rx.try_recv().is_err(),
        "Should not send snapshot event when snapshot is disabled in config"
    );
}
