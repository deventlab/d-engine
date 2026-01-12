use std::sync::Arc;

use d_engine_proto::common::LogId;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::cluster::cluster_conf_update_response;
use d_engine_proto::server::election::VoteRequest;
use tonic::Code;

use crate::MaybeCloneOneshot;
use crate::MockMembership;
use crate::RaftEvent;
use crate::RaftOneshot;
use crate::raft_role::learner_state::LearnerState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use crate::test_utils::mock::mock_raft_context_with_temp;
use crate::test_utils::node_config;
use tokio::sync::{mpsc, watch};

/// Test: LearnerState rejects FlushReadBuffer event
///
/// Scenario: Learner node receives FlushReadBuffer event
/// Expected: Returns RoleViolation error (only Leader can handle this event)
#[tokio::test]
async fn test_learner_rejects_flush_read_buffer_event() {
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let ctx = mock_raft_context("/tmp/test_learner_flush", shutdown_rx, None);
    let mut state = LearnerState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Handle FlushReadBuffer event
    let result = state.handle_raft_event(RaftEvent::FlushReadBuffer, &ctx, role_tx.clone()).await;

    // Verify: Returns RoleViolation error
    assert!(
        result.is_err(),
        "Learner should reject FlushReadBuffer event"
    );

    if let Err(e) = result {
        let error_str = format!("{e:?}");
        assert!(
            error_str.contains("RoleViolation"),
            "Error should be RoleViolation, got: {error_str}"
        );
        assert!(
            error_str.contains("Learner"),
            "Error should mention Learner role"
        );
        assert!(
            error_str.contains("Leader"),
            "Error should mention Leader as required role"
        );
    }

    drop(shutdown_tx);
}

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
/// - error_code = None
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
                error_code: cluster_conf_update_response::ErrorCode::None.into(),
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
        cluster_conf_update_response::ErrorCode::None as i32
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
    let raft_event = RaftEvent::ClientPropose(
        d_engine_proto::client::ClientWriteRequest {
            client_id: 1,
            commands: vec![],
        },
        resp_tx,
    );
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(
        response.error,
        d_engine_proto::error::ErrorCode::NotLeader as i32
    );
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
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    let response = resp_rx.recv().await.unwrap().expect("should get response");
    assert_eq!(
        response.error,
        d_engine_proto::error::ErrorCode::NotLeader as i32
    );
}

/// Test: LearnerState rejects RaftLogCleanUp
///
/// Scenario:
/// - Learner receives RaftLogCleanUp request
/// - Only Follower/Leader can handle log cleanup
///
/// Expected:
/// - Returns Status error with Code::PermissionDenied
///
/// Original: test_handle_raft_event_case8
#[tokio::test]
async fn test_learner_rejects_raft_log_cleanup() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let request = d_engine_proto::server::storage::PurgeLogRequest {
        term: 1,
        leader_id: 1,
        leader_commit: 1,
        last_included: Some(LogId { term: 1, index: 1 }),
        snapshot_checksum: bytes::Bytes::from(vec![1, 2, 3]),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::RaftLogCleanUp(request, resp_tx);

    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    let response = resp_rx.recv().await.unwrap();
    assert!(response.is_err(), "Should return error");
    let err = response.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
}

/// Test: LearnerState rejects JoinCluster
///
/// Scenario:
/// - Learner receives JoinCluster request
/// - Only leader can accept new members
///
/// Expected:
/// - Returns Status error with Code::PermissionDenied
/// - handle_raft_event returns Err()
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

    let mut state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
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

    let mut state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
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

    let mut state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
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

    let mut state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
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
    use d_engine_proto::common::NodeRole;
    use d_engine_proto::common::NodeStatus;
    use d_engine_proto::server::cluster::NodeMeta;
    use mockall::predicate::eq;

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
    use d_engine_proto::common::NodeRole;
    use d_engine_proto::common::NodeStatus;
    use d_engine_proto::server::cluster::NodeMeta;
    use mockall::predicate::eq;

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
    use mockall::predicate::eq;

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

        // [Test CreateSnapshotEvent]
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::CreateSnapshotEvent;
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        assert!(
            matches!(
                e,
                crate::Error::Consensus(crate::ConsensusError::RoleViolation { .. })
            ),
            "CreateSnapshotEvent should return RoleViolation"
        );

        // [Test SnapshotCreated]
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::SnapshotCreated(Err(crate::Error::Fatal("test".to_string())));
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        assert!(
            matches!(
                e,
                crate::Error::Consensus(crate::ConsensusError::RoleViolation { .. })
            ),
            "SnapshotCreated should return RoleViolation"
        );

        // [Test LogPurgeCompleted]
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
}
