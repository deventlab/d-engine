use bytes::Bytes;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tonic::Code;
use tracing::debug;
use tracing_test::traced_test;

use d_engine_core::AppendResponseWithUpdates;
use d_engine_core::ConsensusError;
use d_engine_core::Error;
use d_engine_core::MaybeCloneOneshot;
use d_engine_core::MembershipError;
use d_engine_core::MockMembership;
use d_engine_core::MockReplicationCore;
use d_engine_core::MockTransport;
use d_engine_core::MockTypeConfig;
use d_engine_core::NetworkError;
use d_engine_core::NewCommitData;
use d_engine_core::RaftContext;
use d_engine_core::RaftEvent;
use d_engine_core::RaftOneshot;
use d_engine_core::RoleEvent;
use d_engine_core::SystemError;
use d_engine_core::learner_state::LearnerState;
use d_engine_core::role_state::RaftRoleState;
use d_engine_core::test_utils::mock_membership;
use d_engine_core::test_utils::mock_raft_context;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Learner;
use d_engine_proto::error::ErrorCode;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::JoinRequest;
use d_engine_proto::server::cluster::JoinResponse;
use d_engine_proto::server::cluster::LeaderDiscoveryRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryResponse;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::cluster::cluster_conf_update_response;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::storage::PurgeLogRequest;

/// Validate Follower step up as Candidate in new election round
#[tokio::test]
#[traced_test]
async fn test_tick() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_tick", graceful_rx, None);

    // New state
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);

    assert!(state.tick(&role_tx, &event_tx, &context).await.is_ok());
}

/// # Case 1: Receive Vote Request Event with term is higher than mine
///
/// ## Validate criterias
/// 1. Update to request term
/// 2. receive reponse from Learner with vote_granted = false
/// 3. handle_raft_event returns Ok()
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case1", graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let requet_term = state.current_term() + 10;

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ReceiveVoteRequest(
        VoteRequest {
            term: requet_term,
            candidate_id: 1,
            last_log_index: 11,
            last_log_term: 0,
        },
        resp_tx,
    );

    // handle_raft_event returns Ok()
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    // Update to request term
    assert_eq!(state.current_term(), requet_term);

    // Receive response with vote_granted = false
    assert!(!resp_rx.recv().await.unwrap().unwrap().vote_granted);
}

/// # Case 2: Receive ClusterConf Event
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case2", graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConf(MetadataRequest {}, resp_tx);

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let s = resp_rx.recv().await.unwrap().unwrap_err();
    assert_eq!(s.code(), Code::PermissionDenied);
}

/// # Case3: Successful configuration update
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case3", graceful_rx, None);

    // Mock membership to return success
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

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = d_engine_core::RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2, // Leader ID
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.success);
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::None as i32
    );
}

/// # Case 4.1: As learner, if I receive append request from Leader,
///     and replication_handler::handle_append_entries successfully
///
/// ## Prepration Setup
/// 1. receive Leader append request, with higher term and new commit index
///
/// ## Validation criterias:
/// 1. I should mark new leader id in memberhip
/// 2. I should not receive BecomeFollower event
/// 3. I should update term
/// 4. I should send out new commit signal
/// 5. send out AppendEntriesResponse with success=true
/// 6. `handle_raft_event` fun returns Ok(())
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case4_1() {
    // Prepare Follower State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case4_1", graceful_rx, None);
    let term = 1;
    let new_leader_term = term + 1;
    let expect_new_commit = 2;

    // Mock replication handler
    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_append_entries().returning(move |_, _, _| {
        Ok(AppendResponseWithUpdates {
            response: AppendEntriesResponse::success(
                1,
                new_leader_term,
                Some(LogId {
                    term: new_leader_term,
                    index: 1,
                }),
            ),
            commit_index_update: Some(expect_new_commit),
        })
    });

    let membership = MockMembership::new();

    // Validation criterias
    // 1. I should mark new leader id in memberhip
    // Removed: expect_mark_leader_id() - no longer needed with atomic leader_id

    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    // New state
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(term);

    // Prepare Append entries request
    let append_entries_request = AppendEntriesRequest {
        term: new_leader_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_entries_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Validation criterias: 6. `handle_raft_event` fun returns Ok(())
    // Handle raft event
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    // Validation criterias
    // 2. I should receive LeaderDiscovered event (new leader detected)
    assert!(matches!(
        role_rx.try_recv().unwrap(),
        RoleEvent::LeaderDiscovered(5, _)
    ));

    // 3. I should send out new commit signal
    assert!(matches!(
        role_rx.try_recv().unwrap(),
        RoleEvent::NotifyNewCommitIndex(NewCommitData {
            new_commit_index: _,
            role: _,
            current_term: _
        })
    ));

    // Validation criterias
    // 3. I should update term
    assert_eq!(state.current_term(), new_leader_term);
    assert_eq!(state.commit_index(), expect_new_commit);

    // 5. send out AppendEntriesResponse with success=true
    assert!(resp_rx.recv().await.unwrap().unwrap().is_success());
}

/// # Case 4.2: As learner, if I receive append request from Leader,
///     and request term is lower or equal than mine
///
/// ## Validation criterias:
/// 1. I should not mark new leader id in memberhip
/// 2. I should not receive any event
/// 3. My term shoud not be updated
/// 4. send out AppendEntriesResponse with success=false
/// 5. `handle_raft_event` fun returns Ok(())
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case4_2() {
    // Prepare Follower State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case4_2", graceful_rx, None);
    let term = 2;
    let new_leader_term = term - 1;

    let membership = MockMembership::new();

    // Validation criterias
    // 1. I should mark new leader id in memberhip
    // Removed: expect_mark_leader_id() - no longer needed with atomic leader_id

    context.membership = Arc::new(membership);

    // New state
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(term);

    // Prepare Append entries request
    let append_entries_request = AppendEntriesRequest {
        term: new_leader_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_entries_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Validation criterias: 6. `handle_raft_event` fun returns Ok(())
    // Handle raft event
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    // Validation criterias
    // 2. I should not receive any event
    assert!(role_rx.try_recv().is_err());

    // Validation criterias
    // 3. My term shoud not be updated
    assert_eq!(state.current_term(), term);

    // 5. send out AppendEntriesResponse with success=true
    assert!(resp_rx.recv().await.expect("should succeed").unwrap().is_higher_term());
}
/// # Case 4.3: As learner, if I receive append request from Leader,
///     and replication_handler::handle_append_entries failed with Error
///
/// ## Prepration Setup
/// 1. receive Leader append request, with Error
///
/// ## Validation criterias:
/// 1. I should mark new leader id in memberhip
/// 2. I should receive LeaderDiscovered event even when append fails
/// 3. My term shoud be updated
/// 4. send out AppendEntriesResponse with success=false
/// 5. `handle_raft_event` fun returns Err(())
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case4_3() {
    // Prepare Follower State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case4_3", graceful_rx, None);
    let term = 1;
    let new_leader_term = term + 1;

    // Mock replication handler
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .returning(|_, _, _| Err(Error::Fatal("test".to_string())));

    let membership = MockMembership::new();

    // Validation criterias
    // 1. I should mark new leader id in memberhip
    // Removed: expect_mark_leader_id() - no longer needed with atomic leader_id

    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    // New state
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(term);

    // Prepare Append entries request
    let append_entries_request = AppendEntriesRequest {
        term: new_leader_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_entries_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Validation criterias: 6. `handle_raft_event` fun returns Ok(())
    // Handle raft event
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_err());

    // Validation criterias
    // 2. I should receive LeaderDiscovered event even when append fails
    assert!(matches!(
        role_rx.try_recv().unwrap(),
        RoleEvent::LeaderDiscovered(5, _)
    ));

    // No other events should be sent
    assert!(role_rx.try_recv().is_err());

    // Validation criterias
    // 3. My term shoud be updated
    assert_eq!(state.current_term(), new_leader_term);

    // 5. send out AppendEntriesResponse with success=true
    assert!(!resp_rx.recv().await.unwrap().unwrap().is_success());
}

/// # Case 5: Test handle client propose request
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case5() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case5", graceful_rx, None);

    // New state
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Handle raft event
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::ClientPropose(
        ClientWriteRequest {
            client_id: 1,
            commands: vec![],
        },
        resp_tx,
    );

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    assert_eq!(
        resp_rx.recv().await.unwrap().unwrap().error,
        ErrorCode::NotLeader as i32
    );
}

/// # Case 6: test ClientReadRequest with linear request
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case6() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case6", graceful_rx, None);

    // New state
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: None,
        keys: vec![],
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    assert_eq!(
        resp_rx.recv().await.unwrap().unwrap_err().code(),
        Code::PermissionDenied
    );
}

/// Test handling RaftLogCleanUp event by LearnerState
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case8() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    // Step 1: Setup the test environment
    let context = mock_raft_context("/tmp/test_handle_raft_event_case8", graceful_rx, None);
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Step 2: Prepare the RaftLogCleanUp event
    let request = PurgeLogRequest {
        term: 1,
        leader_id: 1,
        leader_commit: 1,
        last_included: Some(LogId { term: 1, index: 1 }),
        snapshot_checksum: Bytes::from(vec![1, 2, 3]),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::RaftLogCleanUp(request, resp_tx);

    // Step 3: Call handle_raft_event
    let result = state.handle_raft_event(raft_event, &context, role_tx).await;

    // Step 4: Verify the response
    // Should return Ok since we're just sending a response
    assert!(result.is_ok(), "Expected handle_raft_event to return Ok");

    // Step 5: Check the response sent through the channel
    let response = resp_rx.recv().await;

    assert!(response.is_ok(), "Expected response to be sent");
    let status = response.unwrap();
    assert!(status.is_err(), "Expected an error response");
    let err = status.unwrap_err();

    // Step 6: Verify error details
    assert_eq!(err.code(), Code::PermissionDenied);
    assert_eq!(err.message(), "Not Follower");
}

/// Test handling JoinCluster event by CandidateState
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case10() {
    // Step 1: Setup the test environment
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case10", graceful_rx, None);
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Step 2: Prepare the event
    let request = JoinRequest {
        node_id: 2,
        node_role: Learner.into(),
        address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::JoinCluster(request, resp_tx);

    // Step 3: Call handle_raft_event
    let result = state.handle_raft_event(raft_event, &context, mpsc::unbounded_channel().0).await;

    // Step 4: Verify the response
    assert!(
        result.is_err(),
        "Expected handle_raft_event to return error"
    );

    // Step 5: Check the response sent through the channel
    let response = resp_rx.recv().await.expect("Response should be received");
    assert!(response.is_err(), "Expected an error response");
    let status = response.unwrap_err();

    // Step 6: Verify error details
    assert_eq!(status.code(), Code::PermissionDenied);
}

/// Test handling DiscoverLeader event by CandidateState
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case11() {
    // Step 1: Setup the test environment
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case11", graceful_rx, None);
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Step 2: Prepare the event
    let request = LeaderDiscoveryRequest {
        node_id: 2,
        requester_address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::DiscoverLeader(request, resp_tx);

    // Step 3: Call handle_raft_event
    let result = state.handle_raft_event(raft_event, &context, mpsc::unbounded_channel().0).await;

    // Step 4: Verify the response
    assert!(result.is_ok(), "Expected handle_raft_event to return Ok");

    // Step 5: Check the response sent through the channel
    let response = resp_rx.recv().await.expect("Response should be received");
    assert!(response.is_err(), "Expected an error response");
    let status = response.unwrap_err();

    // Step 6: Verify error details
    assert_eq!(status.code(), Code::PermissionDenied);
}

/// Tests successful leader discovery on first attempt
#[tokio::test]
#[traced_test]
async fn test_broadcast_discovery_case1_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_context("test_broadcast_discovery_case1_success", graceful_rx);

    let mut transport = MockTransport::new();
    // Single valid response
    transport.expect_discover_leader().returning(|_, _, _| {
        Ok(vec![LeaderDiscoveryResponse {
            leader_id: 5,
            leader_address: "127.0.0.1:5005".to_string(),
            term: 3,
        }])
    });
    ctx.set_transport(Arc::new(transport));

    let state = LearnerState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    let result = state.broadcast_discovery(Arc::new(mock_membership()), &ctx).await;

    assert!(result.is_ok(), "Should return leader channel");
}

/// Tests discovery failure after max retries
#[tokio::test]
#[traced_test]
async fn test_broadcast_discovery_case2_retry_exhaustion() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_context(
        "test_broadcast_discovery_case2_retry_exhaustion",
        graceful_rx,
    );

    let mut transport = MockTransport::new();
    // Always return empty responses
    transport.expect_discover_leader().returning(|_, _, _| Ok(vec![]));
    ctx.set_transport(Arc::new(transport));

    let state = LearnerState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    let result = state.broadcast_discovery(Arc::new(mock_membership()), &ctx).await;

    assert!(result.is_err(), "Should error after retries");
    assert!(matches!(
        result.unwrap_err(),
        Error::System(SystemError::Network(NetworkError::RetryTimeoutError(_)))
    ));
}

fn mock_context(
    case_name: &str,
    shutdown_signal: watch::Receiver<()>,
) -> RaftContext<MockTypeConfig> {
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join(case_name);

    mock_raft_context(case_path.to_str().unwrap(), shutdown_signal, None)
}

/// Tests leader selection with multiple valid responses
#[tokio::test]
#[traced_test]
async fn test_select_valid_leader_case1_priority() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let ctx = mock_context("test_select_valid_leader_case1_priority", graceful_rx);
    let state = LearnerState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let responses = vec![
        LeaderDiscoveryResponse {
            leader_id: 3,
            term: 5,
            ..Default::default()
        },
        LeaderDiscoveryResponse {
            leader_id: 5,
            term: 7,
            ..Default::default()
        }, // Highest term
        LeaderDiscoveryResponse {
            leader_id: 4,
            term: 7,
            ..Default::default()
        }, // Same term, higher ID
    ];

    let result = state.select_valid_leader(responses).await;

    assert!(result.is_some());
    // Should select leader_id=5 (highest term)
    assert_eq!(result.unwrap(), 5);
}

/// Tests filtering of invalid responses
#[tokio::test]
#[traced_test]
async fn test_select_valid_leader_case2_invalid_responses() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let ctx = mock_context(
        "test_select_valid_leader_case2_invalid_responses",
        graceful_rx,
    );
    let state = LearnerState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let responses = vec![
        LeaderDiscoveryResponse {
            leader_id: 0,
            term: 5,
            ..Default::default()
        }, // Invalid ID
        LeaderDiscoveryResponse {
            leader_id: 3,
            term: 0,
            ..Default::default()
        }, // Invalid term
    ];

    let result = state.select_valid_leader(responses).await;

    assert!(result.is_none(), "Should filter invalid responses");
}

/// # Case 1: Successful join with known leader
#[tokio::test]
#[traced_test]
async fn test_join_cluster_case1_success_known_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_join_cluster_case1", graceful_rx, None);
    let node_id = 100;

    // Mock membership to return known leader
    let membership = MockMembership::new();
    ctx.membership = Arc::new(membership);

    // Mock transport to succeed
    let mut transport = MockTransport::new();
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(JoinResponse {
            success: true,
            error: "".to_string(),
            config: None,
            config_version: 1,
            snapshot_metadata: None,
            leader_id: 3,
        })
    });
    ctx.transport = Arc::new(transport);

    let mut state = LearnerState::<MockTypeConfig>::new(node_id, ctx.node_config.clone());
    // Set known leader in shared_state (case1 expects known leader)
    state.shared_state_mut().set_current_leader(5);

    let result = state.join_cluster(&ctx).await;

    assert!(result.is_ok(), "Join should succeed with known leader");
}

/// # Case 2: Successful join after leader discovery
#[tokio::test]
#[traced_test]
async fn test_join_cluster_case2_success_after_discovery() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_join_cluster_case2", graceful_rx, None);
    let node_id = 100;

    // Mock membership with no known leader
    let membership = MockMembership::new();
    ctx.membership = Arc::new(membership);

    // Mock transport for discovery and join
    let mut transport = MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| {
        Ok(vec![LeaderDiscoveryResponse {
            leader_id: 5,
            leader_address: "127.0.0.1:5005".to_string(),
            term: 3,
        }])
    });
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(JoinResponse {
            success: true,
            error: "".to_string(),
            config: None,
            config_version: 0,
            snapshot_metadata: None,
            leader_id: 2,
        })
    });
    ctx.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(node_id, ctx.node_config.clone());
    let result = state.join_cluster(&ctx).await;

    debug!(?result);

    assert!(result.is_ok(), "Join should succeed after discovery");
}

/// # Case 3: Join failure - leader discovery timeout
#[tokio::test]
#[traced_test]
async fn test_join_cluster_case3_discovery_timeout() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_join_cluster_case3", graceful_rx, None);
    let node_id = 100;

    // Mock membership with no known leader
    let membership = MockMembership::new();
    ctx.membership = Arc::new(membership);

    // Mock transport to timeout during discovery
    let mut transport = MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| {
        sleep(Duration::from_secs(1));
        Ok(vec![])
    });
    ctx.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(node_id, ctx.node_config.clone());
    let result = state.join_cluster(&ctx).await;

    // Should timeout during discovery
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::System(SystemError::Network(NetworkError::RetryTimeoutError(_)))
    ));
}

/// # Case 4: Join failure - leader found but join RPC fails
#[tokio::test]
#[traced_test]
async fn test_join_cluster_case4_join_rpc_failure() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_join_cluster_case4", graceful_rx, None);
    let node_id = 100;

    // Mock membership to return known leader
    let membership = MockMembership::new();
    ctx.membership = Arc::new(membership);

    // Mock transport to fail join RPC
    let mut transport = MockTransport::new();
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Err(NetworkError::ServiceUnavailable("Service unavailable".to_string()).into())
    });

    ctx.transport = Arc::new(transport);

    let mut state = LearnerState::<MockTypeConfig>::new(node_id, ctx.node_config.clone());
    // Set known leader (case4 expects known leader)
    state.shared_state_mut().set_current_leader(5);

    let result = state.join_cluster(&ctx).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::System(SystemError::Network(NetworkError::ServiceUnavailable(_)))
    ));
}

/// # Case 5: Join failure - leader found but invalid response
#[tokio::test]
#[traced_test]
async fn test_join_cluster_case5_invalid_join_response() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_join_cluster_case5", graceful_rx, None);
    let node_id = 100;

    // Mock membership to return known leader
    let membership = MockMembership::new();
    ctx.membership = Arc::new(membership);

    // Mock transport to return failure response
    let mut transport = MockTransport::new();
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(JoinResponse {
            success: false,
            error: "Node rejected".to_string(),
            ..Default::default()
        })
    });
    ctx.transport = Arc::new(transport);

    let mut state = LearnerState::<MockTypeConfig>::new(node_id, ctx.node_config.clone());
    // Set known leader (case5 expects known leader)
    state.shared_state_mut().set_current_leader(5);

    let result = state.join_cluster(&ctx).await;

    debug!(?result);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::Consensus(ConsensusError::Membership(
            MembershipError::JoinClusterFailed(_)
        ))
    ));
}

/// # Case 6: Join with leader redirect
#[tokio::test]
#[traced_test]
async fn test_join_cluster_case6_leader_redirect() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_join_cluster_case6", graceful_rx, None);
    let node_id = 100;

    // Mock membership to return known leader
    let membership = MockMembership::new();
    ctx.membership = Arc::new(membership);

    // Mock transport to redirect to new leader
    let mut transport = MockTransport::new();
    transport.expect_join_cluster().returning(|_, req, _, _| {
        // First call: redirect
        if req.node_id == 100 {
            Err(NetworkError::ServiceUnavailable("Not leader".to_string()).into())
        }
        // Second call: success
        else {
            Ok(JoinResponse {
                success: true,
                error: "".to_string(),
                config: None,
                config_version: 1,
                snapshot_metadata: None,
                leader_id: 3,
            })
        }
    });
    ctx.transport = Arc::new(transport);

    let mut state = LearnerState::<MockTypeConfig>::new(node_id, ctx.node_config.clone());
    // Set known leader (case6 tests redirect scenario)
    state.shared_state_mut().set_current_leader(5);

    let result = state.join_cluster(&ctx).await;

    assert!(result.is_ok(), "Should handle leader redirect");
}

/// # Case 7: Join with large cluster (100 nodes)
#[tokio::test]
#[traced_test]
async fn test_join_cluster_case7_large_cluster() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_join_cluster_case7", graceful_rx, None);
    let node_id = 100;

    // Mock membership with no known leader
    let membership = MockMembership::new();
    ctx.membership = Arc::new(membership);

    // Mock transport to handle large discovery
    let mut transport = MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| {
        Ok(vec![LeaderDiscoveryResponse {
            leader_id: 5,
            leader_address: "127.0.0.1:5005".to_string(),
            term: 3,
        }])
    });
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(JoinResponse {
            success: true,
            error: "".to_string(),
            config: None,
            config_version: 1,
            snapshot_metadata: None,
            leader_id: 3,
        })
    });
    ctx.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(node_id, ctx.node_config.clone());
    let result = state.join_cluster(&ctx).await;

    assert!(result.is_ok(), "Should handle large cluster");
}

#[cfg(test)]
mod role_violation_tests {
    use super::*;

    /// Test handling role violation events by LearnerState
    #[tokio::test]
    async fn test_role_violation_events() {
        // Step 1: Setup the test environment
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let context =
            mock_raft_context("/tmp/test_learner_role_violation_events", graceful_rx, None);
        let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

        // Step 2: Prepare the CreateSnapshotEvent
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::CreateSnapshotEvent;

        // [Test CreateSnapshotEvent]
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        // Verify the error response
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::RoleViolation { .. })
        ));

        // [Test SnapshotCreated]
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::SnapshotCreated(Err(Error::Fatal("test".to_string())));
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        // Verify the error response
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::RoleViolation { .. })
        ));

        // [Test LogPurgeCompleted]
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::LogPurgeCompleted(LogId { term: 1, index: 1 });
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        // Verify the error response
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::RoleViolation { .. })
        ));
    }
}
