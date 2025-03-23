use super::{leader_state::LeaderState, role_state::RaftRoleState};
use crate::{
    alias::POF,
    grpc::rpc_service::{
        AppendEntriesRequest, ClientCommand, ClientProposeRequest, ClientReadRequest,
        ClientRequestError, ClientResponse, ClusteMembershipChangeRequest, ClusterMembership,
        MetadataRequest, VoteRequest, VoteResponse, VotedFor,
    },
    test_utils::{
        enable_logger, mock_peer_channels, mock_raft_context, setup_raft_components, MockBuilder,
        MockTypeConfig,
    },
    utils::util::kv,
    AppendResponseWithUpdates, AppendResults, Error, MaybeCloneOneshot, MaybeCloneOneshotReceiver,
    MaybeCloneOneshotSender, MockElectionCore, MockMembership, MockRaftLog, MockReplicationCore,
    MockStateMachineHandler, MockTransport, NewLeaderInfo, PeerUpdate, RaftEvent, RaftOneshot,
    RaftSettings, RoleEvent, Settings, StateUpdate,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, watch};
use tonic::{Code, Status};

struct TestContext {
    state: LeaderState<MockTypeConfig>,
    replication_handler: MockReplicationCore<MockTypeConfig>,
    raft_log: Arc<MockRaftLog>,
    transport: Arc<MockTransport>,
    arc_settings: Arc<Settings>,
}

/// Initialize the test environment and return the core components
async fn setup_test_case(
    test_name: &str,
    batch_threshold: usize,
    handle_client_proposal_in_batch_expect_times: usize,
) -> TestContext {
    let context = setup_raft_components(&format!("/tmp/{}", test_name), None, false);

    // Configure raft settings
    let raft_settings = RaftSettings {
        rpc_append_entries_in_batch_threshold: batch_threshold,
        ..context.settings.raft_settings.clone()
    };

    // Create Leader state
    let settings = Arc::new(Settings {
        raft_settings: raft_settings.clone(),
        ..context.settings.clone()
    });
    let mut state = LeaderState::new(1, settings.clone());
    state
        .update_commit_index(4)
        .expect("Should succeed to update commit index");

    // Initialize the mock object
    let mut replication_handler = MockReplicationCore::new();
    let mut raft_log = MockRaftLog::new();

    //Configure mock behavior
    replication_handler
        .expect_handle_client_proposal_in_batch()
        .times(handle_client_proposal_in_batch_expect_times)
        .returning(move |_, _, _, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: 5,
                            next_index: 6,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: 5,
                            next_index: 6,
                            success: true,
                        },
                    ),
                ]),
            })
        });

    raft_log
        .expect_calculate_majority_matched_index()
        .returning(|_, _, _| Some(5));

    TestContext {
        state,
        replication_handler,
        raft_log: Arc::new(raft_log),
        transport: Arc::new(MockTransport::new()),
        arc_settings: settings,
    }
}

/// Verify client response
pub async fn assert_client_response(
    mut rx: MaybeCloneOneshotReceiver<std::result::Result<ClientResponse, Status>>,
) {
    match rx.recv().await {
        Ok(Ok(response)) => assert_eq!(
            ClientRequestError::try_from(response.error_code).unwrap(),
            ClientRequestError::NoError
        ),
        Ok(Err(e)) => panic!("Unexpected error response: {:?}", e),
        Err(_) => panic!("Response channel closed unexpectedly"),
    }
}

/// # Case 1.1: Test process_client_propose by simulating client proposal request
/// Validates leader behavior when replicating new client proposals with partially synchronized cluster
///
/// ## Scenario Setup
/// Log State Initialization:
/// - Peer1:
///   - Log entries: [1, 2, 3]
///   - next_index: 3
/// - Peer2:
///   - Log entries: [1, 2, 3, 4]
///   - next_index: 4
/// - Leader:
///   - Log entries: [1, 2, 3, 4]
///   - commit_index: 4 (pre-operation)
/// - rpc_append_entries_in_batch_threshold = 0 > means executed immediatelly
///
/// ## Validation Criteria
/// The test succeeds if all following conditions are met:
/// 1. Log Append Verification:
///    - New log entry (index 5) added to leader's log
/// 2. Commit Index Update:
///    - Leader's commit_index advances to 5
/// 3. Peer state update:
///    - Peer1, next_index: 6
///    - Peer2, next_index: 6
/// 4. Receiver Ok(ClientResponse::write_success) signal
#[tokio::test]
async fn test_process_client_propose_case1_1() {
    // Initialize the test environment (threshold = 0 means immediate execution)
    let mut test_context = setup_test_case("/tmp/test_process_client_propose_case1_1", 0, 1).await;

    // Prepare test request
    let request = ClientProposeRequest {
        client_id: 0,
        commands: vec![],
    };
    let (tx, rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Execute test operation
    let result = test_context
        .state
        .process_client_propose(
            request,
            tx,
            &test_context.replication_handler,
            &vec![],
            &test_context.raft_log,
            &test_context.transport,
            test_context.arc_settings.raft_settings.clone(),
            false,
            &role_tx,
        )
        .await;

    // Verify the result
    match role_rx.recv().await {
        Some(RoleEvent::NotifyNewCommitIndex { new_commit_index }) => {
            assert_eq!(new_commit_index, 5);
        }
        _ => {
            assert!(false);
        }
    }
    assert!(result.is_ok(), "Operation should succeed");
    assert_eq!(test_context.state.commit_index(), 5);
    assert_eq!(test_context.state.next_index(2), Some(6));
    assert_eq!(test_context.state.next_index(3), Some(6));
    assert_client_response(rx).await;
}

/// # Case 1.2: Test process_client_propose by simulating client proposal request
/// Validates two client proposal responses will be returned
///
/// ## Scenario Setup
/// Log State Initialization:
/// - Peer1:
///   - Log entries: [1, 2, 3]
///   - next_index: 3
/// - Peer2:
///   - Log entries: [1, 2, 3, 4]
///   - next_index: 4
/// - Leader:
///   - Log entries: [1, 2, 3, 4]
///   - commit_index: 4 (pre-operation)
/// - rpc_append_entries_in_batch_threshold = 2
///
/// ## Validation Criteria
/// The test succeeds if all following conditions are met:
/// 1. Log Append Verification:
///    - New log entry (index 5) added to leader's log
/// 2. Commit Index Update:
///    - Leader's commit_index advances to 5
/// 3. Peer state update:
///    - Peer1, next_index: 6
///    - Peer2, next_index: 6
/// 4. Receiver two Ok(ClientResponse::write_success) signal
#[tokio::test]
async fn test_process_client_propose_case1_2() {
    // Initialize the test environment (threshold = 0 means immediate execution)
    let mut test_context = setup_test_case("/tmp/test_process_client_propose_case1_2", 0, 2).await;

    // Prepare test request
    let request = ClientProposeRequest {
        client_id: 0,
        commands: vec![],
    };
    let (tx1, rx1) = MaybeCloneOneshot::new();
    let (tx2, rx2) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Execute test operation
    let result1 = test_context
        .state
        .process_client_propose(
            request.clone(),
            tx1,
            &test_context.replication_handler,
            &vec![],
            &test_context.raft_log,
            &test_context.transport,
            test_context.arc_settings.raft_settings.clone(),
            true,
            &role_tx,
        )
        .await;

    let result2 = test_context
        .state
        .process_client_propose(
            request,
            tx2,
            &test_context.replication_handler,
            &vec![],
            &test_context.raft_log,
            &test_context.transport,
            test_context.arc_settings.raft_settings.clone(),
            true,
            &role_tx,
        )
        .await;

    // Verify the result
    match role_rx.recv().await {
        Some(RoleEvent::NotifyNewCommitIndex { new_commit_index }) => {
            assert_eq!(new_commit_index, 5);
        }
        _ => {
            assert!(false);
        }
    }
    assert!(result1.is_ok(), "Operation should succeed");
    assert!(result2.is_ok(), "Operation should succeed");
    assert_eq!(test_context.state.commit_index(), 5);
    assert_eq!(test_context.state.next_index(2), Some(6));
    assert_eq!(test_context.state.next_index(3), Some(6));
    assert_client_response(rx1).await;
    assert_client_response(rx2).await;
}

/// # Case 2: Test process_client_propose by client propose request
///
/// ## Setup
/// - exexute_now = false
/// - batch is not full yet(rpc_append_entries_in_batch_threshold = 100)
///
/// ## Criterias
/// - return Ok()
#[tokio::test]
async fn test_process_client_propose_case2() {
    let context = setup_raft_components("/tmp/test_process_client_propose_case2", None, false);

    let raft_settings = RaftSettings {
        rpc_append_entries_in_batch_threshold: 100,
        ..context.settings.raft_settings.clone()
    };
    let settings = Settings {
        raft_settings: raft_settings.clone(),
        ..context.settings.clone()
    };
    let mut state = LeaderState::<MockTypeConfig>::new(1, Arc::new(settings));

    // 1. Prepare mocks
    let replication_handler = MockReplicationCore::<MockTypeConfig>::new();

    let raft_log = MockRaftLog::new();
    let transport = MockTransport::new();
    let client_propose_request = ClientProposeRequest {
        client_id: 0,
        commands: vec![],
    };

    // 2. Prepare voting members
    let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let voting_members = Vec::new();
    assert!(state
        .process_client_propose(
            client_propose_request,
            resp_tx,
            &replication_handler,
            &voting_members,
            &Arc::new(raft_log),
            &Arc::new(transport),
            raft_settings,
            false,
            &role_tx
        )
        .await
        .is_ok());
}

/// # Case 1: state_machine_handler::update_pending should be executed once
/// if last_applied < commit_index
#[tokio::test]
async fn test_ensure_state_machine_upto_commit_index_case1() {
    // Prepare Leader State
    let context = setup_raft_components(
        "/tmp/test_ensure_state_machine_upto_commit_index_case1",
        None,
        false,
    );
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.arc_settings.clone());

    // Update commit index
    let commit_index = 10;
    state
        .update_commit_index(commit_index)
        .expect("should succeed");

    // Prepare last applied index
    let last_applied = commit_index - 1;

    // Test fun
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_update_pending()
        .times(1)
        .returning(|_| {});
    state
        .ensure_state_machine_upto_commit_index(&Arc::new(state_machine_handler), last_applied)
        .expect("should succeed");
}

/// # Case 2: state_machine_handler::update_pending should not be executed
/// if last_applied >= commit_index
#[tokio::test]
async fn test_ensure_state_machine_upto_commit_index_case2() {
    // Prepare Leader State
    let context = setup_raft_components(
        "/tmp/test_ensure_state_machine_upto_commit_index_case2",
        None,
        false,
    );
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.arc_settings.clone());

    // Update Commit index
    let commit_index = 10;
    state
        .update_commit_index(commit_index)
        .expect("should succeed");

    // Prepare last applied index
    let last_applied = commit_index;

    // Test fun
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_update_pending()
        .times(0)
        .returning(|_| {});
    state
        .ensure_state_machine_upto_commit_index(&Arc::new(state_machine_handler), last_applied)
        .expect("should succeed");
}

fn setup_handle_raft_event_case1_params(
    resp_tx: MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>,
) -> (RaftEvent, Arc<POF<MockTypeConfig>>) {
    let raft_event = crate::RaftEvent::ReceiveVoteRequest(
        VoteRequest {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        },
        resp_tx,
    );
    let peer_channels = Arc::new(mock_peer_channels());
    (raft_event, peer_channels)
}

/// # Case 1.1: Receive Vote Request Event
///
/// ## Preparation setup:
/// 1. handle_vote_request return Ok(None) - reject this vote
///
/// ## Validate criterias
/// 1. receive response with vote_granted = false
/// 2. Role should not step to Follower
#[tokio::test]
async fn test_handle_raft_event_case1_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case1_1", graceful_rx, None);
    let mut election_handler = MockElectionCore::<MockTypeConfig>::new();
    election_handler
        .expect_handle_vote_request()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(StateUpdate {
                new_voted_for: None,
                step_to_follower: false,
            })
        });
    context.election_handler = election_handler;

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.settings.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (raft_event, peer_channels) = setup_handle_raft_event_case1_params(resp_tx);

    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    // Receive response with vote_granted = false
    match resp_rx.recv().await {
        Ok(Ok(r)) => assert!(!r.vote_granted),
        _ => assert!(false),
    }

    // No role event receives
    assert!(role_rx.try_recv().is_err());
}

/// # Case 1.2: Receive Vote Request Event
///
/// ## Preparation setup:
/// 1. handle_vote_request return Ok(Some(VotedFor)) - accept this vote
///
/// ## Validate criterias
/// 1. receive response with vote_granted = true
/// 2. Step to Follower
#[tokio::test]
async fn test_handle_raft_event_case1_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case1_2", graceful_rx, None);
    let mut election_handler = MockElectionCore::<MockTypeConfig>::new();
    election_handler
        .expect_handle_vote_request()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(StateUpdate {
                new_voted_for: Some(VotedFor {
                    voted_for_id: 1,
                    voted_for_term: 1,
                }),
                step_to_follower: true,
            })
        });
    context.election_handler = election_handler;

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.settings.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (raft_event, peer_channels) = setup_handle_raft_event_case1_params(resp_tx);

    let r = state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await;
    println!("r:{:?}", r);
    assert!(r.is_ok());

    // Receive response with vote_granted = true
    match resp_rx.recv().await {
        Ok(Ok(r)) => assert!(r.vote_granted),
        _ => assert!(false),
    }

    // Step to Follower
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::BecomeFollower(_))
    ));
}

/// # Case 1.3: Receive Vote Request Event
///
/// ## Preparation setup:
/// 1. handle_vote_request return Error
///
/// ## Validate criterias
/// 1. receive response with ErrorEmpty
/// 2. handle_raft_event returns Error
/// 3. Role should not step to Follower
#[tokio::test]
async fn test_handle_raft_event_case1_3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case1_3", graceful_rx, None);
    let mut election_handler = MockElectionCore::<MockTypeConfig>::new();
    election_handler
        .expect_handle_vote_request()
        .times(1)
        .returning(|_, _, _, _| Err(Error::TokioSendStatusError("".to_string())));
    context.election_handler = election_handler;

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.settings.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (raft_event, peer_channels) = setup_handle_raft_event_case1_params(resp_tx);

    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_err());

    assert!(resp_rx.recv().await.is_err());

    // No role event receives
    assert!(role_rx.try_recv().is_err());
}

/// # Case 2: Receive ClusterConf Event
#[tokio::test]
async fn test_handle_raft_event_case2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case2", graceful_rx, None);
    let mut membership = MockMembership::new();
    membership
        .expect_retrieve_cluster_membership_config()
        .times(1)
        .returning(|| ClusterMembership { nodes: vec![] });
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.settings.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = crate::RaftEvent::ClusterConf(MetadataRequest {}, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());

    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    if let Ok(Ok(m)) = resp_rx.recv().await {
        assert_eq!(m.nodes, vec![]);
    } else {
        assert!(false);
    }
}

/// # Case 3.1: Receive ClusterConfUpdate Event
///  and update successfully
#[tokio::test]
async fn test_handle_raft_event_case3_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case3_1", graceful_rx, None);
    let mut membership = MockMembership::new();
    membership
        .expect_update_cluster_conf_from_leader()
        .times(1)
        .returning(|_, _| true);
    membership
        .expect_get_cluster_conf_version()
        .times(1)
        .returning(|| 1);
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.settings.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = crate::RaftEvent::ClusterConfUpdate(
        ClusteMembershipChangeRequest {
            id: 1,
            term: 1,
            version: 1,
            cluster_membership: None,
        },
        resp_tx,
    );
    let peer_channels = Arc::new(mock_peer_channels());

    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    match resp_rx.recv().await {
        Ok(r) => match r {
            Ok(response) => assert!(response.success),
            Err(_) => assert!(false),
        },
        Err(_) => assert!(false),
    }
}

/// # Case 3.2: Receive ClusterConfUpdate Event
///  and update failed
#[tokio::test]
async fn test_handle_raft_event_case3_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case3_2", graceful_rx, None);
    let mut membership = MockMembership::new();
    membership
        .expect_update_cluster_conf_from_leader()
        .times(1)
        .returning(|_, _| false);
    membership
        .expect_get_cluster_conf_version()
        .times(1)
        .returning(|| 1);
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.settings.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = crate::RaftEvent::ClusterConfUpdate(
        ClusteMembershipChangeRequest {
            id: 1,
            term: 1,
            version: 1,
            cluster_membership: None,
        },
        resp_tx,
    );
    let peer_channels = Arc::new(mock_peer_channels());

    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    match resp_rx.recv().await {
        Ok(r) => match r {
            Ok(response) => assert!(!response.success),
            Err(_) => assert!(false),
        },
        Err(_) => assert!(false),
    }
}

/// # Case 4.1: As Leader, if I receive append request with (my_term >= append_entries_request.term), then I should reject the request
///
#[tokio::test]
async fn test_handle_raft_event_case4_1() {
    // Prepare Leader State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case4_1", graceful_rx, None);
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.settings.clone());

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
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = crate::RaftEvent::AppendEntries(append_entries_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Execute fun
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("should succeed");

    // Validate request should receive AppendEntriesResponse with success = false
    match resp_rx.recv().await.expect("should succeed") {
        Ok(response) => assert!(!response.success),
        Err(_) => assert!(false),
    }
}

/// # Case 4.2: As Leader, if I receive append request with (my_term < append_entries_request.term)
///
/// ## Criterias:
/// 1. I should step down as Follower(receive RoleEvent::BecomeFollower event)
/// 2. handle_append_entries should not be triggered
///
#[tokio::test]
async fn test_handle_raft_event_case4_2() {
    // Prepare Leader State
    enable_logger();

    // Prepare leader term smaller than request one
    let my_term = 10;
    let request_term = 11;
    let request_term_clone = request_term;

    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .times(0)
        .returning(move |_, _, _, _| {
            Ok(AppendResponseWithUpdates {
                success: true,
                current_term: my_term,
                last_matched_id: 1,
                term_update: Some(request_term_clone),
                commit_index_update: None,
            })
        });
    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_raft_event_case4_2")
        .with_replication_handler(replication_handler)
        .build_context();
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.settings.clone());

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
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = crate::RaftEvent::AppendEntries(append_entries_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Test fun
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("should succeed");

    // Validate criterias: step down as Follower
    match role_rx.try_recv() {
        Ok(new_role) => assert!(matches!(
            new_role,
            RoleEvent::BecomeFollower(Some(_new_leader_id))
        )),
        Err(_) => assert!(false),
    };

    // Validate request should receive AppendEntriesResponse with success = false
    match resp_rx.recv().await.expect("should succeed") {
        Ok(response) => assert!(!response.success),
        Err(_) => assert!(false),
    }
}

/// # Case 5.1: Test handle client propose request
///     if process_client_propose returns Ok()
#[tokio::test]
async fn test_handle_raft_event_case5_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case5_1", graceful_rx, None);

    // New state
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.settings.clone());

    // Handle raft event
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = crate::RaftEvent::ClientPropose(
        ClientProposeRequest {
            client_id: 1,
            commands: vec![],
        },
        resp_tx,
    );
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());
}

/// # Case 5.1: Test handle client propose request
///     if process_client_propose returns Err()
#[tokio::test]
async fn test_handle_raft_event_case5_2() {}

/// # Case 6.1: Test ClientReadRequest event
///     if both peers failed to confirm leader's commit, the lread request should be failed
///
#[tokio::test]
async fn test_handle_raft_event_case6_1() {
    enable_logger();
    // Prepare Leader State
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_client_proposal_in_batch()
        .times(1)
        .returning(|_, _, _, _, _, _, _| Err(Error::AppendEntriesCommitNotConfirmed));

    // Initializing Shutdown Signal
    let (graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_raft_event_case6_1")
        .with_replication_handler(replication_handler)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.settings.clone());

    // Prepare request
    let mut commands = Vec::new();
    commands.push(ClientCommand::get(kv(1)));
    let client_read_request = ClientReadRequest {
        client_id: 1,
        linear: true,
        commands,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = crate::RaftEvent::ClientReadRequest(client_read_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("should succeed");

    match resp_rx.recv().await {
        Ok(Ok(_)) => {
            assert!(false);
        }
        Ok(Err(e)) => {
            assert_eq!(e.code(), Code::FailedPrecondition);
        }
        Err(_e) => {
            assert!(false)
        }
    }
}

/// # Case 6.2: Test ClientReadRequest event
///     if majority peers confirms, the response should be success
///
/// ## Preparaiton setup
/// 1. Leader current commit is 1
/// 2. calculate_majority_matched_index return Some(3), 3 is new commit index
/// 3. handle_client_proposal_in_batch returns Ok(AppendResults{})
///
/// ## Validation criterias:
/// 1. Leader commit should be updated to: 3(new commit index)
/// 2. event "RoleEvent::NotifyNewCommitIndex" should be received
/// 3. resp_rx receives Ok()
#[tokio::test]
async fn test_handle_raft_event_case6_2() {
    enable_logger();
    let expect_new_commit_index = 3;
    // Prepare Leader State
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_client_proposal_in_batch()
        .times(1)
        .returning(|_, _, _, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: 3,
                            next_index: 4,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: 4,
                            next_index: 5,
                            success: true,
                        },
                    ),
                ]),
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(move |_, _, _| Some(expect_new_commit_index));

    // Initializing Shutdown Signal
    let (graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_raft_event_case6_2")
        .with_raft_log(raft_log)
        .with_replication_handler(replication_handler)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.settings.clone());

    // Prepare request
    let mut commands = Vec::new();
    commands.push(ClientCommand::get(kv(1)));
    let client_read_request = ClientReadRequest {
        client_id: 1,
        linear: true,
        commands,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = crate::RaftEvent::ClientReadRequest(client_read_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("should succeed");

    // Validation criteria 1: Leader commit should be updated to: 3(new commit index)
    assert_eq!(state.commit_index(), expect_new_commit_index);

    // Validation criteria 2: event "RoleEvent::NotifyNewCommitIndex" should be received
    match resp_rx.recv().await {
        Ok(Ok(_)) => {
            assert!(true);
        }
        Ok(Err(_)) => {
            assert!(false);
        }
        Err(_e) => {
            assert!(false)
        }
    }

    // Validation criteria 3: resp_rx receives Ok()
    match role_rx.try_recv() {
        Ok(event) => assert!(matches!(
            event,
            RoleEvent::NotifyNewCommitIndex {
                new_commit_index: expect_new_commit_index
            }
        )),
        Err(_) => assert!(false),
    };
}

/// # Case 6.3: Test ClientReadRequest event
///     if new Leader found during the replication,
///
/// ## Preparaiton setup
/// 1. Leader current commit is 1
/// 2. calculate_majority_matched_index return Some(3), 3 is new commit index
/// 3. handle_client_proposal_in_batch returns Err(Error::FoundNewLeaderError)
///
/// ## Validation criterias:
/// 1. Leader commit should still be: 1(new commit index)
/// 2. event "RoleEvent::BecomeFollower" should be received
/// 3. resp_rx receives Err(e)
#[tokio::test]
async fn test_handle_raft_event_case6_3() {
    enable_logger();
    // Prepare Leader State
    let new_leader_id = 7;
    let new_leader_id_clone = new_leader_id;
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_client_proposal_in_batch()
        .times(1)
        .returning(move |_, _, _, _, _, _, _| {
            Err(Error::FoundNewLeaderError(NewLeaderInfo {
                term: 1,
                leader_id: new_leader_id,
            }))
        });

    let expect_new_commit_index = 3;
    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(move |_, _, _| Some(expect_new_commit_index));

    // Initializing Shutdown Signal
    let (graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_raft_event_case6_3")
        .with_replication_handler(replication_handler)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.settings.clone());
    state.update_commit_index(1).expect("should succeed");

    // Prepare request
    let mut commands = Vec::new();
    commands.push(ClientCommand::get(kv(1)));
    let client_read_request = ClientReadRequest {
        client_id: 1,
        linear: true,
        commands,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = crate::RaftEvent::ClientReadRequest(client_read_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("should succeed");

    // Validation criteria 1: Leader commit should be updated to: 3(new commit index)
    assert_eq!(state.commit_index(), 1);

    // Validation criteria 2: event "RoleEvent::BecomeFollower" should be received
    match role_rx.try_recv() {
        Ok(event) => assert!(matches!(
            event,
            RoleEvent::BecomeFollower(Some(new_leader_id_clone))
        )),
        Err(_) => assert!(false),
    };

    // Validation criteria 3: resp_rx receives Err(e)
    match resp_rx.recv().await {
        Ok(Err(_)) => {
            assert!(true)
        }
        _ => {
            assert!(false);
        }
    }
}
