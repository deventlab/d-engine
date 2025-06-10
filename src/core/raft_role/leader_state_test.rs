use super::leader_state::LeaderState;
use super::role_state::RaftRoleState;
use crate::alias::POF;
use crate::client_command_to_entry_payloads;
use crate::convert::safe_kv;
use crate::proto::client::ClientReadRequest;
use crate::proto::client::ClientResponse;
use crate::proto::client::ClientWriteRequest;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::JoinRequest;
use crate::proto::cluster::MetadataRequest;
use crate::proto::common::EntryPayload;
use crate::proto::common::LogId;
use crate::proto::election::VoteRequest;
use crate::proto::election::VoteResponse;
use crate::proto::error::ErrorCode;
use crate::proto::replication::AppendEntriesRequest;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotMetadata;
use crate::test_utils::crate_test_snapshot_stream;
use crate::test_utils::create_test_chunk;
use crate::test_utils::enable_logger;
use crate::test_utils::mock_peer_channels;
use crate::test_utils::mock_raft_context;
use crate::test_utils::node_config;
use crate::test_utils::setup_raft_components;
use crate::test_utils::MockBuilder;
use crate::test_utils::MockNode;
use crate::test_utils::MockTypeConfig;
use crate::test_utils::MOCK_LEADER_STATE_PORT_BASE;
use crate::test_utils::MOCK_ROLE_STATE_PORT_BASE;
use crate::AppendResults;
use crate::ChannelWithAddress;
use crate::ChannelWithAddressAndRole;
use crate::ConsensusError;
use crate::Error;
use crate::MaybeCloneOneshot;
use crate::MaybeCloneOneshotReceiver;
use crate::MaybeCloneOneshotSender;
use crate::MockMembership;
use crate::MockPeerChannels;
use crate::MockPurgeExecutor;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::MockStateMachine;
use crate::MockStateMachineHandler;
use crate::MockTransport;
use crate::NetworkError;
use crate::NewCommitData;
use crate::PeerUpdate;
use crate::QuorumVerificationResult;
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftOneshot;
use crate::RaftRequestWithSignal;
use crate::RaftTypeConfig;
use crate::ReplicationError;
use crate::RoleEvent;
use crate::StorageError;
use crate::SystemError;
use crate::FOLLOWER;
use futures::StreamExt;
use mockall::predicate::eq;
use nanoid::nanoid;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tonic::transport::Endpoint;
use tonic::Code;
use tonic::Status;

struct ProcessRaftRequestTestContext {
    state: LeaderState<MockTypeConfig>,
    raft_context: RaftContext<MockTypeConfig>,
}

/// Initialize the test environment and return the core components
async fn setup_process_raft_request_test_context(
    test_name: &str,
    batch_threshold: usize,
    handle_raft_request_in_batch_expect_times: usize,
    shutdown_signal: watch::Receiver<()>,
) -> ProcessRaftRequestTestContext {
    let mut node_config = node_config(&format!("/tmp/{test_name}",));
    node_config.raft.replication.rpc_append_entries_in_batch_threshold = batch_threshold;
    let mut raft_context = MockBuilder::new(shutdown_signal)
        .wiht_node_config(node_config)
        .build_context();

    let mut state = LeaderState::new(1, raft_context.node_config());
    state
        .update_commit_index(4)
        .expect("Should succeed to update commit index");

    // Initialize the mock object
    let mut replication_handler = MockReplicationCore::new();
    let mut raft_log = MockRaftLog::new();

    //Configure mock behavior
    replication_handler
        .expect_handle_raft_request_in_batch()
        .times(handle_raft_request_in_batch_expect_times)
        .returning(move |_, _, _, _, _| {
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

    raft_context.handlers.replication_handler = replication_handler;
    raft_context.storage.raft_log = Arc::new(raft_log);

    ProcessRaftRequestTestContext {
        state,
        raft_context,
        // replication_handler,
        // raft_log: Arc::new(raft_log),
        // transport: Arc::new(MockTransport::new()),
        // arc_node_config: node_config,
    }
}

/// Verify client response
pub async fn assert_client_response(mut rx: MaybeCloneOneshotReceiver<std::result::Result<ClientResponse, Status>>) {
    match rx.recv().await {
        Ok(Ok(response)) => assert_eq!(ErrorCode::try_from(response.error).unwrap(), ErrorCode::Success),
        Ok(Err(e)) => panic!("Unexpected error response: {e:?}",),
        Err(_) => panic!("Response channel closed unexpectedly"),
    }
}

/// # Case 1.1: Test process_raft_request by simulating client proposal request
/// Validates leader behavior when replicating new client proposals with
/// partially synchronized cluster
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
async fn test_process_raft_request_case1_1() {
    // Initialize the test environment (threshold = 0 means immediate execution)

    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut test_context =
        setup_process_raft_request_test_context("/tmp/test_process_raft_request_case1_1", 0, 1, graceful_rx).await;

    // Prepare test request
    let request = ClientWriteRequest {
        client_id: 0,
        commands: vec![],
    };
    let (tx, rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let peer_channels = Arc::new(mock_peer_channels());
    // Execute test operation
    let result = test_context
        .state
        .process_raft_request(
            RaftRequestWithSignal {
                id: nanoid!(),
                payloads: client_command_to_entry_payloads(request.commands),
                sender: tx,
            },
            &test_context.raft_context,
            peer_channels,
            false,
            &role_tx,
        )
        .await;

    // Verify the result
    if let Some(RoleEvent::NotifyNewCommitIndex(NewCommitData {
        new_commit_index,
        role: _,
        current_term: _,
    })) = role_rx.recv().await
    {
        assert_eq!(new_commit_index, 5);
    }
    assert!(result.is_ok(), "Operation should succeed");
    assert_eq!(test_context.state.commit_index(), 5);
    assert_eq!(test_context.state.next_index(2), Some(6));
    assert_eq!(test_context.state.next_index(3), Some(6));
    assert_client_response(rx).await;
}

/// # Case 1.2: Test process_raft_request by simulating client proposal request
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
async fn test_process_raft_request_case1_2() {
    // Initialize the test environment (threshold = 0 means immediate execution)
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut test_context =
        setup_process_raft_request_test_context("/tmp/test_process_raft_request_case1_2", 0, 2, graceful_rx).await;

    // Prepare test request
    let request = ClientWriteRequest {
        client_id: 0,
        commands: vec![],
    };
    let (tx1, rx1) = MaybeCloneOneshot::new();
    let (tx2, rx2) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let peer_channels = Arc::new(mock_peer_channels());
    // Execute test operation
    let result1 = test_context
        .state
        .process_raft_request(
            RaftRequestWithSignal {
                id: nanoid!(),
                payloads: client_command_to_entry_payloads(request.commands.clone()),
                sender: tx1,
            },
            &test_context.raft_context,
            peer_channels.clone(),
            true,
            &role_tx,
        )
        .await;

    let result2 = test_context
        .state
        .process_raft_request(
            RaftRequestWithSignal {
                id: nanoid!(),
                payloads: client_command_to_entry_payloads(request.commands),
                sender: tx2,
            },
            &test_context.raft_context,
            peer_channels,
            true,
            &role_tx,
        )
        .await;

    // Verify the result
    if let Some(RoleEvent::NotifyNewCommitIndex(NewCommitData {
        new_commit_index,
        role: _,
        current_term: _,
    })) = role_rx.recv().await
    {
        assert_eq!(new_commit_index, 5);
    }

    assert!(result1.is_ok(), "Operation should succeed");
    assert!(result2.is_ok(), "Operation should succeed");
    assert_eq!(test_context.state.commit_index(), 5);
    assert_eq!(test_context.state.next_index(2), Some(6));
    assert_eq!(test_context.state.next_index(3), Some(6));
    assert_client_response(rx1).await;
    assert_client_response(rx2).await;
}

/// # Case 2: Test process_raft_request by client propose request
///
/// ## Setup
/// - execute_now = false
/// - batch is not full yet(rpc_append_entries_in_batch_threshold = 100)
///
/// ## Criterias
/// - return Ok()
#[tokio::test]
async fn test_process_raft_request_case2() {
    // let context = setup_raft_components("/tmp/test_process_raft_request_case2", None, false);
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut test_context =
        setup_process_raft_request_test_context("/tmp/test_process_raft_request_case2", 100, 0, graceful_rx).await;

    // 1. Prepare mocks
    let client_propose_request = ClientWriteRequest {
        client_id: 0,
        commands: vec![],
    };

    // 2. Prepare voting members
    let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    let peer_channels = Arc::new(mock_peer_channels());
    assert!(test_context
        .state
        .process_raft_request(
            RaftRequestWithSignal {
                id: nanoid!(),
                payloads: client_command_to_entry_payloads(client_propose_request.commands),
                sender: resp_tx,
            },
            &test_context.raft_context,
            peer_channels,
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
    let context = setup_raft_components("/tmp/test_ensure_state_machine_upto_commit_index_case1", None, false);
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.arc_node_config.clone());

    // Update commit index
    let commit_index = 10;
    state.update_commit_index(commit_index).expect("should succeed");

    // Prepare last applied index
    let last_applied = commit_index - 1;

    // Test fun
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_update_pending().times(1).returning(|_| {});
    state
        .ensure_state_machine_upto_commit_index(&Arc::new(state_machine_handler), last_applied)
        .expect("should succeed");
}

/// # Case 2: state_machine_handler::update_pending should not be executed
/// if last_applied >= commit_index
#[tokio::test]
async fn test_ensure_state_machine_upto_commit_index_case2() {
    // Prepare Leader State
    let context = setup_raft_components("/tmp/test_ensure_state_machine_upto_commit_index_case2", None, false);
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.arc_node_config.clone());

    // Update Commit index
    let commit_index = 10;
    state.update_commit_index(commit_index).expect("should succeed");

    // Prepare last applied index
    let last_applied = commit_index;

    // Test fun
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_update_pending().times(0).returning(|_| {});
    state
        .ensure_state_machine_upto_commit_index(&Arc::new(state_machine_handler), last_applied)
        .expect("should succeed");
}

fn setup_handle_raft_event_case1_params(
    resp_tx: MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>,
    term: u64,
) -> (RaftEvent, Arc<POF<MockTypeConfig>>) {
    let raft_event = RaftEvent::ReceiveVoteRequest(
        VoteRequest {
            term,
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
///     with my_term >= vote_request.term
///
/// ## Validate criterias
/// 1. receive response with vote_granted = false
/// 2. Role should not step to Follower
/// 3. Term should not be updated
#[tokio::test]
async fn test_handle_raft_event_case1_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case1_1", graceful_rx, None);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let term_before = state.current_term();
    let request_term = term_before;

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (raft_event, peer_channels) = setup_handle_raft_event_case1_params(resp_tx, request_term);

    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    // Receive response with vote_granted = false
    assert!(!resp_rx.recv().await.unwrap().unwrap().vote_granted);

    // No role event receives
    assert!(role_rx.try_recv().is_err());

    // Term should not be updated
    assert_eq!(term_before, state.current_term());
}

/// # Case 1.2: Receive Vote Request Event
///     with my_term < vote_request.term
///
/// ## Validate criterias
/// 1. Should not receive any response
/// 2. Step to Follower
/// 3. Term should be updated
/// 4. Replay raft event
#[tokio::test]
async fn test_handle_raft_event_case1_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case1_2", graceful_rx, None);

    let updated_term = 100;

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (raft_event, peer_channels) = setup_handle_raft_event_case1_params(resp_tx, updated_term);

    let r = state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await;
    println!("r:{r:?}");
    assert!(r.is_ok());

    // Step to Follower
    assert!(matches!(role_rx.try_recv(), Ok(RoleEvent::BecomeFollower(_))));
    assert!(matches!(role_rx.try_recv(), Ok(RoleEvent::ReprocessEvent(_))));

    // Term should be updated
    assert_eq!(state.current_term(), updated_term);

    // Make sure this assert is at the end of the test function.
    // Because we should wait handle_raft_event fun finish running after the role
    // events been consumed above.
    assert!(resp_rx.recv().await.is_err());
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
        .returning(|| ClusterMembership {
            version: 1,
            nodes: vec![],
        });
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConf(MetadataRequest {}, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());

    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    let m = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(m.nodes, vec![]);
}

/// # Test reject_stale_term
#[tokio::test]
async fn test_handle_raft_event_case3_1_reject_stale_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_raft_event_case3_1_reject_stale_term",
        graceful_rx,
        None,
    );
    // Mock membership to return success
    let mut membership = MockMembership::new();
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

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::ClusterConfUpdate(request, resp_tx);

    state
        .handle_raft_event(
            raft_event,
            Arc::new(mock_peer_channels()),
            &context,
            mpsc::unbounded_channel().0,
        )
        .await
        .unwrap();

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);
    assert!(response.is_higher_term());
    assert_eq!(response.term, 5);
}

/// # Test update_step_down
#[tokio::test]
async fn test_handle_raft_event_case3_2_update_step_down() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_raft_event_case3_2_update_step_down",
        graceful_rx,
        None,
    );
    // Mock membership to return success
    let mut membership = MockMembership::new();
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

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConfUpdate(request, resp_tx);

    state
        .handle_raft_event(raft_event, Arc::new(mock_peer_channels()), &context, role_tx)
        .await
        .unwrap();

    // Verify step down to follower
    assert!(matches!(role_rx.try_recv(), Ok(RoleEvent::BecomeFollower(Some(2)))));
    assert!(matches!(role_rx.try_recv(), Ok(RoleEvent::ReprocessEvent(_))));
    assert!(resp_rx.recv().await.is_err()); // Original sender should not get response
}

/// # Case 4.1: As Leader, if I receive append request with (my_term >= append_entries_request.term), then I should reject the request
#[tokio::test]
async fn test_handle_raft_event_case4_1() {
    // Prepare Leader State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case4_1", graceful_rx, None);
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
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_entries_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Execute fun
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("should succeed");

    // Validate request should receive AppendEntriesResponse with success = false
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.is_higher_term());
}

/// # Case 4.2: As Leader, if I receive append request with (my_term < append_entries_request.term)
///
/// ## Criterias:
/// 1. I should step down as Follower(receive RoleEvent::BecomeFollower event)
/// 2. my term should be updated to the request one
/// 3. receive replay event
#[tokio::test]
async fn test_handle_raft_event_case4_2() {
    // Prepare Leader State
    enable_logger();

    // Prepare leader term smaller than request one
    let my_term = 10;
    let request_term = my_term + 1;

    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_raft_event_case4_2")
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
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_entries_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Test fun
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    // Validate criterias: step down as Follower
    assert!(matches!(role_rx.try_recv(), Ok(RoleEvent::BecomeFollower(_))));
    assert!(matches!(role_rx.try_recv().unwrap(), RoleEvent::ReprocessEvent(_)));

    // Validate no response received
    assert!(resp_rx.recv().await.is_err());
}

/// # Case 5.1: Test handle client propose request
///     if process_raft_request returns Ok()
#[tokio::test]
async fn test_handle_raft_event_case5_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case5_1", graceful_rx, None);
    // Setup replication handler to return success
    context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::from([(2, PeerUpdate::success(5, 6))]),
            })
        });
    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(|_, _, _| Some(5));
    context.storage.raft_log = Arc::new(raft_log);

    // New state
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Handle raft event
    let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::ClientPropose(
        ClientWriteRequest {
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
///     if process_raft_request returns Err()
#[tokio::test]
async fn test_handle_raft_event_case5_2() {}

/// # Case 6.1: Test ClientReadRequest event
///     if both peers failed to confirm leader's commit, the lread request
/// should be failed
#[tokio::test]
async fn test_handle_raft_event_case6_1() {
    enable_logger();
    // Prepare Leader State
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::Fatal("".to_string())));

    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_raft_event_case6_1")
        .with_replication_handler(replication_handler)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare request
    let keys = vec![safe_kv(1).to_vec()];

    let client_read_request = ClientReadRequest {
        client_id: 1,
        linear: true,
        keys,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("should succeed");

    let e = resp_rx.recv().await.unwrap().unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition);
}

/// # Case 6.2: Test ClientReadRequest event
///     if majority peers confirms, the response should be success
///
/// ## Preparaiton setup
/// 1. Leader current commit is 1
/// 2. calculate_majority_matched_index return Some(3), 3 is new commit index
/// 3. handle_raft_request_in_batch returns Ok(AppendResults{})
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
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
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
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_raft_event_case6_2")
        .with_raft_log(raft_log)
        .with_replication_handler(replication_handler)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare request
    let keys = vec![safe_kv(1).to_vec()];
    let client_read_request = ClientReadRequest {
        client_id: 1,
        linear: true,
        keys,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("should succeed");

    // Validation criteria 1: Leader commit should be updated to: 3(new commit
    // index)
    assert_eq!(state.commit_index(), expect_new_commit_index);

    // Validation criteria 2: event "RoleEvent::NotifyNewCommitIndex" should be
    // received
    assert!(resp_rx.recv().await.unwrap().is_ok());

    // Validation criteria 3: resp_rx receives Ok()
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

/// # Case 6.3: Test ClientReadRequest event
///     if higher term found during the replication,
///
/// ## Preparaiton setup
/// 1. Leader current commit is 1
/// 2. calculate_majority_matched_index return Some(3), 3 is new commit index
/// 3. handle_raft_request_in_batch returns Err(Error::HigherTermFoundError)
///
/// ## Validation criterias:
/// 1. Leader commit should still be: 1(new commit index)
/// 2. event "RoleEvent::BecomeFollower" should be received
/// 3. resp_rx receives Err(e)
#[tokio::test]
async fn test_handle_raft_event_case6_3() {
    enable_logger();
    // Prepare Leader State
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(move |_, _, _, _, _| {
            Err(Error::Consensus(ConsensusError::Replication(
                ReplicationError::HigherTerm(1),
            )))
        });

    let expect_new_commit_index = 3;
    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(move |_, _, _| Some(expect_new_commit_index));

    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_raft_event_case6_3")
        .with_replication_handler(replication_handler)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_commit_index(1).expect("should succeed");

    // Prepare request
    let keys = vec![safe_kv(1).to_vec()];
    let client_read_request = ClientReadRequest {
        client_id: 1,
        linear: true,
        keys,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("should succeed");

    // Validation criteria 1: Leader commit should be updated to: 3(new commit
    // index)
    assert_eq!(state.commit_index(), 1);

    // Validation criteria 2: event "RoleEvent::BecomeFollower" should be received
    let event = role_rx.try_recv().unwrap();
    assert!(matches!(event, RoleEvent::BecomeFollower(None)));

    // Validation criteria 3: resp_rx receives Err(e)
    assert!(resp_rx.recv().await.unwrap().is_err());
}

#[tokio::test]
async fn test_handle_raft_event_case7() {
    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_raft_event_case7")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();

    let stream = crate_test_snapshot_stream(vec![create_test_chunk(0, b"chunk0", 1, 1, 2)]);
    let raft_event = RaftEvent::InstallSnapshotChunk(Box::new(stream), resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_err());

    // Validation criteria 1: The response should return an error
    // Assert that resp_rx receives permission_denied
    let e = resp_rx.recv().await.unwrap().unwrap_err();
    assert!(matches!(e.code(), Code::PermissionDenied));

    // Validation criteria 2: No role event should be triggered
    assert!(role_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_handle_raft_event_case8() {
    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_handle_raft_event_case8")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();

    let raft_event = RaftEvent::RaftLogCleanUp(
        PurgeLogRequest {
            term: 1,
            leader_id: 1,
            leader_commit: 1,
            last_included: None,
            snapshot_checksum: vec![],
        },
        resp_tx,
    );
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_err());

    // Validation criteria 1: The response should return an error
    let e = resp_rx.recv().await.unwrap().unwrap_err();
    assert!(matches!(e.code(), Code::PermissionDenied));

    // Validation criteria 2: No role event should be triggered
    assert!(role_rx.try_recv().is_err());
}

/// # Case 1: test handle create_snapshot with transport error
#[tokio::test]
async fn test_handle_raft_event_case9_1() {
    enable_logger();

    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case9_1");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler
    let mut state_machine = MockStateMachineHandler::new();
    state_machine.expect_create_snapshot().returning(move || {
        Ok((
            SnapshotMetadata {
                last_included: Some(LogId { term: 3, index: 100 }),
                checksum: "checksum".into(),
            },
            case_path.to_path_buf(),
        ))
    });
    context.handlers.state_machine_handler = Arc::new(state_machine);

    // Prepare leader peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_ROLE_STATE_PORT_BASE + 1, rx1, true)
        .await
        .expect("should succeed");

    // Prepare AppendResults
    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![ChannelWithAddressAndRole {
            id: 2,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        }]
    });
    context.membership = Arc::new(membership);

    let mut transport = MockTransport::new();
    transport
        .expect_send_purge_requests()
        .times(1)
        .returning(|_, _, _| Err(Error::Fatal("Mock transport error".to_string())));
    context.transport = Arc::new(transport);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    let raft_event = RaftEvent::CreateSnapshotEvent;
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_err());

    // Validation criteria 2: No role event should be triggered
    assert!(role_rx.try_recv().is_err());
}

/// # Case2: test handle create_snapshot successful
#[tokio::test]
async fn test_handle_raft_event_case9_2() {
    enable_logger();

    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case9_2");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler
    let mut state_machine = MockStateMachineHandler::new();
    state_machine.expect_create_snapshot().returning(move || {
        Ok((
            SnapshotMetadata {
                last_included: Some(LogId { term: 3, index: 100 }),
                checksum: "checksum".into(),
            },
            case_path.to_path_buf(),
        ))
    });
    context.handlers.state_machine_handler = Arc::new(state_machine);

    // Mock peer configuration
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_ROLE_STATE_PORT_BASE + 2, rx1, true)
        .await
        .expect("should succeed");

    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![ChannelWithAddressAndRole {
            id: 2,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        }]
    });
    context.membership = Arc::new(membership);

    // Mock transport with successful response
    let mut transport = MockTransport::new();
    transport.expect_send_purge_requests().times(1).returning(|_, _, _| {
        Ok(vec![Ok(PurgeLogResponse {
            node_id: 2,
            term: 3,
            success: true,
            last_purged: Some(LogId { term: 3, index: 100 }),
        })])
    });
    context.transport = Arc::new(transport);

    // Mock purge executor
    let mut purge_executor = MockPurgeExecutor::new();
    purge_executor
        .expect_execute_purge()
        .with(eq(LogId { term: 3, index: 100 }))
        .times(1)
        .returning(|_| Ok(()));
    context.handlers.purge_executor = purge_executor;

    // Prepare leader state
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    state.update_current_term(3);
    state.shared_state.commit_index = 150; // Commit index > snapshot index
    state.last_purged_index = Some(LogId { term: 2, index: 80 });
    state.peer_purge_progress.insert(2, 100); // Peer has progressed beyond snapshot

    // Trigger event
    let raft_event = RaftEvent::CreateSnapshotEvent;
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("Should succeed");

    // Validate state updates
    assert_eq!(state.scheduled_purge_upto, Some(LogId { term: 3, index: 100 }));
    assert_eq!(state.last_purged_index, Some(LogId { term: 3, index: 100 }));
    assert_eq!(state.peer_purge_progress.get(&2), Some(&100));
}

/// # Case3: Test snapshot creation failure scenario
#[tokio::test]
async fn test_handle_raft_event_case9_3() {
    enable_logger();

    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case9_3");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler to return error
    let mut state_machine = MockStateMachineHandler::new();
    state_machine
        .expect_create_snapshot()
        .returning(move || Err(StorageError::Snapshot("Test failure".to_string()).into()));
    context.handlers.state_machine_handler = Arc::new(state_machine);

    // Prepare leader state
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    state.update_current_term(3);
    state.shared_state.commit_index = 150;

    // Trigger event
    let raft_event = RaftEvent::CreateSnapshotEvent;
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Execute and validate error is handled gracefully
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("Should handle error without crashing");

    // Validate no state changes occurred
    assert!(state.scheduled_purge_upto.is_none());
    assert_eq!(state.last_purged_index, None);
    assert!(state.peer_purge_progress.is_empty());
}

/// # Case4: Test higher term response triggering leader step-down
#[tokio::test]
async fn test_handle_raft_event_case9_4() {
    enable_logger();

    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case9_4");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler
    let mut state_machine = MockStateMachineHandler::new();
    state_machine.expect_create_snapshot().returning(move || {
        Ok((
            SnapshotMetadata {
                last_included: Some(LogId { term: 3, index: 100 }),
                checksum: "checksum".into(),
            },
            case_path.to_path_buf(),
        ))
    });
    context.handlers.state_machine_handler = Arc::new(state_machine);

    // Mock peer configuration
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_ROLE_STATE_PORT_BASE + 4, rx1, true)
        .await
        .expect("should succeed");

    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![ChannelWithAddressAndRole {
            id: 2,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
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
            last_purged: Some(LogId { term: 3, index: 100 }),
        })])
    });
    context.transport = Arc::new(transport);

    // Prepare leader state
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    state.update_current_term(3);
    state.shared_state.commit_index = 150;
    state.last_purged_index = Some(LogId { term: 2, index: 80 });
    state.peer_purge_progress.insert(2, 100);

    // Trigger event
    let raft_event = RaftEvent::CreateSnapshotEvent;
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("Should handle higher term response");

    // Validate leader stepped down
    let event = role_rx.try_recv().expect("Should receive step down event");
    assert!(matches!(event, RoleEvent::BecomeFollower(None)));

    // Validate term was updated
    assert_eq!(state.current_term(), 4);
}

/// # Case5: Test purge preconditions not met (commit index < snapshot index)
#[tokio::test]
async fn test_handle_raft_event_case9_5() {
    enable_logger();

    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case9_5");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler
    let mut state_machine = MockStateMachineHandler::new();
    state_machine.expect_create_snapshot().returning(move || {
        Ok((
            SnapshotMetadata {
                last_included: Some(LogId { term: 3, index: 100 }),
                checksum: "checksum".into(),
            },
            case_path.to_path_buf(),
        ))
    });
    context.handlers.state_machine_handler = Arc::new(state_machine);

    // Mock peer configuration
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_ROLE_STATE_PORT_BASE + 5, rx1, true)
        .await
        .expect("should succeed");

    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![ChannelWithAddressAndRole {
            id: 2,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        }]
    });
    context.membership = Arc::new(membership);

    // Mock transport with successful response
    let mut transport = MockTransport::new();
    transport.expect_send_purge_requests().times(1).returning(|_, _, _| {
        Ok(vec![Ok(PurgeLogResponse {
            node_id: 2,
            term: 3,
            success: true,
            last_purged: Some(LogId { term: 3, index: 100 }),
        })])
    });
    context.transport = Arc::new(transport);

    // Prepare leader state where commit index < snapshot index
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config());
    state.update_current_term(3);
    state.shared_state.commit_index = 90; // Commit index < snapshot index (100)
    state.last_purged_index = Some(LogId { term: 2, index: 80 });
    state.peer_purge_progress.insert(2, 100);

    // Trigger event
    let raft_event = RaftEvent::CreateSnapshotEvent;
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("Should skip purge");

    // Validate scheduled_purge_upto was NOT set
    assert!(state.scheduled_purge_upto.is_none());
}

/// # Case6: Test peer purge progress tracking
#[tokio::test]
async fn test_handle_raft_event_case9_6() {
    enable_logger();

    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case9_6");

    // Setup
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler
    let mut state_machine = MockStateMachineHandler::new();
    state_machine.expect_create_snapshot().returning(move || {
        Ok((
            SnapshotMetadata {
                last_included: Some(LogId { term: 3, index: 100 }),
                checksum: "checksum".into(),
            },
            case_path.to_path_buf(),
        ))
    });
    context.handlers.state_machine_handler = Arc::new(state_machine);

    // Mock peer configuration (multiple peers)
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_ROLE_STATE_PORT_BASE + 6, rx1, true)
        .await
        .expect("should succeed");

    let (_tx2, rx2) = oneshot::channel::<()>();
    let addr2 = MockNode::simulate_mock_service_without_reps(MOCK_ROLE_STATE_PORT_BASE + 7, rx2, true)
        .await
        .expect("should succeed");

    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![
            ChannelWithAddressAndRole {
                id: 2,
                channel_with_address: addr1.clone(),
                role: FOLLOWER,
            },
            ChannelWithAddressAndRole {
                id: 3,
                channel_with_address: addr2.clone(),
                role: FOLLOWER,
            },
        ]
    });
    context.membership = Arc::new(membership);

    // Mock transport with mixed responses
    let mut transport = MockTransport::new();
    transport.expect_send_purge_requests().times(1).returning(|_, _, _| {
        Ok(vec![
            Ok(PurgeLogResponse {
                node_id: 2,
                term: 3,
                success: true,
                last_purged: Some(LogId { term: 3, index: 100 }),
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

    // Trigger event
    let raft_event = RaftEvent::CreateSnapshotEvent;
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("Should succeed");

    // Validate peer purge progress tracking
    assert_eq!(state.peer_purge_progress.get(&2), Some(&100));
    assert_eq!(state.peer_purge_progress.get(&3), Some(&95));
}

#[test]
fn test_state_size() {
    assert!(size_of::<LeaderState<RaftTypeConfig>>() < 312);
}

/// # Case 1: Valid purge conditions with cluster consensus
#[test]
fn test_can_purge_logs_case1() {
    let node_config = Arc::new(node_config("/tmp/test_can_purge_logs_case1"));
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
    assert!(state.can_purge_logs(Some(LogId { index: 90, term: 1 }), LogId { index: 99, term: 1 }));

    // Violate gap rule: 100 not < 100
    assert!(!state.can_purge_logs(Some(LogId { index: 90, term: 1 }), LogId { index: 100, term: 1 }));
}

/// # Case 2: Reject uncommitted purge (Raft §5.4.2)
#[test]
fn test_can_purge_logs_case2() {
    let node_config = Arc::new(node_config("/tmp/test_can_purge_logs_case2"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, node_config);

    state.shared_state.commit_index = 50;
    state.peer_purge_progress.insert(2, 100);

    // Attempt to purge beyond commit index
    assert!(!state.can_purge_logs(
        Some(LogId { index: 40, term: 1 }),
        LogId { index: 51, term: 1 } // 51 > commit_index(50)
    ));

    // Boundary violation: 50 == commit_index (requires <)
    assert!(!state.can_purge_logs(Some(LogId { index: 40, term: 1 }), LogId { index: 50, term: 1 }));
}

/// # Case 3: Enforce purge sequence monotonicity (Raft §7.2)
#[test]
fn test_can_purge_logs_case3() {
    let node_config = Arc::new(node_config("/tmp/test_can_purge_logs_case3"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, node_config);

    state.shared_state.commit_index = 200;
    state.peer_purge_progress.insert(2, 200);
    state.peer_purge_progress.insert(3, 200);

    // Valid sequence: 100 → 150 → 199
    assert!(state.can_purge_logs(Some(LogId { index: 150, term: 1 }), LogId { index: 199, term: 1 }));

    // Invalid backward purge (150 → 120)
    assert!(!state.can_purge_logs(Some(LogId { index: 150, term: 1 }), LogId { index: 120, term: 1 }));

    // Same index purge attempt
    assert!(!state.can_purge_logs(Some(LogId { index: 150, term: 1 }), LogId { index: 150, term: 1 }));
}

/// # Case 4: Cluster progress verification (Enhanced durability check)
#[test]
fn test_can_purge_logs_case4() {
    let node_config = Arc::new(node_config("/tmp/test_can_purge_logs_case4"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, node_config);

    state.shared_state.commit_index = 100;

    // Single lagging peer (index 99 < 100)
    state.peer_purge_progress.insert(2, 99);
    state.peer_purge_progress.insert(3, 100);
    assert!(state.can_purge_logs(Some(LogId { index: 90, term: 1 }), LogId { index: 99, term: 1 }));

    // All peers at required index
    state.peer_purge_progress.insert(2, 100);
    assert!(state.can_purge_logs(Some(LogId { index: 90, term: 1 }), LogId { index: 99, term: 1 }));
}

/// # Case 5: Initial purge state validation
#[test]
fn test_can_purge_logs_case5() {
    let node_config = Arc::new(node_config("/tmp/test_can_purge_logs_case6"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, node_config);

    state.shared_state.commit_index = 100;
    state.peer_purge_progress.insert(2, 100);
    state.peer_purge_progress.insert(3, 100);

    // First purge (last_purge_index = None)
    assert!(state.can_purge_logs(None, LogId { index: 99, term: 1 }));

    // Must still respect commit_index gap
    assert!(!state.can_purge_logs(
        None,
        LogId { index: 100, term: 1 } // 100 not < 100
    ));
}

/// # Case 1: Quorum achieved
/// - All peers respond successfully
/// - Commit index should advance
/// - Clients receive success responses
#[tokio::test]
async fn test_process_batch_case1_quorum_achieved() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_case1_quorum_achieved", graceful_rx).await;

    // Mock replication to return success with quorum
    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: 6,
                            next_index: 7,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: 6,
                            next_index: 7,
                            success: true,
                        },
                    ),
                ]),
            })
        });
    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(|_, _, _| Some(6));
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    // Prepare batch of 2 requests
    let (tx1, rx1) = MaybeCloneOneshot::new();
    let (tx2, rx2) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);
    let receivers = vec![rx1, rx2];

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let result = context
        .state
        .process_batch(batch, &role_tx, &context.raft_context, Arc::new(mock_peer_channels()))
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.commit_index(), 6);
    assert!(matches!(role_rx.try_recv(), Ok(RoleEvent::NotifyNewCommitIndex(_))));

    // Verify client responses
    for mut rx in receivers {
        let response = rx.recv().await.unwrap().unwrap();
        assert!(response.is_write_success());
    }
}

/// # Case 2.1: Quorum NOT achieved (verifiable)
/// - Majority of peers responded but quorum not achieved
/// - Clients receive RetryRequired responses
#[tokio::test]
async fn test_process_batch_case2_quorum_failed() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_case2_1_quorum_failed", graceful_rx).await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
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
                            match_index: 0,
                            next_index: 1,
                            success: false,
                        },
                    ), // Failed
                ]),
            })
        });

    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    let (tx2, mut rx2) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);
    let result = context
        .state
        .process_batch(
            batch,
            &mpsc::unbounded_channel().0,
            &context.raft_context,
            Arc::new(mock_peer_channels()),
        )
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.commit_index(), 5); // Should not advance

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_retry_required());
    let response = rx2.recv().await.unwrap().unwrap();
    assert!(response.is_retry_required());
}

/// # Case 2.2: Quorum NOT achieved (non-verifiable)
/// - Less than majority of peers responded
/// - Clients receive ProposeFailed responses
#[tokio::test]
async fn test_process_batch_case2_2_quorum_non_verifiable_failure() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = setup_process_batch_test_context(
        "/tmp/test_process_batch_case2_2_quorum_non_verifiable_failure",
        graceful_rx,
    )
    .await;

    let peer2_id = 2;
    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(move |_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates: HashMap::from([(
                    peer2_id,
                    PeerUpdate {
                        match_index: 5,
                        next_index: 6,
                        success: true,
                    },
                )]),
            })
        });
    // Mock peer configuration (multiple peers)
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_ROLE_STATE_PORT_BASE + 8, rx1, true)
        .await
        .expect("should succeed");

    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![
            ChannelWithAddressAndRole {
                id: 2,
                channel_with_address: addr1.clone(),
                role: FOLLOWER,
            },
            ChannelWithAddressAndRole {
                id: 3,
                channel_with_address: addr1.clone(),
                role: FOLLOWER,
            },
        ]
    });
    context.raft_context.membership = Arc::new(membership);

    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    let (tx2, mut rx2) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);

    let result = context
        .state
        .process_batch(
            batch,
            &mpsc::unbounded_channel().0,
            &context.raft_context,
            Arc::new(mock_peer_channels()),
        )
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.commit_index(), 5); // Should not advance

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_propose_failure());
    let response = rx2.recv().await.unwrap().unwrap();
    assert!(response.is_propose_failure());
}

/// # Case 3: Higher term detected
/// - Follower responds with higher term
/// - Leader should step down
/// - Clients receive TermOutdated responses
#[tokio::test]
async fn test_process_batch_case3_higher_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = setup_process_batch_test_context("/tmp/test_process_batch_case3_higher_term", graceful_rx).await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Err(Error::Consensus(ConsensusError::Replication(
                ReplicationError::HigherTerm(10), // Higher term
            )))
        });

    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let result = context
        .state
        .process_batch(batch, &role_tx, &context.raft_context, Arc::new(mock_peer_channels()))
        .await;

    assert!(result.is_err());
    assert_eq!(context.state.current_term(), 10);
    assert!(matches!(role_rx.try_recv(), Ok(RoleEvent::BecomeFollower(_))));

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_term_outdated());
}

/// # Case 4: Partial timeouts (non-verifiable failure)
/// - Some peers succeed, some time out
/// - Clients receive ProposeFailed responses
#[tokio::test]
async fn test_process_batch_case4_partial_timeouts() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_case4_partial_timeouts", graceful_rx).await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates: HashMap::from([(
                    2,
                    PeerUpdate {
                        match_index: 6,
                        next_index: 7,
                        success: true,
                    },
                )]),
            })
        });

    // Prepare leader peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_ROLE_STATE_PORT_BASE + 9, rx1, true)
        .await
        .expect("should succeed");

    // Prepare AppendResults
    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![
            ChannelWithAddressAndRole {
                id: 2,
                channel_with_address: addr1.clone(),
                role: FOLLOWER,
            },
            ChannelWithAddressAndRole {
                id: 3,
                channel_with_address: addr1.clone(),
                role: FOLLOWER,
            },
        ]
    });
    context.raft_context.membership = Arc::new(membership);

    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let result = context
        .state
        .process_batch(
            batch,
            &mpsc::unbounded_channel().0,
            &context.raft_context,
            Arc::new(mock_peer_channels()),
        )
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.commit_index(), 5); // Initial commit index

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_propose_failure());
}

/// # Case 5: All peers timeout (non-verifiable failure)
/// - No successful responses
/// - Commit index unchanged
/// - Clients receive failure responses
#[tokio::test]
async fn test_process_batch_case5_all_timeout() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = setup_process_batch_test_context("/tmp/test_process_batch_case5_all_timeout", graceful_rx).await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates: HashMap::from([]),
            })
        });

    // Prepare leader peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_ROLE_STATE_PORT_BASE + 10, rx1, true)
        .await
        .expect("should succeed");

    // Prepare AppendResults
    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![
            ChannelWithAddressAndRole {
                id: 2,
                channel_with_address: addr1.clone(),
                role: FOLLOWER,
            },
            ChannelWithAddressAndRole {
                id: 3,
                channel_with_address: addr1.clone(),
                role: FOLLOWER,
            },
        ]
    });
    context.raft_context.membership = Arc::new(membership);

    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let result = context
        .state
        .process_batch(
            batch,
            &mpsc::unbounded_channel().0,
            &context.raft_context,
            Arc::new(mock_peer_channels()),
        )
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.commit_index(), 5); // Unchanged

    let response = rx1.recv().await.unwrap().unwrap();
    println!("----- {:?}", &response);
    assert!(response.is_propose_failure());
}

/// # Case 6: Fatal error during replication
/// - Storage failure or unrecoverable error
/// - Clients receive failure responses
#[tokio::test]
async fn test_process_batch_case6_fatal_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = setup_process_batch_test_context("/tmp/test_process_batch_case6_fatal_error", graceful_rx).await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::Fatal("Storage failure".to_string())));

    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let result = context
        .state
        .process_batch(
            batch,
            &mpsc::unbounded_channel().0,
            &context.raft_context,
            Arc::new(mock_peer_channels()),
        )
        .await;

    assert!(result.is_err());

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_propose_failure());
}

// Helper functions
async fn setup_process_batch_test_context(
    path: &str,
    graceful_rx: watch::Receiver<()>,
) -> ProcessRaftRequestTestContext {
    let context = mock_raft_context(path, graceful_rx, None);
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_commit_index(5).unwrap(); // Initial commit index

    ProcessRaftRequestTestContext {
        state,
        raft_context: context,
    }
}

struct ProcessBatchTestContext {
    state: LeaderState<MockTypeConfig>,
    raft_context: RaftContext<MockTypeConfig>,
}

type ClientResponseResult = std::result::Result<ClientResponse, Status>;

fn mock_request(sender: MaybeCloneOneshotSender<ClientResponseResult>) -> RaftRequestWithSignal {
    RaftRequestWithSignal {
        id: nanoid!(),
        payloads: vec![],
        sender,
    }
}

/// # Case 1: Quorum achieved
/// - All peers respond successfully
/// - Returns `Ok(QuorumVerificationResult::Success)`
#[tokio::test]
async fn test_verify_internal_quorum_case1_quorum_achieved() {
    let payloads = vec![EntryPayload::command(vec![])];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_case1_quorum_achieved",
        graceful_rx,
        None,
    );

    // Setup replication handler to return success
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::from([(2, PeerUpdate::success(5, 6)), (3, PeerUpdate::success(5, 6))]),
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(|_, _, _| Some(5));
    raft_context.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = state
        .verify_internal_quorum(payloads, true, &raft_context, peer_channels, &role_tx)
        .await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::Success);
    assert!(matches!(role_rx.try_recv(), Ok(RoleEvent::NotifyNewCommitIndex(_))));
}

/// # Case 2: Quorum NOT achieved (verifiable)
/// - Majority of peers responded but quorum not achieved
/// - Returns `Ok(QuorumVerificationResult::RetryRequired)`
#[tokio::test]
async fn test_verify_internal_quorum_case2_verifiable_failure() {
    let payloads = vec![EntryPayload::command(vec![])];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_case2_verifiable_failure",
        graceful_rx,
        None,
    );

    // Setup replication handler to return verifiable failure
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates: HashMap::from([(2, PeerUpdate::success(5, 6)), (3, PeerUpdate::failed())]),
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state
        .verify_internal_quorum(payloads, true, &raft_context, peer_channels, &role_tx)
        .await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::RetryRequired);
}

/// # Case 3: Quorum NOT achieved (non-verifiable)
/// - Less than majority of peers responded
/// - Returns `Ok(QuorumVerificationResult::LeadershipLost)`
#[tokio::test]
async fn test_verify_internal_quorum_case3_non_verifiable_failure() {
    let payloads = vec![EntryPayload::command(vec![])];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_case3_non_verifiable_failure",
        graceful_rx,
        None,
    );

    // Setup replication handler to return non-verifiable failure
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates: HashMap::from([(2, PeerUpdate::success(5, 6))]),
            })
        });

    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_ROLE_STATE_PORT_BASE + 11, rx1, true)
        .await
        .expect("should succeed");

    // Prepare AppendResults
    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(move |_| {
        vec![
            ChannelWithAddressAndRole {
                id: 2,
                channel_with_address: addr1.clone(),
                role: FOLLOWER,
            },
            ChannelWithAddressAndRole {
                id: 3,
                channel_with_address: addr1.clone(),
                role: FOLLOWER,
            },
            ChannelWithAddressAndRole {
                id: 4,
                channel_with_address: addr1.clone(),
                role: FOLLOWER,
            },
        ]
    });
    raft_context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state
        .verify_internal_quorum(payloads, true, &raft_context, peer_channels, &role_tx)
        .await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::LeadershipLost);
}

/// # Case 4: Partial timeouts
/// - Some peers respond, some time out
/// - Returns `Ok(QuorumVerificationResult::RetryRequired)`
#[tokio::test]
async fn test_verify_internal_quorum_case4_partial_timeouts() {
    let payloads = vec![EntryPayload::command(vec![])];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_case4_partial_timeouts",
        graceful_rx,
        None,
    );

    // Setup replication handler to return partial timeouts
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates: HashMap::from([
                    (2, PeerUpdate::success(5, 6)),
                    (3, PeerUpdate::failed()), // Timeout
                ]),
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state
        .verify_internal_quorum(payloads, true, &raft_context, peer_channels, &role_tx)
        .await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::RetryRequired);
}

/// # Case 5: All timeouts
/// - No peers respond
/// - Returns `Ok(QuorumVerificationResult::RetryRequired)`
#[tokio::test]
async fn test_verify_internal_quorum_case5_all_timeouts() {
    let payloads = vec![EntryPayload::command(vec![])];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context("/tmp/test_verify_internal_quorum_case5_all_timeouts", graceful_rx, None);

    // Setup replication handler to return all timeouts
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates: HashMap::from([(2, PeerUpdate::failed()), (3, PeerUpdate::failed())]),
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state
        .verify_internal_quorum(payloads, true, &raft_context, peer_channels, &role_tx)
        .await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::RetryRequired);
}

/// # Case 6: Higher term detected
/// - Follower responds with higher term
/// - Returns `Err(HigherTerm)`
#[tokio::test]
async fn test_verify_internal_quorum_case6_higher_term() {
    let payloads = vec![EntryPayload::command(vec![])];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context("/tmp/test_verify_internal_quorum_case6_higher_term", graceful_rx, None);

    // Setup replication handler to return higher term error
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Err(Error::Consensus(ConsensusError::Replication(
                ReplicationError::HigherTerm(10),
            )))
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = state
        .verify_internal_quorum(payloads, true, &raft_context, peer_channels, &role_tx)
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::Consensus(ConsensusError::Replication(ReplicationError::HigherTerm(10)))
    ));
    assert!(matches!(role_rx.try_recv(), Ok(RoleEvent::BecomeFollower(_))));
}

/// # Case 7: Critical failure
/// - System or logic error occurs
/// - Returns original error
#[tokio::test]
async fn test_verify_internal_quorum_case7_critical_failure() {
    let payloads = vec![EntryPayload::command(vec![])];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_case7_critical_failure",
        graceful_rx,
        None,
    );

    // Setup replication handler to return critical error
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::Fatal("Storage failure".to_string())));

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state
        .verify_internal_quorum(payloads, true, &raft_context, peer_channels, &role_tx)
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::Fatal(msg) if msg == "Storage failure"
    ));
}

async fn mock_peer(
    port: u64,
    rx: oneshot::Receiver<()>,
) -> ChannelWithAddress {
    MockNode::simulate_mock_service_without_reps(port, rx, true)
        .await
        .expect("should succeed")
}

/// # Case 1: Successful snapshot transfer
#[tokio::test]
async fn test_trigger_snapshot_transfer_case1_success() {
    enable_logger();
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_trigger_snapshot_transfer_case1_success", graceful_rx, None);
    let node_id = 2;

    // Mock state machine handler to return valid stream
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_load_snapshot_data()
        .times(1)
        .returning(|_| {
            let chunks = vec![create_test_chunk(0, b"data", 1, 1, 1)];
            let stream = crate_test_snapshot_stream(chunks);
            Ok(Box::pin(stream.map(|item| {
                item.map_err(|s| NetworkError::TonicStatusError(Box::new(s)).into())
            })))
        });
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock transport to succeed
    let mut transport = MockTransport::new();
    transport
        .expect_install_snapshot()
        .times(1)
        .returning(|_, _, _, _| Ok(()));
    context.transport = Arc::new(transport);

    // Mock peer channels
    let mut peer_channels = MockPeerChannels::new();
    peer_channels.expect_add_peer().returning(|_, _| Ok(()));

    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr = mock_peer(MOCK_LEADER_STATE_PORT_BASE + 1, rx1).await;
    peer_channels
        .expect_get_peer_channel()
        .returning(move |_| Some(addr.clone()));

    // Prepare leader state
    let state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 100, term: 1 }),
        checksum: vec![],
    };

    // Execute test
    let result = state
        .trigger_snapshot_transfer(node_id, metadata, &context, Arc::new(peer_channels))
        .await;

    assert!(result.is_ok(), "Transfer should initiate successfully");

    // Allow time for async task to complete
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// # Case 2: Peer connection not found
#[tokio::test]
async fn test_trigger_snapshot_transfer_case2_peer_not_found() {
    enable_logger();
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_trigger_snapshot_transfer_case2_peer_not_found",
        graceful_rx,
        None,
    );
    let node_id = 99; // Non-existent node

    // Mock state machine handler to return valid stream
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_load_snapshot_data()
        .times(0)
        .returning(|_| {
            let chunks = vec![create_test_chunk(0, b"data", 1, 1, 1)];
            let stream = crate_test_snapshot_stream(chunks);
            Ok(Box::pin(stream.map(|item| {
                item.map_err(|s| NetworkError::TonicStatusError(Box::new(s)).into())
            })))
        });
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Prepare leader state
    let state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 100, term: 1 }),
        checksum: vec![],
    };

    // Mock transport to succeed
    let mut transport = MockTransport::new();
    transport.expect_install_snapshot().times(0); // Should not be called
    context.transport = Arc::new(transport);

    // Mock peer channels to return None for non-existent node
    let mut peer_channels = MockPeerChannels::new();
    peer_channels.expect_add_peer().returning(|_, _| Ok(()));
    peer_channels
        .expect_get_peer_channel()
        .with(eq(node_id))
        .times(1)
        .returning(move |_| None);

    // Execute test
    let result = state
        .trigger_snapshot_transfer(node_id, metadata, &context, Arc::new(peer_channels))
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::System(SystemError::Network(NetworkError::PeerConnectionNotFound(99)))
    ));
}

/// # Case 3: Snapshot data load failure
#[tokio::test]
async fn test_trigger_snapshot_transfer_case3_load_failure() {
    enable_logger();
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_trigger_snapshot_transfer_case3_load_failure",
        graceful_rx,
        None,
    );
    let node_id = 2;

    // Mock state machine handler to fail
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_load_snapshot_data()
        .times(1)
        .returning(|_| Err(StorageError::Snapshot("Test failure".to_string()).into()));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock transport to succeed
    let mut peer_channels = MockPeerChannels::new();
    peer_channels.expect_add_peer().returning(|_, _| Ok(()));
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr = mock_peer(MOCK_LEADER_STATE_PORT_BASE + 3, rx1).await;
    peer_channels
        .expect_get_peer_channel()
        .returning(move |_| Some(addr.clone()));

    // Prepare leader state
    let state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 100, term: 1 }),
        checksum: vec![],
    };

    // Execute test
    let result = state
        .trigger_snapshot_transfer(node_id, metadata, &context, Arc::new(peer_channels))
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::System(SystemError::Storage(StorageError::Snapshot(_)))
    ));
}

/// # Case 4: Network transfer failure
#[tokio::test]
async fn test_trigger_snapshot_transfer_case4_network_failure() {
    enable_logger();
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_trigger_snapshot_transfer_case4_network_failure",
        graceful_rx,
        None,
    );
    let node_id = 2;

    // Mock state machine handler
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_load_snapshot_data()
        .times(1)
        .returning(|_| {
            let chunks = vec![create_test_chunk(0, b"data", 1, 1, 1)];
            let stream = crate_test_snapshot_stream(chunks);
            Ok(Box::pin(stream.map(|item| {
                item.map_err(|s| NetworkError::TonicStatusError(Box::new(s)).into())
            })))
        });
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock transport to fail
    let mut transport = MockTransport::new();
    transport
        .expect_install_snapshot()
        .times(1)
        .returning(|_, _, _, _| Err(NetworkError::SnapshotTransferFailed.into()));
    context.transport = Arc::new(transport);

    // Mock peer channels
    let mut peer_channels = MockPeerChannels::new();
    peer_channels.expect_add_peer().returning(|_, _| Ok(()));
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr = mock_peer(MOCK_LEADER_STATE_PORT_BASE + 4, rx1).await;
    peer_channels
        .expect_get_peer_channel()
        .returning(move |_| Some(addr.clone()));

    // Prepare leader state
    let state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 100, term: 1 }),
        checksum: vec![],
    };

    // Execute test
    let result = state
        .trigger_snapshot_transfer(node_id, metadata, &context, Arc::new(peer_channels))
        .await;

    assert!(result.is_ok(), "Should initiate transfer despite eventual failure");

    // Allow time for async task to complete
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// # Case 5: Large snapshot transfer
#[tokio::test]
async fn test_trigger_snapshot_transfer_case5_large_snapshot() {
    enable_logger();
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_trigger_snapshot_transfer_case5_large_snapshot",
        graceful_rx,
        None,
    );
    let node_id = 2;

    // Create large snapshot (10 chunks)
    let mut chunks = Vec::new();
    for i in 0..10 {
        chunks.push(create_test_chunk(
            i as u32,
            &vec![i as u8; 1024 * 1024], // 1MB per chunk
            1,
            1,
            10,
        ));
    }

    // Mock state machine handler
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_load_snapshot_data()
        .times(1)
        .returning(move |_| {
            let stream = crate_test_snapshot_stream(chunks.clone());
            Ok(Box::pin(stream.map(|item| {
                item.map_err(|s| NetworkError::TonicStatusError(Box::new(s)).into())
            })))
        });
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock transport to succeed
    let mut transport = MockTransport::new();
    transport
        .expect_install_snapshot()
        .times(1)
        .returning(|_, _, _, _| Ok(()));
    context.transport = Arc::new(transport);

    // Mock peer channels
    let mut peer_channels = MockPeerChannels::new();
    peer_channels.expect_add_peer().returning(|_, _| Ok(()));
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr = mock_peer(MOCK_LEADER_STATE_PORT_BASE + 5, rx1).await;
    peer_channels
        .expect_get_peer_channel()
        .returning(move |_| Some(addr.clone()));

    // Prepare leader state
    let state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 100, term: 1 }),
        checksum: vec![],
    };

    // Execute test
    let result = state
        .trigger_snapshot_transfer(node_id, metadata, &context, Arc::new(peer_channels))
        .await;

    assert!(result.is_ok(), "Should handle large snapshot");

    // Allow time for transfer
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// # Case 1: Successful join
#[tokio::test]
async fn test_handle_join_cluster_case1_success() {
    enable_logger();
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context("/tmp/test_handle_join_cluster_case1_success", graceful_rx, None);
    let node_id = 100;
    let address = "127.0.0.1:8080".to_string();

    // Mock membership// Prepare leader peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(MOCK_LEADER_STATE_PORT_BASE + 10, rx1, true)
        .await
        .expect("should succeed");
    let mut membership = MockMembership::new();
    membership.expect_contains_node().returning(|_| false);
    membership.expect_add_learner().returning(|_, _, _| Ok(()));
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(|| ClusterMembership::default());
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership.expect_update_node_status().returning(|_, _| Ok(()));
    membership.expect_voting_members().returning(move |_| {
        vec![ChannelWithAddressAndRole {
            id: 2,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        }]
    });
    raft_context.membership = Arc::new(membership);

    // Mock peer channels
    let mut peer_channels = MockPeerChannels::new();
    peer_channels.expect_add_peer().returning(|_, _| Ok(()));
    peer_channels.expect_get_peer_channel().returning(|_| {
        Some(ChannelWithAddress {
            address: "".to_string(),
            channel: Endpoint::from_static("http://[::]:50051").connect_lazy(),
        })
    });

    // -->  perpare verify_internal_quorum returns success
    // Setup replication handler to return success
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::from([(2, PeerUpdate::success(5, 6)), (3, PeerUpdate::success(5, 6))]),
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(|_, _, _| Some(5));
    raft_context.storage.raft_log = Arc::new(raft_log);
    let mut state_machine = MockStateMachine::new();
    state_machine.expect_snapshot_metadata().returning(move || {
        Some(SnapshotMetadata {
            last_included: Some(LogId { term: 1, index: 1 }),
            checksum: vec![],
        })
    });
    raft_context.storage.state_machine = Arc::new(state_machine);
    // <--

    // Mock state machine handler
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_get_latest_snapshot_metadata()
        .returning(|| None);
    state_machine_handler
        .expect_load_snapshot_data()
        .times(1)
        .returning(|_| {
            let chunks = vec![create_test_chunk(0, b"data", 1, 1, 1)];
            let stream = crate_test_snapshot_stream(chunks);
            Ok(Box::pin(stream.map(|item| {
                item.map_err(|s| NetworkError::TonicStatusError(Box::new(s)).into())
            })))
        });
    raft_context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock quorum verification
    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config.clone());

    let (sender, mut receiver) = MaybeCloneOneshot::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                node_id,
                address: address.clone(),
            },
            sender,
            &raft_context,
            Arc::new(peer_channels),
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_ok());
    let response = receiver.recv().await.unwrap().unwrap();
    assert!(response.success);
    assert_eq!(response.leader_id, 1);
}

/// # Case 2: Node already exists
#[tokio::test]
async fn test_handle_join_cluster_case2_node_exists() {
    enable_logger();
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_join_cluster_case2_node_exists", graceful_rx, None);
    let node_id = 100;

    // Mock membership to report existing node
    let mut membership = MockMembership::new();
    membership.expect_contains_node().returning(|_| true);
    let context = RaftContext {
        membership: Arc::new(membership),
        ..context
    };

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (sender, mut receiver) = MaybeCloneOneshot::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                node_id,
                address: "127.0.0.1:8080".to_string(),
            },
            sender,
            &context,
            Arc::new(mock_peer_channels()),
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_err());
    let status = receiver.recv().await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::Internal);
    assert!(status.message().contains("already been added into cluster config"));
}

/// # Case 3: Quorum not achieved
#[tokio::test]
async fn test_handle_join_cluster_case3_quorum_failed() {
    enable_logger();
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_join_cluster_case3_quorum_failed", graceful_rx, None);
    let node_id = 100;

    // Mock membership
    let mut membership = MockMembership::new();
    membership.expect_contains_node().returning(|_| false);
    membership.expect_add_learner().returning(|_, _, _| Ok(()));
    membership.expect_voting_members().returning(|_| vec![]); // Empty voting members will cause quorum failure
    context.membership = Arc::new(membership);

    // Setup replication handler to simulate quorum not achieved
    context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates: HashMap::new(),
            })
        });

    let mut peer_channels = MockPeerChannels::new();
    peer_channels.expect_add_peer().returning(|_, _| Ok(()));
    peer_channels.expect_get_peer_channel().returning(|_| {
        Some(ChannelWithAddress {
            address: "".to_string(),
            channel: Endpoint::from_static("http://[::]:50051").connect_lazy(),
        })
    });

    // Mock quorum verification to fail
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (sender, mut receiver) = MaybeCloneOneshot::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                node_id,
                address: "127.0.0.1:8080".to_string(),
            },
            sender,
            &context,
            Arc::new(peer_channels),
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_err());
    let status = receiver.recv().await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::Internal);
    assert!(status.message().contains("Commit Timeout"));
}

/// # Case 4: Quorum verification error
#[tokio::test]
async fn test_handle_join_cluster_case4_quorum_error() {
    enable_logger();
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_join_cluster_case4_quorum_error", graceful_rx, None);
    let node_id = 100;

    // Mock membership
    let mut membership = MockMembership::new();
    membership.expect_contains_node().returning(|_| false);
    membership.expect_add_learner().returning(|_, _, _| Ok(()));
    membership.expect_voting_members().returning(|_| {
        vec![ChannelWithAddressAndRole {
            id: 2,
            channel_with_address: ChannelWithAddress {
                address: "".to_string(),
                channel: Endpoint::from_static("http://[::]:50051").connect_lazy(),
            },
            role: FOLLOWER,
        }]
    });
    context.membership = Arc::new(membership);
    // Setup replication handler to return error
    context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::Fatal("Test error".to_string())));

    let mut peer_channels = MockPeerChannels::new();
    peer_channels.expect_add_peer().returning(|_, _| Ok(()));
    peer_channels.expect_get_peer_channel().returning(|_| {
        Some(ChannelWithAddress {
            address: "".to_string(),
            channel: Endpoint::from_static("http://[::]:50051").connect_lazy(),
        })
    });

    // Mock quorum verification to return error
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (sender, mut receiver) = MaybeCloneOneshot::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                node_id,
                address: "127.0.0.1:8080".to_string(),
            },
            sender,
            &context,
            Arc::new(peer_channels),
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_err());
    let status = receiver.recv().await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::Internal);
}

/// # Case 5: Snapshot transfer triggered
#[tokio::test]
async fn test_handle_join_cluster_case5_snapshot_triggered() {
    enable_logger();
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_join_cluster_case5_snapshot_triggered",
        graceful_rx,
        None,
    );
    let node_id = 100;
    let address = "127.0.0.1:8080".to_string();

    // Mock membership
    let mut membership = MockMembership::new();
    membership.expect_contains_node().returning(|_| false);
    membership.expect_add_learner().returning(|_, _, _| Ok(()));
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(|| ClusterMembership::default());
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership.expect_update_node_status().returning(|_, _| Ok(()));
    membership.expect_voting_members().returning(|_| {
        vec![ChannelWithAddressAndRole {
            id: 2,
            channel_with_address: ChannelWithAddress {
                address: "".to_string(),
                channel: Endpoint::from_static("http://[::]:50051").connect_lazy(),
            },
            role: FOLLOWER,
        }]
    });
    context.membership = Arc::new(membership);

    // Mock state machine handler to return snapshot metadata
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_get_latest_snapshot_metadata()
        .returning(|| {
            Some(SnapshotMetadata {
                last_included: Some(LogId { index: 100, term: 1 }),
                checksum: vec![],
            })
        });
    state_machine_handler
        .expect_load_snapshot_data()
        .times(1)
        .returning(|_| {
            let chunks = vec![create_test_chunk(0, b"data", 1, 1, 1)];
            let stream = crate_test_snapshot_stream(chunks);
            Ok(Box::pin(stream.map(|item| {
                item.map_err(|s| NetworkError::TonicStatusError(Box::new(s)).into())
            })))
        });
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Setup replication handler to return success
    context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::from([(2, PeerUpdate::success(5, 6))]),
            })
        });

    // Mock transport
    let mut transport = MockTransport::new();
    transport
        .expect_install_snapshot()
        .times(0)
        .returning(|_, _, _, _| Ok(()));
    context.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(|_, _, _| Some(5));
    context.storage.raft_log = Arc::new(raft_log);
    let mut state_machine = MockStateMachine::new();
    state_machine.expect_snapshot_metadata().returning(move || {
        Some(SnapshotMetadata {
            last_included: Some(LogId { term: 1, index: 1 }),
            checksum: vec![],
        })
    });
    context.storage.state_machine = Arc::new(state_machine);
    let mut peer_channels = MockPeerChannels::new();
    peer_channels.expect_add_peer().returning(|_, _| Ok(()));
    peer_channels.expect_get_peer_channel().returning(|_| {
        Some(ChannelWithAddress {
            address: "".to_string(),
            channel: Endpoint::from_static("http://[::]:50051").connect_lazy(),
        })
    });

    // Mock quorum verification
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (sender, mut receiver) = MaybeCloneOneshot::new();
    let result = state
        .handle_join_cluster(
            JoinRequest { node_id, address },
            sender,
            &context,
            Arc::new(peer_channels),
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_ok());
    let response = receiver.recv().await.unwrap().unwrap();
    assert!(response.success);
}
