use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::StreamExt;
use mockall::predicate::eq;
use nanoid::nanoid;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tonic::transport::Endpoint;
use tonic::Code;
use tonic::Status;
use tracing_test::traced_test;

use super::leader_state::LeaderState;
use super::role_state::RaftRoleState;
use crate::client_command_to_entry_payloads;
use crate::convert::safe_kv;
use crate::proto::client::ClientReadRequest;
use crate::proto::client::ClientResponse;
use crate::proto::client::ClientWriteRequest;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::JoinRequest;
use crate::proto::cluster::LeaderDiscoveryRequest;
use crate::proto::cluster::MetadataRequest;
use crate::proto::cluster::NodeMeta;
use crate::proto::common::EntryPayload;
use crate::proto::common::LogId;
use crate::proto::common::NodeStatus;
use crate::proto::election::VoteRequest;
use crate::proto::election::VoteResponse;
use crate::proto::error::ErrorCode;
use crate::proto::replication::AppendEntriesRequest;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotMetadata;
use crate::test_utils::crate_test_snapshot_stream;
use crate::test_utils::create_test_chunk;
use crate::test_utils::mock_raft_context;
use crate::test_utils::node_config;
use crate::test_utils::setup_raft_components;
use crate::test_utils::MockBuilder;
use crate::test_utils::MockStorageEngine;
use crate::test_utils::MockTypeConfig;
use crate::AppendResults;
use crate::ConsensusError;
use crate::Error;
use crate::MaybeCloneOneshot;
use crate::MaybeCloneOneshotReceiver;
use crate::MaybeCloneOneshotSender;
use crate::MockMembership;
use crate::MockPurgeExecutor;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::MockStateMachine;
use crate::MockStateMachineHandler;
use crate::MockTransport;
use crate::NewCommitData;
use crate::PeerUpdate;
use crate::QuorumVerificationResult;
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftNodeConfig;
use crate::RaftOneshot;
use crate::RaftRequestWithSignal;
use crate::RaftTypeConfig;
use crate::ReplicationError;
use crate::RoleEvent;
use crate::SnapshotError;
use crate::FOLLOWER;
use crate::LEADER;
use crate::LEARNER;

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
    let mut raft_context =
        MockBuilder::new(shutdown_signal).wiht_node_config(node_config).build_context();

    let mut state = LeaderState::new(1, raft_context.node_config());
    state.update_commit_index(4).expect("Should succeed to update commit index");

    // Initialize the mock object
    let mut replication_handler = MockReplicationCore::new();
    let mut raft_log = MockRaftLog::new();

    //Configure mock behavior
    replication_handler
        .expect_handle_raft_request_in_batch()
        .times(handle_raft_request_in_batch_expect_times)
        .returning(move |_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: Some(5),
                            next_index: 6,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: Some(5),
                            next_index: 6,
                            success: true,
                        },
                    ),
                ]),
                learner_progress: HashMap::new(),
            })
        });

    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

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
pub async fn assert_client_response(
    mut rx: MaybeCloneOneshotReceiver<std::result::Result<ClientResponse, Status>>
) {
    match rx.recv().await {
        Ok(Ok(response)) => assert_eq!(
            ErrorCode::try_from(response.error).unwrap(),
            ErrorCode::Success
        ),
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
#[traced_test]
async fn test_process_raft_request_case1_1() {
    // Initialize the test environment (threshold = 0 means immediate execution)

    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut test_context = setup_process_raft_request_test_context(
        "/tmp/test_process_raft_request_case1_1",
        0,
        1,
        graceful_rx,
    )
    .await;

    // Prepare test request
    let request = ClientWriteRequest {
        client_id: 0,
        commands: vec![],
    };
    let (tx, rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

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
#[traced_test]
async fn test_process_raft_request_case1_2() {
    // Initialize the test environment (threshold = 0 means immediate execution)
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut test_context = setup_process_raft_request_test_context(
        "/tmp/test_process_raft_request_case1_2",
        0,
        2,
        graceful_rx,
    )
    .await;

    // Prepare test request
    let request = ClientWriteRequest {
        client_id: 0,
        commands: vec![],
    };
    let (tx1, rx1) = MaybeCloneOneshot::new();
    let (tx2, rx2) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

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
#[traced_test]
async fn test_process_raft_request_case2() {
    // let context = setup_raft_components("/tmp/test_process_raft_request_case2", None, false);
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut test_context = setup_process_raft_request_test_context(
        "/tmp/test_process_raft_request_case2",
        100,
        0,
        graceful_rx,
    )
    .await;

    // 1. Prepare mocks
    let client_propose_request = ClientWriteRequest {
        client_id: 0,
        commands: vec![],
    };

    // 2. Prepare voting members
    let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    assert!(test_context
        .state
        .process_raft_request(
            RaftRequestWithSignal {
                id: nanoid!(),
                payloads: client_command_to_entry_payloads(client_propose_request.commands),
                sender: resp_tx,
            },
            &test_context.raft_context,
            false,
            &role_tx
        )
        .await
        .is_ok());
}

/// # Case 1: state_machine_handler::update_pending should be executed once
/// if last_applied < commit_index
#[tokio::test]
#[traced_test]
async fn test_ensure_state_machine_upto_commit_index_case1() {
    // Prepare Leader State
    let context = setup_raft_components(
        "/tmp/test_ensure_state_machine_upto_commit_index_case1",
        None,
        false,
    );
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
#[traced_test]
async fn test_ensure_state_machine_upto_commit_index_case2() {
    // Prepare Leader State
    let context = setup_raft_components(
        "/tmp/test_ensure_state_machine_upto_commit_index_case2",
        None,
        false,
    );
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

/// # Case 1.1: Receive Vote Request Event
///     with my_term >= vote_request.term
///
/// ## Validate criterias
/// 1. receive response with vote_granted = false
/// 2. Role should not step to Follower
/// 3. Term should not be updated
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case1_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case1_1", graceful_rx, None);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let term_before = state.current_term();
    let request_term = term_before;

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
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

/// # Case 1.2: Receive Vote Request Event
///     with my_term < vote_request.term
///
/// ## Validate criterias
/// 1. Should not receive any response
/// 2. Step to Follower
/// 3. Term should be updated
/// 4. Replay raft event
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case1_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case1_2", graceful_rx, None);

    let updated_term = 100;

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = setup_handle_raft_event_case1_params(resp_tx, updated_term);

    let r = state.handle_raft_event(raft_event, &context, role_tx).await;
    println!("r:{r:?}");
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

/// # Case 2: Receive ClusterConf Event
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case2", graceful_rx, None);
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_retrieve_cluster_membership_config().times(1).returning(|| {
        ClusterMembership {
            version: 1,
            nodes: vec![],
        }
    });
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConf(MetadataRequest {}, resp_tx);

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let m = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(m.nodes, vec![]);
}

/// # Test reject_stale_term
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case3_1_reject_stale_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_raft_event_case3_1_reject_stale_term",
        graceful_rx,
        None,
    );
    // Mock membership to return success
    let mut membership = MockMembership::new();
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

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
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

/// # Test update_step_down
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case3_2_update_step_down() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_raft_event_case3_2_update_step_down",
        graceful_rx,
        None,
    );
    // Mock membership to return success
    let mut membership = MockMembership::new();
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

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
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

/// # Case 4.1: As Leader, if I receive append request with (my_term >= append_entries_request.term), then I should reject the request
#[tokio::test]
#[traced_test]
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

/// # Case 4.2: As Leader, if I receive append request with (my_term < append_entries_request.term)
///
/// ## Criterias:
/// 1. I should step down as Follower(receive RoleEvent::BecomeFollower event)
/// 2. my term should be updated to the request one
/// 3. receive replay event
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case4_2() {
    // Prepare Leader State

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

/// # Case 5.1: Test handle client propose request
///     if process_raft_request returns Ok()
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case5_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case5_1", graceful_rx, None);
    // Setup replication handler to return success
    context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| {
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
    let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
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

/// # Case 5.1: Test handle client propose request
///     if process_raft_request returns Err()
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case5_2() {}

/// # Case 6.1: Test ClientReadRequest event
///     if both peers failed to confirm leader's commit, the lread request
/// should be failed
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case6_1() {
    // Prepare Leader State
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| Err(Error::Fatal("".to_string())));

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

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    state
        .handle_raft_event(raft_event, &context, role_tx)
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
#[traced_test]
async fn test_handle_raft_event_case6_2() {
    let expect_new_commit_index = 3;
    // Prepare Leader State
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| {
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

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state
        .handle_raft_event(raft_event, &context, role_tx)
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
#[traced_test]
async fn test_handle_raft_event_case6_3() {
    // Prepare Leader State
    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_raft_request_in_batch().times(1).returning(
        move |_, _, _, _| {
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

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state
        .handle_raft_event(raft_event, &context, role_tx)
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
#[traced_test]
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

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_err());

    // Validation criteria 1: The response should return an error
    // Assert that resp_rx receives permission_denied
    let e = resp_rx.recv().await.unwrap().unwrap_err();
    assert!(matches!(e.code(), Code::PermissionDenied));

    // Validation criteria 2: No role event should be triggered
    assert!(role_rx.try_recv().is_err());
}

#[tokio::test]
#[traced_test]
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

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_err());

    // Validation criteria 1: The response should return an error
    let e = resp_rx.recv().await.unwrap().unwrap_err();
    assert!(matches!(e.code(), Code::PermissionDenied));

    // Validation criteria 2: No role event should be triggered
    assert!(role_rx.try_recv().is_err());
}

#[cfg(test)]
mod create_snapshot_event_tests {
    use std::sync::atomic::Ordering;

    use super::*;

    /// Test that snapshot creation is started, and duplicate requests are ignored while in
    /// progress.
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

    /// Test that snapshot_in_progress is reset after SnapshotCreated event (success and error)
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
            std::path::PathBuf::from("/tmp/fake"),
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

    /// Test that CreateSnapshotEvent returns immediately and does not block on snapshot creation.
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
}

#[cfg(test)]
mod snapshot_created_event_tests {
    use super::*;
    /// # Case 1: test handle SnapshotCreated with transport error
    #[tokio::test]
    async fn test_handle_snapshot_created_transport_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_handle_snapshot_created_transport_error");

        // Setup
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

        // Prepare AppendResults
        let mut membership = MockMembership::new();
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        membership.expect_voters().returning(move || {
            vec![NodeMeta {
                id: 2,
                address: "http://127.0.0.1:55001".to_string(),
                role: FOLLOWER,
                status: NodeStatus::Active.into(),
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
        state.snapshot_in_progress.store(true, Ordering::SeqCst);

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

    /// # Case2: test handle SnapshotCreated successful
    #[tokio::test]
    async fn test_handle_snapshot_created_successful() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_handle_snapshot_created_successful");

        // Setup
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

        // Mock peer configuration
        let mut membership = MockMembership::new();
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        membership.expect_voters().returning(|| {
            vec![NodeMeta {
                id: 2,
                address: "".to_string(),
                role: FOLLOWER,
                status: NodeStatus::Active.into(),
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

    /// # Case3: Test snapshot creation failure scenario
    #[tokio::test]
    async fn test_handle_snapshot_created_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_handle_snapshot_created_error");

        // Setup
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

        let mut state = LeaderState::<MockTypeConfig>::new(1, Arc::new(RaftNodeConfig::default()));
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

    /// # Case4: Test higher term response triggering leader step-down
    #[tokio::test]
    async fn test_handle_snapshot_created_higher_term() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_handle_snapshot_created_higher_term");

        // Setup
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

        // Mock peer configuration
        let mut membership = MockMembership::new();
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        membership.expect_voters().returning(move || {
            vec![NodeMeta {
                id: 2,
                address: "http://127.0.0.1:55001".to_string(),
                role: FOLLOWER,
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

    /// # Case5: Test purge preconditions not met (commit index < snapshot index)
    #[tokio::test]
    async fn test_handle_snapshot_created_purge_conditions() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_handle_snapshot_created_purge_conditions");

        // Setup
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

        let mut membership = MockMembership::new();
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        membership.expect_voters().returning(move || {
            vec![NodeMeta {
                id: 2,
                address: "http://127.0.0.1:55001".to_string(),
                role: FOLLOWER,
                status: NodeStatus::Active.into(),
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

    /// # Case6: Test peer purge progress tracking
    #[tokio::test]
    async fn test_handle_snapshot_created_peer_progress() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_handle_snapshot_created_peer_progress");

        // Setup
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

        // Mock peer configuration (multiple peers)
        let mut membership = MockMembership::new();
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        membership.expect_voters().returning(move || {
            vec![
                NodeMeta {
                    id: 2,
                    address: "http://127.0.0.1:55001".to_string(),
                    role: FOLLOWER,
                    status: NodeStatus::Active.into(),
                },
                NodeMeta {
                    id: 3,
                    address: "http://127.0.0.1:55002".to_string(),
                    role: FOLLOWER,
                    status: NodeStatus::Active.into(),
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
}

#[cfg(test)]
mod log_purge_completed_event_tests {
    use super::*;
    /// # New test for LogPurgeCompleted event
    #[tokio::test]
    async fn test_handle_log_purge_completed() {
        let temp_dir = tempfile::tempdir().unwrap();
        let case_path = temp_dir.path().join("test_handle_log_purge_completed");

        // Setup
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

        let mut state = LeaderState::<MockTypeConfig>::new(1, Arc::new(RaftNodeConfig::default()));
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
}

/// # Case 1: Successful leader discovery
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case10_1_discover_leader_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_raft_event_case10_1_discover_leader_success",
        graceful_rx,
        None,
    );

    // Mock membership to return leader metadata
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_retrieve_node_meta().returning(|_| {
        Some(NodeMeta {
            id: 1,
            address: "127.0.0.1:50051".to_string(),
            role: LEADER,
            status: NodeStatus::Active.into(),
        })
    });
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();

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

/// # Case 2: Leader metadata not found (should panic)
#[tokio::test]
#[should_panic(expected = "Leader can not find its address? It must be a bug.")]
async fn test_handle_raft_event_case10_2_discover_leader_metadata_not_found() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_raft_event_case10_2_discover_leader_metadata_not_found",
        graceful_rx,
        None,
    );

    // Mock membership to return no metadata
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_retrieve_node_meta().returning(|_| None);
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (resp_tx, _) = MaybeCloneOneshot::new();

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

/// # Case 4: Discovery with different leader terms
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case10_4_different_leader_terms() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_raft_event_case10_4_different_leader_terms",
        graceful_rx,
        None,
    );

    // Mock membership to return leader metadata
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_retrieve_node_meta().returning(|_| {
        Some(NodeMeta {
            id: 1,
            address: "127.0.0.1:50051".to_string(),
            role: LEADER,
            status: NodeStatus::Active.into(),
        })
    });
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();

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

/// # Case 5: Discovery with invalid node ID
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case10_5_invalid_node_id() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_raft_event_case10_5_invalid_node_id",
        graceful_rx,
        None,
    );

    // Mock membership to return leader metadata
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_retrieve_node_meta().returning(|_| {
        Some(NodeMeta {
            id: 1,
            address: "127.0.0.1:50051".to_string(),
            role: LEADER,
            status: NodeStatus::Active.into(),
        })
    });
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();

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

#[test]
fn test_state_size() {
    assert!(size_of::<LeaderState<RaftTypeConfig<MockStorageEngine, MockStateMachine>>>() < 360);
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
    assert!(!state.can_purge_logs(
        Some(LogId { index: 40, term: 1 }),
        LogId { index: 50, term: 1 }
    ));
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

/// # Case 4: Cluster progress verification (Enhanced durability check)
#[test]
fn test_can_purge_logs_case4() {
    let node_config = Arc::new(node_config("/tmp/test_can_purge_logs_case4"));
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
        LogId {
            index: 100,
            term: 1
        } // 100 not < 100
    ));
}

/// # Case 1: Quorum achieved
/// - All peers respond successfully
/// - Commit index should advance
/// - Clients receive success responses
#[tokio::test]
#[traced_test]
async fn test_process_batch_case1_quorum_achieved() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = setup_process_batch_test_context(
        "/tmp/test_process_batch_case1_quorum_achieved",
        graceful_rx,
    )
    .await;

    // Mock replication to return success with quorum
    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: Some(6),
                            next_index: 7,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: Some(6),
                            next_index: 7,
                            success: true,
                        },
                    ),
                ]),
            })
        });
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(6));
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    // Prepare batch of 2 requests
    let (tx1, rx1) = MaybeCloneOneshot::new();
    let (tx2, rx2) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);
    let receivers = vec![rx1, rx2];

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;

    assert!(result.is_ok());
    assert_eq!(context.state.commit_index(), 6);
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));

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
#[traced_test]
async fn test_process_batch_case2_quorum_failed() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = setup_process_batch_test_context(
        "/tmp/test_process_batch_case2_1_quorum_failed",
        graceful_rx,
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: Some(5),
                            next_index: 6,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: None,
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
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
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
#[traced_test]
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
        .returning(move |_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates: HashMap::from([(
                    peer2_id,
                    PeerUpdate {
                        match_index: Some(5),
                        next_index: 6,
                        success: true,
                    },
                )]),
                learner_progress: HashMap::new(),
            })
        });

    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(move || {
        vec![
            NodeMeta {
                id: 2,
                role: FOLLOWER,
                address: "127.0.0.1:0".to_string(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                role: FOLLOWER,
                address: "127.0.0.1:0".to_string(),
                status: NodeStatus::Active.into(),
            },
        ]
    });
    context.raft_context.membership = Arc::new(membership);

    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    let (tx2, mut rx2) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);

    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
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
#[traced_test]
async fn test_process_batch_case3_higher_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_case3_higher_term", graceful_rx)
            .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| {
            Err(Error::Consensus(ConsensusError::Replication(
                ReplicationError::HigherTerm(10), // Higher term
            )))
        });

    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;

    assert!(result.is_err());
    assert_eq!(context.state.current_term(), 10);
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::BecomeFollower(_))
    ));

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_term_outdated());
}

/// # Case 4: Partial timeouts (non-verifiable failure)
/// - Some peers succeed, some time out
/// - Clients receive ProposeFailed responses
#[tokio::test]
#[traced_test]
async fn test_process_batch_case4_partial_timeouts() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = setup_process_batch_test_context(
        "/tmp/test_process_batch_case4_partial_timeouts",
        graceful_rx,
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([(
                    2,
                    PeerUpdate {
                        match_index: Some(6),
                        next_index: 7,
                        success: true,
                    },
                )]),
            })
        });

    // Prepare AppendResults
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(move || {
        vec![
            NodeMeta {
                id: 2,
                address: "http://127.0.0.1:55001".to_string(),
                role: FOLLOWER,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                address: "http://127.0.0.1:55002".to_string(),
                role: FOLLOWER,
                status: NodeStatus::Active.into(),
            },
        ]
    });
    context.raft_context.membership = Arc::new(membership);

    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
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
#[traced_test]
async fn test_process_batch_case5_all_timeout() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_case5_all_timeout", graceful_rx)
            .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([]),
            })
        });
    // Prepare AppendResults
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(move || {
        vec![
            NodeMeta {
                id: 2,
                address: "http://127.0.0.1:55001".to_string(),
                role: FOLLOWER,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                address: "http://127.0.0.1:55002".to_string(),
                role: FOLLOWER,
                status: NodeStatus::Active.into(),
            },
        ]
    });
    context.raft_context.membership = Arc::new(membership);

    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
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
#[traced_test]
async fn test_process_batch_case6_fatal_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_case6_fatal_error", graceful_rx)
            .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| Err(Error::Fatal("Storage failure".to_string())));

    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
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
#[traced_test]
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
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (2, PeerUpdate::success(5, 6)),
                    (3, PeerUpdate::success(5, 6)),
                ]),
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
    raft_context.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::Success);
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));
}

/// # Case 2: Quorum NOT achieved (verifiable)
/// - Majority of peers responded but quorum not achieved
/// - Returns `Ok(QuorumVerificationResult::RetryRequired)`
#[tokio::test]
#[traced_test]
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
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (2, PeerUpdate::success(5, 6)),
                    (3, PeerUpdate::failed()),
                ]),
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::RetryRequired);
}

/// # Case 3: Quorum NOT achieved (non-verifiable)
/// - Less than majority of peers responded
/// - Returns `Ok(QuorumVerificationResult::LeadershipLost)`
#[tokio::test]
#[traced_test]
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
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([(2, PeerUpdate::success(5, 6))]),
            })
        });

    // Prepare AppendResults
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(move || {
        vec![
            NodeMeta {
                id: 2,
                address: "".to_string(),
                role: FOLLOWER,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                address: "".to_string(),
                role: FOLLOWER,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 4,
                address: "".to_string(),
                role: FOLLOWER,
                status: NodeStatus::Active.into(),
            },
        ]
    });
    raft_context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::LeadershipLost);
}

/// # Case 4: Partial timeouts
/// - Some peers respond, some time out
/// - Returns `Ok(QuorumVerificationResult::RetryRequired)`
#[tokio::test]
#[traced_test]
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
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (2, PeerUpdate::success(5, 6)),
                    (3, PeerUpdate::failed()), // Timeout
                ]),
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::RetryRequired);
}

/// # Case 5: All timeouts
/// - No peers respond
/// - Returns `Ok(QuorumVerificationResult::RetryRequired)`
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_case5_all_timeouts() {
    let payloads = vec![EntryPayload::command(vec![])];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_case5_all_timeouts",
        graceful_rx,
        None,
    );

    // Setup replication handler to return all timeouts
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([(2, PeerUpdate::failed()), (3, PeerUpdate::failed())]),
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());
    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::RetryRequired);
}

/// # Case 6: Higher term detected
/// - Follower responds with higher term
/// - Returns `Err(HigherTerm)`
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_case6_higher_term() {
    let payloads = vec![EntryPayload::command(vec![])];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_case6_higher_term",
        graceful_rx,
        None,
    );

    // Setup replication handler to return higher term error
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| {
            Err(Error::Consensus(ConsensusError::Replication(
                ReplicationError::HigherTerm(10),
            )))
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::Consensus(ConsensusError::Replication(ReplicationError::HigherTerm(
            10
        )))
    ));
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::BecomeFollower(_))
    ));
}

/// # Case 7: Critical failure
/// - System or logic error occurs
/// - Returns original error
#[tokio::test]
#[traced_test]
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
        .returning(|_, _, _, _| Err(Error::Fatal("Storage failure".to_string())));

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::Fatal(msg) if msg == "Storage failure"
    ));
}

/// # Case 1: Successful join
#[tokio::test]
#[traced_test]
async fn test_handle_join_cluster_case1_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_handle_join_cluster_case1_success",
        graceful_rx,
        None,
    );
    let node_id = 100;
    let address = "127.0.0.1:8080".to_string();

    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(move || {
        vec![NodeMeta {
            id: 2,
            address: "http://127.0.0.1:55001".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        }]
    });
    membership.expect_contains_node().returning(|_| false);
    membership.expect_replication_peers().returning(Vec::new);
    membership.expect_add_learner().returning(|_, _| Ok(()));
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(ClusterMembership::default);
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership.expect_update_node_status().returning(|_, _| Ok(()));
    membership
        .expect_get_peer_channel()
        .returning(|_, _| Some(Endpoint::from_static("http://[::]:50051").connect_lazy()));
    raft_context.membership = Arc::new(membership);
    let transport = MockTransport::new();
    raft_context.transport = Arc::new(transport);

    // -->  perpare verify_internal_quorum returns success
    // Setup replication handler to return success
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (2, PeerUpdate::success(5, 6)),
                    (3, PeerUpdate::success(5, 6)),
                ]),
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
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
    state_machine_handler.expect_get_latest_snapshot_metadata().returning(|| None);
    raft_context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock quorum verification
    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config.clone());

    let (sender, mut receiver) = MaybeCloneOneshot::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                node_id,
                node_role: LEARNER,
                address: address.clone(),
            },
            sender,
            &raft_context,
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
#[traced_test]
async fn test_handle_join_cluster_case2_node_exists() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context(
        "/tmp/test_handle_join_cluster_case2_node_exists",
        graceful_rx,
        None,
    );
    let node_id = 100;

    // Mock membership to report existing node
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
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
                node_role: LEARNER,
                address: "127.0.0.1:8080".to_string(),
            },
            sender,
            &context,
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_err());
    let status = receiver.recv().await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("already been added into cluster config"));
}

/// # Case 3: Quorum not achieved
#[tokio::test]
#[traced_test]
async fn test_handle_join_cluster_case3_quorum_failed() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_join_cluster_case3_quorum_failed",
        graceful_rx,
        None,
    );
    let node_id = 100;

    // Mock membership
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_contains_node().returning(|_| false);
    membership.expect_add_learner().returning(|_, _| Ok(()));
    membership.expect_voters().returning(Vec::new); // Empty voting members will cause quorum failure
    context.membership = Arc::new(membership);

    // Setup replication handler to simulate quorum not achieved
    context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::new(),
            })
        });

    // Mock quorum verification to fail
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (sender, mut receiver) = MaybeCloneOneshot::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                node_id,
                node_role: LEARNER,
                address: "127.0.0.1:8080".to_string(),
            },
            sender,
            &context,
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_err());
    let status = receiver.recv().await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("Commit Timeout"));
}

/// # Case 4: Quorum verification error
#[tokio::test]
#[traced_test]
async fn test_handle_join_cluster_case4_quorum_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_join_cluster_case4_quorum_error",
        graceful_rx,
        None,
    );
    let node_id = 100;

    // Mock membership
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_contains_node().returning(|_| false);
    membership.expect_add_learner().returning(|_, _| Ok(()));

    membership.expect_voters().returning(move || {
        vec![NodeMeta {
            id: 2,
            address: "http://127.0.0.1:55001".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        }]
    });
    context.membership = Arc::new(membership);
    // Setup replication handler to return error
    context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| Err(Error::Fatal("Test error".to_string())));

    // Mock quorum verification to return error
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (sender, mut receiver) = MaybeCloneOneshot::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                node_id,
                node_role: LEARNER,
                address: "127.0.0.1:8080".to_string(),
            },
            sender,
            &context,
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_err());
    let status = receiver.recv().await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
}

/// # Case 5: Snapshot transfer triggered
#[tokio::test]
#[traced_test]
async fn test_handle_join_cluster_case5_snapshot_triggered() {
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
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_contains_node().returning(|_| false);
    membership.expect_replication_peers().returning(Vec::new);
    membership.expect_add_learner().returning(|_, _| Ok(()));
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(ClusterMembership::default);
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership.expect_update_node_status().returning(|_, _| Ok(()));

    membership.expect_voters().returning(move || {
        vec![NodeMeta {
            id: 2,
            address: "http://127.0.0.1:55001".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        }]
    });
    context.membership = Arc::new(membership);

    // Mock state machine handler to return snapshot metadata
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_get_latest_snapshot_metadata().returning(|| {
        Some(SnapshotMetadata {
            last_included: Some(LogId {
                index: 100,
                term: 1,
            }),
            checksum: vec![],
        })
    });
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Setup replication handler to return success
    context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([(2, PeerUpdate::success(5, 6))]),
            })
        });

    // Mock transport
    let transport = MockTransport::new();
    context.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
    context.storage.raft_log = Arc::new(raft_log);
    let mut state_machine = MockStateMachine::new();
    state_machine.expect_snapshot_metadata().returning(move || {
        Some(SnapshotMetadata {
            last_included: Some(LogId { term: 1, index: 1 }),
            checksum: vec![],
        })
    });
    context.storage.state_machine = Arc::new(state_machine);

    // Mock quorum verification
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (sender, mut receiver) = MaybeCloneOneshot::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                node_id,
                node_role: LEARNER,
                address,
            },
            sender,
            &context,
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_ok());
    let response = receiver.recv().await.unwrap().unwrap();
    assert!(response.success);
}

#[cfg(test)]
mod trigger_background_snapshot_test {
    use std::sync::Arc;

    use futures::stream;

    use super::*;
    use crate::core::raft_role::leader_state::LeaderState;
    use crate::proto::storage::SnapshotChunk;
    use crate::proto::storage::SnapshotMetadata;
    use crate::SnapshotConfig;

    fn mock_membership(should_fail: bool) -> MockMembership<MockTypeConfig> {
        let mut membership = MockMembership::<MockTypeConfig>::new();
        membership.expect_get_peer_channel().returning(move |_, _| {
            if should_fail {
                None
            } else {
                Some(tonic::transport::Endpoint::from_static("http://[::]:12345").connect_lazy())
            }
        });
        membership
    }

    fn mock_state_machine_handler(should_fail: bool) -> MockStateMachineHandler<MockTypeConfig> {
        let mut handler = MockStateMachineHandler::<MockTypeConfig>::new();
        handler.expect_load_snapshot_data().returning(move |_| {
            if should_fail {
                Err(crate::SnapshotError::OperationFailed("mock error".to_string()).into())
            } else {
                let chunk = SnapshotChunk {
                    data: vec![1, 2, 3],
                    ..Default::default()
                };
                Ok(stream::iter(vec![Ok(chunk)]).boxed())
            }
        });

        handler
    }

    fn default_snapshot_config() -> SnapshotConfig {
        SnapshotConfig {
            max_bandwidth_mbps: 0,
            sender_yield_every_n_chunks: 2,
            ..Default::default()
        }
    }

    fn make_metadata() -> SnapshotMetadata {
        SnapshotMetadata {
            last_included: Some(crate::proto::common::LogId { index: 1, term: 1 }),
            ..Default::default()
        }
    }

    /// Test: background snapshot transfer completes successfully.
    #[tokio::test]
    async fn test_trigger_background_snapshot_success() {
        let membership = Arc::new(mock_membership(false));
        let sm_handler = Arc::new(mock_state_machine_handler(false));
        let config = default_snapshot_config();
        let metadata = make_metadata();

        let result = LeaderState::<crate::test_utils::MockTypeConfig>::trigger_background_snapshot(
            2, metadata, sm_handler, membership, config,
        )
        .await;

        assert!(result.is_ok());
    }

    /// Test: background snapshot transfer fails if peer channel is missing.
    #[tokio::test]
    async fn test_trigger_background_snapshot_peer_channel_missing() {
        let membership = Arc::new(mock_membership(true));
        let sm_handler = Arc::new(mock_state_machine_handler(false));
        let config = default_snapshot_config();
        let metadata = make_metadata();

        let result = LeaderState::<crate::test_utils::MockTypeConfig>::trigger_background_snapshot(
            2, metadata, sm_handler, membership, config,
        )
        .await;

        assert!(result.is_ok());
    }

    /// Test: background snapshot transfer fails if snapshot data stream fails.
    #[tokio::test]
    async fn test_trigger_background_snapshot_snapshot_stream_error() {
        let membership = Arc::new(mock_membership(false));
        let sm_handler = Arc::new(mock_state_machine_handler(true));
        let config = default_snapshot_config();
        let metadata = make_metadata();

        let result = LeaderState::<crate::test_utils::MockTypeConfig>::trigger_background_snapshot(
            2, metadata, sm_handler, membership, config,
        )
        .await;

        assert!(result.is_ok());
    }
}

#[cfg(test)]
mod batch_promote_learners_test {
    use std::sync::Arc;

    use mockall::predicate::*;

    use super::*;
    use crate::core::raft_role::leader_state::LeaderState;
    use crate::core::raft_role::RoleEvent;
    use crate::membership::MockMembership;
    use crate::proto::common::NodeStatus;
    use crate::RaftContext;

    enum VerifyInternalQuorumWithRetrySuccess {
        Success,
        Failure,
        Error,
    }

    struct TestContext {
        raft_context: RaftContext<MockTypeConfig>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
        role_rx: mpsc::UnboundedReceiver<RoleEvent>,
        leader_state: LeaderState<MockTypeConfig>,
    }

    async fn setup_test_context(
        test_name: &str,
        current_voters: usize,
        ready_learners: Vec<u32>,
        verify_leadership_limited_retry_success: VerifyInternalQuorumWithRetrySuccess,
    ) -> TestContext {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut node_config = node_config(&format!("/tmp/{test_name}"));
        node_config.raft.learner_catchup_threshold = 100;

        let mut raft_context =
            MockBuilder::new(graceful_rx).wiht_node_config(node_config).build_context();

        // Mock membership
        let mut membership = MockMembership::new();
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        membership.expect_voters().returning(move || {
            (1..=current_voters)
                .map(|id| NodeMeta {
                    id: id as u32,
                    address: format!("addr_{id}"),
                    role: FOLLOWER,
                    status: NodeStatus::Active.into(),
                })
                .collect()
        });
        let mut replication_handler = MockReplicationCore::<MockTypeConfig>::new();
        replication_handler.expect_handle_raft_request_in_batch().times(1).returning(
            move |_, _, _, _| {
                match verify_leadership_limited_retry_success {
                    VerifyInternalQuorumWithRetrySuccess::Success => Ok(AppendResults {
                        commit_quorum_achieved: true,
                        learner_progress: HashMap::new(),
                        peer_updates: HashMap::from([
                            (2, PeerUpdate::success(5, 6)),
                            (3, PeerUpdate::success(5, 6)),
                        ]),
                    }),
                    VerifyInternalQuorumWithRetrySuccess::Failure => Ok(AppendResults {
                        commit_quorum_achieved: false,
                        learner_progress: HashMap::new(),
                        peer_updates: HashMap::from([
                            (2, PeerUpdate::success(5, 6)),
                            (3, PeerUpdate::failed()),
                        ]),
                    }),
                    VerifyInternalQuorumWithRetrySuccess::Error => {
                        Err(Error::Consensus(ConsensusError::Replication(
                            ReplicationError::HigherTerm(10), // Higher term
                        )))
                    }
                }
            },
        );
        raft_context.handlers.replication_handler = replication_handler;

        let mut raft_log = MockRaftLog::new();
        raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
        raft_context.storage.raft_log = Arc::new(raft_log);

        // Mock learner statuses
        for learner_id in &ready_learners {
            membership
                .expect_get_node_status()
                .with(eq(*learner_id))
                .return_const(Some(NodeStatus::Syncing));
        }

        raft_context.membership = Arc::new(membership);

        let leader_state = LeaderState::new(1, raft_context.node_config());
        let (role_tx, role_rx) = mpsc::unbounded_channel();

        TestContext {
            raft_context,
            role_tx: role_tx.clone(),
            role_rx,
            leader_state,
        }
    }

    /// Test successful promotion when quorum is achieved
    #[tokio::test]
    #[ignore]
    async fn test_batch_promote_learners_success() {
        // Use safe cluster configuration: 1 voter promoting 1 learner = 2 voters total
        let mut ctx = setup_test_context(
            "test_success",
            3,
            vec![4, 5],
            VerifyInternalQuorumWithRetrySuccess::Success,
        )
        .await;

        // Mock quorum verification to succeed
        let mut leader_state = ctx.leader_state;

        let result = leader_state
            .batch_promote_learners(vec![4, 5], &ctx.raft_context, &ctx.role_tx)
            .await;

        assert!(result.is_ok());
        let r = ctx.role_rx.recv().await.unwrap();
        println!("Received role: {:?}", r);
    }

    /// Test promotion failure when quorum not achieved
    #[tokio::test]
    #[ignore]
    async fn test_batch_promote_learners_quorum_failed() {
        let ctx = setup_test_context(
            "test_success",
            3,
            vec![4, 5],
            VerifyInternalQuorumWithRetrySuccess::Failure,
        )
        .await;

        // Mock quorum verification to succeed
        let mut leader_state = ctx.leader_state;

        let result = leader_state
            .batch_promote_learners(vec![4, 5], &ctx.raft_context, &ctx.role_tx)
            .await;

        assert!(result.is_err());
    }

    /// Test safety check preventing unsafe promotion
    #[tokio::test]
    #[ignore]
    async fn test_batch_promote_learners_quorum_safety() {
        let ctx = setup_test_context(
            "test_success",
            3,
            vec![4],
            VerifyInternalQuorumWithRetrySuccess::Failure,
        )
        .await;

        // Mock quorum verification to succeed
        let mut leader_state = ctx.leader_state;

        let result = leader_state
            .batch_promote_learners(vec![4], &ctx.raft_context, &ctx.role_tx)
            .await;

        assert!(result.is_ok());
    }

    /// Test error during quorum verification
    #[tokio::test]
    #[ignore]
    async fn test_batch_promote_learners_verification_error() {
        let ctx = setup_test_context(
            "test_success",
            3,
            vec![4],
            VerifyInternalQuorumWithRetrySuccess::Error,
        )
        .await;

        // Mock quorum verification to succeed
        let mut leader_state = ctx.leader_state;

        let result = leader_state
            .batch_promote_learners(vec![4], &ctx.raft_context, &ctx.role_tx)
            .await;

        assert!(result.is_err());
    }
}

#[cfg(test)]
mod pending_promotion_tests {
    use std::time::Duration;

    use parking_lot::Mutex;
    use tokio::time::timeout;
    use tokio::time::Instant;

    use super::*;
    use crate::leader_state::calculate_safe_batch_size;
    use crate::leader_state::PendingPromotion;

    // Test fixture
    struct TestFixture {
        leader_state: LeaderState<MockTypeConfig>,
        raft_context: RaftContext<MockTypeConfig>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
        role_rx: mpsc::UnboundedReceiver<RoleEvent>,
    }

    impl TestFixture {
        async fn new(
            test_name: &str,
            verify_internal_quorum_success: bool,
        ) -> Self {
            // let (_graceful_tx, graceful_rx) = watch::channel(());
            // let mut raft_context = test_utils::mock_raft_context("/tmp/{test_name}", graceful_rx,
            // None);
            let mut raft_context = if verify_internal_quorum_success {
                Self::verify_internal_quorum_achieved_context(test_name).await
            } else {
                Self::verify_internal_quorum_failure_context(test_name).await
            };

            let mut membership = MockMembership::new();
            membership.expect_can_rejoin().returning(|_, _| Ok(()));
            membership.expect_can_rejoin().returning(|_, _| Ok(()));
            membership.expect_get_node_status().returning(|_| Some(NodeStatus::Active));
            membership
                .expect_update_node_status()
                .withf(|_id, status| *status == NodeStatus::StandBy)
                .returning(|_, _| Ok(()));
            membership.expect_voters().returning(|| {
                (2..=3)
                    .map(|id| NodeMeta {
                        id,
                        address: "".to_string(),
                        status: NodeStatus::Active as i32,
                        role: FOLLOWER,
                    })
                    .collect()
            });
            raft_context.membership = Arc::new(membership);

            let (role_tx, role_rx) = mpsc::unbounded_channel();
            let mut node_config = node_config(&format!("/tmp/{test_name}",));
            node_config.raft.replication.rpc_append_entries_in_batch_threshold = 1;
            let leader_state = LeaderState::new(1, Arc::new(node_config));
            TestFixture {
                leader_state,
                raft_context,
                role_tx,
                role_rx,
            }
        }

        async fn verify_internal_quorum_achieved_context(
            test_name: &str
        ) -> RaftContext<MockTypeConfig> {
            let payloads = vec![EntryPayload::command(vec![])];
            let (_graceful_tx, graceful_rx) = watch::channel(());
            let mut raft_context =
                mock_raft_context(&format!("/tmp/{test_name}",), graceful_rx, None);

            // Setup replication handler to return success
            raft_context
                .handlers
                .replication_handler
                .expect_handle_raft_request_in_batch()
                .returning(|_, _, _, _| {
                    Ok(AppendResults {
                        commit_quorum_achieved: true,
                        learner_progress: HashMap::new(),
                        peer_updates: HashMap::from([
                            (2, PeerUpdate::success(5, 6)),
                            (3, PeerUpdate::success(5, 6)),
                        ]),
                    })
                });

            let mut raft_log = MockRaftLog::new();
            raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
            raft_context.storage.raft_log = Arc::new(raft_log);

            let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

            let (role_tx, mut role_rx) = mpsc::unbounded_channel();

            let result =
                state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

            assert_eq!(result.unwrap(), QuorumVerificationResult::Success);
            assert!(matches!(
                role_rx.try_recv(),
                Ok(RoleEvent::NotifyNewCommitIndex(_))
            ));

            raft_context
        }

        async fn verify_internal_quorum_failure_context(
            test_name: &str
        ) -> RaftContext<MockTypeConfig> {
            let payloads = vec![EntryPayload::command(vec![])];
            let (_graceful_tx, graceful_rx) = watch::channel(());
            let mut raft_context =
                mock_raft_context(&format!("/tmp/{test_name}",), graceful_rx, None);

            // Setup replication handler to return verifiable failure
            raft_context
                .handlers
                .replication_handler
                .expect_handle_raft_request_in_batch()
                .returning(|_, _, _, _| {
                    Ok(AppendResults {
                        commit_quorum_achieved: false,
                        learner_progress: HashMap::new(),
                        peer_updates: HashMap::from([
                            (2, PeerUpdate::success(5, 6)),
                            (3, PeerUpdate::failed()),
                        ]),
                    })
                });

            let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

            let (role_tx, _) = mpsc::unbounded_channel();

            let result =
                state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

            assert_eq!(result.unwrap(), QuorumVerificationResult::RetryRequired);

            raft_context
        }
    }

    #[test]
    fn test_pending_promotion_ordering() {
        let now = Instant::now();
        let mut queue = VecDeque::new();

        // Earlier ready time
        queue.push_back(PendingPromotion::new(1, now - Duration::from_secs(10)));

        // Later ready time
        queue.push_back(PendingPromotion::new(2, now));

        // Verify FIFO extraction
        let drained = queue.pop_front().unwrap();
        assert_eq!(drained.node_id, 1);
        assert_eq!(drained.ready_since, now - Duration::from_secs(10));
    }

    #[test]
    fn test_pending_promotion_serialization() {
        let promotion = PendingPromotion::new(1001, Instant::now());

        // Test debug formatting
        assert!(
            format!("{:?}", promotion).contains("1001"),
            "Debug output should contain node ID"
        );
    }

    // Test cases for calculate_safe_batch_size
    #[tokio::test]
    async fn test_calculate_safe_batch_size() {
        let test_cases = vec![
            ((3, 1), 0), // 3 voters + 1 = 4 -> even -> batch size 0
            ((2, 1), 1), // 2 voters + 1 = 3 -> odd -> batch size 1
            ((3, 2), 2), // 3 voters + 2 = 5 -> odd -> batch size 2
            ((2, 2), 1), // 2 voters + 2 = 4 -> even -> batch size 1
        ];

        for ((current, available), expected_batch_size) in test_cases {
            let result = calculate_safe_batch_size(current, available);
            assert_eq!(
                result, expected_batch_size,
                "Expected batch size for (current={}, available={}) is {}",
                current, available, expected_batch_size
            );
        }
    }

    // Test cases for process_pending_promotions
    #[tokio::test]
    async fn test_process_pending_promotions() {
        let mut fixture = TestFixture::new("test_process_pending_promotions", true).await;
        // Setup test data
        fixture.leader_state.pending_promotions = vec![
            PendingPromotion::new(1, Instant::now()),
            PendingPromotion::new(2, Instant::now()),
            PendingPromotion::new(3, Instant::now()),
        ]
        .into_iter()
        .collect();

        fixture
            .leader_state
            .process_pending_promotions(&fixture.raft_context, &fixture.role_tx)
            .await
            .unwrap();
    }

    // Test cases for handle_stale_learner
    #[tokio::test]
    async fn test_handle_stale_learner() {
        let mut fixture = TestFixture::new("test_handle_stale_learner", true).await;
        assert!(fixture
            .leader_state
            .handle_stale_learner(1, &fixture.raft_context)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_partial_batch_promotion() {
        let mut fixture = TestFixture::new("test_partial_batch_promotion", true).await;
        // Setup: 3 voters, 2 pending promotions -> max batch size=1
        let mut membership = MockMembership::new();
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        // mock membership with 3 voters
        membership.expect_voters().returning(|| {
            (2..=3)
                .map(|id| NodeMeta {
                    id,
                    address: "".to_string(),
                    status: NodeStatus::Active as i32,
                    role: FOLLOWER,
                })
                .collect()
        });
        fixture.raft_context.membership = Arc::new(membership);
        fixture.leader_state.pending_promotions = vec![
            PendingPromotion::new(1, Instant::now() - Duration::from_millis(1)),
            PendingPromotion::new(2, Instant::now()),
        ]
        .into();

        fixture
            .leader_state
            .process_pending_promotions(&fixture.raft_context, &fixture.role_tx)
            .await
            .unwrap();

        assert_eq!(fixture.leader_state.pending_promotions.len(), 1);
        assert_eq!(fixture.leader_state.pending_promotions[0].node_id, 2);
    }

    #[tokio::test]
    async fn test_promotion_event_rescheduling() {
        let mut fixture = TestFixture::new("test_promotion_event_rescheduling", true).await;
        // Setup more nodes than can be promoted in one batch
        fixture.leader_state.pending_promotions =
            (1..=10).map(|id| PendingPromotion::new(id, Instant::now())).collect();

        fixture
            .leader_state
            .process_pending_promotions(&fixture.raft_context, &fixture.role_tx)
            .await
            .unwrap();

        // Verify event was rescheduled
        let mut found = false;

        // Wait up to 200ms for events to arrive
        let result = timeout(Duration::from_millis(200), async {
            while let Some(event) = fixture.role_rx.recv().await {
                println!("Event received: {:?}", event);
                if matches!(event, RoleEvent::ReprocessEvent(inner) if matches!(*inner, RaftEvent::PromoteReadyLearners)) {
                    found = true;
                    break;
                }
            }
            Ok::<(), ()>(())
        }).await;

        assert!(result.is_ok(), "Timed out waiting for events");
        assert!(found, "Did not find PromoteReadyLearners event");
    }

    #[tokio::test]
    async fn test_batch_promotion_failure() {
        // Setup failure in verify_leadership_limited_retry
        let mut fixture = TestFixture::new("test_batch_promotion_failure", false).await;

        fixture.leader_state.pending_promotions =
            (1..=2).map(|id| PendingPromotion::new(id, Instant::now())).collect();
        let result = fixture
            .leader_state
            .process_pending_promotions(&fixture.raft_context, &fixture.role_tx)
            .await;

        assert!(result.is_err());
        assert!(!fixture.leader_state.pending_promotions.is_empty());
    }

    #[tokio::test]
    async fn test_leader_stepdown_during_promotion() {
        let mut fixture = TestFixture::new("test_leader_stepdown", false).await;
        fixture.leader_state.pending_promotions =
            (1..=2).map(|id| PendingPromotion::new(id, Instant::now())).collect();

        // Simulate leadership loss
        fixture.leader_state.update_current_term(2);
        let result = fixture
            .leader_state
            .process_pending_promotions(&fixture.raft_context, &fixture.role_tx)
            .await;

        assert!(result.is_err());
        assert_eq!(fixture.leader_state.pending_promotions.len(), 2);
    }

    #[tokio::test]
    async fn test_concurrent_queue_access() {
        let pending_promotions = VecDeque::new();
        let queue = Arc::new(Mutex::new(pending_promotions));

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let q = queue.clone();
                tokio::spawn(async move {
                    let mut guard = q.lock();
                    guard.push_back(PendingPromotion::new(i, Instant::now()));
                })
            })
            .collect();

        futures::future::join_all(handles).await;
        assert_eq!(queue.lock().len(), 10);
    }

    #[tokio::test(start_paused = true)]
    async fn test_stale_check_timing() {
        let node_config = node_config("/tmp/test_stale_check_timing");
        let mut leader = LeaderState::<MockTypeConfig>::new(1, Arc::new(node_config));
        leader.reset_next_membership_maintenance_check(Duration::from_secs(60));

        tokio::time::advance(Duration::from_secs(61)).await;
        assert!(Instant::now() >= leader.next_membership_maintenance_check);
    }

    #[test]
    fn test_config_propagation_to_stale_handling() {
        let mut config = node_config("/tmp/test_config_propagation_to_stale_handling");
        config.raft.membership.promotion.stale_learner_threshold = Duration::from_secs(120);

        let leader = LeaderState::<MockTypeConfig>::new(1, Arc::new(config));
        assert_eq!(leader.pending_promotions.capacity(), 0); // Default
    }

    #[test]
    fn test_batch_size_calculation_fuzz() {
        for voters in 1..100 {
            for pending in 0..20 {
                let size = calculate_safe_batch_size(voters, pending);
                assert!(size <= pending);
                assert!((voters + size) % 2 == 1 || size == 0);
            }
        }
    }
}

#[cfg(test)]
mod stale_learner_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::Instant;

    use super::*;
    use crate::leader_state::PendingPromotion;
    use crate::test_utils;
    use crate::test_utils::*;
    use crate::RaftNodeConfig; // Assuming you have test utilities

    // Setup helper
    fn create_test_leader_state(
        test_name: &str,
        pending_nodes: Vec<(u32, Duration)>,
    ) -> (LeaderState<MockTypeConfig>, MockMembership<MockTypeConfig>) {
        let mut node_config = node_config(&format!("/tmp/{test_name}",));
        node_config.raft.membership.promotion.stale_learner_threshold = Duration::from_secs(30);
        node_config.raft.membership.promotion.stale_check_interval = Duration::from_secs(60);

        let mut leader = LeaderState::new(1, Arc::new(node_config));
        leader.next_membership_maintenance_check = Instant::now();

        // Add pending promotions
        let now = Instant::now();
        for (node_id, age) in pending_nodes {
            leader.pending_promotions.push_back(PendingPromotion {
                node_id,
                ready_since: now - age,
            });
        }

        // Configure membership mock
        let mut membership = MockMembership::new();
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        membership.expect_update_node_status().returning(|_, _| Ok(()));
        (leader, membership)
    }

    // Create mock RaftContext with configurable membership
    fn mock_raft_context(
        test_name: &str,
        membership: Arc<MockMembership<MockTypeConfig>>,
        config: Option<Arc<RaftNodeConfig>>,
    ) -> RaftContext<MockTypeConfig> {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut ctx = test_utils::mock_raft_context("/tmp/{test_name}", graceful_rx, None);
        ctx.membership = membership.clone();
        ctx.node_config =
            config.unwrap_or_else(|| Arc::new(node_config(&format!("/tmp/{test_name}",))));
        ctx
    }

    /// Test lazy staleness sampling optimization
    #[tokio::test]
    async fn test_stale_check_optimization() {
        // Create queue with 200 entries (will only check oldest 100 or 2%)
        let nodes: Vec<(u32, Duration)> =
            (1..=200).map(|id| (id, Duration::from_secs(40))).collect();

        let (mut leader, membership) =
            create_test_leader_state("test_stale_check_optimization", nodes);
        let mut node_config = node_config("/tmp/test_stale_check_optimization");
        node_config.raft.membership.promotion.stale_learner_threshold = Duration::from_secs(30);
        node_config.raft.membership.promotion.stale_check_interval = Duration::from_secs(60);
        let ctx = mock_raft_context(
            "test_stale_check_optimization",
            Arc::new(membership),
            Some(Arc::new(node_config)),
        );

        // Should only check first 100 entries (out of 200)
        leader.conditionally_purge_stale_learners(&ctx).await.unwrap();

        // Should purge exactly 2 entries (1% of 200 = 2)
        assert_eq!(leader.pending_promotions.len(), 198);
    }

    /// Test no purge when not expired
    #[tokio::test]
    async fn test_no_purge_when_fresh() {
        let (mut leader, mut membership) = create_test_leader_state(
            "test_no_purge_when_fresh",
            vec![
                (101, Duration::from_secs(15)),
                (102, Duration::from_secs(20)),
            ],
        );
        // Should do nothing
        membership.expect_update_node_status().never();

        let ctx = mock_raft_context("test_no_purge_when_fresh", Arc::new(membership), None);

        leader.conditionally_purge_stale_learners(&ctx).await.unwrap();
        assert_eq!(leader.pending_promotions.len(), 2);
    }

    /// Test check scheduling logic
    #[tokio::test]
    async fn test_membership_maintenance_scheduling() {
        let (mut leader, _) =
            create_test_leader_state("test_membership_maintenance_scheduling", vec![]);
        let interval = Duration::from_secs(60);
        // First call
        leader.reset_next_membership_maintenance_check(interval);
        let next_check1 = leader.next_membership_maintenance_check;

        // Small delay to ensure time progresses
        tokio::time::sleep(Duration::from_millis(1)).await;

        // Second call
        leader.reset_next_membership_maintenance_check(interval);
        let next_check2 = leader.next_membership_maintenance_check;

        // Verify both are in the future
        assert!(next_check1 > Instant::now());
        assert!(next_check2 > Instant::now());

        // Verify second call moved the timer forward
        assert!(next_check2 > next_check1);

        // Verify both are approximately interval in the future
        let now = Instant::now();
        assert!(duration_diff(next_check1 - now, interval) < Duration::from_millis(10));
        assert!(duration_diff(next_check2 - now, interval) < Duration::from_millis(10));
    }

    fn duration_diff(
        a: Duration,
        b: Duration,
    ) -> Duration {
        a.abs_diff(b)
    }

    /// Test system remains responsive during large queues
    #[tokio::test]
    async fn test_performance_large_queue() {
        let nodes: Vec<(u32, Duration)> =
            (1..=10_000).map(|id| (id, Duration::from_secs(40))).collect();

        let (mut leader, membership) =
            create_test_leader_state("test_performance_large_queue", nodes);
        let ctx = mock_raft_context("test_performance_large_queue", Arc::new(membership), None);

        // Time the staleness check
        let start = Instant::now();
        leader.conditionally_purge_stale_learners(&ctx).await.unwrap();
        let elapsed = start.elapsed();

        // Should take <1ms even for large queues
        println!("Staleness check for 10k nodes: {:?}", elapsed);
        assert!(
            elapsed < Duration::from_millis(20),
            "Staleness check shouldn't process entire queue"
        );
    }

    #[tokio::test]
    async fn test_promotion_timeout_threshold() {
        let (mut leader, membership) = create_test_leader_state(
            "test",
            vec![
                (101, Duration::from_secs(31)), // 1s over threshold
                (102, Duration::from_secs(30)), // exactly at threshold
                (103, Duration::from_secs(29)), // 1s under threshold
            ],
        );
        leader.next_membership_maintenance_check = Instant::now() - Duration::from_secs(1);
        let mut node_config = node_config("/tmp/test_promotion_timeout_threshold");
        node_config.raft.membership.promotion.stale_learner_threshold = Duration::from_secs(30);
        let ctx = mock_raft_context(
            "test_promotion_timeout_threshold",
            Arc::new(membership),
            Some(Arc::new(node_config)),
        );

        leader.conditionally_purge_stale_learners(&ctx).await.unwrap();

        assert_eq!(leader.pending_promotions.len(), 2);
        assert!(leader.pending_promotions.iter().any(|p| p.node_id == 103));
    }

    #[tokio::test]
    async fn test_downgrade_affects_replication() {
        let (mut leader, mut membership) = create_test_leader_state(
            "test",
            vec![
                (101, Duration::from_secs(29)), // 1s under threshold
                (102, Duration::from_secs(30)), // exactly at threshold
                (103, Duration::from_secs(31)), // 1s over threshold
            ],
        );
        membership.expect_get_node_status().returning(|_| Some(NodeStatus::Active));
        let ctx = mock_raft_context(
            "test_downgrade_affects_replication",
            Arc::new(membership),
            None,
        );

        assert!(leader.handle_stale_learner(101, &ctx).await.is_ok());

        // Verify replication was stopped for this node
        assert!(!leader.next_index.contains_key(&101));
    }
}
