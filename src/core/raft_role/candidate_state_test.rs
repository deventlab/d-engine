use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::watch;
use tonic::Code;
use tonic::Status;

use super::candidate_state::CandidateState;
use crate::alias::POF;
use crate::proto::client::ClientReadRequest;
use crate::proto::client::ClientWriteRequest;
use crate::proto::cluster::cluster_conf_update_response;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::ClusterConfUpdateResponse;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::MetadataRequest;
use crate::proto::common::LogId;
use crate::proto::election::VoteRequest;
use crate::proto::election::VoteResponse;
use crate::proto::election::VotedFor;
use crate::proto::error::ErrorCode;
use crate::proto::replication::AppendEntriesRequest;
use crate::proto::replication::AppendEntriesResponse;
use crate::proto::storage::PurgeLogRequest;
use crate::role_state::RaftRoleState;
use crate::test_utils::crate_test_snapshot_stream;
use crate::test_utils::create_test_chunk;
use crate::test_utils::mock_election_core;
use crate::test_utils::mock_peer_channels;
use crate::test_utils::mock_raft_context;
use crate::test_utils::setup_raft_components;
use crate::test_utils::MockTypeConfig;
use crate::ConsensusError;
use crate::ElectionError;
use crate::Error;
use crate::MaybeCloneOneshot;
use crate::MaybeCloneOneshotSender;
use crate::MockElectionCore;
use crate::MockMembership;
use crate::MockReplicationCore;
use crate::MockStateMachineHandler;
use crate::RaftEvent;
use crate::RaftOneshot;
use crate::RoleEvent;

/// # Case 1: Can vote myself
#[tokio::test]
async fn test_can_vote_myself_case1() {
    let context = setup_raft_components("/tmp/test_can_vote_myself_case1", None, false);
    let state = CandidateState::<MockTypeConfig>::new(1, Arc::new(context.node_config.clone()));
    assert!(state.can_vote_myself());
}

/// # Case 2: Can not vote myself
#[tokio::test]
async fn test_can_vote_myself_case2() {
    let context = setup_raft_components("/tmp/test_can_vote_myself_case2", None, false);
    let mut state = CandidateState::<MockTypeConfig>::new(1, Arc::new(context.node_config.clone()));
    let voted_for = VotedFor {
        voted_for_id: state.node_id(),
        voted_for_term: state.current_term(),
    };
    state.update_voted_for(voted_for).expect("should succeed");
    assert!(!state.can_vote_myself());
}

/// # Case 1: Test new election round with success response
///
/// ## Validation criterias:
/// 1. term will be incrased
/// 2. old vote will be reset
/// 3. vote myself as Candidate
#[tokio::test]
async fn test_tick_case1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_tick_case1", graceful_rx, None);
    // Mock election_handler
    let mut election_handler = MockElectionCore::<MockTypeConfig>::new();
    election_handler
        .expect_broadcast_vote_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(()));
    context.handlers.election_handler = election_handler;

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);
    let peer_channels = Arc::new(mock_peer_channels());

    assert!(state.tick(&role_tx, &event_tx, peer_channels, &context).await.is_ok());

    assert_eq!(state.current_term(), 2);
    assert_eq!(
        state.voted_for().unwrap(),
        Some(VotedFor {
            voted_for_id: 1,
            voted_for_term: 2
        })
    );
}

/// # Case 2: Test new election round with higher term found response
///
/// ## Validation criterias:
/// 1. term will be updated to the reponse one
/// 2. send out become follower signal
#[tokio::test]
async fn test_tick_case2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_tick_case2", graceful_rx, None);
    // Mock election_handler
    let mut election_handler = MockElectionCore::<MockTypeConfig>::new();
    election_handler
        .expect_broadcast_vote_requests()
        .times(1)
        .returning(|_, _, _, _, _| {
            Err(Error::Consensus(ConsensusError::Election(ElectionError::HigherTerm(
                100,
            ))))
        });
    context.handlers.election_handler = election_handler;

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);
    let peer_channels = Arc::new(mock_peer_channels());

    assert!(state.tick(&role_tx, &event_tx, peer_channels, &context).await.is_ok());

    assert_eq!(state.current_term(), 100);
    assert!(matches!(role_rx.try_recv().unwrap(), RoleEvent::BecomeFollower(_)));
}

fn setup_handle_raft_event_case1_params(
    resp_tx: MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>,
    term: u64,
) -> (RaftEvent, Arc<POF<MockTypeConfig>>) {
    let raft_event = crate::RaftEvent::ReceiveVoteRequest(
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
///     and check_vote_request_is_legal returns false
///
/// ## Validate criterias
/// 1. receive response with vote_granted = false
/// 2. Role should not step to Follower
/// 3. Term should not be updated
#[tokio::test]
async fn test_handle_raft_event_case1_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case1_1", graceful_rx, None);
    let mut election_core = mock_election_core();
    election_core
        .expect_check_vote_request_is_legal()
        .returning(|_, _, _, _, _| false);
    context.handlers.election_handler = election_core;

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
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
    let r = resp_rx.recv().await.unwrap().unwrap();
    assert!(!r.vote_granted);

    // No role event receives
    assert!(role_rx.try_recv().is_err());

    // Term should not be updated
    assert_eq!(term_before, state.current_term());
}

/// # Case 1.2: Receive Vote Request Event
///     and check_vote_request_is_legal returns true
///
/// ## Validate criterias
/// 1. Should not receive response.
/// 2. Step to Follower
/// 3. Term should be updated
/// 4. Should receive replay event
#[tokio::test]
async fn test_handle_raft_event_case1_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case1_2", graceful_rx, None);
    let mut election_core = mock_election_core();
    election_core
        .expect_check_vote_request_is_legal()
        .returning(|_, _, _, _, _| true);
    context.handlers.election_handler = election_core;

    let updated_term = 100;

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (raft_event, peer_channels) = setup_handle_raft_event_case1_params(resp_tx, updated_term);

    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    // Step to Follower
    assert!(matches!(role_rx.try_recv(), Ok(RoleEvent::BecomeFollower(None))));
    assert!(matches!(role_rx.try_recv().unwrap(), RoleEvent::ReprocessEvent(_)));

    // Term should be updated
    assert_eq!(state.current_term(), updated_term);

    // Should not receive response, (let Follower handle it)
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

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = crate::RaftEvent::ClusterConf(MetadataRequest {}, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());

    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    let m = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(m.nodes, vec![]);
}

/// # Case3: Successful configuration update
#[tokio::test]
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
    membership.expect_current_leader_id().returning(|| Some(2)); // Leader is 2
    context.membership = Arc::new(membership);

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = crate::RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2, // Leader ID
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );
    let peer_channels = Arc::new(mock_peer_channels());

    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.success);
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::None as i32
    );
}

/// # Case 4.1: As candidate, if I receive append request from Leader,
///     and check_append_entries_request_is_legal is true
///
/// ## Prepration Setup
/// 1. receive Leader append request, with higher term and new commit index
///
/// ## Validation criterias:
/// 1. I should mark new leader id in memberhip
/// 2. I should not update term
/// 3. I should receive BecomeFollower event
/// 4. I should replay the raft_event to let Follower continue handle it
/// 5. I should not send out new commit signal
/// 6. Should not receive response, (let Follower handle it)
/// 7. `handle_raft_event` fun returns Ok(())
/// 8. commit should not be updated. We should let Follower continue.
#[tokio::test]
async fn test_handle_raft_event_case4_1() {
    // Prepare Follower State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case4_1", graceful_rx, None);
    let term = 1;
    let new_leader_term = term;
    let new_leader_commit = 5;

    // Mock replication handler
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_check_append_entries_request_is_legal()
        .returning(move |_, _, _| AppendEntriesResponse::success(1, term, None));

    let mut membership = MockMembership::new();

    // Validation criterias
    // 1. I should mark new leader id in memberhip
    membership
        .expect_mark_leader_id()
        .returning(|id| {
            assert_eq!(id, 5);
            Ok(())
        })
        .times(1);

    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(term);

    // Prepare Append entries request
    let append_entries_request = AppendEntriesRequest {
        term: new_leader_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: new_leader_commit,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = crate::RaftEvent::AppendEntries(append_entries_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Validation criterias: 7. `handle_raft_event` fun returns Ok(())
    // Handle raft event
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    // Validation criterias
    // 3. I should  receive BecomeFollower event
    assert!(matches!(role_rx.try_recv(), Ok(RoleEvent::BecomeFollower(None))));
    // 4. I should replay the raft_event to let Follower continue handle it
    assert!(matches!(role_rx.try_recv().unwrap(), RoleEvent::ReprocessEvent(_)));

    // Validation criterias
    // 2. I should update term
    assert_eq!(state.current_term(), new_leader_term);
    // 8. commit should not be updated. We should let Follower continue.
    assert!(state.commit_index() != new_leader_commit);

    // 6. Should not receive response, (let Follower handle it)
    assert!(resp_rx.recv().await.is_err());
}

/// # Case 4.2: As candidate, if I receive append request from Leader,
///     and check_append_entries_request_is_legal is false because of higher term
///
/// ## Validation criterias:
/// 1. I should not mark new leader id in memberhip
/// 2. I should not receive any event
/// 3. My term shoud not be updated
/// 4. send out AppendEntriesResponse with success=false
/// 5. `handle_raft_event` fun returns Err(())
#[tokio::test]
async fn test_handle_raft_event_case4_2() {
    // Prepare Follower State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case4_2", graceful_rx, None);
    let term = 2;
    let new_leader_term = term - 1;

    // Mock replication handler
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_check_append_entries_request_is_legal()
        .returning(move |_, _, _| AppendEntriesResponse::higher_term(1, term));

    let mut membership = MockMembership::new();
    // Validation criterias
    // 1. I should mark new leader id in memberhip
    membership.expect_mark_leader_id().returning(|_| Ok(())).times(0);
    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
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
    let raft_event = crate::RaftEvent::AppendEntries(append_entries_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Validation criterias: 6. `handle_raft_event` fun returns Ok(())
    // Handle raft event
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    // Validation criterias
    // 2. I should not receive any event
    assert!(role_rx.try_recv().is_err());

    // Validation criterias
    // 3. My term shoud not be updated
    assert_eq!(state.current_term(), term);

    // 5. send out AppendEntriesResponse with success=false
    let response = resp_rx.recv().await.expect("should succeed").unwrap();
    assert!(response.is_higher_term());
}

/// # Case 4.3: As candidate, if I receive append request from Leader,
///     and check_append_entries_request_is_legal is false because of conflicts
///
/// ## Validation criterias:
/// 1. I should not mark new leader id in memberhip
/// 2. I should not receive any event
/// 3. My term shoud not be updated
/// 4. send out AppendEntriesResponse with success=false
/// 5. `handle_raft_event` fun returns Err(())
#[tokio::test]
async fn test_handle_raft_event_case4_3() {
    // Prepare Follower State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case4_3", graceful_rx, None);
    let term = 2;
    let new_leader_term = term - 1;

    // Mock replication handler
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_check_append_entries_request_is_legal()
        .returning(move |_, _, _| AppendEntriesResponse::conflict(1, term, None, None));

    let mut membership = MockMembership::new();
    // Validation criterias
    // 1. I should mark new leader id in memberhip
    membership.expect_mark_leader_id().returning(|_| Ok(())).times(0);
    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
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
    let raft_event = crate::RaftEvent::AppendEntries(append_entries_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Validation criterias: 6. `handle_raft_event` fun returns Ok(())
    // Handle raft event
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    // Validation criterias
    // 2. I should not receive any event
    assert!(role_rx.try_recv().is_err());

    // Validation criterias
    // 3. My term shoud not be updated
    assert_eq!(state.current_term(), term);

    // 5. send out AppendEntriesResponse with success=false
    let response = resp_rx.recv().await.expect("should succeed").unwrap();
    assert!(response.is_conflict());
}

/// # Case 5: Test handle client propose request
#[tokio::test]
async fn test_handle_raft_event_case5() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case5", graceful_rx, None);

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Handle raft event
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = crate::RaftEvent::ClientPropose(
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

    let r = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(r.error, ErrorCode::NotLeader as i32);
}

/// # Case 6.1: test ClientReadRequest with linear request
#[tokio::test]
async fn test_handle_raft_event_case6_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case6_1", graceful_rx, None);

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    let client_read_request = ClientReadRequest {
        client_id: 1,
        linear: true,
        keys: vec![],
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = crate::RaftEvent::ClientReadRequest(client_read_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    let s = resp_rx.recv().await.unwrap().unwrap_err();
    assert_eq!(s.code(), Code::PermissionDenied);
}

/// # Case 6.2: test ClientReadRequest with request(linear=false)
#[tokio::test]
async fn test_handle_raft_event_case6_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case6_2", graceful_rx, None);
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_read_from_state_machine()
        .times(1)
        .returning(|_| Some(vec![]));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    let client_read_request = ClientReadRequest {
        client_id: 1,
        linear: false,
        keys: vec![],
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = crate::RaftEvent::ClientReadRequest(client_read_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    let r = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(r.error, ErrorCode::Success as i32);
}

#[test]
fn test_send_replay_raft_event() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_send_replay_raft_event", graceful_rx, None);

    let state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
    assert!(state.send_become_follower_event(&role_tx).is_ok());
    assert!(state
        .send_replay_raft_event(
            &role_tx,
            RaftEvent::ReceiveVoteRequest(
                VoteRequest {
                    term: 1,
                    candidate_id: 1,
                    last_log_index: 0,
                    last_log_term: 0,
                },
                resp_tx,
            ),
        )
        .is_ok());

    assert!(matches!(role_rx.try_recv().unwrap(), RoleEvent::BecomeFollower(None)));
    assert!(matches!(role_rx.try_recv().unwrap(), RoleEvent::ReprocessEvent(_)));
}

// filepath: [candidate_state_test.rs](http://_vscodecontentref_/3)

/// Test handling InstallSnapshotChunk event by CandidateState
#[tokio::test]
async fn test_handle_raft_event_case7() {
    // Step 1: Setup the test environment
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case7", graceful_rx, None);
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Step 2: Prepare the InstallSnapshotChunk event
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let stream = crate_test_snapshot_stream(vec![create_test_chunk(0, b"chunk0", 1, 1, 2)]);
    let raft_event = RaftEvent::InstallSnapshotChunk(Box::new(stream), resp_tx);

    // Step 3: Call handle_raft_event
    let result = state
        .handle_raft_event(
            raft_event,
            Arc::new(mock_peer_channels()),
            &context,
            mpsc::unbounded_channel().0,
        )
        .await;

    // Step 4: Verify the response
    assert!(result.is_err(), "Expected handle_raft_event to return Ok");

    // Step 5: Check the response sent through the channel
    let response = resp_rx.recv().await.expect("Response should be received");
    assert!(response.is_err(), "Expected an error response");
    let status = response.unwrap_err();

    // Step 6: Verify error details
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), "Not Follower or Learner.");
}

/// Test handling RaftLogCleanUp event by CandidateState
#[tokio::test]
async fn test_handle_raft_event_case8() {
    // Step 1: Setup the test environment
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case8", graceful_rx, None);
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Step 2: Prepare the RaftLogCleanUp event
    let request = PurgeLogRequest {
        term: 1,
        leader_id: 1,
        leader_commit: 1,
        last_included: Some(LogId { term: 1, index: 1 }),
        snapshot_checksum: vec![1, 2, 3],
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::RaftLogCleanUp(request, resp_tx);

    // Step 3: Call handle_raft_event
    let result = state
        .handle_raft_event(
            raft_event,
            Arc::new(mock_peer_channels()),
            &context,
            mpsc::unbounded_channel().0,
        )
        .await;

    // Step 4: Verify the response
    assert!(result.is_ok(), "Expected handle_raft_event to return Ok");

    // Step 5: Check the response sent through the channel
    let response = resp_rx.recv().await.expect("Response should be received");
    assert!(response.is_err(), "Expected an error response");
    let status = response.unwrap_err();

    // Step 6: Verify error details
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), "Not Follower");
}

/// Test handling CreateSnapshotEvent event by CandidateState
#[tokio::test]
async fn test_handle_raft_event_case9() {
    // Step 1: Setup the test environment
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case9", graceful_rx, None);
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Step 2: Prepare the CreateSnapshotEvent
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::CreateSnapshotEvent;

    // Step 3: Call handle_raft_event
    let e = state
        .handle_raft_event(raft_event, Arc::new(mock_peer_channels()), &context, role_tx)
        .await
        .unwrap_err();

    // Step 4: Verify the error response
    assert!(matches!(e, Error::Consensus(ConsensusError::RoleViolation { .. })));
}
