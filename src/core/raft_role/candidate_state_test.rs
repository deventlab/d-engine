use tokio::sync::{mpsc, watch};
use tonic::{Code, Status};

use super::candidate_state::CandidateState;
use crate::{
    alias::POF,
    grpc::rpc_service::{
        ClientProposeRequest, ClientReadRequest, ClientRequestError, ClusteMembershipChangeRequest,
        ClusterMembership, MetadataRequest, VoteRequest, VoteResponse, VotedFor,
    },
    role_state::RaftRoleState,
    test_utils::{mock_peer_channels, mock_raft_context, setup_raft_components, MockTypeConfig},
    Error, MaybeCloneOneshot, MaybeCloneOneshotSender, MockElectionCore, MockMembership,
    MockStateMachineHandler, RaftEvent, RaftOneshot, RoleEvent, StateUpdate,
};
use std::sync::Arc;

/// # Case 1: Can vote myself
#[tokio::test]
async fn test_can_vote_myself_case1() {
    let context = setup_raft_components("/tmp/test_can_vote_myself_case1", None, false);
    let state = CandidateState::<MockTypeConfig>::new(1, Arc::new(context.settings.clone()));
    assert!(state.can_vote_myself());
}

/// # Case 2: Can not vote myself
#[tokio::test]
async fn test_can_vote_myself_case2() {
    let context = setup_raft_components("/tmp/test_can_vote_myself_case2", None, false);
    let mut state = CandidateState::<MockTypeConfig>::new(1, Arc::new(context.settings.clone()));
    let voted_for = VotedFor {
        voted_for_id: state.node_id(),
        voted_for_term: state.current_term(),
    };
    state.update_voted_for(voted_for).expect("should succeed");
    assert!(!state.can_vote_myself());
}

/// # Case 1: Test each new election round
///
/// ## Validation criterias:
/// 1. term will be incrased
/// 2. old vote will be reset
/// 3. vote myself as Candidate
///
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
    context.election_handler = election_handler;

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());
    let (role_tx, role_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::channel(1);
    let peer_channels = Arc::new(mock_peer_channels());

    assert!(state
        .tick(&role_tx, &event_tx, peer_channels, &context)
        .await
        .is_ok());

    assert_eq!(state.current_term(), 2);
    assert_eq!(
        state.voted_for().unwrap(),
        Some(VotedFor {
            voted_for_id: 1,
            voted_for_term: 2
        })
    );
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
/// 3. Term should not be updated
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
                term_update: None,
            })
        });
    context.election_handler = election_handler;

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());
    let term_before = state.current_term();

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

    // Term should not be updated
    assert_eq!(term_before, state.current_term());
}

/// # Case 1.2: Receive Vote Request Event
///
/// ## Preparation setup:
/// 1. handle_vote_request return Ok(Some(VotedFor)) - accept this vote
///
/// ## Validate criterias
/// 1. receive response with vote_granted = true
/// 2. Step to Follower
/// 3. Term should be updated
#[tokio::test]
async fn test_handle_raft_event_case1_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case1_2", graceful_rx, None);

    let updated_term = 100;

    let mut election_handler = MockElectionCore::<MockTypeConfig>::new();
    election_handler
        .expect_handle_vote_request()
        .times(1)
        .returning(move |_, _, _, _| {
            Ok(StateUpdate {
                new_voted_for: Some(VotedFor {
                    voted_for_id: 1,
                    voted_for_term: 1,
                }),
                step_to_follower: true,
                term_update: Some(updated_term),
            })
        });
    context.election_handler = election_handler;

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (raft_event, peer_channels) = setup_handle_raft_event_case1_params(resp_tx);

    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

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

    // Term should be updated
    assert_eq!(state.current_term(), updated_term);
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
/// 4. Term should not be updated
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

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());
    let term_before = state.current_term();

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

    // Term should not be updated
    assert_eq!(state.current_term(), term_before);
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

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());

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

/// # Case 3: Receive ClusterConfUpdate Event
#[tokio::test]
async fn test_handle_raft_event_case3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case3", graceful_rx, None);

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());

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
            Ok(_) => assert!(false),
            Err(s) => assert_eq!(s.code(), Code::PermissionDenied),
        },
        Err(_) => assert!(false),
    }
}

/// # Case 5: Test handle client propose request
///
#[tokio::test]
async fn test_handle_raft_event_case5() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case5", graceful_rx, None);

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());

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

    match resp_rx.recv().await {
        Ok(Ok(r)) => assert_eq!(r.error_code, ClientRequestError::NotLeader as i32),
        _ => assert!(false),
    }
}

/// # Case 6.1: test ClientReadRequest with linear request
#[tokio::test]
async fn test_handle_raft_event_case6_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case6_1", graceful_rx, None);

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());
    let client_read_request = ClientReadRequest {
        client_id: 1,
        linear: true,
        commands: vec![],
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = crate::RaftEvent::ClientReadRequest(client_read_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    match resp_rx.recv().await {
        Ok(r) => match r {
            Ok(_) => assert!(false),
            Err(s) => assert_eq!(s.code(), Code::PermissionDenied),
        },
        Err(_) => assert!(false),
    }
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
    context.state_machine_handler = Arc::new(state_machine_handler);

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());

    let client_read_request = ClientReadRequest {
        client_id: 1,
        linear: false,
        commands: vec![],
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = crate::RaftEvent::ClientReadRequest(client_read_request, resp_tx);
    let peer_channels = Arc::new(mock_peer_channels());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    match resp_rx.recv().await {
        Ok(r) => match r {
            Ok(r) => assert_eq!(r.error_code, ClientRequestError::NoError as i32),
            Err(_) => assert!(false),
        },
        Err(_) => assert!(false),
    }
}
