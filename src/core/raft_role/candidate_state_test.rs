use tokio::sync::{mpsc, watch};
use tonic::Status;

use super::candidate_state::CandidateState;
use crate::{
    alias::POF,
    grpc::rpc_service::{VoteRequest, VoteResponse, VotedFor},
    role_state::RaftRoleState,
    test_utils::{mock_peer_channels, mock_raft_context, setup_raft_components, MockTypeConfig},
    Error, MaybeCloneOneshot, MaybeCloneOneshotSender, MockElectionCore, RaftEvent, RaftOneshot,
    RoleEvent, StateUpdate,
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

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());

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

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());

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
