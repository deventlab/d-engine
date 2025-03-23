use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tonic::{Code, Status};

use crate::{
    alias::POF,
    grpc::rpc_service::{
        ClientProposeRequest, ClientReadRequest, ClientRequestError, ClusteMembershipChangeRequest,
        MetadataRequest, VoteRequest, VoteResponse,
    },
    learner_state::LearnerState,
    role_state::RaftRoleState,
    test_utils::{mock_peer_channels, mock_raft_context, MockTypeConfig},
    Error, MaybeCloneOneshot, MaybeCloneOneshotSender, RaftEvent, RaftOneshot, RoleEvent,
};

/// Validate Follower step up as Candidate in new election round
#[tokio::test]
async fn test_tick() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_tick", graceful_rx, None);

    // New state
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.settings.clone());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);
    let peer_channels = Arc::new(mock_peer_channels());

    assert!(state
        .tick(&role_tx, &event_tx, peer_channels, &context)
        .await
        .is_ok());
}

fn setup_handle_raft_event_case1_params(
    resp_tx: MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>,
    term: u64,
) -> (
    RaftEvent,
    Arc<POF<MockTypeConfig>>,
    mpsc::UnboundedSender<RoleEvent>,
) {
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
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    (raft_event, peer_channels, role_tx)
}

/// # Case 1: Receive Vote Request Event with term is higher than mine
///
/// ## Validate criterias
/// 1. Update to request term
/// 2. receive reponse from Learner with vote_granted = false
/// 3. handle_raft_event returns Ok()
#[tokio::test]
async fn test_handle_raft_event_case1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case1", graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.settings.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let requet_term = state.current_term() + 10;
    let (raft_event, peer_channels, role_tx) =
        setup_handle_raft_event_case1_params(resp_tx, requet_term);

    // handle_raft_event returns Ok()
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    // Update to request term
    assert_eq!(state.current_term(), requet_term);

    // Receive response with vote_granted = false
    match resp_rx.recv().await {
        Ok(Ok(r)) => assert!(!r.vote_granted),
        _ => assert!(false),
    }
}

/// # Case 2: Receive ClusterConf Event
#[tokio::test]
async fn test_handle_raft_event_case2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case2", graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.settings.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = crate::RaftEvent::ClusterConf(MetadataRequest {}, resp_tx);
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

/// # Case 3: Receive ClusterConfUpdate Event
#[tokio::test]
async fn test_handle_raft_event_case3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case3", graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.settings.clone());

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
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.settings.clone());

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

/// # Case 6: test ClientReadRequest with linear request
#[tokio::test]
async fn test_handle_raft_event_case6() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case6", graceful_rx, None);

    // New state
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.settings.clone());
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
            Ok(_) => assert!(false),
            Err(s) => assert_eq!(s.code(), Code::PermissionDenied),
        },
        Err(_) => assert!(false),
    }
}
