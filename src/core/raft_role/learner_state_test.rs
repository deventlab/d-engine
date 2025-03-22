use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tonic::Status;

use crate::{
    alias::POF,
    grpc::rpc_service::{VoteRequest, VoteResponse},
    learner_state::LearnerState,
    role_state::RaftRoleState,
    test_utils::{mock_peer_channels, mock_raft_context, MockTypeConfig},
    MaybeCloneOneshot, MaybeCloneOneshotSender, RaftEvent, RaftOneshot, RoleEvent,
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
) -> (
    RaftEvent,
    Arc<POF<MockTypeConfig>>,
    mpsc::UnboundedSender<RoleEvent>,
) {
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
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    (raft_event, peer_channels, role_tx)
}

/// # Case 1: Receive Vote Request Event
///
/// ## Validate criterias
/// 1. Always returns Error
#[tokio::test]
async fn test_handle_raft_event_case1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case1", graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.settings.clone());

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (raft_event, peer_channels, role_tx) = setup_handle_raft_event_case1_params(resp_tx);

    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_err());

    assert!(resp_rx.recv().await.is_err());
}
