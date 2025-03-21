use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use crate::{
    learner_state::LearnerState,
    role_state::RaftRoleState,
    test_utils::{mock_peer_channels, mock_raft_context, MockTypeConfig},
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
