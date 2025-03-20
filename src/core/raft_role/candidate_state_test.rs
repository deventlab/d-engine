use super::candidate_state::CandidateState;
use crate::{
    grpc::rpc_service::VotedFor,
    role_state::RaftRoleState,
    test_utils::{setup_raft_components, MockTypeConfig},
};
use std::sync::Arc;

/// # Case 1: Can vote myself
#[tokio::test]
async fn test_can_vote_myself_case1() {
    let context = setup_raft_components("/tmp/test_can_vote_myself_case1", None, false);
    let mut state = CandidateState::<MockTypeConfig>::new(1, Arc::new(context.settings.clone()));
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

/// # Case 1: each new election round, term will be incrased and old vote will be reset
#[tokio::test]
async fn test_tick_case1() {}
