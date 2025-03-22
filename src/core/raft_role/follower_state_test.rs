use super::{follower_state::FollowerState, HardState};
use crate::{
    alias::POF,
    grpc::rpc_service::{AppendEntriesRequest, VoteRequest, VoteResponse, VotedFor},
    role_state::RaftRoleState,
    test_utils::{mock_peer_channels, mock_raft_context, setup_raft_components, MockTypeConfig},
    AppendResponseWithUpdates, Error, MaybeCloneOneshot, MaybeCloneOneshotSender, MockElectionCore,
    MockMembership, MockReplicationCore, RaftEvent, RaftOneshot, RaftTypeConfig, RoleEvent,
    StateUpdate,
};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tonic::Status;

/// # Case 1: assume it is fresh cluster start
///
/// ## Criterias:
/// 1. commit_index = 0
/// 2. current_term = 1
/// 3. last_applied = 0
/// 4. voted_for = None
/// 5. role is Follower
/// 6. next_index = 1
/// 7. match_index = None
/// 8. noop_log_id = None
/// 9. raft_log len is 0
///
#[test]
fn test_new_with_fresh_start() {
    let components = setup_raft_components("/tmp/test_new_with_fresh_start", None, false);
    let node_id = 1;
    let settings = components.arc_settings.clone();
    let hard_state_from_db = None;
    let last_applied_index_option = None;
    let state = FollowerState::<RaftTypeConfig>::new(
        node_id,
        settings,
        hard_state_from_db,
        last_applied_index_option,
    );

    assert_eq!(state.commit_index(), 0);
    assert_eq!(state.current_term(), 1);
    assert_eq!(state.voted_for().unwrap(), None);
    assert_eq!(state.next_index(state.node_id()), None); //As Follower
    assert_eq!(state.match_index(state.node_id()), None); //As Follower
    assert!(state.noop_log_id().is_err());
}

/// # Case 2: assume it is a cluster restart
///
/// ## Setup:
/// 1. there are 10 entries(term:1) in local log, commit_index=7, current_term=3
/// 2. 5 entry been converted into State Machine
/// 3. have voted for node_id: 3, term:3
///
/// ## Criterias:
/// 1. last_applied = 5
/// 2. commit_index = 5 (=last_applied_index)
/// 3. current_term = 3
/// 4. voted_for = {node_id: 3, term:3}
/// 5. role is Follower
/// 8. noop_log_id = None
/// 9. raft_log len is 10
///
#[test]
fn test_new_with_restart() {
    let voted_for = VotedFor {
        voted_for_id: 3,
        voted_for_term: 2,
    };
    // Fresh start
    {
        let components = setup_raft_components("/tmp/test_new_with_restart", None, false);
        let node_id = 1;
        let settings = components.arc_settings.clone();
        let hard_state_from_db = None;
        let last_applied_index_option = None;
        let mut state = FollowerState::<RaftTypeConfig>::new(
            node_id,
            settings,
            hard_state_from_db,
            last_applied_index_option,
        );

        state.update_current_term(1);
        state.update_commit_index(5).expect("should succeed");
        state.update_voted_for(voted_for).expect("should succeed");
    }

    // Restart
    {
        let components = setup_raft_components("/tmp/test_new_with_restart", None, true);
        let settings = components.arc_settings.clone();
        let node_id = 1;
        let hard_state_from_db = Some(HardState {
            current_term: 2,
            voted_for: Some(VotedFor {
                voted_for_id: 3,
                voted_for_term: 2,
            }),
        });
        let last_applied_index_option = Some(2);
        let state = FollowerState::<RaftTypeConfig>::new(
            node_id,
            settings,
            hard_state_from_db,
            last_applied_index_option,
        );
        assert_eq!(state.commit_index(), 2);
        assert_eq!(state.current_term(), 2);
        assert_eq!(state.voted_for().unwrap(), Some(voted_for));
        assert!(state.noop_log_id().is_err());
    }
}

/// Validate Follower step up as Candidate in new election round
#[tokio::test]
async fn test_tick() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_tick", graceful_rx, None);

    // New state
    let mut state = FollowerState::<MockTypeConfig>::new(1, context.settings.clone(), None, None);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);
    let peer_channels = Arc::new(mock_peer_channels());

    assert!(state
        .tick(&role_tx, &event_tx, peer_channels, &context)
        .await
        .is_ok());
    match role_rx.recv().await {
        Some(RoleEvent::BecomeCandidate) => assert!(true),
        _ => assert!(false),
    }
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

    let mut state = FollowerState::<MockTypeConfig>::new(1, context.settings.clone(), None, None);

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

    let mut state = FollowerState::<MockTypeConfig>::new(1, context.settings.clone(), None, None);

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

    let mut state = FollowerState::<MockTypeConfig>::new(1, context.settings.clone(), None, None);

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

/// # Case 1: As follower, if I receive append request from a new Leader
///
/// ## Prepration Setup
/// 1. receive a new Leader append request
///
/// ## Validation criterias:
/// 1. I should mark new leader id in memberhip
/// 2. I should update term, if Leader has higher term
/// 3. I should send out new commit signal
///
#[tokio::test]
async fn test_handle_raft_event_append_entries_case1() {
    // Prepare Follower State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_raft_event_append_entries_case1",
        graceful_rx,
        None,
    );
    let follower_term = 1;
    let new_leader_term = follower_term + 1;
    let new_leader_term_clone = new_leader_term;
    let expect_new_term = 3;
    let expect_new_term_clone = expect_new_term;
    let expect_new_commit = 2;
    let expect_new_commit_clone = expect_new_commit;

    // Mock replication handler
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .returning(move |_, _, _, _| {
            Ok(AppendResponseWithUpdates {
                success: true,
                current_term: new_leader_term_clone,
                last_matched_id: 1,
                term_update: Some(expect_new_term_clone),
                commit_index_update: Some(expect_new_commit_clone),
            })
        });

    let mut membership = MockMembership::new();
    // Validation criterias
    //1. I should mark new leader id in memberhip
    membership
        .expect_mark_leader_id()
        .returning(|id| {
            assert_eq!(id, 5);
        })
        .times(1);

    context.membership = Arc::new(membership);
    context.replication_handler = replication_handler;

    // New state
    let mut state = FollowerState::<MockTypeConfig>::new(1, context.settings.clone(), None, None);
    state.update_current_term(follower_term);

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
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Handle raft event
    state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .expect("should succeed");

    // Validation criterias
    // 2. I should update term, if Leader has higher term
    assert_eq!(state.current_term(), expect_new_term);
    assert_eq!(state.commit_index(), expect_new_commit);

    match resp_rx.recv().await.expect("should succeed") {
        Ok(response) => assert!(response.success),
        Err(_) => assert!(false),
    }
}
