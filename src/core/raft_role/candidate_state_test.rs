use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::watch;
use tonic::Code;
use tonic::Status;

use super::candidate_state::CandidateState;
use crate::alias::POF;
use crate::proto::AppendEntriesRequest;
use crate::proto::AppendEntriesResponse;
use crate::proto::ClientProposeRequest;
use crate::proto::ClientReadRequest;
use crate::proto::ClusteMembershipChangeRequest;
use crate::proto::ClusterMembership;
use crate::proto::ErrorCode;
use crate::proto::MetadataRequest;
use crate::proto::VoteRequest;
use crate::proto::VoteResponse;
use crate::proto::VotedFor;
use crate::role_state::RaftRoleState;
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
    context.election_handler = election_handler;

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());
    let (role_tx, role_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::channel(1);
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
    context.election_handler = election_handler;

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());
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
    context.election_handler = election_core;

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());
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
    context.election_handler = election_core;

    let updated_term = 100;

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());

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
    context.replication_handler = replication_handler;

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());
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
    context.replication_handler = replication_handler;

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());
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
    match resp_rx.recv().await.expect("should succeed") {
        Ok(response) => assert!(response.is_higher_term()),
        Err(_) => assert!(false),
    }
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
    context.replication_handler = replication_handler;

    // New state
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());
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
    match resp_rx.recv().await.expect("should succeed") {
        Ok(response) => assert!(response.is_conflict()),
        Err(_) => assert!(false),
    }
}

/// # Case 5: Test handle client propose request
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
        Ok(Ok(r)) => assert_eq!(r.error, ErrorCode::NotLeader as i32),
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
            Ok(r) => assert_eq!(r.error, ErrorCode::Success as i32),
            Err(_) => assert!(false),
        },
        Err(_) => assert!(false),
    }
}

#[test]
fn test_send_replay_raft_event() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_send_replay_raft_event", graceful_rx, None);

    let state = CandidateState::<MockTypeConfig>::new(1, context.settings.clone());
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
