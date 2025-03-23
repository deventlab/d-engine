use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tonic::{Code, Status};

use crate::{
    alias::POF,
    grpc::rpc_service::{
        AppendEntriesRequest, ClientProposeRequest, ClientReadRequest, ClientRequestError,
        ClusteMembershipChangeRequest, MetadataRequest, VoteRequest, VoteResponse,
    },
    learner_state::LearnerState,
    role_state::RaftRoleState,
    test_utils::{mock_peer_channels, mock_raft_context, MockTypeConfig},
    AppendResponseWithUpdates, Error, MaybeCloneOneshot, MaybeCloneOneshotSender, MockMembership,
    MockReplicationCore, RaftEvent, RaftOneshot, RoleEvent,
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

/// # Case 4.1: As learner, if I receive append request from Leader,
///     and replication_handler::handle_append_entries successfully
///
/// ## Prepration Setup
/// 1. receive Leader append request,
///     with higher term and new commit index
///
/// ## Validation criterias:
/// 1. I should mark new leader id in memberhip
/// 2. I should not receive BecomeFollower event
/// 3. I should update term
/// 4. I should send out new commit signal
/// 5. send out AppendEntriesResponse with success=true
/// 6. `handle_raft_event` fun returns Ok(())
///
#[tokio::test]
async fn test_handle_raft_event_case4_1() {
    // Prepare Follower State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case4_1", graceful_rx, None);
    let follower_term = 1;
    let new_leader_term = follower_term + 1;
    let expect_new_term = 3;
    let expect_new_commit = 2;

    // Mock replication handler
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .returning(move |_, _, _, _| {
            Ok(AppendResponseWithUpdates {
                success: true,
                current_term: new_leader_term,
                last_matched_id: 1,
                term_update: Some(expect_new_term),
                commit_index_update: Some(expect_new_commit),
            })
        });

    let mut membership = MockMembership::new();

    // Validation criterias
    // 1. I should mark new leader id in memberhip
    membership
        .expect_mark_leader_id()
        .returning(|id| {
            assert_eq!(id, 5);
        })
        .times(1);

    context.membership = Arc::new(membership);
    context.replication_handler = replication_handler;

    // New state
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.settings.clone());
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
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Validation criterias: 6. `handle_raft_event` fun returns Ok(())
    // Handle raft event
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_ok());

    // Validation criterias
    // 2. I should not receive BecomeFollower event
    // 4. I should send out new commit signal
    assert!(matches!(
        role_rx.try_recv().unwrap(),
        RoleEvent::NotifyNewCommitIndex {
            new_commit_index: _
        }
    ));

    // Validation criterias
    // 3. I should update term
    assert_eq!(state.current_term(), expect_new_term);
    assert_eq!(state.commit_index(), expect_new_commit);

    // 5. send out AppendEntriesResponse with success=true
    match resp_rx.recv().await.expect("should succeed") {
        Ok(response) => assert!(response.success),
        Err(_) => assert!(false),
    }
}

/// # Case 4.2: As learner, if I receive append request from Leader,
///     and replication_handler::handle_append_entries failed with Error
///
/// ## Prepration Setup
/// 1. receive Leader append request,
///     with Error
///
/// ## Validation criterias:
/// 1. I should mark new leader id in memberhip
/// 2. I should not receive any event
/// 3. My term shoud not be updated
/// 4. send out AppendEntriesResponse with success=false
/// 5. `handle_raft_event` fun returns Err(())
///
#[tokio::test]
async fn test_handle_raft_event_case4_2() {
    // Prepare Follower State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case4_2", graceful_rx, None);
    let follower_term = 1;
    let new_leader_term = follower_term + 1;

    // Mock replication handler
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .returning(|_, _, _, _| Err(Error::GeneralServerError("test".to_string())));

    let mut membership = MockMembership::new();

    // Validation criterias
    // 1. I should mark new leader id in memberhip
    membership
        .expect_mark_leader_id()
        .returning(|id| {
            assert_eq!(id, 5);
        })
        .times(1);

    context.membership = Arc::new(membership);
    context.replication_handler = replication_handler;

    // New state
    let mut state = LearnerState::<MockTypeConfig>::new(1, context.settings.clone());
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
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Validation criterias: 6. `handle_raft_event` fun returns Ok(())
    // Handle raft event
    assert!(state
        .handle_raft_event(raft_event, peer_channels, &context, role_tx)
        .await
        .is_err());

    // Validation criterias
    // 2. I should not receive any event
    assert!(role_rx.try_recv().is_err());

    // Validation criterias
    // 3. My term shoud not be updated
    assert_eq!(state.current_term(), follower_term);

    // 5. send out AppendEntriesResponse with success=true
    match resp_rx.recv().await.expect("should succeed") {
        Ok(response) => assert!(!response.success),
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
