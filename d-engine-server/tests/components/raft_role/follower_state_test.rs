use bytes::Bytes;
use d_engine_server::test_utils::setup_raft_components;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tonic::Code;
use tonic::Status;
use tracing_test::traced_test;

use d_engine_core::AppendResponseWithUpdates;
use d_engine_core::ConsensusError;
use d_engine_core::Error;
use d_engine_core::HardState;
use d_engine_core::MaybeCloneOneshot;
use d_engine_core::MaybeCloneOneshotSender;
use d_engine_core::MembershipError;
use d_engine_core::MockElectionCore;
use d_engine_core::MockMembership;
use d_engine_core::MockPurgeExecutor;
use d_engine_core::MockReplicationCore;
use d_engine_core::MockStateMachineHandler;
use d_engine_core::MockTypeConfig;
use d_engine_core::NetworkError;
use d_engine_core::NewCommitData;
use d_engine_core::RaftEvent;
use d_engine_core::RaftOneshot;
use d_engine_core::RoleEvent;
use d_engine_core::SnapshotError;
use d_engine_core::StateUpdate;
use d_engine_core::SystemError;
use d_engine_core::follower_state::FollowerState;
use d_engine_core::role_state::RaftRoleState;
use d_engine_core::test_utils::MockBuilder;
use d_engine_core::test_utils::mock_raft_context;
use d_engine_core::test_utils::node_config;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::ReadConsistencyPolicy;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Learner;
use d_engine_proto::error::ErrorCode;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::JoinRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryRequest;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::cluster::cluster_conf_update_response;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::VotedFor;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::storage::PurgeLogRequest;

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
#[tokio::test]
#[traced_test]
async fn test_new_with_fresh_start() {
    let components = setup_raft_components("/tmp/test_new_with_fresh_start", None, false);
    let node_id = 1;
    let node_config = components.arc_node_config.clone();
    let hard_state_from_db = None;
    let last_applied_index_option = None;
    let state = FollowerState::<MockTypeConfig>::new(
        node_id,
        node_config,
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
#[tokio::test]
#[traced_test]
async fn test_new_with_restart() {
    let voted_for = VotedFor {
        voted_for_id: 3,
        voted_for_term: 2,
        committed: false,
    };
    // Fresh start
    {
        let components = setup_raft_components("/tmp/test_new_with_restart", None, false);
        let node_id = 1;
        let node_config = components.arc_node_config.clone();
        let hard_state_from_db = None;
        let last_applied_index_option = None;
        let mut state = FollowerState::<MockTypeConfig>::new(
            node_id,
            node_config,
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
        let node_config = components.arc_node_config.clone();
        let node_id = 1;
        let hard_state_from_db = Some(HardState {
            current_term: 2,
            voted_for: Some(VotedFor {
                voted_for_id: 3,
                voted_for_term: 2,
                committed: false,
            }),
        });
        let last_applied_index_option = Some(2);
        let state = FollowerState::<MockTypeConfig>::new(
            node_id,
            node_config,
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
#[traced_test]
async fn test_tick() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_tick", graceful_rx, None);

    // New state
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);

    assert!(state.tick(&role_tx, &event_tx, &context).await.is_ok());
    let r = role_rx.recv().await.unwrap();
    assert!(matches!(r, RoleEvent::BecomeCandidate));
}

fn setup_handle_raft_event_case1_params(
    resp_tx: MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>
) -> RaftEvent {
    d_engine_core::RaftEvent::ReceiveVoteRequest(
        VoteRequest {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        },
        resp_tx,
    )
}
/// # Case 1.1: Receive Vote Request Event
///
/// ## Scenario Setup:
/// 1. handle_vote_request return Ok(None) - reject this vote
///
/// ## Validate criterias
/// 1. receive response with vote_granted = false
/// 2. Role should not step to Follower
/// 3. Term should not be updated
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case1_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case1_1", graceful_rx, None);
    let mut election_handler = MockElectionCore::<MockTypeConfig>::new();
    election_handler.expect_handle_vote_request().times(1).returning(|_, _, _, _| {
        Ok(StateUpdate {
            new_voted_for: None,
            term_update: None,
        })
    });
    context.handlers.election_handler = election_handler;

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    let term_before = state.current_term();

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = setup_handle_raft_event_case1_params(resp_tx);

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    // Receive response with vote_granted = false
    let r = resp_rx.recv().await.unwrap().unwrap();
    assert!(!r.vote_granted);

    // No role event receives
    assert!(role_rx.try_recv().is_err());

    // Term should not be updated
    assert_eq!(term_before, state.current_term());
}

/// # Case 1.2: Receive Vote Request Event
///
/// ## Scenario Setup:
/// 1. handle_vote_request return Ok(Some(VotedFor)) - accept this vote
///
/// ## Validate criterias
/// 1. receive response with vote_granted = true
/// 2. Follower should not step to Follower again
/// 3. Term should be updated
#[tokio::test]
#[traced_test]
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
                    committed: false,
                }),
                term_update: Some(updated_term),
            })
        });
    context.handlers.election_handler = election_handler;

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = setup_handle_raft_event_case1_params(resp_tx);

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    // Receive response with vote_granted = true
    let r = resp_rx.recv().await.unwrap().unwrap();
    assert!(r.vote_granted);
    // Follower should not step to Follower again
    assert!(role_rx.try_recv().is_err());

    // Term should be updated
    assert_eq!(state.current_term(), updated_term);
}

/// # Case 1.3: Receive Vote Request Event
///
/// ## Scenario Setup:
/// 1. handle_vote_request return Error
///
/// ## Validate criterias
/// 1. receive response with vote_granted = false
/// 2. handle_raft_event returns Error
/// 3. Role should not step to Follower
/// 4. Term should not be updated
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case1_3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case1_3", graceful_rx, None);
    let mut election_handler = MockElectionCore::<MockTypeConfig>::new();
    election_handler.expect_handle_vote_request().times(1).returning(|_, _, _, _| {
        Err(Error::System(SystemError::Network(
            NetworkError::SingalSendFailed("".to_string()),
        )))
    });
    context.handlers.election_handler = election_handler;

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    let term_before = state.current_term();

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = setup_handle_raft_event_case1_params(resp_tx);

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_err());

    let r = resp_rx.recv().await.unwrap().unwrap();
    assert!(!r.vote_granted);

    // Term should not be updated
    assert_eq!(state.current_term(), term_before);
}

/// # Case 2: Receive ClusterConf Event
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case2", graceful_rx, None);
    let mut membership = MockMembership::new();
    membership.expect_retrieve_cluster_membership_config().times(1).returning(|| {
        ClusterMembership {
            version: 1,
            nodes: vec![],
        }
    });
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = d_engine_core::RaftEvent::ClusterConf(MetadataRequest {}, resp_tx);

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let m = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(m.nodes, vec![]);
}

/// # Case3_1: Successful configuration update
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case3_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case3_1", graceful_rx, None);

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

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = d_engine_core::RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2, // Leader ID
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.success);
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::None as i32
    );
}

/// # Case3_2: Reject configuration change from non-leader
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case3_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case3_2", graceful_rx, None);

    // Mock membership to return NOT_LEADER error
    let mut membership = MockMembership::new();
    membership
        .expect_update_cluster_conf_from_leader()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(ClusterConfUpdateResponse {
                id: 1,
                term: 1,
                version: 1,
                success: false,
                error_code: cluster_conf_update_response::ErrorCode::NotLeader.into(),
            })
        });
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership.expect_current_leader_id().returning(|| Some(2)); // Actual leader is 2
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = d_engine_core::RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 3, // Non-leader ID
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::NotLeader as i32
    );
}

/// # Case3_3: Reject configuration change with version conflict
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case3_3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case3_3", graceful_rx, None);

    // Mock membership to return VERSION_CONFLICT error
    let mut membership = MockMembership::new();
    membership
        .expect_update_cluster_conf_from_leader()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(ClusterConfUpdateResponse {
                id: 1,
                term: 1,
                version: 5, // Current version
                success: false,
                error_code: cluster_conf_update_response::ErrorCode::VersionConflict.into(),
            })
        });
    membership.expect_get_cluster_conf_version().returning(|| 5); // Current version is 5
    membership.expect_current_leader_id().returning(|| Some(2)); // Leader is 2
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = d_engine_core::RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2, // Leader ID
            term: 1,
            version: 4, // Stale version
            change: None,
        },
        resp_tx,
    );

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::VersionConflict as i32
    );
    assert_eq!(response.version, 5);
}

/// # Case3_4: Reject configuration change with outdated term
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case3_4() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case3_4", graceful_rx, None);

    // Mock membership to return TERM_OUTDATED error
    let mut membership = MockMembership::new();
    membership
        .expect_update_cluster_conf_from_leader()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(ClusterConfUpdateResponse {
                id: 1,
                term: 5, // Current term
                version: 1,
                success: false,
                error_code: cluster_conf_update_response::ErrorCode::TermOutdated.into(),
            })
        });
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership.expect_current_leader_id().returning(|| Some(2)); // Leader is 2
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(5); // Follower has higher term

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = d_engine_core::RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2,   // Leader ID
            term: 4, // Stale term
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::TermOutdated as i32
    );
    assert_eq!(response.term, 5);
}

/// # Case3_5: Handle internal error during configuration update
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case3_5() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case3_5", graceful_rx, None);

    // Mock membership to return internal error
    let mut membership = MockMembership::new();
    membership
        .expect_update_cluster_conf_from_leader()
        .times(1)
        .returning(|_, _, _, _, _| {
            Err(Error::Consensus(ConsensusError::Membership(
                MembershipError::ConfigChangeUpdateFailed("test".to_string()),
            )))
        });
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership.expect_current_leader_id().returning(|| Some(2)); // Leader is 2
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = d_engine_core::RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2, // Leader ID
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::InternalError as i32
    );
}

/// # Case3_6: Reject configuration change when no leader is known
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case3_6() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case3_6", graceful_rx, None);

    // Mock membership to return NOT_LEADER error with no known leader
    let mut membership = MockMembership::new();
    membership
        .expect_update_cluster_conf_from_leader()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(ClusterConfUpdateResponse {
                id: 1,
                term: 1,
                version: 1,
                success: false,
                error_code: cluster_conf_update_response::ErrorCode::NotLeader.into(),
            })
        });
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership.expect_current_leader_id().returning(|| None); // No known leader
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Prepare function params
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = d_engine_core::RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 3, // Non-leader ID
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::NotLeader as i32
    );
}

/// # Case 4.1: As follower, if I receive append request from Leader,
///     and replication_handler::handle_append_entries successfully
///
/// ## Prepration Setup
/// 1. receive Leader append request, with higher term and new commit index
///
/// ## Validation criterias:
/// 1. I should mark new leader id in memberhip
/// 2. I should not receive BecomeFollower event
/// 3. I should update term
/// 4. I should send out new commit signal
/// 5. send out AppendEntriesResponse with success=true
/// 6. `handle_raft_event` fun returns Ok(())
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case4_1() {
    // Prepare Follower State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case4_1", graceful_rx, None);
    let follower_term = 1;
    let new_leader_term = follower_term + 1;
    let expect_new_commit = 2;

    // Mock replication handler
    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_append_entries().returning(move |_, _, _| {
        Ok(AppendResponseWithUpdates {
            response: AppendEntriesResponse::success(
                1,
                new_leader_term,
                Some(LogId {
                    term: new_leader_term,
                    index: 1,
                }),
            ),
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
            Ok(())
        })
        .times(1);

    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    // New state
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
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
    let raft_event = d_engine_core::RaftEvent::AppendEntries(append_entries_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Validation criterias: 6. `handle_raft_event` fun returns Ok(())
    // Handle raft event
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    // Validation criterias
    // 2. I should receive LeaderDiscovered event (new leader detected)
    assert!(matches!(
        role_rx.try_recv().unwrap(),
        RoleEvent::LeaderDiscovered(5, _)
    ));

    // 3. I should send out new commit signal
    assert!(matches!(
        role_rx.try_recv().unwrap(),
        RoleEvent::NotifyNewCommitIndex(NewCommitData {
            new_commit_index: _,
            role: _,
            current_term: _
        })
    ));

    // Validation criterias
    // 3. I should update term
    assert_eq!(state.current_term(), new_leader_term);
    assert_eq!(state.commit_index(), expect_new_commit);

    // 5. send out AppendEntriesResponse with success=true
    let response = resp_rx.recv().await.expect("should succeed").unwrap();
    assert!(response.is_success());
}

/// # Case 4.2: As follower, if I receive append request from Leader,
///     and request term is lower or equal than mine
///
/// ## Validation criterias:
/// 1. I should not mark new leader id in memberhip
/// 2. I should not receive any event
/// 3. My term shoud not be updated
/// 4. send out AppendEntriesResponse with success=false
/// 5. `handle_raft_event` fun returns Ok(())
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case4_2() {
    // Prepare Follower State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case4_2", graceful_rx, None);
    let follower_term = 2;
    let new_leader_term = follower_term - 1;

    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_check_append_entries_request_is_legal()
        .returning(move |_, _, _| AppendEntriesResponse::success(1, follower_term, None));

    let mut membership = MockMembership::new();

    // Validation criterias
    // 1. I should mark new leader id in memberhip
    membership
        .expect_mark_leader_id()
        .returning(|id| {
            assert_eq!(id, 5);
            Ok(())
        })
        .times(0);

    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    // New state
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
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
    let raft_event = d_engine_core::RaftEvent::AppendEntries(append_entries_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Validation criterias: 6. `handle_raft_event` fun returns Ok(())
    // Handle raft event
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    // Validation criterias
    // 2. I should not receive any event
    assert!(role_rx.try_recv().is_err());

    // Validation criterias
    // 3. My term shoud not be updated
    assert_eq!(state.current_term(), follower_term);

    // 5. send out AppendEntriesResponse with success=true
    let response = resp_rx.recv().await.expect("should succeed").unwrap();
    assert!(response.is_higher_term());
}

/// # Case 4.3: As follower, if I receive append request from Leader,
///     and replication_handler::handle_append_entries failed with Error
///
/// ## Prepration Setup
/// 1. receive Leader append request, with Error
///
/// ## Validation criterias:
/// 1. I should mark new leader id in memberhip
/// 2. I should not receive any event
/// 3. My term shoud not be updated
/// 4. send out AppendEntriesResponse with success=false
/// 5. `handle_raft_event` fun returns Err(())
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case4_3() {
    // Prepare Follower State
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_handle_raft_event_case4_3", graceful_rx, None);
    let follower_term = 1;
    let new_leader_term = follower_term + 1;

    // Mock replication handler
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .returning(|_, _, _| Err(Error::Fatal("test".to_string())));

    let mut membership = MockMembership::new();

    // Validation criterias:
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
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
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
    let raft_event = d_engine_core::RaftEvent::AppendEntries(append_entries_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Validation criterias: 6. `handle_raft_event` fun returns Ok(())
    // Handle raft event
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_err());

    // Validation criterias
    // 2. I should receive LeaderDiscovered event even when append fails
    assert!(matches!(
        role_rx.try_recv().unwrap(),
        RoleEvent::LeaderDiscovered(5, _)
    ));

    // No other events should be sent
    assert!(role_rx.try_recv().is_err());

    // Validation criterias
    // 3. My term shoud not be updated
    assert_eq!(state.current_term(), new_leader_term);

    // 5. send out AppendEntriesResponse with success=true
    let response = resp_rx.recv().await.expect("should succeed").unwrap();
    assert!(!response.is_success());
}

/// # Case 5: Test handle client propose request
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case5() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case5", graceful_rx, None);

    // New state
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Handle raft event
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = d_engine_core::RaftEvent::ClientPropose(
        ClientWriteRequest {
            client_id: 1,
            commands: vec![],
        },
        resp_tx,
    );

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let r = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(r.error, ErrorCode::NotLeader as i32);
}

/// # Case 6.1: test ClientReadRequest with linear request
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case6_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case6_1", graceful_rx, None);

    // New state
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        keys: vec![],
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = d_engine_core::RaftEvent::ClientReadRequest(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let s = resp_rx.recv().await.unwrap().unwrap_err();
    assert_eq!(s.code(), Code::PermissionDenied);
}

/// # Case 6.2: test ClientReadRequest with request(linear=false)
#[tokio::test]
#[traced_test]
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
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
        keys: vec![],
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = d_engine_core::RaftEvent::ClientReadRequest(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let r = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(r.error, ErrorCode::Success as i32);
}

/// # Case8_1: Test purge request without current leader
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case8_1() {
    // Setup Context
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case8_8");
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler to validate purge request
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_validate_purge_request()
        .returning(|_, _, _| Ok(false));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock membership with no current leader
    let mut membership = MockMembership::new();
    membership.expect_current_leader_id().returning(|| None);
    context.membership = Arc::new(membership);

    // Prepare follower state
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let purge_request = PurgeLogRequest {
        term: 3,
        leader_id: 2,
        leader_commit: 100,
        last_included: Some(LogId { term: 3, index: 80 }),
        snapshot_checksum: Bytes::new(),
    };

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::RaftLogCleanUp(purge_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should handle no leader scenario");

    // Validate response indicates failure
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);

    // No role change
    assert!(role_rx.try_recv().is_err());
}

/// # Case8_2: Test successful log cleanup with valid purge request
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case8_2() {
    // Setup Context
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case8_2");
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler to validate purge request
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_validate_purge_request()
        .returning(|_, _, _| Ok(true));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock purge executor to succeed
    let mut purge_executor = MockPurgeExecutor::new();
    purge_executor.expect_execute_purge().times(1).returning(|_| Ok(()));
    context.handlers.purge_executor = Arc::new(purge_executor);

    // Mock membership with current leader
    let mut membership = MockMembership::new();
    membership.expect_current_leader_id().returning(|| Some(2));
    context.membership = Arc::new(membership);

    // Prepare follower state
    let mut state = FollowerState::<MockTypeConfig>::new(
        1,
        context.node_config.clone(),
        None,
        Some(100), // last_applied_index
    );
    state.update_current_term(3);
    state.shared_state.commit_index = 150; // commit_index > purge_index

    // Create valid purge request
    let purge_request = PurgeLogRequest {
        term: 3,
        leader_id: 2,
        leader_commit: 150,
        last_included: Some(LogId {
            term: 3,
            index: 100,
        }),
        snapshot_checksum: Bytes::from(vec![1, 2, 3]),
    };

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::RaftLogCleanUp(purge_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should succeed");

    // Validate response
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.success);
    assert_eq!(
        response.last_purged,
        Some(LogId {
            term: 3,
            index: 100
        })
    );
    assert_eq!(
        state.last_purged_index,
        Some(LogId {
            term: 3,
            index: 100
        })
    );

    // No role change
    assert!(role_rx.try_recv().is_err());
}

/// # Case8_3: Test purge request fails validation
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case8_3() {
    // Setup Context
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case8_3");
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler to fail validation
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_validate_purge_request()
        .returning(|_, _, _| Err(SnapshotError::OperationFailed("test".to_string()).into()));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock membership
    let mut membership = MockMembership::new();
    membership.expect_current_leader_id().return_const(1);
    context.membership = Arc::new(membership);

    // Prepare follower state
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(3);

    let purge_request = PurgeLogRequest {
        term: 3,
        leader_id: 2,
        leader_commit: 100,
        last_included: Some(LogId { term: 3, index: 80 }),
        snapshot_checksum: Bytes::new(),
    };

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::RaftLogCleanUp(purge_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should handle validation error");

    // Validate response indicates failure
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);
    assert_eq!(state.last_purged_index, None);

    // No role change
    assert!(role_rx.try_recv().is_err());
}

/// # Case8_4: Test purge request with invalid leader term
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case8_4() {
    // Setup Context
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case8_4");
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler to validate purge request
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_validate_purge_request()
        .returning(|current_term, _, req| {
            // Leader term (2) is lower than current term (3)
            if req.term < current_term {
                Ok(false)
            } else {
                Ok(true)
            }
        });
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock membership
    let mut membership = MockMembership::new();
    membership.expect_current_leader_id().return_const(1);
    context.membership = Arc::new(membership);

    // Prepare follower state with higher term
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(3); // Current term > leader term

    let purge_request = PurgeLogRequest {
        term: 2, // Lower than follower's term
        leader_id: 2,
        leader_commit: 100,
        last_included: Some(LogId { term: 2, index: 80 }),
        snapshot_checksum: Bytes::new(),
    };

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::RaftLogCleanUp(purge_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should handle invalid term");

    // Validate response indicates failure
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);
    assert_eq!(state.last_purged_index, None);

    // No role change
    assert!(role_rx.try_recv().is_err());
}

/// # Case8_5: Test purge request fails can_purge_logs check
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case8_5() {
    // Setup Context
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case8_5");
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler to validate purge request
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_validate_purge_request()
        .returning(|_, _, _| Ok(true));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock membership
    let mut membership = MockMembership::new();
    membership.expect_current_leader_id().return_const(1);
    context.membership = Arc::new(membership);

    // Prepare follower state where commit index < purge index
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(3);
    state.shared_state.commit_index = 90; // commit_index < purge_index (100)
    state.last_purged_index = Some(LogId { term: 2, index: 80 });

    let purge_request = PurgeLogRequest {
        term: 3,
        leader_id: 2,
        leader_commit: 100,
        last_included: Some(LogId {
            term: 3,
            index: 100,
        }),
        snapshot_checksum: Bytes::new(),
    };

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::RaftLogCleanUp(purge_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should skip purge due to safety check");

    // Validate response indicates failure
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);
    assert_eq!(state.last_purged_index, Some(LogId { term: 2, index: 80 })); // Unchanged

    // No role change
    assert!(role_rx.try_recv().is_err());
}

/// # Case8_6: Test purge request with non-monotonic purge index
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case8_6() {
    // Setup Context
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case8_6");
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler to validate purge request
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_validate_purge_request()
        .returning(|_, _, _| Ok(true));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock membership
    let mut membership = MockMembership::new();
    membership.expect_current_leader_id().return_const(1);
    context.membership = Arc::new(membership);

    // Prepare follower state where last_purged_index > requested purge index
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(3);
    state.shared_state.commit_index = 150;
    state.last_purged_index = Some(LogId { term: 3, index: 90 }); // Higher than requested

    let purge_request = PurgeLogRequest {
        term: 3,
        leader_id: 2,
        leader_commit: 150,
        last_included: Some(LogId { term: 3, index: 80 }), // Lower than current
        snapshot_checksum: Bytes::new(),
    };

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::RaftLogCleanUp(purge_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should skip purge due to non-monotonic request");

    // Validate response indicates failure
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);
    assert_eq!(state.last_purged_index, Some(LogId { term: 3, index: 90 })); // Unchanged

    // No role change
    assert!(role_rx.try_recv().is_err());
}

/// # Case8_7: Test purge execution failure
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case8_7() {
    // Setup Context
    let temp_dir = tempfile::tempdir().unwrap();
    let case_path = temp_dir.path().join("test_handle_raft_event_case8_7");
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = MockBuilder::new(graceful_rx).with_db_path(&case_path).build_context();

    // Mock state machine handler to validate purge request
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_validate_purge_request()
        .returning(|_, _, _| Ok(true));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock purge executor to fail
    let mut purge_executor = MockPurgeExecutor::new();
    purge_executor
        .expect_execute_purge()
        .times(1)
        .returning(|_| Err(SnapshotError::OperationFailed("test".to_string()).into()));
    context.handlers.purge_executor = Arc::new(purge_executor);

    // Mock membership
    let mut membership = MockMembership::new();
    membership.expect_current_leader_id().return_const(1);
    context.membership = Arc::new(membership);

    // Prepare follower state
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(3);
    state.shared_state.commit_index = 150;
    state.last_purged_index = Some(LogId { term: 2, index: 80 });

    let purge_request = PurgeLogRequest {
        term: 3,
        leader_id: 2,
        leader_commit: 150,
        last_included: Some(LogId {
            term: 3,
            index: 100,
        }),
        snapshot_checksum: Bytes::new(),
    };

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::RaftLogCleanUp(purge_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Execute
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Should handle purge failure");

    // Validate response indicates failure
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success);
    assert_eq!(state.last_purged_index, Some(LogId { term: 2, index: 80 })); // Unchanged

    // No role change
    assert!(role_rx.try_recv().is_err());
}

/// Test handling JoinCluster event by CandidateState
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case10() {
    // Step 1: Setup the test environment
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case10", graceful_rx, None);
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Step 2: Prepare the event
    let request = JoinRequest {
        node_id: 2,
        node_role: Learner.into(),
        address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::JoinCluster(request, resp_tx);

    // Step 3: Call handle_raft_event
    let result = state.handle_raft_event(raft_event, &context, mpsc::unbounded_channel().0).await;

    // Step 4: Verify the response
    assert!(
        result.is_err(),
        "Expected handle_raft_event to return error"
    );

    // Step 5: Check the response sent through the channel
    let response = resp_rx.recv().await.expect("Response should be received");
    assert!(response.is_err(), "Expected an error response");
    let status = response.unwrap_err();

    // Step 6: Verify error details
    assert_eq!(status.code(), Code::PermissionDenied);
}

/// Test handling DiscoverLeader event by CandidateState
#[tokio::test]
#[traced_test]
async fn test_handle_raft_event_case11() {
    // Step 1: Setup the test environment
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_handle_raft_event_case11", graceful_rx, None);
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Step 2: Prepare the event
    let request = LeaderDiscoveryRequest {
        node_id: 2,
        requester_address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::DiscoverLeader(request, resp_tx);

    // Step 3: Call handle_raft_event
    let result = state.handle_raft_event(raft_event, &context, mpsc::unbounded_channel().0).await;

    // Step 4: Verify the response
    assert!(result.is_ok(), "Expected handle_raft_event to return Ok");

    // Step 5: Check the response sent through the channel
    let response = resp_rx.recv().await.expect("Response should be received");
    assert!(response.is_err(), "Expected an error response");
    let status = response.unwrap_err();

    // Step 6: Verify error details
    assert_eq!(status.code(), Code::PermissionDenied);
}

/// # Case 1: Valid purge conditions with gap enforcement
#[test]
fn test_can_purge_logs_case1() {
    let dir = tempdir().unwrap();
    let node_config = Arc::new(node_config(dir.path().to_str().unwrap()));
    let mut state = FollowerState::<MockTypeConfig>::new(1, node_config, None, None);

    // Setup state matching Raft paper's log compaction rules
    state.shared_state.commit_index = 100; // Last committed entry at 100

    // Test valid purge range (90 < 99 < 100)
    assert!(state.can_purge_logs(
        Some(LogId { index: 90, term: 1 }), // last_purge_index
        LogId { index: 99, term: 1 }        // last_included_in_request
    ));

    // Edge case: 99 == commit_index - 1 (per gap rule)
    assert!(state.can_purge_logs(
        Some(LogId { index: 90, term: 1 }),
        LogId { index: 99, term: 1 }
    ));

    // Violate gap rule: 100 not < 100
    assert!(!state.can_purge_logs(
        Some(LogId { index: 90, term: 1 }),
        LogId {
            index: 100,
            term: 1
        }
    ));
}

// # Case 2: Reject uncommitted index (Raft §5.4.2)
#[test]
fn test_can_purge_logs_case2() {
    let dir = tempdir().unwrap();
    let node_config = Arc::new(node_config(dir.path().to_str().unwrap()));
    let mut state = FollowerState::<MockTypeConfig>::new(1, node_config, None, None);

    state.shared_state.commit_index = 50;

    // Leader tries to purge beyond commit index
    assert!(!state.can_purge_logs(
        Some(LogId { index: 40, term: 1 }),
        LogId { index: 51, term: 1 } // 51 > commit_index(50)
    ));

    // Boundary check: 50 == commit_index (violates <)
    assert!(!state.can_purge_logs(
        Some(LogId { index: 40, term: 1 }),
        LogId { index: 50, term: 1 }
    ));
}

/// # Case 3: Ensure purge monotonicity (Raft §7.2)
#[test]
fn test_can_purge_logs_case3() {
    let dir = tempdir().unwrap();
    let node_config = Arc::new(node_config(dir.path().to_str().unwrap()));
    let mut state = FollowerState::<MockTypeConfig>::new(1, node_config, None, None);

    state.shared_state.commit_index = 200;

    // Valid sequence: 100 → 150 → 199
    assert!(state.can_purge_logs(
        Some(LogId {
            index: 100,
            term: 1
        }),
        LogId {
            index: 150,
            term: 1
        }
    ));

    // Invalid: Attempt to purge backwards (150 → 120)
    assert!(!state.can_purge_logs(
        Some(LogId {
            index: 150,
            term: 1
        }),
        LogId {
            index: 120,
            term: 1
        }
    ));

    // Same index purge attempt
    assert!(!state.can_purge_logs(
        Some(LogId {
            index: 150,
            term: 1
        }),
        LogId {
            index: 150,
            term: 1
        }
    ));
}

/// # Case 4: Handle initial purge state (no previous purge)
#[test]
fn test_can_purge_logs_case4() {
    let dir = tempdir().unwrap();
    let node_config = Arc::new(node_config(dir.path().to_str().unwrap()));
    let mut state = FollowerState::<MockTypeConfig>::new(1, node_config, None, None);

    state.shared_state.commit_index = 100;

    // First ever purge (last_purge_index = None)
    assert!(state.can_purge_logs(
        None, // No previous purge
        LogId { index: 99, term: 1 }
    ));

    // First purge must still obey commit_index gap
    assert!(!state.can_purge_logs(
        None,
        LogId {
            index: 100,
            term: 1
        } // 100 not < 100
    ));
}

#[cfg(test)]
mod role_violation_tests {
    use super::*;

    /// Test handling role violation events by FollowerState
    #[tokio::test]
    async fn test_role_violation_events() {
        // Step 1: Setup the test environment
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let context = mock_raft_context(
            "/tmp/test_follower_role_violation_events",
            graceful_rx,
            None,
        );
        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

        // Step 2: Prepare the CreateSnapshotEvent
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::CreateSnapshotEvent;

        // [Test CreateSnapshotEvent]
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        // Verify the error response
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::RoleViolation { .. })
        ));

        // [Test SnapshotCreated]
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::SnapshotCreated(Err(Error::Fatal("test".to_string())));
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        // Verify the error response
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::RoleViolation { .. })
        ));

        // [Test LogPurgeCompleted]
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::LogPurgeCompleted(LogId { term: 1, index: 1 });
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        // Verify the error response
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::RoleViolation { .. })
        ));
    }
}

#[cfg(test)]
mod handle_client_read_request {
    use super::*;
    use d_engine_core::RaftNodeConfig;
    use d_engine_core::config::ReadConsistencyPolicy as ServerPolicy;
    use d_engine_core::convert::safe_kv_bytes;
    use d_engine_proto::client::ReadConsistencyPolicy as ClientPolicy;

    /// Test that follower rejects LeaseRead policy
    #[tokio::test]
    #[traced_test]
    async fn test_handle_client_read_lease_read_policy() {
        let (_graceful_tx, graceful_rx) = watch::channel(());

        let mut node_config = RaftNodeConfig::default();
        node_config.raft.read_consistency.allow_client_override = true;
        let context = MockBuilder::new(graceful_rx).with_node_config(node_config).build_context();

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
        let client_read_request = ClientReadRequest {
            client_id: 1,
            consistency_policy: Some(ClientPolicy::LeaseRead as i32),
            keys: vec![safe_kv_bytes(1)],
        };

        let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
        let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);
        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        state
            .handle_raft_event(raft_event, &context, role_tx)
            .await
            .expect("should succeed");

        let response = resp_rx.recv().await.unwrap();
        assert!(response.is_err()); // Should be rejected
    }

    /// Test that follower uses server default policy
    #[tokio::test]
    #[traced_test]
    async fn test_handle_client_read_unspecified_policy() {
        let (_graceful_tx, graceful_rx) = watch::channel(());

        // Server default is LinearizableRead, should be rejected by follower
        let mut node_config = RaftNodeConfig::default();
        node_config.raft.read_consistency.default_policy = ServerPolicy::LinearizableRead;

        let context = MockBuilder::new(graceful_rx).with_node_config(node_config).build_context();

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
        let client_read_request = ClientReadRequest {
            client_id: 1,
            consistency_policy: None, // Use server default
            keys: vec![safe_kv_bytes(1)],
        };

        let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
        let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);
        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        state
            .handle_raft_event(raft_event, &context, role_tx)
            .await
            .expect("should succeed");

        let response = resp_rx.recv().await.unwrap();
        assert!(response.is_err()); // Should be rejected (requires leader)
    }

    /// Test EventualConsistency policy allows follower reads
    #[tokio::test]
    #[traced_test]
    async fn test_handle_client_read_eventual_consistency_policy() {
        let (_graceful_tx, graceful_rx) = watch::channel(());

        // Configure server to allow client override
        let mut node_config = RaftNodeConfig::default();
        node_config.raft.read_consistency.default_policy = ServerPolicy::EventualConsistency;

        let context = MockBuilder::new(graceful_rx).with_node_config(node_config).build_context();

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
        let client_read_request = ClientReadRequest {
            client_id: 1,
            consistency_policy: None, // Use server default (EventualConsistency)
            keys: vec![safe_kv_bytes(1)],
        };

        let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
        let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);
        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        state
            .handle_raft_event(raft_event, &context, role_tx)
            .await
            .expect("should succeed");

        let response = resp_rx.recv().await.unwrap().unwrap();
        assert_eq!(response.error, ErrorCode::Success as i32); // Should succeed
    }
}

// Note: Integration tests for Follower leader discovery notification (ADR-012)
// are covered by unit tests in d-engine-core/src/raft_test.rs
// See notify_leader_elected_tests module for comprehensive test coverage
