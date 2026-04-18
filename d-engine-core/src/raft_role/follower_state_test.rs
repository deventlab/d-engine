use crate::test_utils::create_test_snapshot_stream;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::ReadConsistencyPolicy;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole;
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
use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::snapshot_ack::ChunkStatus;
use std::sync::Arc;
use tonic::Code;
use tonic::Status;

use crate::AppendResponseWithUpdates;
use crate::ClientCmd;
use crate::Error;
use crate::HardState;
use crate::MaybeCloneOneshot;
use crate::MaybeCloneOneshotSender;
use crate::MockElectionCore;
use crate::MockMembership;
use crate::MockReplicationCore;
use crate::MockStateMachineHandler;
use crate::NetworkError;
use crate::NewCommitData;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftOneshot;
use crate::RoleEvent;
use crate::StateUpdate;
use crate::SystemError;
use crate::raft_role::follower_state::FollowerState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use crate::test_utils::mock::mock_raft_context_with_temp;
use crate::test_utils::node_config;
use mockall::predicate::eq;
use tokio::sync::{mpsc, watch};

// ============================================================================
// Helper Functions
// ============================================================================

fn create_vote_request_event(
    term: u64,
    candidate_id: u32,
    resp_tx: MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>,
) -> RaftEvent {
    RaftEvent::ReceiveVoteRequest(
        VoteRequest {
            term,
            candidate_id,
            last_log_index: 0,
            last_log_term: 0,
        },
        resp_tx,
    )
}

/// Test: FollowerState drain_read_buffer returns NotLeader error
///
/// Scenario: Call drain_read_buffer() on Follower
/// Expected: Returns NotLeader error (Follower doesn't buffer reads)
#[tokio::test]
async fn test_follower_drain_read_buffer_returns_error() {
    let mut state = FollowerState::<MockTypeConfig>::new(
        1,
        Arc::new(node_config("/tmp/test_follower_drain")),
        None,
        None,
    );

    // Action: Call drain_read_buffer()
    let result = state.drain_read_buffer();

    // Verify: Returns NotLeader error
    assert!(
        result.is_err(),
        "Follower drain_read_buffer should return error"
    );

    if let Err(e) = result {
        let error_str = format!("{e:?}");
        assert!(
            error_str.contains("NotLeader"),
            "Error should be NotLeader, got: {error_str}"
        );
    }
}

/// Test: FollowerState initialization with fresh start
///
/// Scenario:
/// - First time node startup (no persisted state)
/// - No hard_state from database
/// - No last_applied index
///
/// Expected:
/// - commit_index = 0
/// - current_term = 1 (initial term)
/// - voted_for = None
/// - next_index = None (Follower doesn't track this)
/// - match_index = None (Follower doesn't track this)
/// - noop_log_id returns error (only Leader has this)
///
/// Original: test_new_with_fresh_start
#[tokio::test]
async fn test_new_initializes_fresh_state_correctly() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let node_id = 1;
    let hard_state_from_db = None;
    let last_applied_index_option = None;

    let state = FollowerState::<MockTypeConfig>::new(
        node_id,
        context.node_config.clone(),
        hard_state_from_db,
        last_applied_index_option,
    );

    assert_eq!(
        state.commit_index(),
        0,
        "Fresh start should have commit_index=0"
    );
    assert_eq!(state.current_term(), 1, "Fresh start should have term=1");
    assert_eq!(
        state.voted_for().unwrap(),
        None,
        "Fresh start should not have voted"
    );
    assert_eq!(
        state.next_index(state.node_id()),
        None,
        "Follower doesn't track next_index"
    );
    assert_eq!(
        state.match_index(state.node_id()),
        None,
        "Follower doesn't track match_index"
    );
    assert!(state.noop_log_id().is_err(), "Only Leader has noop_log_id");
}

/// Test: FollowerState initialization from persisted state (restart)
///
/// Scenario:
/// - Node restarts after crash/shutdown
/// - Has persisted hard_state (term=2, voted_for=node 3)
/// - Has last_applied index = 2
///
/// Expected:
/// - Restores term from hard_state (term=2)
/// - Restores voted_for from hard_state
/// - Sets commit_index = last_applied (2) for safety
///
/// This validates correct state recovery on restart.
///
/// Original: test_new_with_restart
#[tokio::test]
async fn test_new_restores_persisted_state_on_restart() {
    let voted_for = VotedFor {
        voted_for_id: 3,
        voted_for_term: 2,
        committed: false,
    };

    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

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
        context.node_config.clone(),
        hard_state_from_db,
        last_applied_index_option,
    );

    assert_eq!(
        state.commit_index(),
        2,
        "Should restore commit_index from last_applied"
    );
    assert_eq!(
        state.current_term(),
        2,
        "Should restore term from hard_state"
    );
    assert_eq!(
        state.voted_for().unwrap(),
        Some(voted_for),
        "Should restore voted_for"
    );
    assert!(
        state.noop_log_id().is_err(),
        "Follower doesn't have noop_log_id"
    );
}

/// Test: FollowerState rejects VoteRequest when handle_vote_request returns None
///
/// Scenario:
/// - Follower receives VoteRequest
/// - handle_vote_request returns Ok(StateUpdate { new_voted_for: None, ... })
/// - Vote is rejected (e.g., already voted for different candidate, stale term)
///
/// Expected:
/// - Response with vote_granted=false
/// - No role change
/// - Term unchanged
///
/// Original: test_handle_raft_event_case1_1
#[tokio::test]
async fn test_handle_vote_request_rejects_when_handler_returns_none() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

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

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = create_vote_request_event(1, 1, resp_tx);

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let r = resp_rx.recv().await.unwrap().unwrap();
    assert!(!r.vote_granted, "Should reject vote");
    assert!(role_rx.try_recv().is_err(), "No role change");
    assert_eq!(term_before, state.current_term(), "Term unchanged");
}

/// Test: FollowerState grants VoteRequest when handle_vote_request returns Some
///
/// Scenario:
/// - Follower receives VoteRequest with higher term
/// - handle_vote_request returns Ok(StateUpdate { new_voted_for: Some(...), term_update: Some(100) })
/// - Vote is granted
///
/// Expected:
/// - Response with vote_granted=true
/// - Term updated to 100
/// - No role change (stays Follower)
///
/// Original: test_handle_raft_event_case1_2
#[tokio::test]
async fn test_handle_vote_request_grants_and_updates_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

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

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = create_vote_request_event(1, 1, resp_tx);

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let r = resp_rx.recv().await.unwrap().unwrap();
    assert!(r.vote_granted, "Should grant vote");
    assert!(role_rx.try_recv().is_err(), "Should remain Follower");
    assert_eq!(state.current_term(), updated_term, "Term should update");
}

/// Test: FollowerState handles vote request error from handler
///
/// Scenario:
/// - Follower receives VoteRequest
/// - handle_vote_request returns Error (e.g., network/system error)
///
/// Expected:
/// - Response with vote_granted=false
/// - handle_raft_event returns Error
/// - Term unchanged
///
/// Original: test_handle_raft_event_case1_3
#[tokio::test]
async fn test_handle_vote_request_returns_error_on_handler_failure() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

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

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = create_vote_request_event(1, 1, resp_tx);

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_err());

    let r = resp_rx.recv().await.unwrap().unwrap();
    assert!(!r.vote_granted, "Should reject on error");
    assert_eq!(state.current_term(), term_before, "Term unchanged");
}

/// Test: FollowerState handles ClusterConf metadata request
///
/// Scenario:
/// - Follower receives MetadataRequest (ClusterConf event)
///
/// Expected:
/// - Returns current cluster membership configuration
///
/// Original: test_handle_raft_event_case2
#[tokio::test]
async fn test_handle_cluster_conf_metadata_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut membership = MockMembership::new();
    membership.expect_retrieve_cluster_membership_config().times(1).returning(
        |_current_leader_id| ClusterMembership {
            version: 1,
            nodes: vec![],
            current_leader_id: None,
        },
    );
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConf(MetadataRequest {}, resp_tx);

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let m = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(m.nodes, vec![]);
}

/// Test: FollowerState handles successful ClusterConfUpdate
///
/// Scenario:
/// - Follower receives ClusterConfUpdate from leader
/// - Membership update succeeds
///
/// Expected:
/// - Returns success response with error_code=Unspecified
///
/// Original: test_handle_raft_event_case3_1
#[tokio::test]
async fn test_handle_cluster_conf_update_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

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
                error_code: cluster_conf_update_response::ErrorCode::Unspecified.into(),
            })
        });
    membership.expect_get_cluster_conf_version().returning(|| 1);
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2,
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
        cluster_conf_update_response::ErrorCode::Unspecified as i32
    );
}

/// Test: FollowerState tick triggers election timeout
///
/// Scenario:
/// - Follower receives tick event
/// - Election timeout has expired (no heartbeat from leader)
///
/// Expected:
/// - Sends BecomeCandidate event
/// - Transitions to Candidate role
///
/// This validates the core Raft rule: Follower starts election
/// when it doesn't hear from leader within election timeout.
///
/// Original: test_tick
#[tokio::test(start_paused = true)]
async fn test_tick_triggers_election_on_timeout() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);

    let election_timeout_max = context.node_config.raft.election.election_timeout_max;
    tokio::time::advance(tokio::time::Duration::from_millis(election_timeout_max + 1)).await;

    assert!(
        state.tick(&role_tx, &event_tx, &context).await.is_ok(),
        "Tick should succeed"
    );

    let r = role_rx.recv().await.unwrap();
    assert!(
        matches!(r, RoleEvent::BecomeCandidate),
        "Should send BecomeCandidate event on timeout"
    );
}

// ============================================================================
// AppendEntries Tests
// ============================================================================

/// Test: FollowerState successfully handles AppendEntries from leader
///
/// Scenario:
/// - Follower (term=1) receives AppendEntries from leader (term=2)
/// - Leader has higher term and new commit index
/// - replication_handler.handle_append_entries returns success
///
/// Expected:
/// 1. Sends LeaderDiscovered event (marks new leader)
/// 2. Sends NotifyNewCommitIndex event
/// 3. Updates current_term to leader's term (2)
/// 4. Updates commit_index to new value (2)
/// 5. Returns AppendEntriesResponse with success=true
/// 6. handle_raft_event returns Ok(())
///
/// This validates the core Raft rule: Follower accepts entries from
/// valid leader and updates its state accordingly.
///
/// Original: test_handle_raft_event_case4_1
#[tokio::test]
async fn test_handle_append_entries_success_from_new_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let follower_term = 1;
    let new_leader_term = follower_term + 1;
    let expect_new_commit = 2;

    // Mock replication handler to return success
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

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(follower_term);

    // Prepare AppendEntries request from leader
    let append_entries_request = AppendEntriesRequest {
        term: new_leader_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_entries_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Action: Handle AppendEntries event
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: LeaderDiscovered event sent
    assert!(
        matches!(
            role_rx.try_recv().unwrap(),
            RoleEvent::LeaderDiscovered(5, _)
        ),
        "Should send LeaderDiscovered event"
    );

    // Verify: NotifyNewCommitIndex event sent
    assert!(
        matches!(
            role_rx.try_recv().unwrap(),
            RoleEvent::NotifyNewCommitIndex(NewCommitData {
                new_commit_index: _,
                role: _,
                current_term: _
            })
        ),
        "Should send NotifyNewCommitIndex event"
    );

    // Verify: Term and commit_index updated
    assert_eq!(
        state.current_term(),
        new_leader_term,
        "Should update term to leader's term"
    );
    assert_eq!(
        state.commit_index(),
        expect_new_commit,
        "Should update commit_index"
    );

    // Verify: Response with success=true
    let response = resp_rx.recv().await.expect("should receive response").unwrap();
    assert!(response.is_success(), "Response should indicate success");
}

/// Test: FollowerState rejects AppendEntries with stale term
///
/// Scenario:
/// - Follower (term=2) receives AppendEntries from stale leader (term=1)
/// - Request term is lower than follower's term
/// - replication_handler.check_append_entries_request_is_legal is called
///
/// Expected:
/// 1. No events sent (no role change, no commit update)
/// 2. Term unchanged (remains at 2)
/// 3. Returns AppendEntriesResponse with is_higher_term=true
/// 4. handle_raft_event returns Ok(())
///
/// This validates the core Raft rule: Reject requests with stale term.
///
/// Original: test_handle_raft_event_case4_2
#[tokio::test]
async fn test_handle_append_entries_rejects_stale_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let follower_term = 2;
    let stale_leader_term = follower_term - 1;

    // Mock replication handler to check request is legal
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_check_append_entries_request_is_legal()
        .returning(move |_, _, _| AppendEntriesResponse::success(1, follower_term, None));

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(follower_term);

    // Prepare AppendEntries request with stale term
    let append_entries_request = AppendEntriesRequest {
        term: stale_leader_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_entries_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Action: Handle AppendEntries event
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: No events sent
    assert!(role_rx.try_recv().is_err(), "Should not send any events");

    // Verify: Term unchanged
    assert_eq!(
        state.current_term(),
        follower_term,
        "Term should remain unchanged"
    );

    // Verify: Response with is_higher_term=true
    let response = resp_rx.recv().await.expect("should receive response").unwrap();
    assert!(
        response.is_higher_term(),
        "Response should indicate higher term"
    );
}

/// Test: FollowerState handles AppendEntries failure from handler
///
/// Scenario:
/// - Follower receives AppendEntries from leader (valid term)
/// - replication_handler.handle_append_entries returns Error (e.g., log conflict, disk error)
///
/// Expected:
/// 1. Sends LeaderDiscovered event (leader is valid, even though append failed)
/// 2. No other events sent (no commit update)
/// 3. Term updated to leader's term
/// 4. Returns AppendEntriesResponse with success=false
/// 5. handle_raft_event returns Err()
///
/// This validates correct error handling: Leader is recognized, but
/// append operation failed and must be retried.
///
/// Original: test_handle_raft_event_case4_3
#[tokio::test]
async fn test_handle_append_entries_with_handler_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let follower_term = 1;
    let new_leader_term = follower_term + 1;

    // Mock replication handler to return error
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .returning(|_, _, _| Err(Error::Fatal("test error".to_string())));

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(follower_term);

    // Prepare AppendEntries request
    let append_entries_request = AppendEntriesRequest {
        term: new_leader_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_entries_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Action: Handle AppendEntries event
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_err(),
        "handle_raft_event should return error"
    );

    // Verify: LeaderDiscovered event sent (leader is valid)
    assert!(
        matches!(
            role_rx.try_recv().unwrap(),
            RoleEvent::LeaderDiscovered(5, _)
        ),
        "Should send LeaderDiscovered event even on error"
    );

    // Verify: No other events
    assert!(
        role_rx.try_recv().is_err(),
        "No other events should be sent"
    );

    // Verify: Term updated
    assert_eq!(
        state.current_term(),
        new_leader_term,
        "Should update term even on error"
    );

    // Verify: Response with success=false
    let response = resp_rx.recv().await.expect("should receive response").unwrap();
    assert!(!response.is_success(), "Response should indicate failure");
}

// ============================================================================
// ClusterConfUpdate Tests
// ============================================================================

/// Test: FollowerState handles ClusterConfUpdate with NOT_LEADER error
///
/// Scenario:
/// - Follower receives ClusterConfUpdate request from non-leader node
/// - Membership handler returns NOT_LEADER error
///
/// Expected:
/// - Returns response with success=false
/// - error_code = NOT_LEADER
/// - handle_raft_event returns Ok(())
///
/// This validates rejection of configuration changes from non-leader nodes.
///
/// Original: test_handle_raft_event_case3_2
#[tokio::test]
async fn test_handle_cluster_conf_update_rejects_non_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

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
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConfUpdate(
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
    assert!(!response.success, "Should reject non-leader request");
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::NotLeader as i32
    );
}

/// Test: FollowerState handles ClusterConfUpdate with VERSION_CONFLICT
///
/// Scenario:
/// - Follower receives ClusterConfUpdate with stale version (4)
/// - Current cluster config version is 5
/// - Membership handler returns VERSION_CONFLICT error
///
/// Expected:
/// - Returns response with success=false
/// - error_code = VERSION_CONFLICT
/// - Response includes current version (5)
/// - handle_raft_event returns Ok(())
///
/// This validates version conflict detection for configuration changes.
///
/// Original: test_handle_raft_event_case3_3
#[tokio::test]
async fn test_handle_cluster_conf_update_detects_version_conflict() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

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
    membership.expect_get_cluster_conf_version().returning(|| 5);
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2,
            term: 1,
            version: 4, // Stale version
            change: None,
        },
        resp_tx,
    );

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success, "Should reject stale version");
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::VersionConflict as i32
    );
    assert_eq!(response.version, 5, "Should return current version");
}

/// Test: FollowerState handles ClusterConfUpdate with TERM_OUTDATED
///
/// Scenario:
/// - Follower has current term = 5
/// - Receives ClusterConfUpdate with stale term = 4
/// - Membership handler returns TERM_OUTDATED error
///
/// Expected:
/// - Returns response with success=false
/// - error_code = TERM_OUTDATED
/// - Response includes current term (5)
/// - handle_raft_event returns Ok(())
///
/// This validates term checking for configuration changes.
///
/// Original: test_handle_raft_event_case3_4
#[tokio::test]
async fn test_handle_cluster_conf_update_rejects_stale_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

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
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(5); // Follower has higher term

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2,
            term: 4, // Stale term
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success, "Should reject stale term");
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::TermOutdated as i32
    );
    assert_eq!(response.term, 5, "Should return current term");
}

/// Test: FollowerState handles ClusterConfUpdate with internal error
///
/// Scenario:
/// - Follower receives ClusterConfUpdate from leader
/// - Membership handler encounters internal error during update
///
/// Expected:
/// - Returns response with success=false
/// - error_code = INTERNAL_ERROR
/// - handle_raft_event returns Ok(())
///
/// This validates error handling for internal failures during configuration updates.
///
/// Original: test_handle_raft_event_case3_5
#[tokio::test]
async fn test_handle_cluster_conf_update_handles_internal_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut membership = MockMembership::new();
    membership
        .expect_update_cluster_conf_from_leader()
        .times(1)
        .returning(|_, _, _, _, _| {
            Err(Error::Consensus(crate::ConsensusError::Membership(
                crate::MembershipError::ConfigChangeUpdateFailed("test error".to_string()),
            )))
        });
    membership.expect_get_cluster_conf_version().returning(|| 1);
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2,
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success, "Should fail on internal error");
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::InternalError as i32
    );
}

/// Test: FollowerState handles ClusterConfUpdate when no leader is known
///
/// Scenario:
/// - Follower receives ClusterConfUpdate but doesn't know current leader
/// - Membership handler returns NOT_LEADER error
///
/// Expected:
/// - Returns response with success=false
/// - error_code = NOT_LEADER
/// - handle_raft_event returns Ok(())
///
/// This validates behavior when configuration change is attempted but
/// cluster leadership is unknown.
///
/// Original: test_handle_raft_event_case3_6
#[tokio::test]
async fn test_handle_cluster_conf_update_when_leader_unknown() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

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
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 3,
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success, "Should reject when leader unknown");
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::NotLeader as i32
    );
}

// ============================================================================
// Client Request Tests
// ============================================================================

/// Test: FollowerState redirects ClientWriteRequest to leader
///
/// Scenario:
/// - Follower receives ClientWriteRequest (write must go to leader)
/// - Follower is not the leader
///
/// Expected:
/// - Returns response with error_code = NOT_LEADER
/// - handle_raft_event returns Ok(())
/// - No state changes
///
/// This validates the core Raft rule: Only leader can process writes.
/// Follower must redirect client to leader.
///
/// Original: test_handle_raft_event_case5
#[tokio::test]
async fn test_handle_client_write_request_redirects_to_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Propose(
        ClientWriteRequest {
            client_id: 1,
            command: Some(WriteCommand::default()),
        },
        resp_tx,
    );

    // Action: Handle ClientWriteRequest
    state.push_client_cmd(cmd, &context);

    // Verify: Response with NOT_LEADER error
    let result = resp_rx.recv().await.expect("channel should not be closed");
    assert!(result.is_err(), "Should return NOT_LEADER error");
    let err = result.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("Not leader"));
}

/// Test: FollowerState rejects ClientReadRequest with LinearizableRead policy
///
/// Scenario:
/// - Follower receives ClientReadRequest with LinearizableRead consistency
/// - LinearizableRead requires leader involvement (lease-based or ReadIndex)
///
/// Expected:
/// - Returns response with error_code = NOT_LEADER
/// - handle_raft_event returns Ok(())
///
/// This validates that linearizable reads must go through leader to ensure
/// consistency guarantees (leader lease or ReadIndex protocol).
///
/// Original: test_handle_raft_event_case6_1
#[tokio::test]
async fn test_handle_client_read_request_linearizable_redirects_to_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        keys: vec![],
    };

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    // Action: Handle ClientReadRequest
    state.push_client_cmd(cmd, &context);

    // Verify: Response with NOT_LEADER error
    let result = resp_rx.recv().await.expect("channel should not be closed");
    assert!(result.is_err(), "Should return NOT_LEADER error");
    let err = result.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("Not leader"));
}

/// Test: FollowerState handles ClientReadRequest with EventualConsistency policy
///
/// Scenario:
/// - Follower receives ClientReadRequest with EventualConsistency policy
/// - EventualConsistency allows reading from follower (stale reads acceptable)
/// - StateMachine handler returns data successfully
///
/// Expected:
/// - Calls state_machine_handler.read_from_state_machine()
/// - Returns response with error_code = SUCCESS
/// - Returns data from state machine
/// - handle_raft_event returns Ok(())
///
/// This validates that followers can serve eventual consistency reads,
/// improving read scalability at the cost of potentially stale data.
///
/// Original: test_handle_raft_event_case6_2
#[tokio::test]
async fn test_handle_client_read_request_eventual_consistency_succeeds() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Mock state machine to return data
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_read_from_state_machine()
        .times(1)
        .returning(|_| Some(vec![]));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
        keys: vec![],
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    // Action: Handle ClientReadRequest
    state.push_client_cmd(cmd, &context);

    // Verify: Response with SUCCESS
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(
        response.error,
        ErrorCode::Success as i32,
        "EventualConsistency read should succeed on follower"
    );
}

#[tokio::test]
async fn test_handle_join_cluster_rejects_on_follower() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let request = JoinRequest {
        status: d_engine_proto::common::NodeStatus::Promotable as i32,
        node_id: 2,
        node_role: NodeRole::Learner.into(),
        address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::JoinCluster(request, resp_tx);
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Handle JoinCluster event
    let result = state.handle_raft_event(raft_event, &context, role_tx).await;

    // Verify: Returns error
    assert!(
        result.is_err(),
        "Follower should reject JoinCluster request"
    );

    // Verify: Response with PermissionDenied
    let response = resp_rx.recv().await.expect("Should receive response");
    assert!(response.is_err(), "Response should be error");
    let status = response.unwrap_err();
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "Should return PermissionDenied"
    );
}

/// Test: FollowerState rejects LeaderDiscovery request
///
/// Scenario:
/// - Follower receives LeaderDiscoveryRequest (client wants to find leader)
/// - Follower doesn't know current leader or is not the leader
///
/// Expected:
/// - Returns Status error with Code::PermissionDenied
/// - handle_raft_event returns Ok() (not a fatal error)
/// - No state changes
///
/// This validates that follower correctly rejects leader discovery
/// when it cannot provide leader information.
///
/// Original: test_handle_raft_event_case11
#[tokio::test]
async fn test_handle_leader_discovery_rejects_on_follower() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let request = LeaderDiscoveryRequest {
        node_id: 2,
        requester_address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::DiscoverLeader(request, resp_tx);
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Handle DiscoverLeader event
    let result = state.handle_raft_event(raft_event, &context, role_tx).await;

    // Verify: Returns Ok (not a fatal error)
    assert!(
        result.is_ok(),
        "handle_raft_event should return Ok for DiscoverLeader"
    );

    // Verify: Response with PermissionDenied
    let response = resp_rx.recv().await.expect("Should receive response");
    assert!(response.is_err(), "Response should be error");
    let status = response.unwrap_err();
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "Should return PermissionDenied"
    );
}

// ============================================================================
// Log Purge Safety Tests (can_purge_logs)
// ============================================================================

/// Test: can_purge_logs validates safe purge range
///
/// Scenario:
/// - commit_index = 100
/// - last_purge_index = 90
/// - Request to purge up to index 99
///
/// Expected:
/// - Returns true (99 < 100, satisfies gap requirement)
/// - Edge case: 99 == commit_index - 1 is valid
/// - Invalid: 100 not < 100 (violates gap rule)
///
/// This validates Raft log compaction safety: must maintain
/// at least one entry between purge and commit for consistency.
///
/// Original: test_can_purge_logs_case1
#[test]
fn test_can_purge_logs_validates_safe_range() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Setup state matching Raft paper's log compaction rules
    state.shared_state.commit_index = 100; // Last committed entry at 100

    // Test valid purge range (90 < 99 < 100)
    assert!(
        state.can_purge_logs(
            Some(LogId { index: 90, term: 1 }), // last_purge_index
            LogId { index: 99, term: 1 }        // last_included_in_request
        ),
        "Should allow purge when last_included < commit_index"
    );

    // Edge case: 99 == commit_index - 1 (per gap rule)
    assert!(
        state.can_purge_logs(
            Some(LogId { index: 90, term: 1 }),
            LogId { index: 99, term: 1 }
        ),
        "Should allow purge up to commit_index - 1"
    );

    // Violate gap rule: 100 not < 100
    assert!(
        !state.can_purge_logs(
            Some(LogId { index: 90, term: 1 }),
            LogId {
                index: 100,
                term: 1
            }
        ),
        "Should reject purge at commit_index (violates gap)"
    );
}

/// Test: can_purge_logs rejects uncommitted index
///
/// Scenario:
/// - commit_index = 50
/// - Request to purge beyond commit_index
///
/// Expected:
/// - Returns false for purge_index > commit_index
/// - Returns false for purge_index == commit_index
///
/// This validates Raft §5.4.2: never purge uncommitted entries.
///
/// Original: test_can_purge_logs_case2
#[test]
fn test_can_purge_logs_rejects_uncommitted_index() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    state.shared_state.commit_index = 50;

    // Leader tries to purge beyond commit index
    assert!(
        !state.can_purge_logs(
            Some(LogId { index: 40, term: 1 }),
            LogId { index: 51, term: 1 } // 51 > commit_index(50)
        ),
        "Should reject purge beyond commit_index"
    );

    // Boundary check: 50 == commit_index (violates <)
    assert!(
        !state.can_purge_logs(
            Some(LogId { index: 40, term: 1 }),
            LogId { index: 50, term: 1 }
        ),
        "Should reject purge at commit_index"
    );
}

/// Test: can_purge_logs ensures monotonicity
///
/// Scenario:
/// - commit_index = 200
/// - last_purge_index advances: 100 → 150
/// - Attempt to purge backwards or same index
///
/// Expected:
/// - Returns true for monotonic advance (100 → 150)
/// - Returns false for backwards purge (150 → 120)
/// - Returns false for same index purge (150 → 150)
///
/// This validates Raft §7.2: purge index must always advance.
///
/// Original: test_can_purge_logs_case3
#[test]
fn test_can_purge_logs_ensures_monotonicity() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    state.shared_state.commit_index = 200;

    // Valid sequence: 100 → 150
    assert!(
        state.can_purge_logs(
            Some(LogId {
                index: 100,
                term: 1
            }),
            LogId {
                index: 150,
                term: 1
            }
        ),
        "Should allow monotonic purge advance"
    );

    // Invalid: Attempt to purge backwards (150 → 120)
    assert!(
        !state.can_purge_logs(
            Some(LogId {
                index: 150,
                term: 1
            }),
            LogId {
                index: 120,
                term: 1
            }
        ),
        "Should reject backwards purge"
    );

    // Same index purge attempt
    assert!(
        !state.can_purge_logs(
            Some(LogId {
                index: 150,
                term: 1
            }),
            LogId {
                index: 150,
                term: 1
            }
        ),
        "Should reject same index purge"
    );
}

/// Test: can_purge_logs handles initial purge state
///
/// Scenario:
/// - commit_index = 100
/// - No previous purge (last_purge_index = None)
/// - First purge request
///
/// Expected:
/// - Returns true for valid first purge (index < commit_index)
/// - Returns false if first purge violates gap rule
///
/// This validates initial purge must still respect safety rules.
///
/// Original: test_can_purge_logs_case4
#[test]
fn test_can_purge_logs_handles_initial_state() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    state.shared_state.commit_index = 100;

    // First ever purge (last_purge_index = None)
    assert!(
        state.can_purge_logs(
            None, // No previous purge
            LogId { index: 99, term: 1 }
        ),
        "Should allow first purge with valid index"
    );

    // First purge must still obey commit_index gap
    assert!(
        !state.can_purge_logs(
            None,
            LogId {
                index: 100,
                term: 1
            } // 100 not < 100
        ),
        "First purge must still respect gap rule"
    );
}

// ============================================================================
// Role Violation Tests Module
// ============================================================================

mod role_violation_tests {
    use super::*;
    use crate::ConsensusError;

    /// Test: FollowerState rejects role violation events
    ///
    /// Scenario:
    /// - Follower receives events that only Leader can handle:
    ///   - CreateSnapshotEvent
    ///   - SnapshotCreated
    ///   - LogPurgeCompleted
    ///
    /// Expected:
    /// - All return Error::Consensus(ConsensusError::RoleViolation)
    /// - handle_raft_event returns Err()
    /// - No state changes
    ///
    /// This validates role-based access control for Raft events.
    ///
    /// Original: test_role_violation_events (in module)
    #[tokio::test]
    async fn test_follower_rejects_leader_only_events() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

        // [Test LogPurgeCompleted]
        // Follower now handles CreateSnapshotEvent and SnapshotCreated independently (Raft §7)
        // but should NOT receive LogPurgeCompleted from external sources
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::LogPurgeCompleted(LogId { term: 1, index: 1 });
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        assert!(
            matches!(e, Error::Consensus(ConsensusError::RoleViolation { .. })),
            "LogPurgeCompleted should return RoleViolation"
        );
    }

    /// Test: Follower ignores duplicate CreateSnapshotEvent while snapshot is in progress
    ///
    /// Purpose:
    /// Validates that the snapshot_in_progress flag prevents concurrent snapshot creation,
    /// ensuring snapshot consistency and avoiding resource waste from duplicate operations.
    ///
    /// Scenario:
    /// - First CreateSnapshotEvent is received and sets snapshot_in_progress=true
    /// - Second CreateSnapshotEvent arrives before first completes
    ///
    /// Expected:
    /// - First event: Returns Ok(), starts async snapshot creation
    /// - Second event: Returns Ok() immediately without starting new snapshot (logged as skipped)
    /// - snapshot_in_progress flag protects against concurrent creation
    #[tokio::test]
    async fn test_follower_ignores_duplicate_create_snapshot_event() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

        // First CreateSnapshotEvent - should succeed
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let result1 = state
            .handle_raft_event(RaftEvent::CreateSnapshotEvent, &context, role_tx.clone())
            .await;
        assert!(result1.is_ok(), "First CreateSnapshotEvent should succeed");

        // Verify flag is set
        assert!(
            state.snapshot_in_progress.load(std::sync::atomic::Ordering::SeqCst),
            "snapshot_in_progress should be true after first event"
        );

        // Second CreateSnapshotEvent - should be ignored
        let result2 =
            state.handle_raft_event(RaftEvent::CreateSnapshotEvent, &context, role_tx).await;
        assert!(
            result2.is_ok(),
            "Second CreateSnapshotEvent should return Ok (ignored)"
        );

        // Flag should still be true (first snapshot still in progress)
        assert!(
            state.snapshot_in_progress.load(std::sync::atomic::Ordering::SeqCst),
            "snapshot_in_progress should remain true"
        );
    }

    /// Test: Follower resets snapshot_in_progress flag after SnapshotCreated (success case)
    ///
    /// Purpose:
    /// Validates that the snapshot_in_progress flag is correctly reset after snapshot completion,
    /// allowing subsequent snapshots to be created when needed.
    ///
    /// Scenario:
    /// - snapshot_in_progress is manually set to true (simulating ongoing snapshot)
    /// - SnapshotCreated event with successful result is received
    ///
    /// Expected:
    /// - Event handler returns Ok()
    /// - snapshot_in_progress flag is reset to false
    /// - System is ready to accept new CreateSnapshotEvent
    ///
    /// This ensures the flag lifecycle is: false → true (on create) → false (on complete)
    #[tokio::test]
    async fn test_follower_resets_snapshot_flag_on_success() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

        // Simulate snapshot in progress
        state.snapshot_in_progress.store(true, std::sync::atomic::Ordering::SeqCst);

        // Create successful snapshot result
        let metadata = d_engine_proto::server::storage::SnapshotMetadata {
            last_included: Some(LogId { term: 1, index: 50 }),
            checksum: bytes::Bytes::new(),
        };
        let snapshot_result = Ok((metadata, std::path::PathBuf::from("/tmp/test_snapshot.bin")));

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let result = state
            .handle_raft_event(
                RaftEvent::SnapshotCreated(snapshot_result),
                &context,
                role_tx,
            )
            .await;

        assert!(result.is_ok(), "SnapshotCreated should succeed");

        // Verify flag is reset
        assert!(
            !state.snapshot_in_progress.load(std::sync::atomic::Ordering::SeqCst),
            "snapshot_in_progress should be false after SnapshotCreated"
        );
    }

    /// Test: Follower resets snapshot_in_progress flag after SnapshotCreated (failure case)
    ///
    /// Purpose:
    /// Validates that the snapshot_in_progress flag is reset even when snapshot creation fails,
    /// allowing the system to retry snapshot creation later without being permanently blocked.
    ///
    /// Scenario:
    /// - snapshot_in_progress is set to true
    /// - SnapshotCreated event with error result is received
    ///
    /// Expected:
    /// - Event handler returns Ok() (error is logged but not propagated)
    /// - snapshot_in_progress flag is reset to false
    /// - System can retry snapshot creation on next ApplyCompleted trigger
    ///
    /// This ensures failure recovery: the flag doesn't stay locked after an error
    #[tokio::test]
    async fn test_follower_resets_snapshot_flag_on_failure() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

        // Simulate snapshot in progress
        state.snapshot_in_progress.store(true, std::sync::atomic::Ordering::SeqCst);

        // Create failed snapshot result
        let snapshot_result = Err(Error::Fatal("Snapshot creation failed".to_string()));

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let result = state
            .handle_raft_event(
                RaftEvent::SnapshotCreated(snapshot_result),
                &context,
                role_tx,
            )
            .await;

        assert!(
            result.is_ok(),
            "SnapshotCreated with error should return Ok"
        );

        // Verify flag is reset even on failure
        assert!(
            !state.snapshot_in_progress.load(std::sync::atomic::Ordering::SeqCst),
            "snapshot_in_progress should be false after failed SnapshotCreated"
        );
    }
}

// ============================================================================
// ClientRead Consistency Policy Tests Module
// ============================================================================

mod handle_client_read_request {
    use super::*;
    use crate::RaftNodeConfig;
    use crate::config::ReadConsistencyPolicy as ServerPolicy;
    use crate::convert::safe_kv_bytes;
    use d_engine_proto::client::ReadConsistencyPolicy as ClientPolicy;

    /// Test: Follower rejects LeaseRead policy
    ///
    /// Scenario:
    /// - Client requests read with LeaseRead consistency policy
    /// - LeaseRead requires leader lease for linearizability
    ///
    /// Expected:
    /// - Returns response with error_code = NOT_LEADER
    /// - handle_raft_event returns Ok()
    ///
    /// This validates that follower correctly rejects lease-based reads
    /// which require leader involvement.
    ///
    /// Original: test_handle_client_read_lease_read_policy (in module)
    #[tokio::test]
    async fn test_follower_rejects_lease_read_policy() {
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
        let cmd = ClientCmd::Read(client_read_request, resp_tx);

        state.push_client_cmd(cmd, &context);

        let result = resp_rx.recv().await.expect("channel should not be closed");
        assert!(result.is_err(), "LeaseRead should be rejected by follower");
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("Not leader"));
    }

    /// Test: Follower uses server default policy (LinearizableRead)
    ///
    /// Scenario:
    /// - Client sends read request without specifying consistency policy
    /// - Server default is LinearizableRead
    /// - Follower cannot serve linearizable reads
    ///
    /// Expected:
    /// - Returns response with error_code = NOT_LEADER
    /// - handle_raft_event returns Ok()
    ///
    /// This validates that follower respects server default policy
    /// and redirects when it cannot satisfy the consistency requirement.
    ///
    /// Original: test_handle_client_read_unspecified_policy (in module)
    #[tokio::test]
    async fn test_follower_applies_server_default_policy() {
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
        let cmd = ClientCmd::Read(client_read_request, resp_tx);

        state.push_client_cmd(cmd, &context);

        let result = resp_rx.recv().await.expect("channel should not be closed");
        assert!(
            result.is_err(),
            "Default LinearizableRead should be rejected by follower"
        );
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("Not leader"));
    }

    /// Test: Follower serves EventualConsistency reads
    ///
    /// Scenario:
    /// - Server default policy is EventualConsistency
    /// - Client sends read request (uses server default)
    /// - Follower can serve eventual consistency reads
    ///
    /// Expected:
    /// - Returns response with error_code = SUCCESS
    /// - Data served from follower's state machine
    /// - handle_raft_event returns Ok()
    ///
    /// This validates that followers can serve stale reads when
    /// eventual consistency is acceptable, improving read scalability.
    ///
    /// Original: test_handle_client_read_eventual_consistency_policy (in module)
    #[tokio::test]
    async fn test_follower_serves_eventual_consistency_reads() {
        let (_graceful_tx, graceful_rx) = watch::channel(());

        // Configure server to allow eventual consistency reads
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
        let cmd = ClientCmd::Read(client_read_request, resp_tx);

        state.push_client_cmd(cmd, &context);

        let response = resp_rx.recv().await.unwrap().unwrap();
        assert_eq!(
            response.error,
            ErrorCode::Success as i32,
            "EventualConsistency read should succeed on follower"
        );
    }

    /// Test: Follower ignores client-specified LinearizableRead when override is disabled,
    /// falls back to server default EventualConsistency, and serves the read locally.
    ///
    /// Scenario:
    /// - Server default policy = EventualConsistency
    /// - allow_client_override = false (server enforces its own policy)
    /// - Client explicitly specifies LinearizableRead (should be ignored)
    ///
    /// Expected:
    /// - Follower falls back to server default (EventualConsistency)
    /// - Read is served locally from state machine
    /// - Returns response with error_code = SUCCESS
    ///
    /// This validates that when allow_client_override=false, the server default
    /// always wins regardless of what the client requests. A client requesting
    /// stronger consistency than the server default is silently downgraded.
    #[tokio::test]
    async fn test_follower_client_override_disabled_falls_back_to_server_eventual() {
        let (_graceful_tx, graceful_rx) = watch::channel(());

        // Server enforces EventualConsistency; client override is forbidden
        let mut node_config = RaftNodeConfig::default();
        node_config.raft.read_consistency.default_policy = ServerPolicy::EventualConsistency;
        node_config.raft.read_consistency.allow_client_override = false;

        let context = MockBuilder::new(graceful_rx).with_node_config(node_config).build_context();

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

        // Client requests LinearizableRead but server will ignore it
        let client_read_request = ClientReadRequest {
            client_id: 1,
            consistency_policy: Some(ClientPolicy::LinearizableRead as i32),
            keys: vec![safe_kv_bytes(1)],
        };

        let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
        let cmd = ClientCmd::Read(client_read_request, resp_tx);

        state.push_client_cmd(cmd, &context);

        // Follower must serve successfully using server default (EventualConsistency)
        let response = resp_rx.recv().await.unwrap().unwrap();
        assert_eq!(
            response.error,
            ErrorCode::Success as i32,
            "Follower should serve read using server default EventualConsistency, ignoring client LinearizableRead"
        );
    }

    /// Test: Follower ignores client-specified EventualConsistency when override is disabled,
    /// falls back to server default LinearizableRead, and rejects the request (not a leader).
    ///
    /// Scenario:
    /// - Server default policy = LinearizableRead
    /// - allow_client_override = false (server enforces its own policy)
    /// - Client explicitly specifies EventualConsistency (should be ignored)
    ///
    /// Expected:
    /// - Follower falls back to server default (LinearizableRead)
    /// - LinearizableRead requires leader — follower rejects with NOT_LEADER
    /// - Returns FailedPrecondition status
    ///
    /// This validates that allow_client_override=false prevents clients from
    /// downgrading consistency requirements (a potential security/correctness concern).
    /// The server must enforce its minimum consistency guarantee.
    #[tokio::test]
    async fn test_follower_client_override_disabled_falls_back_to_server_linear_rejects() {
        let (_graceful_tx, graceful_rx) = watch::channel(());

        // Server enforces LinearizableRead; client override is forbidden
        let mut node_config = RaftNodeConfig::default();
        node_config.raft.read_consistency.default_policy = ServerPolicy::LinearizableRead;
        node_config.raft.read_consistency.allow_client_override = false;

        let context = MockBuilder::new(graceful_rx).with_node_config(node_config).build_context();

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

        // Client requests EventualConsistency but server will ignore it
        let client_read_request = ClientReadRequest {
            client_id: 1,
            consistency_policy: Some(ClientPolicy::EventualConsistency as i32),
            keys: vec![safe_kv_bytes(1)],
        };

        let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
        let cmd = ClientCmd::Read(client_read_request, resp_tx);

        state.push_client_cmd(cmd, &context);

        // Follower must reject: server default is LinearizableRead which requires leader
        let result = resp_rx.recv().await.expect("channel should not be closed");
        assert!(
            result.is_err(),
            "Follower must reject LinearizableRead (requires leader)"
        );
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("Not leader"));
    }
}

/// Test: Follower handles FatalError and returns error
///
/// Verifies that when Follower receives FatalError from any component,
/// it returns Error::Fatal and stops further processing.
///
/// # Test Scenario
/// Follower receives FatalError event from state machine while in follower role.
/// Follower should recognize the fatal error and return Error::Fatal.
///
/// # Given
/// - Follower in normal state
/// - FatalError event from StateMachine component
///
/// # When
/// - Follower handles FatalError event via handle_raft_event()
///
/// # Then
/// - handle_raft_event() returns Error::Fatal
/// - Error message contains source and error details
/// - No role transition events are sent
#[tokio::test]
async fn test_follower_handles_fatal_error_returns_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context(
        "/tmp/test_follower_handles_fatal_error_returns_error",
        graceful_rx,
        None,
    );

    let hard_state = context.storage.raft_log.load_hard_state().expect("Failed to load hard state");
    let mut follower =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), hard_state, Some(0));

    // Create FatalError event
    let fatal_error = RaftEvent::FatalError {
        source: "StateMachine".to_string(),
        error: "Disk failure".to_string(),
    };

    // Create role event channel
    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();

    // Handle the FatalError event
    let result = follower.handle_raft_event(fatal_error, &context, role_tx).await;

    // VERIFY 1: handle_raft_event() returns Error::Fatal
    assert!(
        result.is_err(),
        "Expected handle_raft_event to return Err, got: {result:?}"
    );

    // VERIFY 2: Error is Fatal and contains source information
    match result.unwrap_err() {
        Error::Fatal(msg) => {
            assert!(
                msg.contains("StateMachine"),
                "Error message should mention source, got: {msg}"
            );
        }
        other => panic!("Expected Error::Fatal, got: {other:?}"),
    }

    // VERIFY 3: No role events sent
    assert!(
        role_rx.try_recv().is_err(),
        "No role transition events should be sent during FatalError handling"
    );
}

/// Test: Follower ApplyCompleted triggers snapshot when condition is met
///
/// Purpose: Verify that followers independently create snapshots per Raft §7.
/// This ensures follower snapshot progress allows leader to advance its purge_safe_index
/// and prevent unbounded log growth.
///
/// Scenario:
/// - Follower receives ApplyCompleted event after state machine apply
/// - Snapshot is enabled in config
/// - State machine handler indicates snapshot should be taken
///
/// Expected:
/// - CreateSnapshotEvent is sent back to role event loop for processing
/// - Event is reprocessed as RoleEvent::ReprocessEvent
#[tokio::test]
async fn test_apply_completed_triggers_snapshot_when_condition_met() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Create a mock state machine handler that returns true for should_snapshot
    let mut mock_sm_handler = crate::MockStateMachineHandler::new();
    mock_sm_handler
        .expect_should_snapshot()
        .with(eq(NewCommitData {
            new_commit_index: 100,
            role: NodeRole::Follower as i32,
            current_term: 1,
        }))
        .times(1)
        .returning(|_| true);

    // Build context with mock state machine handler before context creation
    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(mock_sm_handler)
        .build_context();

    let hard_state = context.storage.raft_log.load_hard_state().expect("Failed to load hard state");
    let mut follower =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), hard_state, Some(0));

    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();

    // ACTION: Handle ApplyCompleted event
    let result = follower.handle_apply_completed(100, vec![], &context, &role_tx).await;

    // VERIFY 1: Event handling succeeds
    assert!(
        result.is_ok(),
        "ApplyCompleted should be handled successfully, got: {result:?}"
    );

    // VERIFY 2: CreateSnapshotEvent is sent as RoleEvent::ReprocessEvent
    let event = role_rx.try_recv().expect("Should receive snapshot event");
    match event {
        RoleEvent::ReprocessEvent(boxed_event) => {
            match *boxed_event {
                RaftEvent::CreateSnapshotEvent => {
                    // Success! Event is correctly wrapped
                }
                other => panic!("Expected CreateSnapshotEvent, got: {other:?}"),
            }
        }
        other => panic!("Expected RoleEvent::ReprocessEvent, got: {other:?}"),
    }

    // VERIFY 3: No additional events queued
    assert!(
        role_rx.try_recv().is_err(),
        "Should only send one snapshot event"
    );
}

/// Test: Follower ApplyCompleted does NOT trigger snapshot when condition is not met
///
/// Purpose: Verify that followers respect snapshot conditions and don't create unnecessary snapshots.
///
/// Scenario:
/// - Follower receives ApplyCompleted event
/// - Snapshot is enabled in config
/// - State machine handler indicates snapshot should NOT be taken (returns false)
///
/// Expected:
/// - No CreateSnapshotEvent is sent
/// - ApplyCompleted is processed normally without side effects
#[tokio::test]
async fn test_apply_completed_does_not_trigger_snapshot_when_condition_not_met() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Create a mock state machine handler that returns false for should_snapshot
    let mut mock_sm_handler = crate::MockStateMachineHandler::new();
    mock_sm_handler
        .expect_should_snapshot()
        .with(eq(NewCommitData {
            new_commit_index: 50,
            role: NodeRole::Follower as i32,
            current_term: 1,
        }))
        .times(1)
        .returning(|_| false);

    // Build context with mock state machine handler before context creation
    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(mock_sm_handler)
        .build_context();

    let hard_state = context.storage.raft_log.load_hard_state().expect("Failed to load hard state");
    let mut follower =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), hard_state, Some(0));

    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();

    // ACTION: Handle ApplyCompleted event
    let result = follower.handle_apply_completed(50, vec![], &context, &role_tx).await;

    // VERIFY 1: Event handling succeeds
    assert!(
        result.is_ok(),
        "ApplyCompleted should be handled successfully"
    );

    // VERIFY 2: No snapshot event is sent
    assert!(
        role_rx.try_recv().is_err(),
        "Should not send snapshot event when condition is not met"
    );
}

/// Test: Follower ApplyCompleted respects snapshot config disabled state
///
/// Purpose: Verify that snapshots are not triggered when snapshot feature is disabled.
///
/// Scenario:
/// - Follower receives ApplyCompleted event
/// - Snapshot is DISABLED in config (enable = false)
/// - State machine handler would indicate snapshot (returns true)
///
/// Expected:
/// - No CreateSnapshotEvent is sent (config takes precedence)
/// - ApplyCompleted is processed without attempting snapshot
#[tokio::test]
async fn test_apply_completed_respects_snapshot_disabled_config() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Create a mock state machine handler
    let mock_sm_handler = crate::MockStateMachineHandler::new();

    // Build context with snapshot disabled and mock handler
    let mut node_config = node_config("/tmp/test_follower_snapshot_disabled");
    node_config.raft.snapshot.enable = false;

    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(mock_sm_handler)
        .with_node_config(node_config)
        .build_context();

    let hard_state = context.storage.raft_log.load_hard_state().expect("Failed to load hard state");
    let mut follower =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), hard_state, Some(0));

    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();

    // ACTION: Handle ApplyCompleted event
    let result = follower.handle_apply_completed(100, vec![], &context, &role_tx).await;

    // VERIFY 1: Event handling succeeds
    assert!(
        result.is_ok(),
        "ApplyCompleted should be handled successfully"
    );

    // VERIFY 2: No snapshot event is sent (snapshot disabled in config)
    assert!(
        role_rx.try_recv().is_err(),
        "Should not send snapshot event when snapshot is disabled in config"
    );
}

// ============================================================================
// Role-Specific Behavior Tests
// ============================================================================

/// Follower - Lease/Linear Read Rejection
///
/// **Objective**: Verify Follower correctly rejects strong consistency reads
/// (Lease and Linearizable) with NOT_LEADER error
///
/// **Scenario**:
/// - Follower node receives Lease read request
/// - Follower node receives Linearizable read request
///
/// **Expected**:
/// - Both requests immediately rejected in push_client_cmd()
/// - Error: NOT_LEADER with Leader information
/// - No buffer entry
/// - Response time < 1ms (immediate rejection)
#[tokio::test]
async fn test_follower_rejects_strong_consistency_reads() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Test 1: Lease read should be rejected
    {
        let (response_tx, mut response_rx) = MaybeCloneOneshot::new();
        let read_req = ClientReadRequest {
            client_id: 1,
            keys: vec![bytes::Bytes::from("lease_key")],
            consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
        };

        let start = tokio::time::Instant::now();

        state.push_client_cmd(ClientCmd::Read(read_req, response_tx), &context);

        let result = response_rx.recv().await;
        let elapsed = start.elapsed();

        assert!(result.is_ok(), "Should receive response from Follower");

        // Verify: Response time < 10ms (immediate rejection)
        assert!(
            elapsed.as_millis() < 10,
            "Lease read rejection should be immediate, took {:?}ms",
            elapsed.as_millis()
        );

        // Verify: Response is NOT_LEADER error
        if let Ok(Err(err)) = result {
            let err_str = format!("{err:?}");
            assert!(
                err_str.contains("Not leader")
                    || err_str.contains("NotLeader")
                    || err_str.contains("NOT_LEADER")
                    || err_str.contains("FailedPrecondition"),
                "Expected NOT_LEADER error for Lease read, got: {err:?}"
            );
        } else {
            panic!("Lease read to Follower should return NOT_LEADER error, got: {result:?}");
        }
    }

    // Test 2: Linearizable read should be rejected
    {
        let (response_tx, mut response_rx) = MaybeCloneOneshot::new();
        let read_req = ClientReadRequest {
            client_id: 1,
            keys: vec![bytes::Bytes::from("linear_key")],
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        };

        let start = tokio::time::Instant::now();

        state.push_client_cmd(ClientCmd::Read(read_req, response_tx), &context);

        let result = response_rx.recv().await;
        let elapsed = start.elapsed();

        assert!(result.is_ok(), "Should receive response from Follower");

        // Verify: Response time < 1ms (immediate rejection)
        assert!(
            elapsed.as_millis() < 10,
            "Linear read rejection should be immediate, took {:?}ms",
            elapsed.as_millis()
        );

        // Verify: Response is NOT_LEADER error
        if let Ok(Err(err)) = result {
            let err_str = format!("{err:?}");
            assert!(
                err_str.contains("Not leader")
                    || err_str.contains("NotLeader")
                    || err_str.contains("NOT_LEADER")
                    || err_str.contains("FailedPrecondition"),
                "Expected NOT_LEADER error for Linear read, got: {err:?}"
            );
        } else {
            panic!("Linear read to Follower should return NOT_LEADER error, got: {result:?}");
        }
    }
}

// ============================================================================
// MemFirst ACK Tests
// ============================================================================

/// Follower ACKs leader immediately after memory write (MemFirst).
///
/// The IO thread continues to fsync asynchronously. Safety: before commit,
/// the leader's durable_index >= N (quorum uses durable_index).
#[tokio::test]
async fn test_follower_acks_immediately_after_memory_write() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let leader_term = 2u64;
    let appended_index = 5u64;

    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_append_entries().returning(move |_, _, _| {
        Ok(AppendResponseWithUpdates {
            response: AppendEntriesResponse::success(
                1,
                leader_term,
                Some(LogId {
                    term: leader_term,
                    index: appended_index,
                }),
            ),
            commit_index_update: None,
        })
    });
    context.handlers.replication_handler = replication_handler;
    context.membership = Arc::new(MockMembership::new());

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(leader_term);

    let append_request = AppendEntriesRequest {
        term: leader_term,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_request, resp_tx);
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    // MemFirst: ACK sent immediately, no waiting for fsync
    let response = resp_rx.try_recv().expect("ACK must be sent immediately after memory write");
    assert!(response.unwrap().is_success());
}

/// Follower sends ACK immediately for heartbeat (no entries).
#[tokio::test]
async fn test_follower_acks_immediately_for_heartbeat() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let leader_term = 2u64;

    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_append_entries().returning(move |_, _, _| {
        Ok(AppendResponseWithUpdates {
            response: AppendEntriesResponse::success(1, leader_term, None),
            commit_index_update: None,
        })
    });
    context.handlers.replication_handler = replication_handler;
    context.membership = Arc::new(MockMembership::new());

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(leader_term);

    let append_request = AppendEntriesRequest {
        term: leader_term,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_request, resp_tx);
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    let response = resp_rx.try_recv().expect("Heartbeat ACK must be sent immediately");
    assert!(response.unwrap().is_success());
}

/// commit_index advances immediately on AppendEntries, ACK is also sent immediately.
#[tokio::test]
async fn test_follower_commit_index_and_ack_both_sent_immediately() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let leader_term = 2u64;
    let appended_index = 5u64;
    let new_commit = 3u64;

    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_append_entries().returning(move |_, _, _| {
        Ok(AppendResponseWithUpdates {
            response: AppendEntriesResponse::success(
                1,
                leader_term,
                Some(LogId {
                    term: leader_term,
                    index: appended_index,
                }),
            ),
            commit_index_update: Some(new_commit),
        })
    });
    context.handlers.replication_handler = replication_handler;
    context.membership = Arc::new(MockMembership::new());

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.update_current_term(leader_term);

    let append_request = AppendEntriesRequest {
        term: leader_term,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit_index: new_commit,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_request, resp_tx);
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    assert_eq!(
        state.commit_index(),
        new_commit,
        "commit_index must advance immediately"
    );
    let response = resp_rx.try_recv().expect("ACK must be sent immediately");
    assert!(response.unwrap().is_success());
}

// ============================================================================
// InstallSnapshotChunk Tests
// ============================================================================

/// Follower reports success when snapshot is fully transferred and applied.
///
/// # Given
/// - apply_snapshot_stream_from_leader returns Ok(())
///
/// # When
/// - Leader pushes a snapshot (InstallSnapshotChunk event)
///
/// # Then
/// - Response success: true
#[tokio::test]
async fn test_follower_install_snapshot_reports_success_when_apply_succeeds() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_apply_snapshot_stream_from_leader().once().returning(
        |_term, _stream, ack_tx, _config| {
            let _ = ack_tx.try_send(SnapshotAck {
                seq: 0,
                status: ChunkStatus::Accepted as i32,
                next_requested: 1,
            });
            Ok(())
        },
    );
    sm_handler.expect_get_latest_snapshot_metadata().returning(|| None);
    context.handlers.state_machine_handler = Arc::new(sm_handler);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let stream = create_test_snapshot_stream(vec![SnapshotChunk::default()]);
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    state
        .handle_raft_event(
            RaftEvent::InstallSnapshotChunk(Box::new(stream), resp_tx),
            &context,
            role_tx,
        )
        .await
        .unwrap();

    let response = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx.recv())
        .await
        .expect("response must arrive within 2s")
        .expect("recv must not fail")
        .expect("response must be Ok");

    assert!(
        response.success,
        "Follower must report success when apply succeeds"
    );
}

/// Follower must NOT report success when apply fails after transfer completes (#308).
///
/// # Raft §7 + #308
/// The previous implementation derived success from the last per-chunk ACK status.
/// When all chunks are received (last ACK = Accepted) but apply_snapshot_from_file
/// then fails, the spawned ACK-handler still sends success:true — causing the leader
/// to advance match_index and stop retrying, leaving the follower permanently behind.
///
/// # Given
/// - apply_snapshot_stream_from_leader: sends Accepted ACK (transfer succeeded),
///   then returns Err (apply_snapshot_from_file failed)
///
/// # When
/// - Leader pushes a snapshot (InstallSnapshotChunk event)
///
/// # Then
/// - Response MUST be success: false
#[tokio::test]
async fn test_follower_install_snapshot_reports_failure_when_apply_fails_after_transfer() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_apply_snapshot_stream_from_leader().once().returning(
        |_term, _stream, ack_tx, _config| {
            // Transfer phase succeeds: all chunks accepted
            let _ = ack_tx.try_send(SnapshotAck {
                seq: 0,
                status: ChunkStatus::Accepted as i32,
                next_requested: 1,
            });
            // Apply phase fails (apply_snapshot_from_file returned Err)
            Err(crate::Error::Fatal(
                "apply_snapshot_from_file failed".into(),
            ))
        },
    );
    context.handlers.state_machine_handler = Arc::new(sm_handler);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let stream = create_test_snapshot_stream(vec![SnapshotChunk::default()]);
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Follower absorbs the error and continues (does not propagate)
    let _ = state
        .handle_raft_event(
            RaftEvent::InstallSnapshotChunk(Box::new(stream), resp_tx),
            &context,
            role_tx,
        )
        .await;

    let response = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx.recv())
        .await
        .expect("response must arrive within 2s")
        .expect("recv must not fail")
        .expect("response must be Ok(SnapshotResponse)");

    assert!(
        !response.success,
        "Follower must NOT report success when apply failed after transfer (got success:true — #308 bug)"
    );
}

// ============================================================================
// ClusterConf current_leader_id Correctness Tests
// ============================================================================

/// Follower ClusterConf always exposes the current known leader ID.
///
/// Unlike the leader (which must hide its ID until noop commits — see T1/T3 in
/// `event_handling_test.rs`), a follower learns the leader ID exclusively via
/// AppendEntries requests. AppendEntries only arrive after the leader has committed
/// its noop entry, so a follower's `current_leader` is always safe to expose.
///
/// This test documents the intentional asymmetry:
/// - Leader: hides `current_leader_id` until noop commits
/// - Follower: always exposes `current_leader_id` if known (safe by construction)
///
/// # Contract
/// `retrieve_cluster_membership_config` must receive `Some(leader_id)` when the
/// follower has observed at least one AppendEntries from that leader.
///
/// # Test status: PASSES before and after fix (follower behaviour is already correct)
#[tokio::test]
async fn test_follower_cluster_conf_always_exposes_current_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut membership = MockMembership::new();
    // Assert: follower passes the known leader ID (Some(3)) — never None when leader is known
    membership
        .expect_retrieve_cluster_membership_config()
        .times(1)
        .with(eq(Some(3u32)))
        .returning(|_| ClusterMembership {
            version: 1,
            nodes: vec![],
            current_leader_id: Some(3),
        });
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    // Simulate: follower learned about leader 3 via AppendEntries (leader already past noop)
    state.shared_state.set_current_leader(3);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    assert!(
        state
            .handle_raft_event(
                RaftEvent::ClusterConf(MetadataRequest {}, resp_tx),
                &context,
                role_tx
            )
            .await
            .is_ok()
    );
    let m = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(
        m.current_leader_id,
        Some(3),
        "follower must expose known leader ID"
    );
}
