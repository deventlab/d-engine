use std::sync::Arc;

use bytes::Bytes;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::ReadConsistencyPolicy;
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
use d_engine_proto::server::storage::PurgeLogRequest;
use tonic::Code;
use tonic::Status;

use crate::AppendResponseWithUpdates;
use crate::Error;
use crate::HardState;
use crate::MaybeCloneOneshot;
use crate::MaybeCloneOneshotSender;
use crate::MockElectionCore;
use crate::MockMembership;
use crate::MockPurgeExecutor;
use crate::MockReplicationCore;
use crate::MockStateMachineHandler;
use crate::NetworkError;
use crate::NewCommitData;
use crate::RaftEvent;
use crate::RaftOneshot;
use crate::RoleEvent;
use crate::SnapshotError;
use crate::StateUpdate;
use crate::SystemError;
use crate::raft_role::follower_state::FollowerState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use crate::test_utils::mock::mock_raft_context_with_temp;
use crate::test_utils::node_config;
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

/// Test: FollowerState rejects FlushReadBuffer event
///
/// Scenario: Follower node receives FlushReadBuffer event
/// Expected: Returns RoleViolation error (only Leader can handle this event)
#[tokio::test]
async fn test_follower_rejects_flush_read_buffer_event() {
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let ctx = mock_raft_context("/tmp/test_follower_flush", shutdown_rx, None);
    let mut state = FollowerState::<MockTypeConfig>::new(1, ctx.node_config.clone(), None, None);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Handle FlushReadBuffer event
    let result = state.handle_raft_event(RaftEvent::FlushReadBuffer, &ctx, role_tx.clone()).await;

    // Verify: Returns RoleViolation error
    assert!(
        result.is_err(),
        "Follower should reject FlushReadBuffer event"
    );

    if let Err(e) = result {
        let error_str = format!("{e:?}");
        assert!(
            error_str.contains("RoleViolation"),
            "Error should be RoleViolation, got: {error_str}"
        );
        assert!(
            error_str.contains("Follower"),
            "Error should mention Follower role"
        );
        assert!(
            error_str.contains("Leader"),
            "Error should mention Leader as required role"
        );
    }

    drop(shutdown_tx);
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
/// - Returns success response with error_code=None
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
                error_code: cluster_conf_update_response::ErrorCode::None.into(),
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
        cluster_conf_update_response::ErrorCode::None as i32
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
#[tokio::test]
async fn test_tick_triggers_election_on_timeout() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);

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
    let raft_event = RaftEvent::ClientPropose(
        ClientWriteRequest {
            client_id: 1,
            commands: vec![],
        },
        resp_tx,
    );

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Handle ClientWriteRequest
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: Response with NOT_LEADER error
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(
        response.error,
        ErrorCode::NotLeader as i32,
        "Should return NOT_LEADER error"
    );
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
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Handle ClientReadRequest with LinearizableRead
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: Response with NOT_LEADER error
    let response = resp_rx.recv().await.unwrap().expect("should get response");
    assert_eq!(
        response.error,
        ErrorCode::NotLeader as i32,
        "LinearizableRead should redirect to leader"
    );
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
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Handle ClientReadRequest with EventualConsistency
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: Response with SUCCESS
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(
        response.error,
        ErrorCode::Success as i32,
        "EventualConsistency read should succeed on follower"
    );
}

// ============================================================================
// RaftLogCleanUp (Log Purge) Tests
// ============================================================================

/// Test: FollowerState handles RaftLogCleanUp validation failure
///
/// Scenario:
/// - Follower receives PurgeLogRequest from leader
/// - StateMachine validation returns false (purge not allowed)
///
/// Expected:
/// - Returns response with success=false
/// - No purge executed
/// - last_purged_index unchanged
/// - handle_raft_event returns Ok(())
///
/// This validates that purge safety checks prevent unsafe log truncation.
///
/// Original: test_handle_raft_event_case8_1
#[tokio::test]
async fn test_handle_raft_log_cleanup_validation_fails() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Mock state machine to reject purge validation
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_validate_purge_request()
        .returning(|_, _, _| Ok(false));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

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

    // Action: Handle RaftLogCleanUp event
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: Response indicates failure
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success, "Purge should be rejected");

    // Verify: No role change
    assert!(role_rx.try_recv().is_err(), "No role change expected");
}

/// Test: FollowerState successfully executes RaftLogCleanUp
///
/// Scenario:
/// - Follower receives valid PurgeLogRequest from leader
/// - StateMachine validation succeeds
/// - PurgeExecutor successfully executes purge
/// - commit_index > purge_index (safety requirement)
///
/// Expected:
/// - Returns response with success=true
/// - last_purged_index updated to purge index
/// - Purge executor called once
/// - handle_raft_event returns Ok(())
///
/// This validates successful log purge after snapshot creation.
///
/// Original: test_handle_raft_event_case8_2
#[tokio::test]
async fn test_handle_raft_log_cleanup_succeeds() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Mock state machine to accept purge validation
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_validate_purge_request()
        .returning(|_, _, _| Ok(true));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Mock purge executor to succeed
    let mut purge_executor = MockPurgeExecutor::new();
    purge_executor.expect_execute_purge().times(1).returning(|_| Ok(()));
    context.handlers.purge_executor = Arc::new(purge_executor);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    // Prepare follower state with sufficient commit_index
    let mut state = FollowerState::<MockTypeConfig>::new(
        1,
        context.node_config.clone(),
        None,
        Some(100), // last_applied_index
    );
    state.update_current_term(3);
    state.shared_state.commit_index = 150; // commit_index > purge_index (100)

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

    // Action: Handle RaftLogCleanUp event
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: Response indicates success
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.success, "Purge should succeed");
    assert_eq!(
        response.last_purged,
        Some(LogId {
            term: 3,
            index: 100
        }),
        "Should return purged index"
    );

    // Verify: State updated
    assert_eq!(
        state.last_purged_index,
        Some(LogId {
            term: 3,
            index: 100
        }),
        "last_purged_index should be updated"
    );

    // Verify: No role change
    assert!(role_rx.try_recv().is_err(), "No role change expected");
}

/// Test: FollowerState handles RaftLogCleanUp validation error
///
/// Scenario:
/// - Follower receives PurgeLogRequest from leader
/// - StateMachine validation returns Error (e.g., snapshot corruption)
///
/// Expected:
/// - Returns response with success=false
/// - No purge executed
/// - last_purged_index unchanged
/// - handle_raft_event returns Ok(())
///
/// This validates error handling when snapshot validation fails.
///
/// Original: test_handle_raft_event_case8_3
#[tokio::test]
async fn test_handle_raft_log_cleanup_validation_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Mock state machine to return validation error
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_validate_purge_request()
        .returning(|_, _, _| Err(SnapshotError::OperationFailed("test".to_string()).into()));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

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

    // Action: Handle RaftLogCleanUp event
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: Response indicates failure
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success, "Purge should fail on validation error");
    assert_eq!(state.last_purged_index, None, "last_purged_index unchanged");

    // Verify: No role change
    assert!(role_rx.try_recv().is_err(), "No role change expected");
}

/// Test: FollowerState rejects RaftLogCleanUp with stale term
///
/// Scenario:
/// - Follower (term=3) receives PurgeLogRequest with lower term (term=2)
/// - Request term is outdated
///
/// Expected:
/// - Returns response with success=false
/// - No purge executed
/// - last_purged_index unchanged
/// - handle_raft_event returns Ok(())
///
/// This validates term checking prevents purge from stale leader.
///
/// Original: test_handle_raft_event_case8_4
#[tokio::test]
async fn test_handle_raft_log_cleanup_rejects_stale_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Mock state machine to validate based on term
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

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

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

    // Action: Handle RaftLogCleanUp event
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: Response indicates failure
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success, "Should reject stale term");
    assert_eq!(state.last_purged_index, None, "last_purged_index unchanged");

    // Verify: No role change
    assert!(role_rx.try_recv().is_err(), "No role change expected");
}

/// Test: FollowerState rejects RaftLogCleanUp when commit_index insufficient
///
/// Scenario:
/// - Follower receives PurgeLogRequest with purge_index=100
/// - Follower's commit_index=90 (< purge_index)
/// - Safety check fails: can't purge uncommitted logs
///
/// Expected:
/// - Returns response with success=false
/// - No purge executed
/// - last_purged_index unchanged
/// - handle_raft_event returns Ok(())
///
/// This validates safety: never purge logs beyond commit_index.
///
/// Original: test_handle_raft_event_case8_5
#[tokio::test]
async fn test_handle_raft_log_cleanup_safety_check_fails() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Mock state machine to accept validation
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_validate_purge_request()
        .returning(|_, _, _| Ok(true));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    // Prepare follower state where commit_index < purge_index
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

    // Action: Handle RaftLogCleanUp event
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: Response indicates failure
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success, "Should fail safety check");
    assert_eq!(
        state.last_purged_index,
        Some(LogId { term: 2, index: 80 }),
        "last_purged_index unchanged"
    );

    // Verify: No role change
    assert!(role_rx.try_recv().is_err(), "No role change expected");
}

/// Test: FollowerState rejects RaftLogCleanUp with non-monotonic purge index
///
/// Scenario:
/// - Follower has last_purged_index=90
/// - Receives PurgeLogRequest with purge_index=80 (< last_purged_index)
/// - Request tries to purge already purged logs
///
/// Expected:
/// - Returns response with success=false
/// - No purge executed
/// - last_purged_index unchanged
/// - handle_raft_event returns Ok(())
///
/// This validates monotonicity: purge index must always advance.
///
/// Original: test_handle_raft_event_case8_6
#[tokio::test]
async fn test_handle_raft_log_cleanup_rejects_non_monotonic_purge() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Mock state machine to accept validation
    let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
    state_machine_handler
        .expect_validate_purge_request()
        .returning(|_, _, _| Ok(true));
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    let membership = MockMembership::new();
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

    // Action: Handle RaftLogCleanUp event
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: Response indicates failure
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success, "Should reject non-monotonic purge");
    assert_eq!(
        state.last_purged_index,
        Some(LogId { term: 3, index: 90 }),
        "last_purged_index unchanged"
    );

    // Verify: No role change
    assert!(role_rx.try_recv().is_err(), "No role change expected");
}

/// Test: FollowerState handles RaftLogCleanUp execution failure
///
/// Scenario:
/// - Follower receives valid PurgeLogRequest
/// - Validation succeeds
/// - PurgeExecutor fails during execution (e.g., disk error)
///
/// Expected:
/// - Returns response with success=false
/// - last_purged_index unchanged
/// - handle_raft_event returns Ok(())
///
/// This validates error handling when purge execution fails.
///
/// Original: test_handle_raft_event_case8_7
#[tokio::test]
async fn test_handle_raft_log_cleanup_execution_fails() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Mock state machine to accept validation
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

    let membership = MockMembership::new();
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

    // Action: Handle RaftLogCleanUp event
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "handle_raft_event should succeed"
    );

    // Verify: Response indicates failure
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.success, "Should fail on execution error");
    assert_eq!(
        state.last_purged_index,
        Some(LogId { term: 2, index: 80 }),
        "last_purged_index unchanged"
    );

    // Verify: No role change
    assert!(role_rx.try_recv().is_err(), "No role change expected");
}

// ============================================================================
// Cluster Management Tests
// ============================================================================

/// Test: FollowerState rejects JoinCluster request
///
/// Scenario:
/// - Follower receives JoinCluster request (new node wants to join)
/// - Only leader can handle cluster membership changes
///
/// Expected:
/// - Returns Status error with Code::PermissionDenied
/// - handle_raft_event returns Err()
/// - No state changes
///
/// This validates that only leader can accept new cluster members.
///
/// Original: test_handle_raft_event_case10
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

        // [Test CreateSnapshotEvent]
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::CreateSnapshotEvent;
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        assert!(
            matches!(e, Error::Consensus(ConsensusError::RoleViolation { .. })),
            "CreateSnapshotEvent should return RoleViolation"
        );

        // [Test SnapshotCreated]
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::SnapshotCreated(Err(Error::Fatal("test".to_string())));
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        assert!(
            matches!(e, Error::Consensus(ConsensusError::RoleViolation { .. })),
            "SnapshotCreated should return RoleViolation"
        );

        // [Test LogPurgeCompleted]
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::LogPurgeCompleted(LogId { term: 1, index: 1 });
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();

        assert!(
            matches!(e, Error::Consensus(ConsensusError::RoleViolation { .. })),
            "LogPurgeCompleted should return RoleViolation"
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
        let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);
        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        state
            .handle_raft_event(raft_event, &context, role_tx)
            .await
            .expect("should succeed");

        let response = resp_rx.recv().await.unwrap().expect("should get response");
        assert_eq!(
            response.error,
            ErrorCode::NotLeader as i32,
            "LeaseRead should be rejected by follower"
        );
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
        let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);
        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        state
            .handle_raft_event(raft_event, &context, role_tx)
            .await
            .expect("should succeed");

        let response = resp_rx.recv().await.unwrap().expect("should get response");
        assert_eq!(
            response.error,
            ErrorCode::NotLeader as i32,
            "Default LinearizableRead should be rejected by follower"
        );
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
        let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);
        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        state
            .handle_raft_event(raft_event, &context, role_tx)
            .await
            .expect("should succeed");

        let response = resp_rx.recv().await.unwrap().unwrap();
        assert_eq!(
            response.error,
            ErrorCode::Success as i32,
            "EventualConsistency read should succeed on follower"
        );
    }
}
