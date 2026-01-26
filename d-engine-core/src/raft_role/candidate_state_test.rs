use std::sync::Arc;

use bytes::Bytes;
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
use d_engine_proto::server::election::VotedFor;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::storage::PurgeLogRequest;
use tonic::Code;

use crate::ConsensusError;
use crate::ElectionError;
use crate::Error;
use crate::MaybeCloneOneshot;
use crate::MockElectionCore;
use crate::MockMembership;
use crate::MockReplicationCore;
use crate::MockStateMachineHandler;
use crate::RaftEvent;
use crate::RaftOneshot;
use crate::RoleEvent;
use crate::raft_role::candidate_state::CandidateState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::create_test_chunk;
use crate::test_utils::create_test_snapshot_stream;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_election_core;
use crate::test_utils::mock::mock_raft_context;
use crate::test_utils::node_config;
use tokio::sync::{mpsc, watch};

/// Test: CandidateState rejects FlushReadBuffer event
///
/// Scenario: Candidate node receives FlushReadBuffer event
/// Expected: Returns RoleViolation error (only Leader can handle this event)
#[tokio::test]
async fn test_candidate_rejects_flush_read_buffer_event() {
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let ctx = mock_raft_context("/tmp/test_candidate_flush", shutdown_rx, None);
    let mut state = CandidateState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Handle FlushReadBuffer event
    let result = state.handle_raft_event(RaftEvent::FlushReadBuffer, &ctx, role_tx.clone()).await;

    // Verify: Returns RoleViolation error
    assert!(
        result.is_err(),
        "Candidate should reject FlushReadBuffer event"
    );

    if let Err(e) = result {
        let error_str = format!("{e:?}");
        assert!(
            error_str.contains("RoleViolation"),
            "Error should be RoleViolation, got: {error_str}"
        );
        assert!(
            error_str.contains("Candidate"),
            "Error should mention Candidate role"
        );
        assert!(
            error_str.contains("Leader"),
            "Error should mention Leader as required role"
        );
    }

    drop(shutdown_tx);
}

/// Test: CandidateState can_vote_myself returns true for new candidate
///
/// Scenario:
/// - Create new CandidateState instance
/// - Node has not voted yet (voted_for is None)
///
/// Expected:
/// - can_vote_myself() returns true
/// - Candidate can vote for itself to start election
///
/// This validates the initial state of a candidate that has just
/// transitioned from Follower and can vote for itself.
///
/// Original test location:
/// `d-engine-server/tests/components/raft_role/candidate_state_test.rs::test_can_vote_myself_case1`
#[tokio::test]
async fn test_can_vote_myself_returns_true_for_new_candidate() {
    // Create minimal node_config with TempDir
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut node_config = crate::RaftNodeConfig::new().expect("Should create default config");
    node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
    let node_config = node_config.validate().expect("Should validate config");

    // Create new CandidateState (has not voted yet)
    let state = CandidateState::<MockTypeConfig>::new(1, Arc::new(node_config));

    // Verify: Should be able to vote for itself
    assert!(
        state.can_vote_myself(),
        "New candidate should be able to vote for itself"
    );
}

/// Test: CandidateState can_vote_myself returns false after voting
///
/// Scenario:
/// - Create new CandidateState instance
/// - Update voted_for to itself (simulate self-vote in current term)
/// - Check if can vote again
///
/// Expected:
/// - can_vote_myself() returns false
/// - Candidate cannot vote again in same term
///
/// This validates the Raft rule: at most one vote per term.
/// Even though the candidate voted for itself, it should not be
/// able to vote again in the same term.
///
/// Original test location:
/// `d-engine-server/tests/components/raft_role/candidate_state_test.rs::test_can_vote_myself_case2`
#[tokio::test]
async fn test_can_vote_myself_returns_false_after_voting() {
    // Create minimal node_config with TempDir
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut node_config = crate::RaftNodeConfig::new().expect("Should create default config");
    node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
    let node_config = node_config.validate().expect("Should validate config");

    // Create new CandidateState
    let mut state = CandidateState::<MockTypeConfig>::new(1, Arc::new(node_config));

    // Simulate voting for itself
    let voted_for = VotedFor {
        voted_for_id: state.node_id(),
        voted_for_term: state.current_term(),
        committed: false,
    };
    state.update_voted_for(voted_for).expect("Should succeed to update voted_for");

    // Verify: Cannot vote again in same term
    assert!(
        !state.can_vote_myself(),
        "Candidate should not be able to vote again after already voting"
    );
}

/// Test: CandidateState drain_read_buffer returns NotLeader error
///
/// Scenario: Call drain_read_buffer() on Candidate
/// Expected: Returns NotLeader error (Candidate doesn't buffer reads)
#[tokio::test]
async fn test_candidate_drain_read_buffer_returns_error() {
    let mut state = CandidateState::<MockTypeConfig>::new(
        1,
        Arc::new(node_config("/tmp/test_candidate_drain")),
    );

    // Action: Call drain_read_buffer()
    let result = state.drain_read_buffer();

    // Verify: Returns NotLeader error
    assert!(
        result.is_err(),
        "Candidate drain_read_buffer should return error"
    );

    if let Err(e) = result {
        let error_str = format!("{e:?}");
        assert!(
            error_str.contains("NotLeader"),
            "Error should be NotLeader, got: {error_str}"
        );
    }
}

/// Test: CandidateState tick triggers new election round on success
///
/// Scenario:
/// - Candidate ticks (election timeout)
/// - broadcast_vote_requests succeeds
/// - No higher term discovered
///
/// Expected:
/// - Term increments (1 → 2)
/// - Votes for itself in new term
/// - voted_for updated to (node_id=1, term=2)
///
/// This validates the core candidate behavior: when election timeout expires,
/// start a new election round with incremented term.
///
/// Original test location:
/// `d-engine-server/tests/components/raft_role/candidate_state_test.rs::test_tick_case1`
#[tokio::test]
async fn test_tick_triggers_new_election_round_on_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_tick_success", graceful_rx, None);

    // Mock election_handler to succeed
    let mut election_handler = MockElectionCore::<MockTypeConfig>::new();
    election_handler
        .expect_broadcast_vote_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(()));
    context.handlers.election_handler = election_handler;

    // Create new candidate state (term=1)
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);

    // Execute tick
    assert!(
        state.tick(&role_tx, &event_tx, &context).await.is_ok(),
        "Tick should succeed"
    );

    // Verify: term incremented
    assert_eq!(state.current_term(), 2, "Term should increment to 2");

    // Verify: voted for itself in new term
    assert_eq!(
        state.voted_for().unwrap(),
        Some(VotedFor {
            voted_for_id: 1,
            voted_for_term: 2,
            committed: false,
        }),
        "Should vote for itself in new term"
    );
}

/// Test: CandidateState tick discovers higher term and steps down
///
/// Scenario:
/// - Candidate ticks (election timeout)
/// - broadcast_vote_requests returns HigherTerm(100) error
/// - Another node has higher term
///
/// Expected:
/// - Term updates to 100 (from error)
/// - Sends BecomeFollower event
/// - Steps down to Follower role
///
/// This validates the Raft rule: when discovering higher term,
/// immediately step down to Follower.
///
/// Original test location:
/// `d-engine-server/tests/components/raft_role/candidate_state_test.rs::test_tick_case2`
#[tokio::test]
async fn test_tick_discovers_higher_term_and_steps_down() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_tick_higher_term", graceful_rx, None);

    // Mock election_handler to return HigherTerm error
    let mut election_handler = MockElectionCore::<MockTypeConfig>::new();
    election_handler
        .expect_broadcast_vote_requests()
        .times(1)
        .returning(|_, _, _, _, _| {
            Err(Error::Consensus(ConsensusError::Election(
                ElectionError::HigherTerm(100),
            )))
        });
    context.handlers.election_handler = election_handler;

    // Create new candidate state (term=1)
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);

    // Execute tick
    assert!(
        state.tick(&role_tx, &event_tx, &context).await.is_ok(),
        "Tick should succeed even with HigherTerm error"
    );

    // Verify: term updated to higher term
    assert_eq!(state.current_term(), 100, "Term should update to 100");

    // Verify: sent BecomeFollower event
    assert!(
        matches!(role_rx.try_recv().unwrap(), RoleEvent::BecomeFollower(_)),
        "Should send BecomeFollower event"
    );
}

/// Test: CandidateState rejects VoteRequest when check_vote_request_is_legal returns false
///
/// Scenario:
/// - Candidate receives VoteRequest
/// - Election handler's check_vote_request_is_legal returns false
/// - Request is not legal (e.g., stale term, less recent log)
///
/// Expected:
/// - Responds with vote_granted=false
/// - Does NOT step down to Follower
/// - Term remains unchanged
///
/// Original test location:
/// `d-engine-server/tests/components/raft_role/candidate_state_test.rs::test_handle_raft_event_case1_1`
#[tokio::test]
async fn test_handle_vote_request_rejects_illegal_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_vote_reject", graceful_rx, None);

    // Mock election handler to reject vote request
    let mut election_core = mock_election_core();
    election_core
        .expect_check_vote_request_is_legal()
        .returning(|_, _, _, _, _| false);
    context.handlers.election_handler = election_core;

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    let term_before = state.current_term();
    let request_term = term_before;

    // Prepare VoteRequest event
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ReceiveVoteRequest(
        VoteRequest {
            term: request_term,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        },
        resp_tx,
    );

    // Execute
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "Should handle event successfully"
    );

    // Verify: response with vote_granted=false
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.vote_granted, "Should reject vote");

    // Verify: no role change event
    assert!(
        role_rx.try_recv().is_err(),
        "Should not send role change event"
    );

    // Verify: term unchanged
    assert_eq!(state.current_term(), term_before, "Term should not change");
}

/// Test: CandidateState grants VoteRequest and steps down when legal
///
/// Scenario:
/// - Candidate receives VoteRequest with higher term
/// - Election handler's check_vote_request_is_legal returns true
/// - Request is legal (higher term, at least as recent log)
///
/// Expected:
/// - Steps down to Follower (sends BecomeFollower event)
/// - Term updates to request term (100)
/// - Sends ReprocessEvent to let Follower handle the vote
/// - Does NOT send response (Follower will handle it)
///
/// This validates the Raft rule: when receiving valid request with higher term,
/// step down and let the new role process it.
///
/// Original test location:
/// `d-engine-server/tests/components/raft_role/candidate_state_test.rs::test_handle_raft_event_case1_2`
#[tokio::test]
async fn test_handle_vote_request_grants_and_steps_down_when_legal() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_vote_grant", graceful_rx, None);

    // Mock election handler to accept vote request
    let mut election_core = mock_election_core();
    election_core
        .expect_check_vote_request_is_legal()
        .returning(|_, _, _, _, _| true);
    context.handlers.election_handler = election_core;

    let updated_term = 100;
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare VoteRequest event with higher term
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ReceiveVoteRequest(
        VoteRequest {
            term: updated_term,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        },
        resp_tx,
    );

    // Execute
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "Should handle event successfully"
    );

    // Verify: steps down to Follower
    assert!(
        matches!(role_rx.try_recv(), Ok(RoleEvent::BecomeFollower(None))),
        "Should send BecomeFollower event"
    );

    // Verify: reprocess event
    assert!(
        matches!(role_rx.try_recv().unwrap(), RoleEvent::ReprocessEvent(_)),
        "Should send ReprocessEvent for Follower to handle"
    );

    // Verify: term updated
    assert_eq!(
        state.current_term(),
        updated_term,
        "Term should update to 100"
    );

    // Verify: no response sent (Follower will handle)
    assert!(
        resp_rx.recv().await.is_err(),
        "Should not send response, let Follower handle it"
    );
}

/// Test: CandidateState handles ClusterConf (metadata) request
///
/// Scenario:
/// - Candidate receives MetadataRequest (ClusterConf event)
/// - Requests current cluster membership configuration
///
/// Expected:
/// - Returns ClusterMembership with current configuration
/// - No role change
///
/// This validates that Candidate can serve read-only cluster metadata queries.
///
/// Original test location:
/// `d-engine-server/tests/components/raft_role/candidate_state_test.rs::test_handle_raft_event_case2`
#[tokio::test]
async fn test_handle_cluster_conf_metadata_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_cluster_conf", graceful_rx, None);

    // Mock membership to return cluster configuration
    let mut membership = MockMembership::new();
    membership.expect_retrieve_cluster_membership_config().times(1).returning(
        |_current_leader_id| ClusterMembership {
            version: 1,
            nodes: vec![],
            current_leader_id: None,
        },
    );
    context.membership = Arc::new(membership);

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare ClusterConf event
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConf(MetadataRequest {}, resp_tx);

    // Execute
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "Should handle ClusterConf event"
    );

    // Verify: returns cluster membership
    let membership_response = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(
        membership_response.nodes,
        vec![],
        "Should return cluster nodes"
    );
}

/// Test: CandidateState handles ClusterConfUpdate successfully
///
/// Scenario:
/// - Candidate receives ClusterConfChangeRequest from leader (id=2)
/// - Membership update succeeds
///
/// Expected:
/// - Returns success response
/// - error_code is None
///
/// This validates that Candidate can apply configuration changes from leader.
///
/// Original test location:
/// `d-engine-server/tests/components/raft_role/candidate_state_test.rs::test_handle_raft_event_case3`
#[tokio::test]
async fn test_handle_cluster_conf_update_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_conf_update", graceful_rx, None);

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
    context.membership = Arc::new(membership);

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Prepare ClusterConfUpdate event
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let raft_event = RaftEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2, // Leader ID
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    // Execute
    assert!(
        state.handle_raft_event(raft_event, &context, role_tx).await.is_ok(),
        "Should handle ClusterConfUpdate event"
    );

    // Verify: success response
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.success, "Update should succeed");
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::None as i32,
        "Should have no error"
    );
}

/// Test: CandidateState handles AppendEntries and steps down to Follower
///
/// Scenario:
/// - Candidate receives AppendEntries from new leader
/// - check_append_entries_request_is_legal returns success
/// - Request has valid term and commit index
///
/// Expected:
/// - Steps down to Follower (sends BecomeFollower event)
/// - Sends ReprocessEvent for Follower to handle
/// - Term updates to leader's term
/// - Commit index NOT updated (Follower will handle)
/// - No response sent (Follower will send it)
///
/// Original: test_handle_raft_event_case4_1
#[tokio::test]
async fn test_handle_append_entries_steps_down_to_follower() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_append_entries_step_down", graceful_rx, None);
    let term = 1;
    let new_leader_term = term;
    let new_leader_commit = 5;

    // Mock replication handler
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_check_append_entries_request_is_legal()
        .returning(move |_, _, _| AppendEntriesResponse::success(1, term, None));

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(term);

    // Prepare AppendEntries request
    let append_entries_request = AppendEntriesRequest {
        term: new_leader_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: new_leader_commit,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::AppendEntries(append_entries_request, resp_tx);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::BecomeFollower(None))
    ));
    assert!(matches!(
        role_rx.try_recv().unwrap(),
        RoleEvent::ReprocessEvent(_)
    ));
    assert_eq!(state.current_term(), new_leader_term);
    assert!(state.commit_index() != new_leader_commit);
    assert!(resp_rx.recv().await.is_err());
}

/// Test: CandidateState rejects AppendEntries with higher local term
///
/// Original: test_handle_raft_event_case4_2
#[tokio::test]
async fn test_handle_append_entries_rejects_lower_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_append_reject_lower", graceful_rx, None);
    let term = 2;
    let new_leader_term = term - 1;

    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_check_append_entries_request_is_legal()
        .returning(move |_, _, _| AppendEntriesResponse::higher_term(1, term));

    context.membership = Arc::new(MockMembership::new());
    context.handlers.replication_handler = replication_handler;

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(term);

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

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());
    assert!(role_rx.try_recv().is_err());
    assert_eq!(state.current_term(), term);

    let response = resp_rx.recv().await.expect("should succeed").unwrap();
    assert!(response.is_higher_term());
}

/// Test: CandidateState handles AppendEntries conflict
///
/// Original: test_handle_raft_event_case4_3
#[tokio::test]
async fn test_handle_append_entries_conflict() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_append_conflict", graceful_rx, None);
    let term = 2;
    let new_leader_term = term - 1;

    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_check_append_entries_request_is_legal()
        .returning(move |_, _, _| AppendEntriesResponse::conflict(1, term, None, None));

    context.membership = Arc::new(MockMembership::new());
    context.handlers.replication_handler = replication_handler;

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(term);

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

    assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());
    assert!(role_rx.try_recv().is_err());
    assert_eq!(state.current_term(), term);

    let response = resp_rx.recv().await.expect("should succeed").unwrap();
    assert!(response.is_conflict());
}

/// Test: CandidateState handles ClientPropose (write request)
///
/// Original: test_handle_raft_event_case5
#[tokio::test]
async fn test_handle_client_write_returns_not_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_client_write", graceful_rx, None);

    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::ClientPropose(
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

/// Test: send_replay_raft_event sends correct events
///
/// Original: test_send_replay_raft_event
#[test]
fn test_send_become_follower_and_replay_events() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_replay_event", graceful_rx, None);

    let state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();

    assert!(state.send_become_follower_event(&role_tx).is_ok());
    assert!(
        state
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
            .is_ok()
    );

    assert!(matches!(
        role_rx.try_recv().unwrap(),
        RoleEvent::BecomeFollower(None)
    ));
    assert!(matches!(
        role_rx.try_recv().unwrap(),
        RoleEvent::ReprocessEvent(_)
    ));
}

/// Test: InstallSnapshotChunk returns PermissionDenied
///
/// Original: test_handle_raft_event_case7
#[tokio::test]
async fn test_handle_install_snapshot_returns_permission_denied() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_install_snapshot", graceful_rx, None);
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let stream = create_test_snapshot_stream(vec![create_test_chunk(0, b"chunk0", 1, 1, 2)]);
    let raft_event = RaftEvent::InstallSnapshotChunk(Box::new(stream), resp_tx);

    let result = state.handle_raft_event(raft_event, &context, mpsc::unbounded_channel().0).await;

    assert!(result.is_err(), "Expected error");
    let response = resp_rx.recv().await.expect("Response should be received");
    assert!(response.is_err(), "Expected error response");
    let status = response.unwrap_err();

    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), "Not Follower or Learner.");
}

/// Test: RaftLogCleanUp returns PermissionDenied
///
/// Original: test_handle_raft_event_case8
#[tokio::test]
async fn test_handle_raft_log_cleanup_returns_permission_denied() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_log_cleanup", graceful_rx, None);
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    let request = PurgeLogRequest {
        term: 1,
        leader_id: 1,
        leader_commit: 1,
        last_included: Some(LogId { term: 1, index: 1 }),
        snapshot_checksum: Bytes::from(vec![1, 2, 3]),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::RaftLogCleanUp(request, resp_tx);

    let result = state.handle_raft_event(raft_event, &context, mpsc::unbounded_channel().0).await;

    assert!(result.is_ok(), "Expected Ok");
    let response = resp_rx.recv().await.expect("Response should be received");
    assert!(response.is_err(), "Expected error response");
    let status = response.unwrap_err();

    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), "Not Follower");
}

/// Test: JoinCluster returns PermissionDenied
///
/// Original: test_handle_raft_event_case10
#[tokio::test]
async fn test_handle_join_cluster_returns_permission_denied() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_join_cluster", graceful_rx, None);
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    let request = JoinRequest {
        status: d_engine_proto::common::NodeStatus::Promotable as i32,
        node_id: 2,
        node_role: Learner.into(),
        address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::JoinCluster(request, resp_tx);

    let result = state.handle_raft_event(raft_event, &context, mpsc::unbounded_channel().0).await;

    assert!(result.is_err(), "Expected error");
    let response = resp_rx.recv().await.expect("Response should be received");
    assert!(response.is_err(), "Expected error response");
    let status = response.unwrap_err();

    assert_eq!(status.code(), Code::PermissionDenied);
}

/// Test: DiscoverLeader returns PermissionDenied
///
/// Original: test_handle_raft_event_case11
#[tokio::test]
async fn test_handle_discover_leader_returns_permission_denied() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_discover_leader", graceful_rx, None);
    let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    let request = LeaderDiscoveryRequest {
        node_id: 2,
        requester_address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::DiscoverLeader(request, resp_tx);

    let result = state.handle_raft_event(raft_event, &context, mpsc::unbounded_channel().0).await;

    assert!(result.is_ok(), "Expected Ok");
    let response = resp_rx.recv().await.expect("Response should be received");
    assert!(response.is_err(), "Expected error response");
    let status = response.unwrap_err();

    assert_eq!(status.code(), Code::PermissionDenied);
}

#[cfg(test)]
mod role_violation_tests {
    use super::*;

    /// Test: Role violation events return RoleViolation error
    ///
    /// Original: test_role_violation_events
    #[tokio::test]
    async fn test_candidate_role_violation_errors() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let context = mock_raft_context("/tmp/test_role_violation", graceful_rx, None);
        let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

        // Test CreateSnapshotEvent
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::CreateSnapshotEvent;
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::RoleViolation { .. })
        ));

        // Test SnapshotCreated
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::SnapshotCreated(Err(Error::Fatal("test".to_string())));
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::RoleViolation { .. })
        ));

        // Test LogPurgeCompleted
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let raft_event = RaftEvent::LogPurgeCompleted(LogId { term: 1, index: 1 });
        let e = state.handle_raft_event(raft_event, &context, role_tx).await.unwrap_err();
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::RoleViolation { .. })
        ));
    }
}

#[cfg(test)]
mod handle_client_read_request {
    use super::*;

    /// Test: ClientReadRequest with LinearizableRead returns NotLeader
    ///
    /// Original: test_handle_raft_event_case6_1
    #[tokio::test]
    async fn test_client_read_linearizable_returns_not_leader() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let context = mock_raft_context("/tmp/test_read_linearizable", graceful_rx, None);

        let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
        let client_read_request = ClientReadRequest {
            client_id: 1,
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
            keys: vec![],
        };
        let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
        let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

        let response = resp_rx.recv().await.unwrap().expect("should get response");
        assert_eq!(response.error, ErrorCode::NotLeader as i32);
    }

    /// Test: ClientReadRequest with EventualConsistency succeeds
    ///
    /// Original: test_handle_raft_event_case6_2
    #[tokio::test]
    async fn test_client_read_eventual_consistency_succeeds() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context("/tmp/test_read_eventual", graceful_rx, None);

        let mut state_machine_handler = MockStateMachineHandler::<MockTypeConfig>::new();
        state_machine_handler
            .expect_read_from_state_machine()
            .times(1)
            .returning(|_| Some(vec![]));
        context.handlers.state_machine_handler = Arc::new(state_machine_handler);

        let mut state = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

        let client_read_request = ClientReadRequest {
            client_id: 1,
            consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
            keys: vec![],
        };
        let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
        let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        assert!(state.handle_raft_event(raft_event, &context, role_tx).await.is_ok());

        let r = resp_rx.recv().await.unwrap().unwrap();
        assert_eq!(r.error, ErrorCode::Success as i32);
    }
}

/// Test: Candidate handles FatalError and returns error
///
/// Verifies that when Candidate receives FatalError from any component,
/// it returns Error::Fatal and stops further processing.
///
/// # Test Scenario
/// Candidate receives FatalError event from state machine while in candidate role (during election).
/// Candidate should recognize the fatal error and return Error::Fatal.
///
/// # Given
/// - Candidate in normal state (in election)
/// - FatalError event from StateMachine component
///
/// # When
/// - Candidate handles FatalError event via handle_raft_event()
///
/// # Then
/// - handle_raft_event() returns Error::Fatal
/// - Error message contains source and error details
/// - No role transition events are sent (election is aborted)
#[tokio::test]
async fn test_candidate_handles_fatal_error_returns_error() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let context = crate::test_utils::mock::mock_raft_context(
        "/tmp/test_candidate_handles_fatal_error_returns_error",
        shutdown_rx,
        None,
    );

    let mut candidate = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Create FatalError event
    let fatal_error = RaftEvent::FatalError {
        source: "StateMachine".to_string(),
        error: "Network failure - cannot persist state".to_string(),
    };

    // Create role event channel
    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();

    // Handle the FatalError event
    let result = candidate.handle_raft_event(fatal_error, &context, role_tx).await;

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

    // VERIFY 3: No role events sent (election is aborted)
    assert!(
        role_rx.try_recv().is_err(),
        "No role transition events should be sent during FatalError handling"
    );
}
