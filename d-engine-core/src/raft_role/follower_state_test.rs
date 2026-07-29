use crate::client::ClientReadRequest;
use crate::client::ClientResponse;
use crate::client::ClientWriteRequest;
use crate::client::ErrorCode;
use crate::client::LeaderHint;
use crate::config::ReadConsistencyPolicy;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole;
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
use crate::InboundEvent;
use crate::InternalEvent;
use crate::MaybeCloneOneshot;
use crate::MaybeCloneOneshotSender;
use crate::MockElectionCore;
use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::MockStateMachineHandler;
use crate::NetworkError;
use crate::NewCommitData;
use crate::RaftContext;
use crate::RaftLog;
use crate::RaftOneshot;
use crate::StateUpdate;
use crate::SystemError;
use crate::raft_role::candidate_state::CandidateState;
use crate::raft_role::follower_state::FollowerState;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::learner_state::LearnerState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_membership;
use crate::test_utils::mock::mock_raft_context;
use crate::test_utils::mock::mock_raft_context_with_temp;
use crate::test_utils::mock::mock_raft_log;
use crate::test_utils::node_config;
use d_engine_proto::client::WriteCommand;
use mockall::predicate::eq;
use prost::Message;
use tokio::sync::{mpsc, watch};

// ============================================================================
// Helper Functions
// ============================================================================

/// Build a mock RaftContext whose `Membership::get_address` resolves only
/// `known_id` -> `known_address`; any other id returns `None`.
///
/// `mock_raft_context_with_temp`'s `peers_meta_option` does NOT wire into
/// `MockMembership` (it only sets `node_config.cluster.initial_cluster`, a
/// config field `MockMembership` never reads) — tests that need
/// `get_address` to resolve must configure it explicitly via
/// `MockBuilder::with_membership`, which is what this helper does.
fn mock_context_with_leader_address(
    graceful_rx: watch::Receiver<()>,
    known_id: u32,
    known_address: &str,
) -> (RaftContext<MockTypeConfig>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut node_config = node_config(temp_dir.path().to_str().unwrap());
    node_config.raft.batching.max_batch_size = 1;
    node_config.retry.auto_discovery.timeout_ms = 10;

    let known_address = known_address.to_string();
    let mut membership = mock_membership();
    membership.expect_get_address().returning(move |id| {
        if id == known_id {
            Some(known_address.clone())
        } else {
            None
        }
    });

    let context = MockBuilder::new(graceful_rx)
        .with_node_config(node_config)
        .with_membership(membership)
        .build_context();
    (context, temp_dir)
}

fn create_vote_request_event(
    term: u64,
    candidate_id: u32,
    resp_tx: MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>,
) -> InboundEvent {
    InboundEvent::ReceiveVoteRequest(
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
/// Original: test_handle_inbound_event_case1_1
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
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = create_vote_request_event(1, 1, resp_tx);

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

    let r = resp_rx.recv().await.unwrap().unwrap();
    assert!(!r.vote_granted, "Should reject vote");
    assert!(internal_event_rx.try_recv().is_err(), "No role change");
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
/// Original: test_handle_inbound_event_case1_2
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
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = create_vote_request_event(1, 1, resp_tx);

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

    let r = resp_rx.recv().await.unwrap().unwrap();
    assert!(r.vote_granted, "Should grant vote");
    assert!(
        internal_event_rx.try_recv().is_err(),
        "Should remain Follower"
    );
    assert_eq!(state.current_term(), updated_term, "Term should update");
}

/// Test: Follower persists hard_state BEFORE the vote-granted response is observable
/// to the caller.
///
/// Scenario:
/// - Same setup as test_handle_vote_request_grants_and_updates_term above: Follower
///   grants a vote, term updates to 100.
/// - `RaftLog::save_hard_state` is documented as required "Before voting for a candidate"
///   (raft_log.rs "When to Call") — this is the Raft paper's core safety requirement:
///   currentTerm/votedFor must be durable before the RPC response is sent, otherwise a
///   crash right after responding "yes" can make the node forget it already voted this
///   term and grant a second, conflicting vote after restart (double-voting, breaks
///   election safety).
///
/// Expected:
/// - `save_hard_state` is called exactly once with term=100 and the granted `voted_for`.
/// - The call happens BEFORE `resp_rx` observes the `vote_granted=true` response
#[tokio::test]
async fn test_handle_vote_request_grants_vote_persists_hard_state_before_responding() {
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

    let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
    let resp_rx = Arc::new(std::sync::Mutex::new(resp_rx));
    let resp_rx_for_ordering_check = Arc::clone(&resp_rx);

    // Custom MockRaftLog wired directly into context — NOT mock_raft_log()'s permissive
    // default, whose no-op save_hard_state stub is registered first and (per mockall's
    // FIFO expectation matching) would silently absorb every call before this stricter
    // expectation ever gets a chance to run.
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_log_id().returning(|| None);
    raft_log
        .expect_save_hard_state()
        .withf(move |s| {
            // Ordering proof: at the moment save_hard_state runs, the vote-granted
            // response must not have been sent yet — try_recv() on the still-empty
            // broadcast channel returns Err. Mirrors the candidate_state.rs tests;
            // MaybeCloneOneshotReceiver exposes a non-blocking try_recv() in test
            // builds, so the same channel-peek technique applies here too.
            assert!(
                resp_rx_for_ordering_check.lock().unwrap().try_recv().is_err(),
                "save_hard_state must be called BEFORE the vote-granted response is sent"
            );
            s.current_term == updated_term
                && s.voted_for
                    == Some(VotedFor {
                        voted_for_id: 1,
                        voted_for_term: 1,
                        committed: false,
                    })
        })
        .times(1)
        .returning(|_| Ok(()));
    context.storage.raft_log = Arc::new(raft_log);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = create_vote_request_event(1, 1, resp_tx);

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

    // By this point handle_inbound_event has fully completed, so the response (if any)
    // was already broadcast synchronously — try_recv() should find it immediately.
    let r = resp_rx.lock().unwrap().try_recv().unwrap().unwrap();
    assert!(r.vote_granted, "Should grant vote");
    assert!(
        internal_event_rx.try_recv().is_err(),
        "Should remain Follower"
    );
    assert_eq!(state.current_term(), updated_term, "Term should update");
}

/// Test: Granting a vote resets the election timer.
///
/// Scenario:
/// - Follower's timer is set to an already-expired deadline before the request
///   arrives (simulates "haven't heard from anyone in a while").
/// - `VoteRequest` arrives and is legitimately granted.
///
/// Expected:
/// - After processing, `state.is_timer_expired()` is false — the timer was
///   reset to a fresh future deadline as part of granting the vote, giving the
///   new candidate a fair window to complete its election before this node
///   also times out and starts a competing campaign (see decision doc for
///   ticket #422: this exact gap caused a real election-storm regression,
///   confirmed pre-existing via `git diff` before this test was added).
///
/// Known bug (fixed as of 2026-07-18, ticket #422 follow-up): the vote-granting
/// branch in `handle_inbound_event`'s `ReceiveVoteRequest` arm previously never
/// called `self.reset_timer()`.
#[tokio::test]
async fn test_handle_vote_request_grants_vote_resets_election_timer() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // This test only cares about timer behavior, not persistence — use the
    // permissive default raft_log so save_hard_state is a harmless no-op.
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
                term_update: Some(100),
            })
        });
    context.handlers.election_handler = election_handler;

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    // Force the timer into an already-expired state before the request arrives.
    state.timer.next_deadline = tokio::time::Instant::now() - tokio::time::Duration::from_millis(1);
    assert!(
        state.is_timer_expired(),
        "precondition: timer must start out expired"
    );

    let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = create_vote_request_event(1, 1, resp_tx);

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

    assert!(
        !state.is_timer_expired(),
        "granting a vote must reset the election timer — otherwise this node can time out \
         and disrupt the candidate it just voted for before that candidate's first \
         heartbeat arrives"
    );
}

/// Test: A VoteRequest rejected for an illegitimate term does NOT reset the timer.
///
/// Scenario:
/// - VoteRequest carries a term that is not >= this follower's own term (stale
///   candidate), so the request is illegitimate and must be rejected outright.
///
/// Expected:
/// - The timer is untouched. If every incoming VoteRequest reset the timer
///   regardless of legitimacy, a stale/misbehaving peer could keep this node
///   from ever timing out and starting its own election — an availability bug
///   distinct from (but as real as) the missing-reset one above. This is the
///   negative case that proves the fix is scoped correctly, not blanket-applied.
#[tokio::test]
async fn test_handle_vote_request_rejected_does_not_reset_timer() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Illegitimate request — handler rejects it (e.g. stale term), so no vote is granted.
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

    let deadline_before = state.timer.next_deadline;

    let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = create_vote_request_event(1, 1, resp_tx);

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

    assert_eq!(
        state.timer.next_deadline, deadline_before,
        "a rejected vote request must NOT reset the timer — otherwise a stale/misbehaving \
         peer could keep this node from ever starting its own election"
    );
}

/// Test: hard_state persists BEFORE `LeaderDiscovered` is observable, on the
/// shared AppendEntries path (role_state.rs::handle_append_entries_request_workflow,
/// used by both Follower and Learner).
///
/// Scenario:
/// - Follower has never seen a leader (`current_leader()` is `None`).
/// - Receives a legitimate AppendEntries from a real leader for the first time.
///
/// Expected:
/// - `save_hard_state` is called with the leader's committed vote.
/// - The call happens BEFORE `InternalEvent::LeaderDiscovered` is sent — mirrors
///   the BecomeFollower ordering tests in candidate_state_test.rs, applied here
///   to the notification this codebase uses for `wait_ready()`.
///
/// Known bug (fixed as of 2026-07-18, ticket #422 follow-up): this handler used
/// to send `LeaderDiscovered` before the single consolidated `commit_hard_state`
/// call that replaced the old separate update_voted_for/update_current_term
/// sequence — confirmed via code review during the #422 investigation, not
/// caught by any test until now.
#[tokio::test]
async fn test_handle_append_entries_persists_before_leader_discovered_notification() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let follower_term = 1;
    let new_leader_term = follower_term + 1;
    let new_leader_id = 5;

    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_append_entries().returning(move |_, _, _| {
        Ok(AppendResponseWithUpdates {
            response: AppendEntriesResponse::success(1, new_leader_term, None),
            commit_index_update: None,
        })
    });
    context.membership = Arc::new(MockMembership::new());
    context.handlers.replication_handler = replication_handler;

    let (internal_event_tx, internal_event_rx) = mpsc::unbounded_channel();
    let internal_event_rx = Arc::new(std::sync::Mutex::new(internal_event_rx));
    let rx_for_ordering_check = Arc::clone(&internal_event_rx);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log
        .expect_save_hard_state()
        .withf(move |s| {
            // Ordering proof: at the moment save_hard_state runs, LeaderDiscovered
            // must not have been sent yet — try_recv() on the still-empty channel
            // returns Err. Mirrors candidate_state_test.rs's step-down ordering test.
            assert!(
                rx_for_ordering_check.lock().unwrap().try_recv().is_err(),
                "save_hard_state must be called BEFORE LeaderDiscovered is sent"
            );
            s.current_term == new_leader_term
                && s.voted_for
                    == Some(VotedFor {
                        voted_for_id: new_leader_id,
                        voted_for_term: new_leader_term,
                        committed: true,
                    })
        })
        .times(1)
        .returning(|_| Ok(()));
    context.storage.raft_log = Arc::new(raft_log);

    // Fresh state: never seen a leader, so this AppendEntries is a genuine
    // first discovery and must fire LeaderDiscovered.
    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.shared_state_mut().update_current_term(follower_term);

    let append_entries_request = AppendEntriesRequest {
        term: new_leader_term,
        leader_id: new_leader_id,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
    let inbound_event = InboundEvent::AppendEntries(append_entries_request, vec![resp_tx]);

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

    assert!(
        matches!(
            internal_event_rx.lock().unwrap().try_recv().unwrap(),
            InternalEvent::LeaderDiscovered(id, term) if id == new_leader_id && term == new_leader_term
        ),
        "Should send LeaderDiscovered only after hard_state is durably persisted"
    );
}

/// Test: FollowerState handles vote request error from handler
///
/// Scenario:
/// - Follower receives VoteRequest
/// - handle_vote_request returns Error (e.g., network/system error)
///
/// Expected:
/// - Response with vote_granted=false
/// - handle_inbound_event returns Error
/// - Term unchanged
///
/// Original: test_handle_inbound_event_case1_3
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
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = create_vote_request_event(1, 1, resp_tx);

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_err()
    );

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
/// Original: test_handle_inbound_event_case2
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
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = InboundEvent::ClusterConf(MetadataRequest {}, resp_tx);

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

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
/// Original: test_handle_inbound_event_case3_1
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
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = InboundEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2,
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

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
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);

    let election_timeout_max = context.node_config.raft.election.election_timeout_max;
    tokio::time::advance(tokio::time::Duration::from_millis(election_timeout_max + 1)).await;

    assert!(
        state.tick(&internal_event_tx, &event_tx, &context).await.is_ok(),
        "Tick should succeed"
    );

    let r = internal_event_rx.recv().await.unwrap();
    assert!(
        matches!(r, InternalEvent::BecomeCandidate),
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
/// 6. handle_inbound_event returns Ok(())
///
/// This validates the core Raft rule: Follower accepts entries from
/// valid leader and updates its state accordingly.
///
/// Original: test_handle_inbound_event_case4_1
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
    state.shared_state_mut().update_current_term(follower_term);

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
    let inbound_event = InboundEvent::AppendEntries(append_entries_request, vec![resp_tx]);

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();

    // Action: Handle AppendEntries event
    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok(),
        "handle_inbound_event should succeed"
    );

    // Verify: LeaderDiscovered event sent
    assert!(
        matches!(
            internal_event_rx.try_recv().unwrap(),
            InternalEvent::LeaderDiscovered(5, _)
        ),
        "Should send LeaderDiscovered event"
    );

    // Verify: NotifyNewCommitIndex event sent
    assert!(
        matches!(
            internal_event_rx.try_recv().unwrap(),
            InternalEvent::NotifyNewCommitIndex(NewCommitData {
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
/// 4. handle_inbound_event returns Ok(())
///
/// This validates the core Raft rule: Reject requests with stale term.
///
/// Original: test_handle_inbound_event_case4_2
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
    state.shared_state_mut().update_current_term(follower_term);

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
    let inbound_event = InboundEvent::AppendEntries(append_entries_request, vec![resp_tx]);

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();

    // Action: Handle AppendEntries event
    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok(),
        "handle_inbound_event should succeed"
    );

    // Verify: No events sent
    assert!(
        internal_event_rx.try_recv().is_err(),
        "Should not send any events"
    );

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
/// 5. handle_inbound_event returns Err()
///
/// This validates correct error handling: Leader is recognized, but
/// append operation failed and must be retried.
///
/// Original: test_handle_inbound_event_case4_3
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
    state.shared_state_mut().update_current_term(follower_term);

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
    let inbound_event = InboundEvent::AppendEntries(append_entries_request, vec![resp_tx]);

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();

    // Action: Handle AppendEntries event
    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_err(),
        "handle_inbound_event should return error"
    );

    // Verify: LeaderDiscovered event sent (leader is valid)
    assert!(
        matches!(
            internal_event_rx.try_recv().unwrap(),
            InternalEvent::LeaderDiscovered(5, _)
        ),
        "Should send LeaderDiscovered event even on error"
    );

    // Verify: No other events
    assert!(
        internal_event_rx.try_recv().is_err(),
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
/// - handle_inbound_event returns Ok(())
///
/// This validates rejection of configuration changes from non-leader nodes.
///
/// Original: test_handle_inbound_event_case3_2
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
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = InboundEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 3, // Non-leader ID
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

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
/// - handle_inbound_event returns Ok(())
///
/// This validates version conflict detection for configuration changes.
///
/// Original: test_handle_inbound_event_case3_3
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
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = InboundEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2,
            term: 1,
            version: 4, // Stale version
            change: None,
        },
        resp_tx,
    );

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

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
/// - handle_inbound_event returns Ok(())
///
/// This validates term checking for configuration changes.
///
/// Original: test_handle_inbound_event_case3_4
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
    state.shared_state_mut().update_current_term(5); // Follower has higher term

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = InboundEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2,
            term: 4, // Stale term
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

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
/// - handle_inbound_event returns Ok(())
///
/// This validates error handling for internal failures during configuration updates.
///
/// Original: test_handle_inbound_event_case3_5
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
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = InboundEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2,
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

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
/// - handle_inbound_event returns Ok(())
///
/// This validates behavior when configuration change is attempted but
/// cluster leadership is unknown.
///
/// Original: test_handle_inbound_event_case3_6
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
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = InboundEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 3,
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

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
/// - Follower is not the leader, but knows who is (id + address in membership)
///
/// Expected:
/// - Returns Ok(ClientResponse) with error = NotLeader
/// - `leader_hint` is populated so the caller can actually redirect, not just
///   told "not leader" with nowhere to go
/// - No state changes
///
/// This validates the core Raft rule: Only leader can process writes.
///
/// Original: test_handle_inbound_event_case5
#[tokio::test]
async fn test_handle_client_write_request_redirects_to_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_context_with_leader_address(graceful_rx, 2, "127.0.0.1:9082");

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.shared_state().set_current_leader(2);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Propose(
        ClientWriteRequest {
            client_id: 1,
            command: Some(bytes::Bytes::from(
                WriteCommand::delete(bytes::Bytes::new()).encode_to_vec(),
            )),
        },
        resp_tx,
    );

    // Action: Handle ClientWriteRequest
    state.push_client_cmd(cmd, &context);

    // Verify: NOT_LEADER response, structured so the caller can redirect
    let result = resp_rx.recv().await.expect("channel should not be closed");
    match result {
        Ok(ClientResponse {
            error, leader_hint, ..
        }) => {
            assert_eq!(error, ErrorCode::NotLeader);
            assert_eq!(
                leader_hint,
                Some(LeaderHint {
                    leader_id: 2,
                    address: "127.0.0.1:9082".to_string(),
                }),
                "must carry the leader's id and address so the caller can redirect"
            );
        }
        Err(status) => panic!(
            "expected Ok(ClientResponse {{ error: NotLeader, leader_hint: Some(_) }}), got bare Err(Status): {status:?}"
        ),
    }
}

// ============================================================================
// create_not_leader_response() Tests
// ============================================================================

/// Leader known and its address is in cluster membership -> hint populated.
#[tokio::test]
async fn test_create_not_leader_response_returns_hint_when_leader_address_known() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_context_with_leader_address(graceful_rx, 2, "127.0.0.1:9082");

    let state = FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.shared_state().set_current_leader(2);

    let response = state.create_not_leader_response(&context);

    assert_eq!(response.error, ErrorCode::NotLeader);
    assert_eq!(
        response.leader_hint,
        Some(LeaderHint {
            leader_id: 2,
            address: "127.0.0.1:9082".to_string(),
        }),
        "must return the leader's id and address so the caller can redirect"
    );
}

/// Leader id known but missing from membership (stale view) -> no hint, not a crash.
#[tokio::test]
async fn test_create_not_leader_response_returns_no_hint_when_leader_address_unknown() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    // Membership only knows about node 2 — id 99 (set as leader below) resolves to None.
    let (context, _temp_dir) = mock_context_with_leader_address(graceful_rx, 2, "127.0.0.1:9082");

    let state = FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.shared_state().set_current_leader(99); // no membership entry for id 99

    let response = state.create_not_leader_response(&context);

    assert_eq!(response.error, ErrorCode::NotLeader);
    assert_eq!(
        response.leader_hint, None,
        "must degrade to no hint, not panic, when the known leader id isn't in membership"
    );
}

/// No leader known at all (e.g. mid-election) -> no hint.
#[tokio::test]
async fn test_create_not_leader_response_returns_no_hint_when_leader_unknown() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let state = FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    // current_leader() defaults to None; not set here.

    let response = state.create_not_leader_response(&context);

    assert_eq!(response.error, ErrorCode::NotLeader);
    assert_eq!(response.leader_hint, None);
}

/// Test: FollowerState rejects ClientCmd::Scan with NotLeader error
///
/// Scan reads require linearizable prefix enumeration from the leader.
/// Followers must reject with Ok(ClientResponse{ error: NotLeader, .. }), same
/// as Propose/Read, so the client can redirect to the current leader.
#[tokio::test]
async fn test_scan_cmd_follower_rejects_with_not_leader() {
    use bytes::Bytes;

    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Scan(Bytes::from("/services/"), resp_tx);

    state.push_client_cmd(cmd, &context);

    let result = resp_rx.recv().await.expect("channel should not be closed");
    match result {
        Ok(ClientResponse { error, .. }) => {
            assert_eq!(error, ErrorCode::NotLeader);
        }
        Err(status) => panic!(
            "expected Ok(ClientResponse {{ error: NotLeader, .. }}), got bare Err(Status): {status:?}"
        ),
    }
}

/// Test: FollowerState rejects ClientReadRequest with LinearizableRead policy
///
/// Scenario:
/// - Follower receives ClientReadRequest with LinearizableRead consistency
/// - LinearizableRead requires leader involvement (lease-based or ReadIndex)
///
/// Expected:
/// - Returns response with error_code = NOT_LEADER
/// - handle_inbound_event returns Ok(())
///
/// This validates that linearizable reads must go through leader to ensure
/// consistency guarantees (leader lease or ReadIndex protocol).
///
/// Original: test_handle_inbound_event_case6_1
#[tokio::test]
async fn test_handle_client_read_request_linearizable_redirects_to_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead),
        keys: vec![],
    };

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    // Action: Handle ClientReadRequest
    state.push_client_cmd(cmd, &context);

    // Verify: Response with NOT_LEADER error
    let result = resp_rx.recv().await.expect("channel should not be closed");
    match result {
        Ok(ClientResponse { error, .. }) => {
            assert_eq!(error, ErrorCode::NotLeader);
        }
        Err(status) => panic!(
            "expected Ok(ClientResponse {{ error: NotLeader, .. }}), got bare Err(Status): {status:?}"
        ),
    }
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
/// - handle_inbound_event returns Ok(())
///
/// This validates that followers can serve eventual consistency reads,
/// improving read scalability at the cost of potentially stale data.
///
/// Original: test_handle_inbound_event_case6_2
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
        consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency),
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
        ErrorCode::Success,
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
    let inbound_event = InboundEvent::JoinCluster(request, resp_tx);
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // Action: Handle JoinCluster event
    let result = state.handle_inbound_event(inbound_event, &context, internal_event_tx).await;

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

/// Test: Follower returns Unavailable when it has not yet learned the leader.
///
/// Scenario:
/// - Follower has no current_leader (e.g. node just started, no heartbeat received yet)
/// - Client sends DiscoverLeader
///
/// Expected:
/// - handle_inbound_event returns Ok() (not a fatal error)
/// - Response carries Status::Unavailable — tells the client to retry later
///
/// Original: test_handle_inbound_event_case11
#[tokio::test]
async fn test_discover_leader_returns_unavailable_when_leader_unknown() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    // No set_current_leader call — follower has no leader info.

    let request = LeaderDiscoveryRequest {
        node_id: 2,
        requester_address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let inbound_event = InboundEvent::DiscoverLeader(request, resp_tx);
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    let result = state.handle_inbound_event(inbound_event, &context, internal_event_tx).await;

    assert!(result.is_ok(), "handle_inbound_event must not be fatal");

    let response = resp_rx.recv().await.expect("Should receive response");
    assert!(response.is_err());
    assert_eq!(
        response.unwrap_err().code(),
        Code::Unavailable,
        "Unknown leader → Unavailable (client should retry)"
    );
}

/// Test: Follower redirects client to known leader address.
///
/// Scenario:
/// - Follower received a heartbeat from node 3 and stored it as current_leader
/// - Client sends DiscoverLeader
/// - Membership returns node 3's address
///
/// Expected:
/// - Response carries leader_id=3, leader_address, and current term
///
/// Note: The returned leader_id may be stale if the leader stepped down after the last heartbeat.
/// This is acceptable — the client will get a NotLeader error from that node and retry.
/// The term returned alongside helps the client detect staleness.
#[tokio::test]
async fn test_discover_leader_returns_known_leader_address() {
    use d_engine_proto::common::{NodeRole::Leader, NodeStatus};
    use d_engine_proto::server::cluster::NodeMeta;

    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut membership = MockMembership::new();
    membership
        .expect_retrieve_node_meta()
        .with(mockall::predicate::eq(3u32))
        .returning(|_| {
            Some(NodeMeta {
                id: 3,
                address: "10.0.0.3:50051".to_string(),
                role: Leader.into(),
                status: NodeStatus::Active.into(),
            })
        });
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.shared_state.set_current_leader(3);
    state.shared_state_mut().update_current_term(5);

    let request = LeaderDiscoveryRequest {
        node_id: 2,
        requester_address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    let result = state
        .handle_inbound_event(
            InboundEvent::DiscoverLeader(request, resp_tx),
            &context,
            internal_event_tx,
        )
        .await;

    assert!(result.is_ok());
    let response = resp_rx.recv().await.expect("Should receive response").unwrap();
    assert_eq!(response.leader_id, 3);
    assert_eq!(response.leader_address, "10.0.0.3:50051");
    assert_eq!(
        response.term, 5,
        "Response must carry current term for staleness detection"
    );
}

/// Test: Follower knows a leader ID but cannot find its metadata — returns NotFound.
///
/// Scenario:
/// - Follower has current_leader=3 but membership has no metadata for node 3
///   (e.g. membership config not yet propagated after cluster reconfiguration)
///
/// Expected:
/// - Response carries Status::NotFound
#[tokio::test]
async fn test_discover_leader_returns_not_found_when_metadata_missing() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut membership = MockMembership::new();
    membership.expect_retrieve_node_meta().returning(|_| None);
    context.membership = Arc::new(membership);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    state.shared_state.set_current_leader(3);

    let request = LeaderDiscoveryRequest {
        node_id: 2,
        requester_address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    let result = state
        .handle_inbound_event(
            InboundEvent::DiscoverLeader(request, resp_tx),
            &context,
            internal_event_tx,
        )
        .await;

    assert!(result.is_ok());
    let response = resp_rx.recv().await.expect("Should receive response");
    assert_eq!(
        response.unwrap_err().code(),
        Code::NotFound,
        "Known leader ID but missing metadata → NotFound"
    );
}

// ============================================================================
// Snapshot Tests Module
// ============================================================================

mod snapshot_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// Test: Follower gracefully ignores stale leader-only internal events
    ///
    /// Protocol scenario: a leader steps down to follower while internal events
    /// (LogPurgeCompleted, PromoteReadyLearners, StepDownSelfRemoved, MembershipApplied)
    /// are still queued in internal_event_rx.  All must be silently ignored — returning an error
    /// here would trigger a non-fatal warning log and waste a loop iteration for no reason.
    ///
    /// Expected: all return Ok(()) — no panic, no state change.
    #[tokio::test]
    async fn test_follower_ignores_stale_leader_internal_events() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
        let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

        // LogPurgeCompleted: leader-only, but stale events after step-down must not error.
        // Must also clear scheduled_purge_upto to prevent duplicate execution.
        state.scheduled_purge_upto = Some(LogId { term: 1, index: 1 });
        assert!(
            state.handle_log_purge_completed(LogId { term: 1, index: 1 }).is_ok(),
            "Stale LogPurgeCompleted should be silently ignored"
        );
        assert!(
            state.scheduled_purge_upto.is_none(),
            "handle_log_purge_completed must clear scheduled_purge_upto"
        );

        // PromoteReadyLearners: leader-only, same reasoning
        assert!(
            state.handle_promote_ready_learners(&context, &internal_event_tx).await.is_ok(),
            "Stale PromoteReadyLearners should be silently ignored"
        );

        // StepDownSelfRemoved: only leader can self-remove, stale event must not error
        assert!(
            state.handle_self_removed(&internal_event_tx).is_ok(),
            "Stale StepDownSelfRemoved should be silently ignored"
        );

        // MembershipApplied: follower has no cache to refresh — pure no-op
        assert!(
            state.handle_membership_applied(&context, &internal_event_tx).await.is_ok(),
            "MembershipApplied on follower should be a no-op"
        );
    }

    /// Test: Follower ignores duplicate CreateSnapshot while one is already in progress
    ///
    /// The `snapshot_in_progress` flag guards against concurrent snapshot creation.
    /// A second trigger (e.g. from two rapid ApplyCompleted events) must be silently
    /// dropped rather than spawning a second background task.
    ///
    /// Expected:
    /// - First call: Ok(), sets snapshot_in_progress = true, spawns background task
    /// - Second call: Ok(), skips (flag already set), flag remains true
    #[tokio::test]
    async fn test_follower_ignores_duplicate_create_snapshot() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
        let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

        // First trigger — starts background snapshot
        let result1 = state.handle_create_snapshot(&context, &internal_event_tx).await;
        assert!(
            result1.is_ok(),
            "First handle_create_snapshot should succeed"
        );
        assert!(
            state.snapshot_in_progress.load(Ordering::SeqCst),
            "snapshot_in_progress should be true after first trigger"
        );

        // Second trigger while first is still running — must be a no-op
        let result2 = state.handle_create_snapshot(&context, &internal_event_tx).await;
        assert!(
            result2.is_ok(),
            "Second handle_create_snapshot should return Ok (skipped)"
        );
        assert!(
            state.snapshot_in_progress.load(Ordering::SeqCst),
            "snapshot_in_progress should remain true"
        );
    }

    /// Test: Follower resets snapshot_in_progress and updates last_purged_index on success
    ///
    /// Per Raft §7, followers independently purge logs after a successful snapshot.
    /// This test verifies both the flag lifecycle and the purge side-effect.
    ///
    /// Scenario:
    /// - snapshot_in_progress is pre-set to true (simulating an in-flight snapshot)
    /// - SnapshotCreated arrives with a successful result (last_included = index 50)
    ///
    /// Expected:
    /// - snapshot_in_progress reset to false
    /// - last_purged_index updated to Some(LogId { term: 1, index: 50 })
    #[tokio::test]
    async fn test_follower_snapshot_created_success_resets_flag_and_purges_logs() {
        let (_graceful_tx, graceful_rx) = watch::channel(());

        // Default retained_log_entries = 1, last_included.index = 50 → purge_upto_index = 49
        let mut raft_log = mock_raft_log();
        raft_log
            .expect_entry_term()
            .withf(|&idx| idx == 49)
            .times(1)
            .returning(|_| Some(1));
        let _temp_dir = tempfile::tempdir().unwrap();
        let mut nc = node_config(_temp_dir.path().to_str().unwrap());
        nc.raft.snapshot.retained_log_entries = 1;
        let context = MockBuilder::new(graceful_rx)
            .with_raft_log(raft_log)
            .with_node_config(nc)
            .build_context();

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

        state.snapshot_in_progress.store(true, Ordering::SeqCst);
        state.update_commit_index(100).unwrap();

        let last_included = LogId { term: 1, index: 50 };
        let metadata = d_engine_proto::server::storage::SnapshotMetadata {
            last_included: Some(last_included),
            checksum: bytes::Bytes::new(),
        };
        let snapshot_result = Ok((metadata, std::path::PathBuf::from("/tmp/snap.bin")));

        let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
        let result = state
            .handle_snapshot_created(snapshot_result, &context, &internal_event_tx)
            .await;

        assert!(result.is_ok(), "handle_snapshot_created should succeed");
        assert!(
            !state.snapshot_in_progress.load(Ordering::SeqCst),
            "snapshot_in_progress must be false after completion"
        );
        // last_purged_index is updated via LogPurgeCompleted event (processed by event loop).
        // Verify the event was dispatched with purge_upto = last_included - retained_log_entries.
        let event = internal_event_rx
            .try_recv()
            .expect("LogPurgeCompleted event must be dispatched after successful purge");
        assert!(
            matches!(
                event,
                InternalEvent::LogPurgeCompleted(LogId { index: 49, .. })
            ),
            "purge boundary must be last_included.index(50) - retained(1) = 49, got: {event:?}"
        );
    }

    /// Test: Follower resets snapshot_in_progress on failure but does NOT purge logs
    ///
    /// A failed snapshot must not advance the purge boundary — the logs are still needed.
    /// The flag must still be cleared so the next ApplyCompleted can retry.
    ///
    /// Expected:
    /// - snapshot_in_progress reset to false
    /// - last_purged_index remains None (no purge on failure)
    /// - handler returns Ok() (error is logged, not propagated)
    #[tokio::test]
    async fn test_follower_snapshot_created_failure_resets_flag_no_purge() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
        state.snapshot_in_progress.store(true, Ordering::SeqCst);

        let snapshot_result = Err(Error::Fatal("Snapshot creation failed".to_string()));

        let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
        let result = state
            .handle_snapshot_created(snapshot_result, &context, &internal_event_tx)
            .await;

        assert!(
            result.is_ok(),
            "handle_snapshot_created should return Ok even on failure"
        );
        assert!(
            !state.snapshot_in_progress.load(Ordering::SeqCst),
            "snapshot_in_progress must be cleared even after failure"
        );
        assert_eq!(
            state.last_purged_index, None,
            "last_purged_index must not advance when snapshot failed"
        );
    }

    /// Test: Complete snapshot lifecycle — create, complete, create again
    ///
    /// Validates the full flag cycle: false → true (create) → false (created ok) → true (create again).
    /// Ensures the follower can take multiple snapshots over its lifetime without getting stuck.
    #[tokio::test]
    async fn test_follower_snapshot_lifecycle() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft_log = mock_raft_log();
        raft_log.expect_entry_term().times(1).returning(|_| Some(1));
        let _temp_dir = tempfile::tempdir().unwrap();
        let mut nc = node_config(_temp_dir.path().to_str().unwrap());
        nc.raft.snapshot.retained_log_entries = 1;
        let context = MockBuilder::new(graceful_rx)
            .with_raft_log(raft_log)
            .with_node_config(nc)
            .build_context();

        let mut state =
            FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
        let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

        // Phase 1: trigger snapshot
        assert!(state.handle_create_snapshot(&context, &internal_event_tx).await.is_ok());
        assert!(
            state.snapshot_in_progress.load(Ordering::SeqCst),
            "flag set after create"
        );

        // Phase 2: snapshot completes successfully
        let metadata = d_engine_proto::server::storage::SnapshotMetadata {
            last_included: Some(LogId {
                term: 1,
                index: 100,
            }),
            checksum: bytes::Bytes::new(),
        };
        let ok_result = Ok((metadata, std::path::PathBuf::from("/tmp/snap1.bin")));
        assert!(
            state
                .handle_snapshot_created(ok_result, &context, &internal_event_tx)
                .await
                .is_ok()
        );
        assert!(
            !state.snapshot_in_progress.load(Ordering::SeqCst),
            "flag cleared after created"
        );

        // Phase 3: second snapshot can now be triggered
        assert!(state.handle_create_snapshot(&context, &internal_event_tx).await.is_ok());
        assert!(
            state.snapshot_in_progress.load(Ordering::SeqCst),
            "flag set again for second snapshot"
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
    use crate::config::ReadConsistencyPolicy as ClientPolicy;
    use crate::convert::safe_kv_bytes;

    /// Test: Follower rejects LeaseRead policy
    ///
    /// Scenario:
    /// - Client requests read with LeaseRead consistency policy
    /// - LeaseRead requires leader lease for linearizability
    ///
    /// Expected:
    /// - Returns response with error_code = NOT_LEADER
    /// - handle_inbound_event returns Ok()
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
            consistency_policy: Some(ClientPolicy::LeaseRead),
            keys: vec![safe_kv_bytes(1)],
        };

        let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
        let cmd = ClientCmd::Read(client_read_request, resp_tx);

        state.push_client_cmd(cmd, &context);

        let result = resp_rx.recv().await.expect("channel should not be closed");
        match result {
            Ok(ClientResponse { error, .. }) => {
                assert_eq!(
                    error,
                    ErrorCode::NotLeader,
                    "LeaseRead should be rejected by follower"
                );
            }
            Err(status) => panic!(
                "expected Ok(ClientResponse {{ error: NotLeader, .. }}), got bare Err(Status): {status:?}"
            ),
        }
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
    /// - handle_inbound_event returns Ok()
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
        match result {
            Ok(ClientResponse { error, .. }) => {
                assert_eq!(
                    error,
                    ErrorCode::NotLeader,
                    "Default LinearizableRead should be rejected by follower"
                );
            }
            Err(status) => panic!(
                "expected Ok(ClientResponse {{ error: NotLeader, .. }}), got bare Err(Status): {status:?}"
            ),
        }
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
    /// - handle_inbound_event returns Ok()
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
            ErrorCode::Success,
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
            consistency_policy: Some(ClientPolicy::LinearizableRead),
            keys: vec![safe_kv_bytes(1)],
        };

        let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
        let cmd = ClientCmd::Read(client_read_request, resp_tx);

        state.push_client_cmd(cmd, &context);

        // Follower must serve successfully using server default (EventualConsistency)
        let response = resp_rx.recv().await.unwrap().unwrap();
        assert_eq!(
            response.error,
            ErrorCode::Success,
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
            consistency_policy: Some(ClientPolicy::EventualConsistency),
            keys: vec![safe_kv_bytes(1)],
        };

        let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
        let cmd = ClientCmd::Read(client_read_request, resp_tx);

        state.push_client_cmd(cmd, &context);

        // Follower must reject: server default is LinearizableRead which requires leader
        let result = resp_rx.recv().await.expect("channel should not be closed");
        match result {
            Ok(ClientResponse { error, .. }) => {
                assert_eq!(
                    error,
                    ErrorCode::NotLeader,
                    "Follower must reject LinearizableRead (requires leader)"
                );
            }
            Err(status) => panic!(
                "expected Ok(ClientResponse {{ error: NotLeader, .. }}), got bare Err(Status): {status:?}"
            ),
        }
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
/// - Follower handles FatalError event via handle_inbound_event()
///
/// # Then
/// - handle_inbound_event() returns Error::Fatal
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
    let fatal_error = InboundEvent::FatalError {
        source: "StateMachine".to_string(),
        error: "Disk failure".to_string(),
    };

    // Create internal event channel
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

    // Handle the FatalError event
    let result = follower.handle_inbound_event(fatal_error, &context, internal_event_tx).await;

    // VERIFY 1: handle_inbound_event() returns Error::Fatal
    assert!(
        result.is_err(),
        "Expected handle_inbound_event to return Err, got: {result:?}"
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

    // VERIFY 3: No internal events sent
    assert!(
        internal_event_rx.try_recv().is_err(),
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
/// - InternalEvent::CreateSnapshotEvent is sent directly on internal_event_tx (P2 unbounded)
/// - No ReprocessEvent wrapper — direct send eliminates the bounded event_tx deadlock path
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

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

    // ACTION: Handle ApplyCompleted event
    let result = follower.handle_apply_completed(100, vec![], &context, &internal_event_tx).await;

    // VERIFY 1: Event handling succeeds
    assert!(
        result.is_ok(),
        "ApplyCompleted should be handled successfully, got: {result:?}"
    );

    // VERIFY 2: CreateSnapshotEvent is sent directly on internal_event_tx (P2 unbounded)
    // check_and_trigger_snapshot no longer wraps in ReprocessEvent — direct send avoids
    // the extra round-trip through event_tx that caused the original deadlock risk.
    let event = internal_event_rx.try_recv().expect("Should receive snapshot trigger event");
    assert!(
        matches!(event, InternalEvent::CreateSnapshotEvent),
        "Expected InternalEvent::CreateSnapshotEvent, got: {event:?}"
    );

    // VERIFY 3: No additional events queued
    assert!(
        internal_event_rx.try_recv().is_err(),
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

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

    // ACTION: Handle ApplyCompleted event
    let result = follower.handle_apply_completed(50, vec![], &context, &internal_event_tx).await;

    // VERIFY 1: Event handling succeeds
    assert!(
        result.is_ok(),
        "ApplyCompleted should be handled successfully"
    );

    // VERIFY 2: No snapshot event is sent
    assert!(
        internal_event_rx.try_recv().is_err(),
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

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

    // ACTION: Handle ApplyCompleted event
    let result = follower.handle_apply_completed(100, vec![], &context, &internal_event_tx).await;

    // VERIFY 1: Event handling succeeds
    assert!(
        result.is_ok(),
        "ApplyCompleted should be handled successfully"
    );

    // VERIFY 2: No snapshot event is sent (snapshot disabled in config)
    assert!(
        internal_event_rx.try_recv().is_err(),
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
            consistency_policy: Some(ReadConsistencyPolicy::LeaseRead),
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
        if let Ok(Ok(ClientResponse { error, .. })) = result {
            assert_eq!(
                error,
                ErrorCode::NotLeader,
                "Expected NOT_LEADER error for Lease read"
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
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead),
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
        if let Ok(Ok(ClientResponse { error, .. })) = result {
            assert_eq!(
                error,
                ErrorCode::NotLeader,
                "Expected NOT_LEADER error for Linear read"
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
    state.shared_state_mut().update_current_term(leader_term);

    let append_request = AppendEntriesRequest {
        term: leader_term,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let inbound_event = InboundEvent::AppendEntries(append_request, vec![resp_tx]);
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

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
    state.shared_state_mut().update_current_term(leader_term);

    let append_request = AppendEntriesRequest {
        term: leader_term,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let inbound_event = InboundEvent::AppendEntries(append_request, vec![resp_tx]);
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

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
    state.shared_state_mut().update_current_term(leader_term);

    let append_request = AppendEntriesRequest {
        term: leader_term,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit_index: new_commit,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let inbound_event = InboundEvent::AppendEntries(append_request, vec![resp_tx]);
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok()
    );

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

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let (tx, rx) = mpsc::channel(32);
    tx.send(SnapshotChunk::default()).await.unwrap();
    drop(tx);
    state
        .handle_inbound_event(
            InboundEvent::InstallSnapshotChunk(rx, resp_tx),
            &context,
            internal_event_tx,
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

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // Follower absorbs the error and continues (does not propagate)
    let (tx, rx) = mpsc::channel(32);
    tx.send(SnapshotChunk::default()).await.unwrap();
    drop(tx);
    let _ = state
        .handle_inbound_event(
            InboundEvent::InstallSnapshotChunk(rx, resp_tx),
            &context,
            internal_event_tx,
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
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    assert!(
        state
            .handle_inbound_event(
                InboundEvent::ClusterConf(MetadataRequest {}, resp_tx),
                &context,
                internal_event_tx
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

// ============================================================================
// StreamSnapshot Rejection Tests
// ============================================================================

// ============================================================================
// Role Transition Tests — scheduled_purge_upto / last_purged_index
// ============================================================================

/// From<&CandidateState>: last_purged_index preserved, scheduled_purge_upto reset.
///
/// Candidate has no scheduled_purge_upto field; the resulting follower always starts
/// with None so it does not replay a stale purge boundary from a prior term.
#[test]
fn test_follower_from_candidate_preserves_last_purged_index_resets_scheduled() {
    let cfg = Arc::new(node_config("/tmp/test_follower_from_candidate_purge"));
    let mut candidate = CandidateState::<MockTypeConfig>::new(1, cfg);
    candidate.last_purged_index = Some(LogId { term: 2, index: 10 });

    let follower = FollowerState::from(&candidate);

    assert_eq!(
        follower.last_purged_index,
        Some(LogId { term: 2, index: 10 })
    );
    assert_eq!(follower.scheduled_purge_upto, None);
}

/// From<&LeaderState>: both fields preserved across leader-to-follower stepdown.
///
/// A leader with a pending purge intent (scheduled_purge_upto) must hand it off so the
/// follower can resume the purge without recomputing the boundary.
#[test]
fn test_follower_from_leader_preserves_both_purge_fields() {
    let cfg = Arc::new(node_config("/tmp/test_follower_from_leader_purge"));
    let mut leader = LeaderState::<MockTypeConfig>::new(1, cfg);
    leader.last_purged_index = Some(LogId { term: 2, index: 20 });
    leader.scheduled_purge_upto = Some(LogId { term: 2, index: 18 });

    let follower = FollowerState::from(&leader);

    assert_eq!(
        follower.last_purged_index,
        Some(LogId { term: 2, index: 20 })
    );
    assert_eq!(
        follower.scheduled_purge_upto,
        Some(LogId { term: 2, index: 18 })
    );
}

/// From<&LearnerState>: both fields preserved across learner-to-follower promotion.
#[test]
fn test_follower_from_learner_preserves_both_purge_fields() {
    let cfg = Arc::new(node_config("/tmp/test_follower_from_learner_purge"));
    let mut learner = LearnerState::<MockTypeConfig>::new(1, cfg);
    learner.last_purged_index = Some(LogId { term: 1, index: 7 });
    learner.scheduled_purge_upto = Some(LogId { term: 1, index: 5 });

    let follower = FollowerState::from(&learner);

    assert_eq!(
        follower.last_purged_index,
        Some(LogId { term: 1, index: 7 })
    );
    assert_eq!(
        follower.scheduled_purge_upto,
        Some(LogId { term: 1, index: 5 })
    );
}

/// Test: Follower rejects StreamSnapshot — only Leader streams snapshots to Learners.
///
/// Scenario:
/// - Follower receives StreamSnapshot (misdirected from a Learner)
///
/// Expected:
/// - startup_tx receives Err(FailedPrecondition) — tells caller this node is not the leader
/// - handle_inbound_event returns Ok() (not a fatal error)
#[tokio::test]
async fn test_follower_rejects_stream_snapshot() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);

    let (_ack_tx, ack_rx) =
        tokio::sync::mpsc::channel::<d_engine_proto::server::storage::SnapshotAck>(4);
    let (chunk_tx, _chunk_rx) = tokio::sync::mpsc::channel::<
        std::sync::Arc<d_engine_proto::server::storage::SnapshotChunk>,
    >(4);
    let (startup_tx, startup_rx) =
        tokio::sync::oneshot::channel::<std::result::Result<(), tonic::Status>>();

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    let result = state
        .handle_inbound_event(
            InboundEvent::StreamSnapshot(ack_rx, chunk_tx, startup_tx),
            &context,
            internal_event_tx,
        )
        .await;

    assert!(result.is_ok(), "StreamSnapshot rejection must not be fatal");

    let startup_result = startup_rx.await.expect("startup_tx must be sent");
    assert!(startup_result.is_err());
    assert_eq!(
        startup_result.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "Follower must reply FailedPrecondition — not the leader"
    );
}
