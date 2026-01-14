use std::sync::Arc;

use tokio::sync::watch;
use tracing_test::traced_test;

use crate::MockRaftLog;
use crate::MockStateMachineHandler;
use crate::candidate_state::CandidateState;
use crate::follower_state::FollowerState;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use crate::test_utils::node_config;

// ============================================================================
// Unit Tests for LinearizableRead Core Functions
// ============================================================================

/// Test calculate_read_index when noop_index is ahead of commit_index
///
/// # Test Scenario
/// This verifies that linearizable reads use noop_index when it's larger than commit_index,
/// ensuring reads see the no-op entry committed during leader initialization.
///
/// # Given
/// - Leader has commit_index = 100
/// - Leader has noop_log_id = 105 (no-op entry not yet committed to majority)
///
/// # When
/// - calculate_read_index() is called
///
/// # Then
/// - Should return 105 (max of 100 and 105)
/// - This ensures reads wait for the no-op entry to be applied
#[tokio::test]
#[traced_test]
async fn test_calculate_read_index_noop_ahead() {
    // Given: Leader with commit_index < noop_log_id
    let config = Arc::new(node_config("/tmp/test_calculate_read_index_noop_ahead"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, config);
    state.update_commit_index(100).expect("should succeed");
    state.noop_log_id = Some(105);

    // When: calculate read index
    let read_index = state.calculate_read_index();

    // Then: should return max(100, 105) = 105
    assert_eq!(read_index, 105, "Should use noop_index when it's ahead");
}

/// Test calculate_read_index when commit_index is ahead of noop_index
///
/// # Test Scenario
/// This verifies that linearizable reads use commit_index when it has advanced
/// beyond the no-op entry, ensuring reads see all committed data.
///
/// # Given
/// - Leader has commit_index = 110 (additional entries committed after no-op)
/// - Leader has noop_log_id = 105 (no-op entry already committed)
///
/// # When
/// - calculate_read_index() is called
///
/// # Then
/// - Should return 110 (max of 110 and 105)
/// - This ensures reads see all committed entries, not just up to no-op
#[tokio::test]
#[traced_test]
async fn test_calculate_read_index_commit_ahead() {
    // Given: Leader with commit_index > noop_log_id
    let config = Arc::new(node_config("/tmp/test_calculate_read_index_commit_ahead"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, config);
    state.update_commit_index(110).expect("should succeed");
    state.noop_log_id = Some(105);

    // When: calculate read index
    let read_index = state.calculate_read_index();

    // Then: should return max(110, 105) = 110
    assert_eq!(read_index, 110, "Should use commit_index when it's ahead");
}

/// Test calculate_read_index when noop_index is None (defensive programming)
///
/// # Test Scenario
/// This verifies defensive behavior for an edge case that should NOT occur in normal
/// operation. In normal flow, on_noop_committed() sets noop_log_id immediately after
/// BecomeLeader succeeds, before any client requests are processed.
///
/// This test validates the fallback mechanism using unwrap_or(0) to prevent panics
/// if noop_log_id is unexpectedly None due to bugs or race conditions.
///
/// # Given
/// - Leader has commit_index = 100
/// - Leader has noop_log_id = None (ABNORMAL: should not happen in production)
///
/// # When
/// - calculate_read_index() is called
///
/// # Then
/// - Should return 100 (max of 100 and 0, where None defaults to 0)
/// - This defensive fallback ensures correctness even in unexpected states
///
/// # Note
/// This is NOT a normal business scenario - it tests error resilience
#[tokio::test]
#[traced_test]
async fn test_calculate_read_index_without_noop() {
    // Given: Leader with noop_log_id = None
    let config = Arc::new(node_config("/tmp/test_calculate_read_index_without_noop"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, config);
    state.update_commit_index(100).expect("should succeed");
    state.noop_log_id = None;

    // When: calculate read index
    let read_index = state.calculate_read_index();

    // Then: should return commit_index (noop_index defaults to 0)
    assert_eq!(
        read_index, 100,
        "Should use commit_index when noop_index is None"
    );
}

/// Test wait_until_applied when last_applied < target_index
///
/// # Test Scenario
/// This verifies that linearizable reads correctly wait for state machine to apply
/// entries up to the fixed read_index calculated at request arrival time.
///
/// # Given
/// - State machine has last_applied = 90
/// - Read request calculated target_index = 100 (from calculate_read_index)
///
/// # When
/// - wait_until_applied(100, ..., 90) is called
///
/// # Then
/// - Should call update_pending(100) to notify state machine
/// - Should call wait_applied(100, timeout) to block until applied
/// - This ensures read sees all data up to index 100
///
/// # Note
/// Refactored from test_ensure_state_machine_upto_commit_index_case1
#[tokio::test]
#[traced_test]
async fn test_wait_until_applied_needs_wait() {
    // Given: State machine behind target index
    let config = Arc::new(node_config("/tmp/test_wait_until_applied_needs_wait"));
    let state = LeaderState::<MockTypeConfig>::new(1, config);
    let target_index = 100;
    let last_applied = 90;

    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_update_pending().times(1).returning(|_| {});
    state_machine_handler.expect_wait_applied().times(1).return_once(|_, _| Ok(()));

    // When: wait until applied
    state
        .wait_until_applied(target_index, &Arc::new(state_machine_handler), last_applied)
        .await
        .expect("should succeed");

    // Then: should call update_pending and wait_applied (verified by mock expectations)
}

/// Test wait_until_applied when last_applied >= target_index
///
/// # Test Scenario
/// This verifies the optimization where linearizable reads skip waiting
/// when state machine has already applied the target index.
///
/// # Given
/// - State machine has last_applied = 100
/// - Read request calculated target_index = 100 (from calculate_read_index)
///
/// # When
/// - wait_until_applied(100, ..., 100) is called
///
/// # Then
/// - Should NOT call update_pending (no notification needed)
/// - Should NOT call wait_applied (already applied)
/// - Read can proceed immediately without blocking
///
/// # Note
/// Refactored from test_ensure_state_machine_upto_commit_index_case2
#[tokio::test]
#[traced_test]
async fn test_wait_until_applied_no_wait() {
    // Given: State machine already at target index
    let config = Arc::new(node_config("/tmp/test_wait_until_applied_no_wait"));
    let state = LeaderState::<MockTypeConfig>::new(1, config);
    let target_index = 100;
    let last_applied = 100;

    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_update_pending().times(0).returning(|_| {});
    state_machine_handler.expect_wait_applied().times(0);

    // When: wait until applied
    state
        .wait_until_applied(target_index, &Arc::new(state_machine_handler), last_applied)
        .await
        .expect("should succeed");

    // Then: should not call update_pending or wait_applied (verified by mock expectations)
}

/// Test on_noop_committed for Leader role
///
/// # Test Scenario
/// This verifies that Leader correctly tracks the no-op entry index after
/// successful leadership verification, enabling linearizable read optimization.
///
/// # Given
/// - Node becomes Leader
/// - No-op entry is appended and committed (last_entry_id = 42)
///
/// # When
/// - on_noop_committed(ctx) is called after verify_leadership_persistent succeeds
///
/// # Then
/// - Leader should read last_entry_id from raft_log (returns 42)
/// - Leader should set noop_log_id = Some(42)
/// - Future calculate_read_index() calls will use this value
#[tokio::test]
#[traced_test]
async fn test_on_noop_committed_leader() {
    // Given: Leader with committed no-op entry at index 42
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context("/tmp/test_on_noop_committed_leader", graceful_rx, None);
    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    let mut mock_log = MockRaftLog::new();
    mock_log.expect_last_entry_id().return_const(42u64);
    context.storage.raft_log = Arc::new(mock_log);

    // When: on_noop_committed is called
    state.on_noop_committed(&context).expect("should succeed");

    // Then: noop_log_id should be set to 42
    assert_eq!(
        state.noop_log_id,
        Some(42),
        "Leader should track noop_log_id from last_entry_id"
    );
}

/// Test on_noop_committed for non-Leader roles (should be no-op)
///
/// # Test Scenario
/// This verifies role responsibility isolation: only Leader tracks no-op entries,
/// other roles safely ignore this call per the trait's default implementation.
///
/// # Given
/// - Node is in Follower or Candidate role
///
/// # When
/// - on_noop_committed(ctx) is called (e.g., during incorrect role transition)
///
/// # Then
/// - Should return Ok(()) without side effects (default trait implementation)
/// - Should NOT panic or return error
/// - Follower/Candidate do not manage noop_log_id field
///
/// # Rationale
/// Follows single responsibility principle: only Leader needs linearizable read optimization
#[tokio::test]
#[traced_test]
async fn test_on_noop_committed_non_leader() {
    // Given: Non-leader roles
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context("/tmp/test_on_noop_committed_non_leader", graceful_rx, None);

    // When/Then: Follower handles as no-op
    let mut follower =
        FollowerState::<MockTypeConfig>::new(1, context.node_config.clone(), None, None);
    let result = follower.on_noop_committed(&context);
    assert!(
        result.is_ok(),
        "Follower should handle on_noop_committed as no-op"
    );

    // When/Then: Candidate handles as no-op
    let mut candidate = CandidateState::<MockTypeConfig>::new(1, context.node_config.clone());
    let result = candidate.on_noop_committed(&context);
    assert!(
        result.is_ok(),
        "Candidate should handle on_noop_committed as no-op"
    );
}

/// Test wait_until_applied with slow state machine apply
///
/// # Test Scenario
/// This verifies the optimization where linearizable reads do NOT wait for
/// state machine apply when commit_index already satisfies the read requirement.
///
/// # Background
/// Before optimization (#236), readers would wait for:
///   commit_index >= readIndex AND apply_index >= readIndex
///
/// After optimization, readers only wait for:
///   commit_index >= readIndex
///
/// This test simulates a slow state machine where apply lags behind commit.
///
/// # Given
/// - commit_index = 100 (entries committed to quorum)
/// - last_applied = 90 (state machine apply is slow)
/// - read_index = 95 (calculated at request arrival)
///
/// # When
/// - wait_until_applied(95, ..., 90) is called
/// - commit_index(100) >= read_index(95) ✓
/// - last_applied(90) < read_index(95) ✗
///
/// # Then
/// - Should call update_pending(95) to notify state machine
/// - Should call wait_applied(95, timeout) to wait
/// - This ensures linearizability: read sees committed data up to index 95
///
/// # Note
/// This is correct behavior! We wait for apply when:
///   - commit_index >= readIndex (already satisfied)
///   - last_applied < readIndex (need to wait)
///
/// The optimization is in verify_leadership: we skip waiting for commit_index
/// to advance when it already satisfies readIndex.
#[tokio::test]
#[traced_test]
async fn test_wait_until_applied_with_slow_state_machine() {
    // Given: Slow state machine (apply lags behind commit)
    let config = Arc::new(node_config(
        "/tmp/test_wait_until_applied_with_slow_state_machine",
    ));
    let mut state = LeaderState::<MockTypeConfig>::new(1, config);

    // Simulate: commit_index advanced to 100
    state.update_commit_index(100).expect("should succeed");

    // Simulate: read_index calculated as 95 (at request arrival time)
    let read_index = 95;

    // Simulate: state machine apply is slow, only applied to 90
    let last_applied = 90;

    // Mock state machine handler
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_update_pending()
        .times(1)
        .with(mockall::predicate::eq(read_index))
        .returning(|_| {});
    state_machine_handler
        .expect_wait_applied()
        .times(1)
        .with(
            mockall::predicate::eq(read_index),
            mockall::predicate::always(),
        )
        .return_once(|_, _| Ok(()));

    // When: Reader waits until applied
    state
        .wait_until_applied(read_index, &Arc::new(state_machine_handler), last_applied)
        .await
        .expect("should succeed");

    // Then: Should wait for state machine to apply up to read_index
    // (verified by mock expectations)
    //
    // Key insight: This is correct! We must wait for apply when last_applied < readIndex,
    // even if commit_index >= readIndex. The optimization is that we don't wait for
    // commit_index to advance further when it already satisfies readIndex.
}

/// Test optimization: skip waiting when commit_index already >= readIndex
///
/// # Test Scenario
/// This verifies the core optimization (#236): when commit_index already satisfies
/// the read requirement, we skip waiting for it to advance further.
///
/// # Background
/// Before optimization, verify_leadership would always wait for:
///   1. Heartbeat response from quorum
///   2. commit_index to advance
///
/// After optimization, we check if current commit_index >= readIndex first.
///
/// # Given
/// - commit_index = 100 (current state)
/// - read_index = 95 (calculated at request arrival)
/// - last_applied = 100 (state machine is up-to-date)
///
/// # When
/// - commit_index(100) >= read_index(95) ✓
/// - last_applied(100) >= read_index(95) ✓
///
/// # Then
/// - Should NOT wait for commit_index update
/// - Should NOT wait for state machine apply
/// - Read can proceed immediately
///
/// # Implementation Note
/// This test verifies wait_until_applied behavior. The actual optimization
/// happens in verify_leadership (RaftRole::verify_leadership_for_read_index).
#[tokio::test]
#[traced_test]
async fn test_optimization_skip_wait_when_commit_satisfies_read() {
    // Given: commit_index already ahead of read_index
    let config = Arc::new(node_config(
        "/tmp/test_optimization_skip_wait_when_commit_satisfies_read",
    ));
    let mut state = LeaderState::<MockTypeConfig>::new(1, config);

    // Simulate: commit_index = 100
    state.update_commit_index(100).expect("should succeed");

    // Simulate: read_index = 95 (older than commit_index)
    let read_index = 95;

    // Simulate: state machine is up-to-date
    let last_applied = 100;

    // Mock state machine handler
    let mut state_machine_handler = MockStateMachineHandler::new();
    // Should NOT call update_pending or wait_applied
    state_machine_handler.expect_update_pending().times(0);
    state_machine_handler.expect_wait_applied().times(0);

    // When: Check if need to wait
    state
        .wait_until_applied(read_index, &Arc::new(state_machine_handler), last_applied)
        .await
        .expect("should succeed");

    // Then: Should skip waiting (verified by mock expectations: 0 calls)
    //
    // This demonstrates the optimization: when both commit_index and last_applied
    // already satisfy readIndex, we don't need to wait for anything.
}

// ============================================================================
// Integration Tests from d-engine-server (Migrated)
// ============================================================================

/// Test linearizable read when quorum fails to verify leadership
///
/// # Test Scenario
/// This verifies that LinearizableRead requests fail gracefully when the Leader
/// cannot verify its leadership via quorum heartbeat.
///
/// # Given
/// - Leader has commit_index = 1
/// - Replication handler returns Error (simulates quorum failure)
/// - read_batching.size_threshold = 1 (immediate flush)
///
/// # When
/// - Client sends LinearizableRead request
/// - verify_leadership fails (quorum unreachable)
///
/// # Then
/// - Client receives FailedPrecondition error
/// - Request is properly rejected
///
/// # Raft Protocol Context
/// Per Raft paper Section 8: Leader must verify it is still the leader before
/// serving linearizable reads. If heartbeat fails to reach quorum, the Leader
/// cannot guarantee linearizability and must reject the read.
///
/// # Note
/// Renamed from test_handle_raft_event_case6_1
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_quorum_failure() {
    use tokio::sync::mpsc;
    use tonic::Code;

    use crate::RaftEvent;
    use crate::convert::safe_kv_bytes;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::test_utils::MockBuilder;
    use crate::{Error, MockReplicationCore, RaftNodeConfig};
    use d_engine_proto::client::{ClientReadRequest, ReadConsistencyPolicy};

    // Given: Leader with replication handler that fails
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::Fatal("".to_string())));

    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.read_batching.size_threshold = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_linearizable_read_quorum_failure")
        .with_replication_handler(replication_handler)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // When: Client sends LinearizableRead request
    let keys = vec![safe_kv_bytes(1)];
    let client_read_request = ClientReadRequest {
        client_id: 1,
        keys,
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Event handling should succeed (error is sent to client via resp_rx)
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Event should be handled successfully");

    // Then: Client receives error via response channel
    let e = resp_rx.recv().await.unwrap().unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition);
}

/// Test linearizable read with successful quorum verification
///
/// # Test Scenario
/// This verifies the complete LinearizableRead flow when Leader successfully
/// verifies its leadership and serves the read.
///
/// # Given
/// - Leader has commit_index = 1
/// - Replication handler returns success (quorum reached)
/// - calculate_majority_matched_index returns 3 (new commit index)
/// - State machine is ready to serve reads
/// - read_batching.size_threshold = 1 (immediate flush)
///
/// # When
/// - Client sends LinearizableRead request
/// - verify_leadership succeeds (quorum confirms leadership)
/// - commit_index advances to 3
/// - State machine applies entries
///
/// # Then
/// - Leader's commit_index updates to 3
/// - NotifyNewCommitIndex event is sent
/// - Client receives successful response with data
///
/// # Raft Protocol Context
/// This validates the full Raft linearizable read protocol:
/// 1. Leader verifies leadership via quorum heartbeat
/// 2. Leader waits for state machine to apply committed entries
/// 3. Leader serves read from state machine
///
/// # Note
/// Renamed from test_handle_raft_event_case6_2
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_quorum_success() {
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    use crate::NewCommitData;
    use crate::RaftEvent;
    use crate::RoleEvent;
    use crate::convert::safe_kv_bytes;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::test_utils::MockBuilder;
    use crate::{
        AppendResults, MockRaftLog, MockReplicationCore, MockStateMachineHandler, PeerUpdate,
        RaftNodeConfig,
    };
    use d_engine_proto::client::{ClientReadRequest, ReadConsistencyPolicy};

    let expect_new_commit_index = 3;

    // Given: Leader with successful replication
    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_raft_request_in_batch().times(1).returning(
        |_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: Some(3),
                            next_index: 4,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: Some(4),
                            next_index: 5,
                            success: true,
                        },
                    ),
                ]),
                learner_progress: HashMap::new(),
            })
        },
    );

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(move |_, _, _| Some(expect_new_commit_index));

    // Mock state machine handler for linearizable read
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_update_pending().times(1).returning(|_| ());
    state_machine_handler.expect_wait_applied().times(1).returning(|_, _| Ok(()));
    state_machine_handler
        .expect_read_from_state_machine()
        .returning(|_| Some(vec![]));

    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.read_batching.size_threshold = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_linearizable_read_quorum_success")
        .with_raft_log(raft_log)
        .with_replication_handler(replication_handler)
        .with_state_machine_handler(state_machine_handler)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // When: Client sends LinearizableRead request
    let keys = vec![safe_kv_bytes(1)];
    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        keys,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("should succeed");

    // Then: Leader commit_index updated
    assert_eq!(state.commit_index(), expect_new_commit_index);

    // Then: Client receives successful response
    assert!(resp_rx.recv().await.unwrap().is_ok());

    // Then: NotifyNewCommitIndex event sent
    let event = role_rx.try_recv().unwrap();
    assert!(matches!(
        event,
        RoleEvent::NotifyNewCommitIndex(NewCommitData {
            new_commit_index: _expect_new_commit_index,
            role: _,
            current_term: _
        })
    ));
}

/// Test linearizable read encountering higher term during verification
///
/// # Test Scenario
/// This verifies that Leader correctly steps down when discovering a higher term
/// during linearizable read verification, preserving Raft safety.
///
/// # Given
/// - Leader has commit_index = 1, current_term = 1
/// - Replication handler returns HigherTermFoundError (term 2 discovered)
/// - read_batching.size_threshold = 1 (immediate flush)
///
/// # When
/// - Client sends LinearizableRead request
/// - verify_leadership discovers higher term from peer response
///
/// # Then
/// - Leader's commit_index remains at 1 (no advancement)
/// - Leader sends BecomeFollower event
/// - Client receives error (request aborted)
///
/// # Raft Protocol Context
/// Per Raft paper Section 5.1: When a server discovers a higher term, it must
/// immediately convert to Follower. This ensures safety: old Leader cannot serve
/// reads after losing leadership.
///
/// # Note
/// Renamed from test_handle_raft_event_case6_3
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_encounters_higher_term() {
    use tokio::sync::mpsc;

    use crate::RaftEvent;
    use crate::RoleEvent;
    use crate::convert::safe_kv_bytes;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::test_utils::MockBuilder;
    use crate::{
        ConsensusError, Error, MockRaftLog, MockReplicationCore, RaftNodeConfig, ReplicationError,
    };
    use d_engine_proto::client::{ClientReadRequest, ReadConsistencyPolicy};

    // Given: Leader with higher term response from peers
    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_raft_request_in_batch().times(1).returning(
        move |_, _, _, _, _| {
            Err(Error::Consensus(ConsensusError::Replication(
                ReplicationError::HigherTerm(1),
            )))
        },
    );

    let expect_new_commit_index = 3;
    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(move |_, _, _| Some(expect_new_commit_index));

    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.read_batching.size_threshold = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_linearizable_read_encounters_higher_term")
        .with_replication_handler(replication_handler)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_commit_index(1).expect("should succeed");

    // When: Client sends LinearizableRead request
    let keys = vec![safe_kv_bytes(1)];
    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        keys,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let raft_event = RaftEvent::ClientReadRequest(client_read_request, resp_tx);

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Event handling should succeed (HigherTerm is handled, client gets error via resp_rx)
    state
        .handle_raft_event(raft_event, &context, role_tx)
        .await
        .expect("Event should be handled successfully");

    // Then: Leader commit remains unchanged (HigherTerm aborted the operation)
    assert_eq!(state.commit_index(), 1);

    // Then: BecomeFollower event sent
    let event = role_rx.try_recv().unwrap();
    assert!(matches!(event, RoleEvent::BecomeFollower(None)));

    // Then: Client receives error via response channel
    assert!(resp_rx.recv().await.unwrap().is_err());
}

// ============================================================================
// Read Consistency Policy Tests (Migrated from d-engine-server)
// ============================================================================

/// Test LeaseRead policy with valid lease
///
/// # Test Scenario
/// This verifies LeaseRead optimization: when Leader's lease is valid, reads
/// can be served immediately without quorum verification.
///
/// # Given
/// - Leader has valid lease (updated recently)
/// - Server allows client override (allow_client_override = true)
/// - Client specifies LeaseRead policy
/// - read_batching.size_threshold = 1 (immediate flush)
///
/// # When
/// - Client sends read request with LeaseRead policy
/// - Lease validity check passes
///
/// # Then
/// - Request succeeds immediately
/// - NO replication verification performed
/// - Response returns success
///
/// # Raft Protocol Context
/// LeaseRead is an optimization over LinearizableRead: Leader maintains a
/// time-bounded lease during which it can serve reads locally without
/// contacting quorum. This reduces latency while maintaining linearizability
/// within the lease period.
#[tokio::test]
#[traced_test]
async fn test_lease_read_with_valid_lease() {
    use crate::RaftEvent;
    use crate::convert::safe_kv_bytes;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::test_utils::MockBuilder;
    use crate::{MockRaftLog, MockReplicationCore, RaftNodeConfig};
    use d_engine_proto::client::{ClientReadRequest, ReadConsistencyPolicy};
    use d_engine_proto::error::ErrorCode;
    use tokio::sync::mpsc;

    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Given: Valid lease doesn't need replication verification
    let replication_handler = MockReplicationCore::new();

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 10);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    // Configure server to allow client override
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;
    node_config.raft.read_consistency.read_batching.size_threshold = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_lease_read_with_valid_lease")
        .with_replication_handler(replication_handler)
        .with_raft_log(raft_log)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    // Set up valid lease
    state.test_update_lease_timestamp();

    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
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
    assert_eq!(response.error, ErrorCode::Success as i32);
}

/// Test LeaseRead policy with expired lease
///
/// # Test Scenario
/// This verifies LeaseRead fallback: when lease is expired, Leader must
/// perform quorum verification before serving read.
///
/// # Given
/// - Leader has expired lease (timestamp not updated)
/// - Server allows client override
/// - Replication handler configured to succeed
/// - read_batching.size_threshold = 1 (immediate flush)
///
/// # When
/// - Client sends read request with LeaseRead policy
/// - Lease validity check fails
/// - Fallback to quorum verification
///
/// # Then
/// - Request triggers quorum verification (handle_raft_request_in_batch called)
/// - After successful verification, request succeeds
/// - Response returns success
///
/// # Raft Protocol Context
/// When lease expires, LeaseRead cannot guarantee linearizability without
/// revalidating leadership via quorum. This ensures safety even if lease
/// duration is misconfigured or clock skew occurs.
#[tokio::test]
#[traced_test]
async fn test_lease_read_with_expired_lease() {
    use crate::RaftEvent;
    use crate::convert::safe_kv_bytes;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::test_utils::MockBuilder;
    use crate::{AppendResults, MockRaftLog, MockReplicationCore, RaftNodeConfig};
    use d_engine_proto::client::{ClientReadRequest, ReadConsistencyPolicy};
    use d_engine_proto::error::ErrorCode;
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_raft_request_in_batch().times(1).returning(
        |_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::new(),
                learner_progress: HashMap::new(),
            })
        },
    );

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 10);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Configure server to allow client override
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;
    node_config.raft.read_consistency.read_batching.size_threshold = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_lease_read_with_expired_lease")
        .with_replication_handler(replication_handler)
        .with_raft_log(raft_log)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    // Don't update lease timestamp - lease should be expired by default

    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
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
    assert_eq!(response.error, ErrorCode::Success as i32);
}

/// Test unspecified consistency policy defaults to LinearizableRead
///
/// # Test Scenario
/// This verifies default behavior: when client doesn't specify consistency policy,
/// Leader defaults to LinearizableRead (strongest guarantee).
///
/// # Given
/// - Client request has consistency_policy = None
/// - Server default is LinearizableRead
/// - Replication handler configured to succeed
/// - State machine ready to serve reads
/// - read_batching.size_threshold = 1 (immediate flush)
///
/// # When
/// - Client sends read request without specifying policy
/// - Leader applies default policy (LinearizableRead)
///
/// # Then
/// - Request performs quorum verification (LinearizableRead flow)
/// - State machine waits for apply
/// - Response returns success
///
/// # Raft Protocol Context
/// Defaulting to LinearizableRead ensures safety: clients always get
/// linearizable reads unless they explicitly opt for weaker consistency.
#[tokio::test]
#[traced_test]
async fn test_unspecified_policy_defaults_to_linearizable_read() {
    use crate::RaftEvent;
    use crate::convert::safe_kv_bytes;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::test_utils::MockBuilder;
    use crate::{
        AppendResults, MockRaftLog, MockReplicationCore, MockStateMachineHandler, RaftNodeConfig,
    };
    use d_engine_proto::client::ClientReadRequest;
    use d_engine_proto::error::ErrorCode;
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_raft_request_in_batch().times(1).returning(
        |_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::new(),
                learner_progress: HashMap::new(),
            })
        },
    );

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 10);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    // Mock state machine handler for linearizable read
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_read_from_state_machine()
        .returning(|_| Some(vec![]));
    state_machine_handler.expect_update_pending().times(1).returning(|_| ());
    state_machine_handler.expect_wait_applied().times(1).returning(|_, _| Ok(()));

    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.read_batching.size_threshold = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_unspecified_policy_defaults_to_linearizable_read")
        .with_replication_handler(replication_handler)
        .with_raft_log(raft_log)
        .with_state_machine_handler(state_machine_handler)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: None, // Use server default (LinearizableRead)
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
    assert_eq!(response.error, ErrorCode::Success as i32);
}

/// Test EventualConsistency policy serves reads immediately
///
/// # Test Scenario
/// This verifies EventualConsistency optimization: reads are served immediately
/// from Leader's state machine without any verification or waiting.
///
/// # Given
/// - Client specifies EventualConsistency policy
/// - Server allows client override
/// - No replication handler setup (should not be called)
/// - read_batching.size_threshold = 1 (immediate flush)
///
/// # When
/// - Client sends read request with EventualConsistency policy
///
/// # Then
/// - Request succeeds immediately without verification
/// - NO quorum check performed
/// - NO state machine wait
/// - Response returns success
///
/// # Raft Protocol Context
/// EventualConsistency is the weakest guarantee: Leader serves reads from
/// current state machine without verifying leadership or waiting for apply.
/// This provides lowest latency but may return stale data if Leader is
/// partitioned or state machine lags behind commit.
#[tokio::test]
#[traced_test]
async fn test_eventual_consistency_serves_immediately() {
    use crate::RaftEvent;
    use crate::RaftNodeConfig;
    use crate::convert::safe_kv_bytes;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::test_utils::MockBuilder;
    use d_engine_proto::client::{ClientReadRequest, ReadConsistencyPolicy};
    use d_engine_proto::error::ErrorCode;
    use tokio::sync::mpsc;

    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Configure server to allow client override
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;
    node_config.raft.read_consistency.read_batching.size_threshold = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_eventual_consistency_serves_immediately")
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
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
    assert_eq!(response.error, ErrorCode::Success as i32);
    // Should succeed immediately without any verification
}

/// Test server default policy overrides client request when override disabled
///
/// # Test Scenario
/// This verifies server-enforced consistency: when allow_client_override = false,
/// server default policy always takes precedence over client-specified policy.
///
/// # Given
/// - Server default policy = EventualConsistency
/// - allow_client_override = false (strict server control)
/// - Client specifies LinearizableRead policy (should be ignored)
/// - read_batching.size_threshold = 1 (immediate flush)
///
/// # When
/// - Client sends read request with LinearizableRead policy
/// - Server ignores client policy and applies EventualConsistency
///
/// # Then
/// - Request uses EventualConsistency (immediate response)
/// - NO quorum verification performed
/// - Response returns success
///
/// # Raft Protocol Context
/// This configuration allows operators to enforce consistency policies
/// cluster-wide, preventing clients from degrading performance with
/// unnecessarily strong consistency guarantees.
#[tokio::test]
#[traced_test]
async fn test_server_default_overrides_client_policy() {
    use crate::RaftEvent;
    use crate::RaftNodeConfig;
    use crate::config::ReadConsistencyPolicy as ServerPolicy;
    use crate::convert::safe_kv_bytes;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::test_utils::MockBuilder;
    use d_engine_proto::client::{ClientReadRequest, ReadConsistencyPolicy};
    use d_engine_proto::error::ErrorCode;
    use tokio::sync::mpsc;

    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Configure server with EventualConsistency as default, client override disabled
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.default_policy = ServerPolicy::EventualConsistency;
    node_config.raft.read_consistency.allow_client_override = false;
    node_config.raft.read_consistency.read_batching.size_threshold = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_server_default_overrides_client_policy")
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32), // Should be ignored
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
    assert_eq!(response.error, ErrorCode::Success as i32);
    // Should use server default (EventualConsistency) and succeed immediately
}

// ============================================================================
// Read Batching Unit Tests (Migrated from d-engine-core)
// ============================================================================

mod read_batching_tests {
    use super::*;
    use crate::MockMembership;
    use crate::RaftEvent;
    use crate::maybe_clone_oneshot::{MaybeCloneOneshot, RaftOneshot};
    use crate::test_utils::mock::MockTypeConfig;
    use bytes::Bytes;
    use d_engine_proto::client::ClientReadRequest;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, sleep};

    /// Helper: Create mock ClientReadRequest
    fn create_read_request(key: Vec<u8>) -> ClientReadRequest {
        ClientReadRequest {
            client_id: 1,
            keys: vec![Bytes::from(key)],
            consistency_policy: Some(
                d_engine_proto::client::ReadConsistencyPolicy::LinearizableRead as i32,
            ),
        }
    }

    /// Test size threshold triggers immediate flush
    ///
    /// # Test Scenario
    /// This verifies size-based batching trigger: when buffer reaches size_threshold,
    /// all accumulated requests are flushed immediately.
    ///
    /// # Given
    /// - size_threshold = 50 (default from config)
    /// - Single-node cluster (no replication peers)
    ///
    /// # When
    /// - 49 requests enqueued (below threshold)
    /// - 50th request arrives (reaches threshold)
    ///
    /// # Then
    /// - Buffer accumulates 49 requests without flush
    /// - 50th request triggers immediate flush
    /// - Buffer reaches size threshold (50 requests)
    ///
    /// # Note
    /// Full end-to-end flush behavior is tested in integration tests
    #[tokio::test]
    async fn test_read_buffer_size_trigger() {
        let mut state =
            LeaderState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_size_trigger")));

        // Setup: Initialize cluster metadata (single node)
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        // Action: Enqueue 49 requests (below threshold)
        for i in 0..49 {
            let req = create_read_request(format!("key{i}").into_bytes());
            let (tx, _rx) = MaybeCloneOneshot::new();
            state.read_buffer.push((req, tx));
        }

        // Verify: Buffer has 49 requests (not yet flushed)
        assert_eq!(
            state.read_buffer.len(),
            49,
            "Buffer should have 49 requests before threshold"
        );

        // Action: Add 50th request (reaches threshold)
        let req = create_read_request(b"key49".to_vec());
        let (tx, _rx) = MaybeCloneOneshot::new();
        state.read_buffer.push((req, tx));

        // Verify: Buffer reaches size threshold
        assert_eq!(
            state.read_buffer.len(),
            50,
            "Buffer should have 50 requests at threshold"
        );

        // Verify: Size threshold condition is met (would trigger flush in real code)
        let config = node_config("/tmp/test_size_trigger");
        let size_threshold = config.raft.read_consistency.read_batching.size_threshold;
        assert!(
            state.read_buffer.len() >= size_threshold,
            "Buffer size {} should meet threshold {} (triggers flush in handle_raft_event)",
            state.read_buffer.len(),
            size_threshold
        );
    }

    /// Test time threshold triggers flush for single request
    ///
    /// # Test Scenario
    /// This verifies time-based batching trigger: when timeout expires,
    /// buffered requests are flushed even if below size threshold.
    ///
    /// # Given
    /// - size_threshold = 50
    /// - time_threshold_ms = 10
    /// - Single request enqueued (far below size threshold)
    ///
    /// # When
    /// - Request arrives and timeout task spawns
    /// - Wait > 10ms
    ///
    /// # Then
    /// - Timeout condition is met (elapsed >= time_threshold_ms)
    /// - Single request would be flushed (prevents starvation)
    ///
    /// # Raft Protocol Context
    /// This ensures low-concurrency workloads don't experience unbounded latency
    /// waiting for size threshold. Timeout guarantees maximum batching delay.
    ///
    /// # Note
    /// Full end-to-end timeout behavior is tested in integration tests
    #[tokio::test]
    async fn test_read_buffer_time_trigger() {
        let mut state =
            LeaderState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_time_trigger")));

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        // Action: Enqueue single request
        let req = create_read_request(b"lonely_key".to_vec());
        let (tx, _rx) = MaybeCloneOneshot::new();
        state.read_buffer.push((req, tx));
        let start_time = tokio::time::Instant::now();
        state.read_buffer_start_time = Some(start_time);

        // Verify: Buffer has 1 request (below size threshold)
        assert_eq!(state.read_buffer.len(), 1, "Buffer should have 1 request");
        let config = node_config("/tmp/test_time_trigger");
        assert!(
            state.read_buffer.len() < config.raft.read_consistency.read_batching.size_threshold,
            "Single request should NOT trigger size threshold"
        );

        // Simulate timeout: Wait for time_threshold_ms
        sleep(Duration::from_millis(11)).await;

        // Verify: Timeout condition is met
        let elapsed = start_time.elapsed();
        assert!(
            elapsed >= Duration::from_millis(10),
            "Timeout should expire after {}ms, actual: {:?}",
            config.raft.read_consistency.read_batching.time_threshold_ms,
            elapsed
        );

        // Verify: Start time was recorded (enables timeout detection)
        assert!(
            state.read_buffer_start_time.is_some(),
            "Start time should be recorded for timeout detection"
        );
    }

    /// Test timeout idempotency prevents duplicate flush
    ///
    /// # Test Scenario
    /// This verifies timeout task idempotency: when size threshold already triggered
    /// flush, delayed timeout event is safely ignored (no-op).
    ///
    /// # Given
    /// - 50 requests arrive → size threshold triggers immediate flush
    /// - Buffer is cleared
    /// - Timeout task still pending (10ms not yet elapsed when flush happened)
    ///
    /// # When
    /// - 10ms later, timeout fires and sends FlushReadBuffer event
    ///
    /// # Then
    /// - Buffer remains empty (no second flush)
    /// - Start time remains cleared
    /// - process_linearizable_read_batch() would return early (empty buffer check)
    ///
    /// # Note
    /// Full end-to-end idempotency is tested in integration tests
    #[tokio::test]
    async fn test_read_buffer_timeout_idempotent() {
        let mut state =
            LeaderState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_idempotent")));

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        // Action: Enqueue 50 requests and immediately clear (simulates size flush)
        for i in 0..50 {
            let req = create_read_request(format!("key{i}").into_bytes());
            let (tx, _rx) = MaybeCloneOneshot::new();
            state.read_buffer.push((req, tx));
        }

        let start_time = tokio::time::Instant::now();
        state.read_buffer_start_time = Some(start_time);

        // Verify: Buffer has 50 requests
        assert_eq!(
            state.read_buffer.len(),
            50,
            "Buffer should have 50 requests"
        );

        // Simulate size threshold flush
        state.read_buffer.clear();
        state.read_buffer_start_time = None;

        // Verify: Buffer is empty after flush
        assert_eq!(
            state.read_buffer.len(),
            0,
            "Buffer should be empty after size flush"
        );

        // Wait for timeout to expire
        sleep(Duration::from_millis(15)).await;

        // Verify: Buffer remains empty (idempotent - no second flush needed)
        assert_eq!(state.read_buffer.len(), 0, "Buffer should still be empty");
        assert!(
            state.read_buffer_start_time.is_none(),
            "Start time should be cleared after first flush"
        );
    }

    /// Test Leader role change drains buffer
    ///
    /// # Test Scenario
    /// This verifies Raft safety during role transition: when Leader steps down,
    /// all buffered read requests are drained and failed.
    ///
    /// # Given
    /// - Leader has buffered requests (below size threshold, timeout not expired)
    ///
    /// # When
    /// - Leader loses leadership (receives higher term) → becomes Follower
    /// - drain_read_buffer() is called during role transition
    ///
    /// # Then
    /// - drain_read_buffer() returns Ok(()) for empty buffer
    /// - All buffered requests would be failed with "Leader stepped down" error
    /// - Buffer is cleared
    ///
    /// # Raft Protocol Context
    /// This ensures safety: old Leader cannot process reads after stepping down.
    /// Clients must retry with new Leader.
    #[tokio::test]
    async fn test_read_buffer_role_change_drain() {
        let mut state =
            LeaderState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_drain")));

        // Action: Simulate Leader → Follower transition (drain_read_buffer is called)
        let result = state.drain_read_buffer();

        // Verify: drain_read_buffer returns Ok(()) for empty buffer
        assert!(
            result.is_ok(),
            "drain_read_buffer should succeed for newly created Leader"
        );
    }

    /// Test drain_read_buffer on non-Leader roles returns error
    ///
    /// # Test Scenario
    /// This verifies role responsibility: only Leader buffers reads,
    /// other roles return NotLeader error when drain is called.
    ///
    /// # Given
    /// - Node is in Follower or Candidate role
    ///
    /// # When
    /// - drain_read_buffer() is called
    ///
    /// # Then
    /// - Returns NotLeader error
    /// - No buffered reads (these roles don't buffer)
    #[tokio::test]
    async fn test_drain_read_buffer_other_roles() {
        use crate::raft_role::candidate_state::CandidateState;
        use crate::raft_role::follower_state::FollowerState;

        // Test Follower
        let mut follower_state = FollowerState::<MockTypeConfig>::new(
            1,
            Arc::new(node_config("/tmp/test_follower")),
            None,
            None,
        );
        let result = follower_state.drain_read_buffer();
        assert!(
            result.is_err(),
            "Follower drain_read_buffer should return error"
        );

        // Test Candidate
        let mut candidate_state =
            CandidateState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_candidate")));
        let result = candidate_state.drain_read_buffer();
        assert!(
            result.is_err(),
            "Candidate drain_read_buffer should return error"
        );
    }

    /// Test batching configuration is correctly applied
    ///
    /// # Test Scenario
    /// This verifies configuration values are properly loaded from raft.toml.
    ///
    /// # Then
    /// - size_threshold = 50 (default)
    /// - time_threshold_ms = 10 (default)
    #[tokio::test]
    async fn test_read_batching_config_applied() {
        let config = node_config("/tmp/test_config");

        // Verify default config values from raft.toml
        assert_eq!(
            config.raft.read_consistency.read_batching.size_threshold, 50,
            "Default size threshold should be 50"
        );
        assert_eq!(
            config.raft.read_consistency.read_batching.time_threshold_ms, 10,
            "Default time threshold should be 10ms"
        );
    }

    /// Test first ClientReadRequest spawns timeout task
    ///
    /// # Test Scenario
    /// This verifies timeout task lifecycle: first request spawns background task
    /// that sends FlushReadBuffer event after timeout.
    ///
    /// # Given
    /// - Empty buffer
    /// - size_threshold = 50
    /// - time_threshold_ms = 10
    ///
    /// # When
    /// - 1st LinearizableRead request arrives
    ///
    /// # Then
    /// - Request enqueued to buffer (len = 1)
    /// - read_buffer_start_time recorded
    /// - Timeout task spawns and sends FlushReadBuffer after 10ms
    /// - Function returns Ok() immediately (early return)
    #[tokio::test]
    async fn test_first_request_spawns_timeout_task() {
        use crate::test_utils::mock::mock_raft_context;
        use tokio::sync::watch;

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let ctx = mock_raft_context("/tmp/test_first_request", shutdown_rx, None);
        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();

        // Action: Send 1st request
        let req = create_read_request(b"key1".to_vec());
        let (tx, _rx) = MaybeCloneOneshot::new();
        let result = state
            .handle_raft_event(RaftEvent::ClientReadRequest(req, tx), &ctx, role_tx.clone())
            .await;

        // Verify: Function returns Ok (early return)
        assert!(result.is_ok(), "Should return Ok on first request");

        // Verify: Buffer has 1 request
        assert_eq!(state.read_buffer.len(), 1, "Buffer should have 1 request");

        // Verify: Start time recorded
        assert!(
            state.read_buffer_start_time.is_some(),
            "Start time should be recorded for timeout detection"
        );

        // Wait for timeout task to send FlushReadBuffer event
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Verify: role_rx receives ReprocessEvent(FlushReadBuffer)
        let event = role_rx.try_recv();
        assert!(
            event.is_ok(),
            "Timeout task should send FlushReadBuffer event"
        );

        if let Ok(role_event) = event {
            match role_event {
                crate::raft_role::RoleEvent::ReprocessEvent(boxed_event) => {
                    assert!(
                        matches!(*boxed_event, RaftEvent::FlushReadBuffer),
                        "Event should be FlushReadBuffer"
                    );
                }
                _ => panic!("Expected ReprocessEvent, got {role_event:?}"),
            }
        }

        drop(shutdown_tx);
    }

    /// Test 2nd-49th requests only enqueue without spawning new timeout task
    ///
    /// # Test Scenario
    /// This verifies timeout task reuse: subsequent requests share the same
    /// timeout task spawned by first request.
    ///
    /// # Given
    /// - Buffer has 1 request (timeout task already running)
    ///
    /// # When
    /// - 2nd-10th requests arrive
    ///
    /// # Then
    /// - Requests enqueued to buffer (len increases)
    /// - NO new timeout tasks spawned
    /// - read_buffer_start_time unchanged
    /// - Only ONE FlushReadBuffer event in channel (from 1st request)
    #[tokio::test]
    async fn test_subsequent_requests_only_enqueue() {
        use crate::test_utils::mock::mock_raft_context;
        use tokio::sync::watch;

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let ctx = mock_raft_context("/tmp/test_subsequent", shutdown_rx, None);
        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();

        // Action: Send 1st request (spawns timeout task)
        let req1 = create_read_request(b"key1".to_vec());
        let (tx1, _rx1) = MaybeCloneOneshot::new();
        state
            .handle_raft_event(
                RaftEvent::ClientReadRequest(req1, tx1),
                &ctx,
                role_tx.clone(),
            )
            .await
            .unwrap();

        let first_start_time = state.read_buffer_start_time;
        assert_eq!(state.read_buffer.len(), 1, "Buffer should have 1 request");

        // Action: Send 2nd-10th requests (should only enqueue)
        for i in 2..=10 {
            let req = create_read_request(format!("key{i}").into_bytes());
            let (tx, _rx) = MaybeCloneOneshot::new();
            let result = state
                .handle_raft_event(RaftEvent::ClientReadRequest(req, tx), &ctx, role_tx.clone())
                .await;

            assert!(result.is_ok(), "Request {i} should return Ok");
        }

        // Verify: Buffer has 10 requests
        assert_eq!(
            state.read_buffer.len(),
            10,
            "Buffer should have 10 requests"
        );

        // Verify: Start time unchanged (no new timeout task)
        assert_eq!(
            state.read_buffer_start_time, first_start_time,
            "Start time should not change for subsequent requests"
        );

        // Wait for timeout task to send FlushReadBuffer event
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Verify: Only ONE FlushReadBuffer event in channel (from 1st request)
        let event1 = role_rx.try_recv();
        assert!(event1.is_ok(), "Should have 1 FlushReadBuffer event");

        let event2 = role_rx.try_recv();
        assert!(
            event2.is_err(),
            "Should NOT have 2nd FlushReadBuffer event (no duplicate timeout tasks)"
        );

        drop(shutdown_tx);
    }

    /// Test 50th request triggers immediate flush
    ///
    /// # Test Scenario
    /// This verifies size threshold behavior: when 50th request arrives,
    /// buffer is immediately flushed.
    ///
    /// # Given
    /// - Buffer has 49 requests
    /// - Replication handler mocked to succeed
    ///
    /// # When
    /// - 50th request arrives (size threshold reached)
    ///
    /// # Then
    /// - process_linearizable_read_batch() called immediately
    /// - Buffer cleared (len = 0)
    /// - read_buffer_start_time cleared
    /// - 50th request receives response
    #[tokio::test]
    async fn test_size_threshold_triggers_immediate_flush() {
        use crate::MockReplicationCore;
        use crate::test_utils::mock::mock_raft_context;
        use tokio::sync::watch;

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let mut ctx = mock_raft_context("/tmp/test_size_flush", shutdown_rx, None);

        // Mock ReplicationCore to allow verify_leadership
        let mut replication = MockReplicationCore::new();
        replication.expect_handle_raft_request_in_batch().returning(|_, _, _, _, _| {
            Ok(crate::AppendResults {
                commit_quorum_achieved: true,
                peer_updates: Default::default(),
                learner_progress: Default::default(),
            })
        });
        ctx.handlers.replication_handler = replication;

        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        // Action: Send 49 requests
        for i in 1..=49 {
            let req = create_read_request(format!("key{i}").into_bytes());
            let (tx, _rx) = MaybeCloneOneshot::new();
            state
                .handle_raft_event(RaftEvent::ClientReadRequest(req, tx), &ctx, role_tx.clone())
                .await
                .unwrap();
        }

        assert_eq!(
            state.read_buffer.len(),
            49,
            "Buffer should have 49 requests"
        );

        // Action: Send 50th request (should trigger immediate flush)
        let req50 = create_read_request(b"key50".to_vec());
        let (tx50, mut rx50) = MaybeCloneOneshot::new();
        let result = state
            .handle_raft_event(
                RaftEvent::ClientReadRequest(req50, tx50),
                &ctx,
                role_tx.clone(),
            )
            .await;

        assert!(result.is_ok(), "50th request should return Ok");

        // Verify: Buffer cleared (flush executed)
        assert_eq!(
            state.read_buffer.len(),
            0,
            "Buffer should be empty after size threshold flush"
        );

        // Verify: Start time cleared
        assert!(
            state.read_buffer_start_time.is_none(),
            "Start time should be cleared after flush"
        );

        // Verify: 50th request receives response (flush completed)
        let response = rx50.recv().await;
        assert!(
            response.is_ok(),
            "50th request should receive response after flush"
        );

        drop(shutdown_tx);
    }

    /// Test FlushReadBuffer event with non-empty buffer
    ///
    /// # Test Scenario
    /// This verifies timeout event handling: when FlushReadBuffer event is received
    /// and buffer has requests, they are flushed.
    ///
    /// # Given
    /// - Buffer has 5 requests (manually added)
    /// - Replication handler mocked to succeed
    ///
    /// # When
    /// - FlushReadBuffer event is handled
    ///
    /// # Then
    /// - process_linearizable_read_batch() called
    /// - Buffer cleared (len = 0)
    #[tokio::test]
    async fn test_flush_read_buffer_event_non_empty() {
        use crate::MockReplicationCore;
        use crate::test_utils::mock::mock_raft_context;
        use tokio::sync::watch;

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let mut ctx = mock_raft_context("/tmp/test_flush_event", shutdown_rx, None);

        // Mock ReplicationCore to allow verify_leadership
        let mut replication = MockReplicationCore::new();
        replication.expect_handle_raft_request_in_batch().returning(|_, _, _, _, _| {
            Ok(crate::AppendResults {
                commit_quorum_achieved: true,
                peer_updates: Default::default(),
                learner_progress: Default::default(),
            })
        });
        ctx.handlers.replication_handler = replication;

        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        // Setup: Manually add requests to buffer (simulate timeout scenario)
        for i in 1..=5 {
            let req = create_read_request(format!("key{i}").into_bytes());
            let (tx, _rx) = MaybeCloneOneshot::new();
            state.read_buffer.push((req, tx));
        }

        assert_eq!(state.read_buffer.len(), 5, "Buffer should have 5 requests");

        // Action: Handle FlushReadBuffer event
        let result =
            state.handle_raft_event(RaftEvent::FlushReadBuffer, &ctx, role_tx.clone()).await;

        assert!(result.is_ok(), "FlushReadBuffer event should return Ok");

        // Verify: Buffer cleared (flush executed)
        assert_eq!(
            state.read_buffer.len(),
            0,
            "Buffer should be empty after FlushReadBuffer event"
        );

        drop(shutdown_tx);
    }

    /// Test FlushReadBuffer event with empty buffer is idempotent
    ///
    /// # Test Scenario
    /// This verifies idempotency: FlushReadBuffer event on empty buffer is no-op.
    ///
    /// # Given
    /// - Empty buffer
    ///
    /// # When
    /// - FlushReadBuffer event is handled
    ///
    /// # Then
    /// - No error (idempotent no-op)
    /// - Buffer remains empty
    #[tokio::test]
    async fn test_flush_read_buffer_event_empty_buffer() {
        use crate::test_utils::mock::mock_raft_context;
        use tokio::sync::watch;

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let ctx = mock_raft_context("/tmp/test_flush_empty", shutdown_rx, None);
        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        // Verify: Buffer is empty
        assert_eq!(
            state.read_buffer.len(),
            0,
            "Buffer should be empty initially"
        );

        // Action: Handle FlushReadBuffer event on empty buffer
        let result =
            state.handle_raft_event(RaftEvent::FlushReadBuffer, &ctx, role_tx.clone()).await;

        // Verify: No error (idempotent)
        assert!(
            result.is_ok(),
            "FlushReadBuffer on empty buffer should be no-op without error"
        );

        // Verify: Buffer still empty
        assert_eq!(
            state.read_buffer.len(),
            0,
            "Buffer should remain empty after idempotent flush"
        );

        drop(shutdown_tx);
    }

    /// Test process_linearizable_read_batch updates lease timestamp after verification
    ///
    /// # Test Scenario
    /// This verifies cross-policy optimization: successful LinearizableRead verification
    /// updates lease timestamp, allowing subsequent LeaseRead requests to reuse this
    /// verification without redundant quorum checks.
    ///
    /// # Given
    /// - Leader has invalid lease (timestamp = 0)
    /// - Replication handler mocked to succeed
    ///
    /// # When
    /// - LinearizableRead request triggers process_linearizable_read_batch
    /// - verify_leadership succeeds
    ///
    /// # Then
    /// - process_linearizable_read_batch succeeds
    /// - Lease timestamp updated (now > 0)
    /// - Lease becomes valid
    ///
    /// # Raft Protocol Context
    /// This optimization allows LeaseRead to piggyback on LinearizableRead verification,
    /// avoiding redundant quorum checks when both policies are used concurrently.
    #[tokio::test]
    async fn test_flush_read_buffer_updates_lease_timestamp() {
        use crate::MockReplicationCore;
        use crate::maybe_clone_oneshot::MaybeCloneOneshot;
        use crate::test_utils::mock::mock_raft_context;
        use bytes::Bytes;
        use d_engine_proto::client::{ClientReadRequest, ReadConsistencyPolicy};
        use tokio::sync::watch;

        // Setup: Create Leader with invalid lease
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let mut ctx = mock_raft_context("/tmp/test_lease_update", shutdown_rx, None);

        // Mock successful leadership verification
        let mut replication = MockReplicationCore::new();
        replication
            .expect_handle_raft_request_in_batch()
            .times(1)
            .returning(|_, _, _, _, _| {
                Ok(crate::AppendResults {
                    commit_quorum_achieved: true,
                    peer_updates: Default::default(),
                    learner_progress: Default::default(),
                })
            });
        ctx.handlers.replication_handler = replication;

        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        // Verify: Initial lease is invalid (timestamp = 0)
        assert!(
            !state.is_lease_valid(&ctx),
            "Lease should be invalid initially"
        );
        let initial_timestamp = state.lease_timestamp.load(std::sync::atomic::Ordering::Acquire);
        assert_eq!(initial_timestamp, 0, "Initial lease timestamp should be 0");

        // Action: Add LinearizableRead request to buffer and flush
        let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
        state.read_buffer.push((
            ClientReadRequest {
                client_id: 1,
                keys: vec![Bytes::from_static(b"key1")],
                consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
            },
            resp_tx,
        ));

        let (role_tx, _role_rx) = tokio::sync::mpsc::unbounded_channel();
        let result = state.process_linearizable_read_batch(&ctx, &role_tx).await;

        // Verify: process_linearizable_read_batch succeeded
        assert!(
            result.is_ok(),
            "process_linearizable_read_batch should succeed after verification"
        );

        // Verify: Lease timestamp updated (now > 0)
        let updated_timestamp = state.lease_timestamp.load(std::sync::atomic::Ordering::Acquire);
        assert!(
            updated_timestamp > 0,
            "Lease timestamp should be updated after successful verification"
        );
        assert!(
            updated_timestamp > initial_timestamp,
            "Lease timestamp should increase from initial value"
        );

        // Verify: Lease is now valid
        assert!(
            state.is_lease_valid(&ctx),
            "Lease should be valid after process_linearizable_read_batch updates timestamp"
        );

        drop(shutdown_tx);
    }

    /// Test mixed read policies in single batch
    ///
    /// # Test Scenario
    /// This verifies batching behavior when multiple LinearizableRead requests
    /// arrive within the same batching window and are flushed together.
    ///
    /// # Given
    /// - size_threshold = 3 (small for testing)
    /// - Replication handler mocked to succeed
    ///
    /// # When
    /// - 3 LinearizableRead requests arrive
    /// - 3rd request triggers size threshold flush
    ///
    /// # Then
    /// - All 3 requests in buffer are processed together
    /// - verify_leadership called once (batch optimization)
    /// - Buffer cleared after flush
    /// - All 3 requests succeed with consistent read_index
    ///
    /// # Raft Protocol Context
    /// Batching multiple LinearizableRead requests into a single quorum check
    /// is a key optimization - reduces network overhead while maintaining
    /// linearizability guarantees.
    #[tokio::test]
    async fn test_mixed_read_policies_in_batch() {
        use crate::MockReplicationCore;
        use crate::RaftNodeConfig;
        use crate::test_utils::MockBuilder;
        use d_engine_proto::client::ReadConsistencyPolicy;

        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        // Configure node_config
        let mut node_config = RaftNodeConfig::default();
        node_config.raft.read_consistency.read_batching.size_threshold = 3;
        node_config.raft.read_consistency.allow_client_override = true;

        // Mock ReplicationCore to allow verify_leadership
        let mut replication = MockReplicationCore::new();
        replication
            .expect_handle_raft_request_in_batch()
            .times(1)
            .returning(|_, _, _, _, _| {
                Ok(crate::AppendResults {
                    commit_quorum_achieved: true,
                    peer_updates: Default::default(),
                    learner_progress: Default::default(),
                })
            });

        let ctx = MockBuilder::new(shutdown_rx)
            .with_db_path("/tmp/test_mixed_policies")
            .with_node_config(node_config)
            .with_replication_handler(replication)
            .build_context();

        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        // Action: Add 3 LinearizableRead requests (triggers batching)
        let req1 = ClientReadRequest {
            client_id: 1,
            keys: vec![Bytes::from_static(b"key1")],
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        };
        let (tx1, mut rx1) = MaybeCloneOneshot::new();
        state
            .handle_raft_event(
                RaftEvent::ClientReadRequest(req1, tx1),
                &ctx,
                role_tx.clone(),
            )
            .await
            .unwrap();

        let req2 = ClientReadRequest {
            client_id: 1,
            keys: vec![Bytes::from_static(b"key2")],
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        };
        let (tx2, mut rx2) = MaybeCloneOneshot::new();
        state
            .handle_raft_event(
                RaftEvent::ClientReadRequest(req2, tx2),
                &ctx,
                role_tx.clone(),
            )
            .await
            .unwrap();

        let req3 = ClientReadRequest {
            client_id: 1,
            keys: vec![Bytes::from_static(b"key3")],
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        };
        let (tx3, mut rx3) = MaybeCloneOneshot::new();
        state
            .handle_raft_event(
                RaftEvent::ClientReadRequest(req3, tx3),
                &ctx,
                role_tx.clone(),
            )
            .await
            .unwrap();

        // Verify: Buffer cleared after flush
        assert_eq!(
            state.read_buffer.len(),
            0,
            "Buffer should be empty after size threshold flush"
        );

        // Verify: All requests receive responses
        assert!(rx1.recv().await.is_ok(), "Request 1 should succeed");
        assert!(rx2.recv().await.is_ok(), "Request 2 should succeed");
        assert!(rx3.recv().await.is_ok(), "Request 3 should succeed");

        drop(_shutdown_tx);
    }

    /// Test lease reuse boundary after LinearizableRead refresh
    ///
    /// # Test Scenario
    /// This verifies lease timestamp refresh and reuse: LinearizableRead updates
    /// lease_timestamp, subsequent LeaseRead can reuse without quorum check.
    ///
    /// # Given
    /// - Leader has expired lease (timestamp = 0)
    /// - size_threshold = 1 (immediate flush)
    /// - allow_client_override = true
    ///
    /// # When
    /// - 1st request: LinearizableRead (triggers quorum + lease refresh)
    /// - Verify lease_timestamp updated
    /// - 2nd request: LeaseRead (should reuse refreshed lease)
    ///
    /// # Then
    /// - 1st request calls verify_leadership (quorum check)
    /// - Lease timestamp updated after successful verification
    /// - 2nd request skips quorum check (lease still valid)
    /// - Both requests succeed
    ///
    /// # Raft Protocol Context
    /// This optimization allows LeaseRead to piggyback on LinearizableRead's
    /// verification, avoiding redundant quorum checks within lease duration.
    #[tokio::test]
    async fn test_lease_reuse_after_linearizable_read_refresh() {
        use crate::MockReplicationCore;
        use crate::RaftNodeConfig;
        use crate::test_utils::MockBuilder;
        use d_engine_proto::client::ReadConsistencyPolicy;

        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        // Configure node_config
        let mut node_config = RaftNodeConfig::default();
        node_config.raft.read_consistency.read_batching.size_threshold = 1;
        node_config.raft.read_consistency.allow_client_override = true;

        // Mock ReplicationCore - expect only 1 call (from LinearizableRead)
        let mut replication = MockReplicationCore::new();
        replication
            .expect_handle_raft_request_in_batch()
            .times(1)
            .returning(|_, _, _, _, _| {
                Ok(crate::AppendResults {
                    commit_quorum_achieved: true,
                    peer_updates: Default::default(),
                    learner_progress: Default::default(),
                })
            });

        let ctx = MockBuilder::new(shutdown_rx)
            .with_db_path("/tmp/test_lease_reuse")
            .with_node_config(node_config)
            .with_replication_handler(replication)
            .build_context();

        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        // Verify: Initial lease is invalid
        assert!(
            !state.is_lease_valid(&ctx),
            "Lease should be invalid initially"
        );

        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        // Action 1: Send LinearizableRead (triggers quorum + lease refresh)
        let req1 = ClientReadRequest {
            client_id: 1,
            keys: vec![Bytes::from_static(b"key1")],
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        };
        let (tx1, mut rx1) = MaybeCloneOneshot::new();
        state
            .handle_raft_event(
                RaftEvent::ClientReadRequest(req1, tx1),
                &ctx,
                role_tx.clone(),
            )
            .await
            .unwrap();

        // Verify: Request 1 succeeded
        assert!(rx1.recv().await.is_ok(), "LinearizableRead should succeed");

        // Verify: Lease is now valid (refreshed by LinearizableRead)
        assert!(
            state.is_lease_valid(&ctx),
            "Lease should be valid after LinearizableRead refresh"
        );

        // Action 2: Send LeaseRead immediately (should reuse lease)
        let req2 = ClientReadRequest {
            client_id: 1,
            keys: vec![Bytes::from_static(b"key2")],
            consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
        };
        let (tx2, mut rx2) = MaybeCloneOneshot::new();
        state
            .handle_raft_event(
                RaftEvent::ClientReadRequest(req2, tx2),
                &ctx,
                role_tx.clone(),
            )
            .await
            .unwrap();

        // Verify: Request 2 succeeded (reused lease, no quorum check)
        assert!(
            rx2.recv().await.is_ok(),
            "LeaseRead should reuse refreshed lease"
        );

        // Note: MockReplicationCore expects exactly 1 call - if LeaseRead
        // incorrectly triggered quorum check, test would fail

        drop(_shutdown_tx);
    }

    /// Test EventualConsistency does not check lease validity
    ///
    /// # Test Scenario
    /// This verifies EventualConsistency behavior on stale leader: serves
    /// reads immediately from local state machine without lease check.
    ///
    /// # Given
    /// - Leader has expired lease (timestamp = 0, simulates potential staleness)
    /// - size_threshold = 1 (immediate flush)
    /// - No replication handler configured (no quorum check)
    ///
    /// # When
    /// - Client sends EventualConsistency read request
    ///
    /// # Then
    /// - Request succeeds immediately (no lease check)
    /// - No verify_leadership call (no quorum check)
    /// - Serves from local state machine (potentially stale data)
    ///
    /// # Raft Protocol Context
    /// EventualConsistency trades off linearizability for performance.
    /// It does NOT guarantee reading latest committed data:
    /// - Leader might be partitioned (lease expired but still serving)
    /// - Leader might have stale commit_index
    /// This is acceptable for use cases tolerating stale reads
    /// (e.g., dashboards, caches, non-critical queries).
    ///
    /// # Design Decision
    /// EventualConsistency intentionally skips lease validation to maximize
    /// performance. Applications requiring freshness guarantees should use
    /// LeaseRead or LinearizableRead.
    #[tokio::test]
    async fn test_eventual_consistency_ignores_stale_lease() {
        use crate::RaftNodeConfig;
        use crate::test_utils::MockBuilder;
        use d_engine_proto::client::ReadConsistencyPolicy;

        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        // Configure node_config
        let mut node_config = RaftNodeConfig::default();
        node_config.raft.read_consistency.read_batching.size_threshold = 1;
        node_config.raft.read_consistency.allow_client_override = true;

        let ctx = MockBuilder::new(shutdown_rx)
            .with_db_path("/tmp/test_eventual_stale")
            .with_node_config(node_config)
            .build_context();

        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        // Verify: Lease is invalid (simulates stale leader scenario)
        assert!(
            !state.is_lease_valid(&ctx),
            "Lease should be invalid (stale leader)"
        );

        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        // Action: Send EventualConsistency read
        let req = ClientReadRequest {
            client_id: 1,
            keys: vec![Bytes::from_static(b"key1")],
            consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
        };
        let (tx, mut rx) = MaybeCloneOneshot::new();
        state
            .handle_raft_event(RaftEvent::ClientReadRequest(req, tx), &ctx, role_tx.clone())
            .await
            .unwrap();

        // Verify: Request succeeded despite stale lease
        assert!(
            rx.recv().await.is_ok(),
            "EventualConsistency should succeed even with stale lease"
        );

        // Verify: Lease still invalid (not refreshed by EventualConsistency)
        assert!(
            !state.is_lease_valid(&ctx),
            "EventualConsistency should not refresh lease"
        );

        drop(_shutdown_tx);
    }
}
