use std::sync::Arc;

use tokio::sync::watch;
use tracing_test::traced_test;

use crate::MockRaftLog;
use crate::MockStateMachineHandler;
use crate::candidate_state::CandidateState;
use crate::follower_state::FollowerState;
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
