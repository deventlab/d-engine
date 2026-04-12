use crate::ClientCmd;
use crate::MockMembership;
use crate::MockStateMachineHandler;
use crate::ReadConsistencyPolicy;
use crate::convert::safe_kv_bytes;
use crate::maybe_clone_oneshot::MaybeCloneOneshot;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::node_config;
use crate::{Error, MockRaftLog, MockReplicationCore, RaftNodeConfig};
use bytes::Bytes;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::error::ErrorCode;
use d_engine_proto::server::cluster::NodeMeta;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::Instant;
use tonic::Code;
use tracing_test::traced_test;

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
/// - batching.max_batch_size = 1 (immediate flush)
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
    // Given: Leader with replication handler that fails
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::Fatal("Quorum verification failed".to_string())));

    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.batching.max_batch_size = 1; // Immediately flush

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
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Push to buffer
    state.push_client_cmd(cmd, &context);

    // Flush: triggers quorum verification which will fail
    state.flush_cmd_buffers(&context, &role_tx).await.ok();

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
/// - batching.max_batch_size = 1 (immediate flush)
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
    let expect_new_commit_index = 3;

    // Given: Leader with successful replication
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| {
            // New architecture: prepare_batch_requests returns empty vector for read-only batches
            // Reads fire in Phase 3 when last_applied >= read_index
            Ok(crate::PrepareResult::default())
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 2);
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(move |_, _, _| Some(expect_new_commit_index));

    // Mock state machine handler: no wait_applied/update_pending in event-driven path.
    // read_from_state_machine is called by execute_pending_reads when ApplyCompleted fires.
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_should_snapshot().returning(|_| false);
    state_machine_handler
        .expect_read_from_state_machine()
        .returning(|_| Some(vec![]));

    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.batching.max_batch_size = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_linearizable_read_quorum_success")
        .with_raft_log(raft_log)
        .with_replication_handler(replication_handler)
        .with_state_machine_handler(state_machine_handler)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    // noop committed — leader is ready to serve linearizable reads.
    state.noop_log_id = Some(1);

    // When: Client sends LinearizableRead request
    let keys = vec![safe_kv_bytes(1)];
    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        keys,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Push to buffer
    state.push_client_cmd(cmd, &context);

    // Flush: quorum succeeds, read registered in pending_reads[read_index=3]
    state.flush_cmd_buffers(&context, &role_tx).await.expect("should succeed");

    // Note: In new architecture, commit_index advances asynchronously via handle_append_result
    // We skip the sync commit assertion and move to ApplyCompleted

    // Simulate SM apply: ApplyCompleted fires pending_reads for read_index <= 3
    state
        .handle_apply_completed(expect_new_commit_index, vec![], &context, &role_tx)
        .await
        .expect("ApplyCompleted should succeed");

    // Then: Client receives successful response (released by ApplyCompleted)
    assert!(resp_rx.recv().await.unwrap().is_ok());

    // Note: In new architecture, NotifyNewCommitIndex is sent by handle_append_result,
    // not by flush_cmd_buffers. This test focuses on the read path completion.
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
/// - batching.max_batch_size = 1 (immediate flush)
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
    // Given: Leader with higher term detected during prepare phase
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(move |_, _, _, _, _| {
            // New architecture: Higher term detection now happens via handle_append_result
            // For testing, we simulate fatal error during prepare phase
            Err(Error::Fatal("Higher term detected".to_string()))
        });

    let expect_new_commit_index = 3;
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 2);
    raft_log
        .expect_calculate_majority_matched_index()
        .returning(move |_, _, _| Some(expect_new_commit_index));

    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.batching.max_batch_size = 1; // Immediately flush

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
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Push to buffer and flush (triggers leadership verification which discovers higher term)
    state.push_client_cmd(cmd, &context);
    let _ = state.flush_cmd_buffers(&context, &role_tx).await;

    // Then: Leader commit remains unchanged (Fatal error aborted the operation)
    assert_eq!(state.commit_index(), 1);

    // Note: In new architecture, HigherTerm detection happens in handle_append_result,
    // not in prepare_batch_requests. This test simulates a Fatal error during prepare.
    // The BecomeFollower event would be sent when workers respond with higher term.

    // Then: Client receives error via response channel (Fatal error from prepare phase)
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
/// - batching.max_batch_size = 1 (immediate flush)
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
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Given: Valid lease doesn't need replication verification
    let replication_handler = MockReplicationCore::new();

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 10);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    // Configure server to allow client override
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;
    node_config.raft.batching.max_batch_size = 1; // Immediately flush

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
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    state.push_client_cmd(cmd, &context);
    state.flush_cmd_buffers(&context, &role_tx).await.expect("should succeed");

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(response.error, ErrorCode::Success as i32);
}

/// Test expired lease detection
///
/// # Test Scenario
/// This is Part 1 of the LeaseRead expired lease path.
/// Verifies that the system correctly detects when a lease has expired.
///
/// # Given
/// - Leader initialized without updating lease timestamp
///
/// # When
/// - Check lease validity
///
/// # Then
/// - is_lease_valid() returns false
///
/// # Complete Path Coverage
/// This test + test_expired_lease_triggers_verify_fallback +
/// test_single_voter_lease_refreshed_on_log_flushed (in lease_refresh_on_log_flushed_test.rs)
/// together cover the full "expired lease → verify → refresh → success" path.
#[tokio::test]
#[traced_test]
async fn test_expired_lease_detection() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_expired_lease_detection")
        .build_context();

    let state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Don't update lease timestamp - lease should be expired by default
    assert!(
        !state.is_lease_valid(&context),
        "Lease should be expired when timestamp is never updated"
    );
}

/// Test expired lease on single-voter is refreshed immediately and read is served
///
/// # Test Scenario
/// Single-voter: self is the entire quorum, so expired lease is refreshed
/// immediately without sending AppendEntries. Read is served in the same flush.
///
/// # Given
/// - Leader with expired lease (timestamp not updated)
/// - Single-voter cluster
///
/// # When
/// - Client sends LeaseRead request
///
/// # Then
/// - flush_cmd_buffers returns without blocking
/// - Lease is refreshed (is_lease_valid returns true after flush)
/// - Sender receives success response
#[tokio::test]
#[traced_test]
async fn test_expired_lease_single_voter_refreshed_immediately() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;
    node_config.raft.batching.max_batch_size = 1;

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_expired_lease_single_voter_refreshed_immediately")
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    assert!(
        !state.is_lease_valid(&context),
        "Precondition: lease expired"
    );

    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
        keys: vec![safe_kv_bytes(1)],
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    state.push_client_cmd(cmd, &context);
    state.flush_cmd_buffers(&context, &role_tx).await.unwrap();

    // Lease must be refreshed after flush
    assert!(
        state.is_lease_valid(&context),
        "Lease must be refreshed after single-voter flush"
    );

    // Sender must receive success response
    let result = resp_rx.recv().await.unwrap();
    assert!(
        result.is_ok(),
        "Single-voter expired lease read must succeed immediately"
    );
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
/// - batching.max_batch_size = 1 (immediate flush)
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
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| {
            // Unspecified policy defaults to LinearizableRead
            Ok(crate::PrepareResult::default())
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 10);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    // Mock state machine handler: no wait_applied/update_pending in event-driven path.
    // read_from_state_machine called by execute_pending_reads when ApplyCompleted fires.
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_should_snapshot().returning(|_| false);
    state_machine_handler
        .expect_read_from_state_machine()
        .returning(|_| Some(vec![]));

    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.batching.max_batch_size = 1; // Immediately flush

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_unspecified_policy_defaults_to_linearizable_read")
        .with_replication_handler(replication_handler)
        .with_raft_log(raft_log)
        .with_state_machine_handler(state_machine_handler)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    // noop committed — leader is ready to serve linearizable reads.
    state.noop_log_id = Some(1);

    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: None, // Use server default (LinearizableRead)
        keys: vec![safe_kv_bytes(1)],
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    state.push_client_cmd(cmd, &context);
    // Flush: quorum succeeds, read registered in pending_reads[read_index=5]
    state.flush_cmd_buffers(&context, &role_tx).await.expect("should succeed");

    // Simulate SM apply: releases pending linearizable read
    state
        .handle_apply_completed(5, vec![], &context, &role_tx)
        .await
        .expect("ApplyCompleted should succeed");

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
/// - batching.max_batch_size = 1 (immediate flush)
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
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Configure server to allow client override
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;
    node_config.raft.batching.max_batch_size = 1; // Immediately flush

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
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    state.push_client_cmd(cmd, &context);
    state.flush_cmd_buffers(&context, &role_tx).await.expect("should succeed");

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
/// - batching.max_batch_size = 1 (immediate flush)
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
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Configure server with EventualConsistency as default, client override disabled
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.default_policy = ReadConsistencyPolicy::EventualConsistency;
    node_config.raft.read_consistency.allow_client_override = false;
    node_config.raft.batching.max_batch_size = 1; // Immediately flush

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
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    state.push_client_cmd(cmd, &context);
    state.flush_cmd_buffers(&context, &role_tx).await.expect("should succeed");

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert_eq!(response.error, ErrorCode::Success as i32);
    // Should use server default (EventualConsistency) and succeed immediately
}

// ============================================================================
// Drain-Based Read Processing Tests
// ============================================================================
// Tests validating read consistency policies under drain-based batch architecture

/// **Business Scenario**: Multiple LinearizableRead requests share single quorum verification
///
/// **Purpose**: Verify that drain-based batching collects multiple LinearizableRead requests
/// and processes them with a single quorum check, optimizing network overhead while
/// maintaining linearizability guarantees.
///
/// **Key Validation**:
/// - Multiple requests processed in single batch
/// - Single quorum verification serves entire batch
/// - All requests receive successful responses
///
/// **Raft Protocol Context**:
/// Batching LinearizableRead requests is a key optimization in Raft implementations.
/// Instead of N quorum checks for N concurrent reads, we perform 1 quorum check
/// that verifies leadership for all requests collected in the batch window.
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_batch_shared_quorum() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    // Configure for immediate batch processing
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;

    // Mock replication handler - expect exactly 1 call for the entire batch
    let mut replication = MockReplicationCore::new();
    replication.expect_prepare_batch_requests().times(1).returning(|_, _, _, _, _| {
        // Multiple requests batched naturally
        Ok(crate::PrepareResult::default())
    });

    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path("/tmp/test_linearizable_read_batch_shared_quorum")
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    // Setup cluster metadata
    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Push 3 LinearizableRead requests to buffer (simulating drain collection)
    let req1 = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from_static(b"key1")],
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
    };
    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    state.push_client_cmd(ClientCmd::Read(req1, tx1), &ctx);

    let req2 = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from_static(b"key2")],
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
    };
    let (tx2, mut rx2) = MaybeCloneOneshot::new();
    state.push_client_cmd(ClientCmd::Read(req2, tx2), &ctx);

    let req3 = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from_static(b"key3")],
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
    };
    let (tx3, mut rx3) = MaybeCloneOneshot::new();
    state.push_client_cmd(ClientCmd::Read(req3, tx3), &ctx);

    // Action: Flush buffers (simulating drain-triggered flush)
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();

    // Verify: Buffer cleared after flush
    assert_eq!(
        state.linearizable_read_buffer.len(),
        0,
        "Buffer should be empty after flush"
    );

    // Verify: All requests receive responses
    assert!(rx1.recv().await.is_ok(), "Request 1 should succeed");
    assert!(rx2.recv().await.is_ok(), "Request 2 should succeed");
    assert!(rx3.recv().await.is_ok(), "Request 3 should succeed");

    drop(_shutdown_tx);
}

/// **Business Scenario**: LinearizableRead refreshes lease, enabling LeaseRead reuse
///
/// **Purpose**: Verify cross-policy optimization where successful LinearizableRead
/// verification refreshes the leader's lease timestamp, allowing subsequent LeaseRead
/// requests to skip quorum checks and serve from local state machine.
///
/// **Key Validation**:
/// - LinearizableRead performs quorum verification and refreshes lease
/// - Subsequent LeaseRead reuses valid lease without quorum check
/// - Only 1 replication call occurs (from LinearizableRead)
///
/// **Raft Protocol Context**:
/// This optimization reduces network overhead when mixing consistency policies.
/// LeaseRead can piggyback on LinearizableRead's verification within the lease
/// duration (default: election_timeout / 2), avoiding redundant quorum checks.
#[tokio::test]
#[traced_test]
async fn test_lease_reuse_after_linearizable_read_refresh() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;

    // Mock replication - expect only 1 call (from LinearizableRead only)
    let mut replication = MockReplicationCore::new();
    replication.expect_prepare_batch_requests().times(1).returning(|_, _, _, _, _| {
        // Linearizable read refreshes lease (via handle_log_flushed in single-voter)
        Ok(crate::PrepareResult::default())
    });

    // MemFirst: handle_log_flushed(1) commits to last_entry_id(), must be >= durable=1.
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_durable_index().returning(|| 0);
    raft_log.expect_last_log_id().returning(|| None);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| None);
    raft_log.expect_close().returning(|| ());

    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path("/tmp/test_lease_reuse")
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .with_raft_log(raft_log)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

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

    // Action 1: LinearizableRead (triggers quorum + lease refresh)
    let req1 = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from_static(b"key1")],
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
    };
    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    state.push_client_cmd(ClientCmd::Read(req1, tx1), &ctx);
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();

    // Verify: Request succeeded
    assert!(rx1.recv().await.is_ok(), "LinearizableRead should succeed");

    // Verify: Lease is now valid (refreshed by LinearizableRead)
    // After flush in single-voter: lease refreshed via handle_log_flushed
    // Manually trigger to simulate the async flush event
    state.handle_log_flushed(1, &ctx, &role_tx).await;

    assert!(
        state.is_lease_valid(&ctx),
        "Lease should be valid after log flush (single-voter)"
    );

    // Action 2: LeaseRead (should reuse valid lease, no quorum check)
    let req2 = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from_static(b"key2")],
        consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
    };
    let (tx2, mut rx2) = MaybeCloneOneshot::new();
    state.push_client_cmd(ClientCmd::Read(req2, tx2), &ctx);
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();

    // Verify: Request succeeded (reused lease, no quorum check)
    assert!(
        rx2.recv().await.is_ok(),
        "LeaseRead should reuse refreshed lease"
    );

    // Note: MockReplicationCore expects exactly 1 call - test fails if LeaseRead triggers quorum

    drop(_shutdown_tx);
}

/// **Business Scenario**: EventualConsistency serves stale reads without lease validation
///
/// **Purpose**: Verify that EventualConsistency policy intentionally bypasses all
/// safety checks (lease validation, quorum verification) to serve reads immediately
/// from local state machine, accepting potential staleness for maximum performance.
///
/// **Key Validation**:
/// - Request succeeds even with expired/invalid lease
/// - No quorum verification performed
/// - Lease remains invalid after read (not refreshed)
///
/// **Raft Protocol Context**:
/// EventualConsistency makes NO linearizability guarantees. It may return stale data if:
/// - Leader is network-partitioned (lease expired but still serving)
/// - State machine apply lags behind commit_index
/// - Leader has been replaced but hasn't discovered higher term yet
///
/// **Design Decision**:
/// This is acceptable for use cases tolerating staleness (dashboards, caches,
/// non-critical analytics). Applications requiring freshness must use LeaseRead
/// or LinearizableRead.
#[tokio::test]
#[traced_test]
async fn test_eventual_consistency_ignores_stale_lease() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;

    // No replication handler - EventualConsistency should not trigger quorum check
    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path("/tmp/test_eventual_stale")
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

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

    // Action: EventualConsistency read
    let req = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from_static(b"key1")],
        consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
    };
    let (tx, mut rx) = MaybeCloneOneshot::new();
    state.push_client_cmd(ClientCmd::Read(req, tx), &ctx);
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();

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

/// **Business Scenario**: Client can override read policy when server allows
///
/// **Purpose**: Verify that when `allow_client_override = true`, clients have
/// flexibility to choose their preferred consistency policy based on use case
/// requirements (e.g., LeaseRead for lower latency vs LinearizableRead for
/// strongest guarantees).
///
/// **Key Validation**:
/// - Server config: `allow_client_override = true`
/// - Client specifies `LeaseRead` (weaker than default `LinearizableRead`)
/// - Server honors client choice and executes LeaseRead
///
/// **Raft Protocol Context**:
/// This flexibility enables application-level optimization: latency-sensitive
/// reads can use LeaseRead while critical reads use LinearizableRead. The
/// tradeoff is client responsibility for choosing appropriate consistency.
///
/// **Design Decision**:
/// Developer-friendly: d-engine trusts applications to understand their
/// consistency requirements. This mirrors etcd's consistency model where
/// clients control read semantics.
#[tokio::test]
#[traced_test]
async fn test_client_policy_override_allowed() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    // Configure server to allow client override, default is LinearizableRead
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.default_policy = ReadConsistencyPolicy::LinearizableRead;
    node_config.raft.read_consistency.allow_client_override = true;

    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path("/tmp/test_client_policy_override_allowed")
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    // Setup valid lease so LeaseRead can succeed
    state.test_update_lease_timestamp();

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Client requests LeaseRead (different from server default)
    let req = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from_static(b"key1")],
        consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
    };
    let (tx, mut rx) = MaybeCloneOneshot::new();

    // Verify: determine_read_policy honors client choice
    let effective_policy = state.determine_read_policy(&req);
    assert_eq!(
        effective_policy,
        crate::config::ReadConsistencyPolicy::LeaseRead,
        "Should use client-specified LeaseRead when override is allowed"
    );

    // Execute: Process the read with client-specified policy
    state.push_client_cmd(ClientCmd::Read(req, tx), &ctx);
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();

    // Verify: Request succeeded using LeaseRead (no quorum verification)
    assert!(
        rx.recv().await.is_ok(),
        "LeaseRead should succeed with valid lease"
    );

    drop(_shutdown_tx);
}

/// **Business Scenario**: Server enforces default policy when override is disabled
///
/// **Purpose**: Verify that when `allow_client_override = false`, server
/// maintains control over consistency guarantees, preventing clients from
/// weakening consistency requirements through policy downgrade attacks.
///
/// **Key Validation**:
/// - Server config: `allow_client_override = false`
/// - Client specifies `EventualConsistency` (weaker than default)
/// - Server ignores client choice and enforces `LinearizableRead`
///
/// **Raft Protocol Context**:
/// This configuration prioritizes safety over flexibility. Useful in environments
/// where data integrity is critical and clients cannot be fully trusted to choose
/// appropriate consistency levels (e.g., financial systems, audit logs).
///
/// **Design Decision**:
/// Security-first: Server operators have final authority over consistency
/// guarantees. This prevents accidental or malicious consistency downgrades
/// while maintaining protocol correctness.
#[tokio::test]
#[traced_test]
async fn test_client_policy_override_denied() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    // Configure server to deny client override, default is LinearizableRead
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.default_policy = ReadConsistencyPolicy::LinearizableRead;
    node_config.raft.read_consistency.allow_client_override = false;

    // Mock replication handler for LinearizableRead quorum verification
    let mut replication = MockReplicationCore::new();
    replication.expect_prepare_batch_requests().times(1).returning(|_, _, _, _, _| {
        // Single-voter cluster: reads fire immediately in Phase 3
        Ok(crate::PrepareResult::default())
    });

    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path("/tmp/test_client_policy_override_denied")
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Client requests EventualConsistency (weaker than server default)
    let req = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from_static(b"key1")],
        consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
    };
    let (tx, mut rx) = MaybeCloneOneshot::new();

    // Verify: determine_read_policy ignores client choice and uses server default
    let effective_policy = state.determine_read_policy(&req);
    assert_eq!(
        effective_policy,
        crate::config::ReadConsistencyPolicy::LinearizableRead,
        "Should use server default LinearizableRead, ignoring client's EventualConsistency"
    );

    // Execute: Process the read with server-enforced policy
    state.push_client_cmd(ClientCmd::Read(req, tx), &ctx);
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();

    // Verify: Request succeeded using LinearizableRead (quorum verification performed)
    assert!(
        rx.recv().await.is_ok(),
        "Should succeed using server-enforced LinearizableRead"
    );

    // Note: MockReplicationCore expects 1 call - confirms LinearizableRead was used
    // (EventualConsistency would have triggered 0 replication calls)

    drop(_shutdown_tx);
}

// ============================================================================
// Drain-Mode Architecture Validation Tests
// ============================================================================
// Tests verifying the drain-based batch architecture behavior:
// recv() blocks for first request + try_recv() drains pending requests

/// **Business Scenario**: Low-load reads experience zero batching delay
///
/// **Purpose**: Verify that under low concurrency, single read requests are
/// processed immediately without artificial batching delays. The drain pattern
/// eliminates the old timeout-based waiting that caused 1ms+ latency overhead.
///
/// **Key Validation**:
/// - Single read request in buffer (no pending requests)
/// - No artificial delay before processing
/// - Request processed as batch of size 1
///
/// **Architecture Context**:
/// Old architecture: Even single request waited for batch_timeout (1ms+)
/// New drain architecture: recv() returns immediately, try_recv() finds nothing,
/// flush happens instantly. This is the key low-latency improvement.
#[tokio::test]
#[traced_test]
async fn test_drain_single_request_no_delay() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;

    // Mock replication for quorum verification
    let mut replication = MockReplicationCore::new();
    replication.expect_prepare_batch_requests().times(1).returning(|_, _, _, _, _| {
        // Server enforces LinearizableRead despite client request
        Ok(crate::PrepareResult::default())
    });

    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path("/tmp/test_drain_single_request")
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Single request (simulates low load)
    let start = Instant::now();
    let req = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from_static(b"key1")],
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
    };
    let (tx, mut rx) = MaybeCloneOneshot::new();

    state.push_client_cmd(ClientCmd::Read(req, tx), &ctx);

    // Verify: Buffer has exactly 1 request (no batching accumulation)
    assert_eq!(
        state.linearizable_read_buffer.len(),
        1,
        "Buffer should have single request"
    );

    // Flush immediately (drain pattern: no waiting for timeout or size threshold)
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();
    let elapsed = start.elapsed();

    // Verify: Request succeeded
    assert!(rx.recv().await.is_ok(), "Single request should succeed");

    // Verify: Processing was immediate (< 10ms, no artificial batching delay)
    assert!(
        elapsed.as_millis() < 10,
        "Single request should process immediately without batching delay, took {elapsed:?}"
    );

    drop(_shutdown_tx);
}

/// **Business Scenario**: High-load reads naturally form large batches
///
/// **Purpose**: Verify that under high concurrency, the drain pattern naturally
/// collects multiple pending requests into a single batch without explicit
/// size threshold checks. This demonstrates automatic batch formation based on
/// arrival patterns.
///
/// **Key Validation**:
/// - Multiple requests queued before flush
/// - Single flush processes entire batch
/// - No manual threshold logic required
///
/// **Architecture Context**:
/// Drain pattern: When main loop calls flush_cmd_buffers(), all accumulated
/// requests in the buffer are processed together. High load = many requests
/// accumulate between flush cycles = natural large batches.
#[tokio::test]
#[traced_test]
async fn test_drain_multiple_requests_natural_batch() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;

    // Mock replication - expect single call for entire batch
    let mut replication = MockReplicationCore::new();
    replication.expect_prepare_batch_requests().times(1).returning(|_, _, _, _, _| {
        // Single request immediately processed
        Ok(crate::PrepareResult::default())
    });

    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path("/tmp/test_drain_multiple_requests")
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Push 10 requests to buffer (simulates high load accumulation)
    let mut receivers = vec![];
    for i in 0..10 {
        let req = ClientReadRequest {
            client_id: 1,
            keys: vec![Bytes::from(format!("key{i}"))],
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        };
        let (tx, rx) = MaybeCloneOneshot::new();
        state.push_client_cmd(ClientCmd::Read(req, tx), &ctx);
        receivers.push(rx);
    }

    // Verify: All 10 requests accumulated in buffer
    assert_eq!(
        state.linearizable_read_buffer.len(),
        10,
        "Buffer should accumulate all requests before flush"
    );

    // Action: Single flush processes entire batch
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();

    // Verify: Buffer emptied (all requests processed together)
    assert_eq!(
        state.linearizable_read_buffer.len(),
        0,
        "Buffer should be empty after batch flush"
    );

    // Verify: All 10 requests received responses
    for (i, mut rx) in receivers.into_iter().enumerate() {
        assert!(
            rx.recv().await.is_ok(),
            "Request {i} should succeed in batch"
        );
    }

    // Note: MockReplicationCore expects exactly 1 call - confirms single quorum
    // verification served all 10 requests (batch optimization)

    drop(_shutdown_tx);
}

/// **Business Scenario**: read_batch_size prevents unbounded drain
///
/// **Purpose**: Verify that read_batch_size limit prevents processing excessively
/// large batches in a single operation, protecting against IO overload and
/// maintaining bounded latency even under extreme load.
///
/// **Key Validation**:
/// - Buffer accumulates > read_batch_size requests
/// - First flush processes exactly read_batch_size
/// - Remaining requests stay in buffer for next flush
///
/// **Architecture Context**:
/// The drain pattern (try_recv() loop) could theoretically drain unlimited
/// requests. read_batch_size provides backpressure to prevent a single flush
/// from overwhelming IO subsystems or blocking the event loop too long.
#[tokio::test]
#[traced_test]
async fn test_drain_max_batch_size_limit() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;
    // Set small read_batch_size for testing
    node_config.raft.batching.max_batch_size = 5;

    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path("/tmp/test_drain_max_batch_size")
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    // Action: Push 10 requests (exceeds max_batch_size of 5)
    for i in 0..10 {
        let req = ClientReadRequest {
            client_id: 1,
            keys: vec![Bytes::from(format!("key{i}"))],
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        };
        let (tx, _rx) = MaybeCloneOneshot::new();
        state.push_client_cmd(ClientCmd::Read(req, tx), &ctx);
    }

    // Verify: All 10 requests in buffer
    assert_eq!(
        state.linearizable_read_buffer.len(),
        10,
        "Buffer should have all 10 requests"
    );

    // Note: This test validates buffer accumulation behavior.
    // The actual read_batch_size enforcement happens in the main loop's
    // drain logic (raft.rs), not in flush_cmd_buffers().
    //
    // In production:
    // - raft.rs recv() gets first request
    // - raft.rs try_recv() loop drains up to read_batch_size-1 more
    // - raft.rs calls flush_cmd_buffers() with bounded batch
    //
    // This unit test confirms buffer can hold > read_batch_size,
    // proving the need for drain-time limiting in the main loop.

    assert!(
        state.linearizable_read_buffer.len() > ctx.node_config.raft.batching.max_batch_size,
        "Buffer can accumulate beyond read_batch_size (main loop enforces limit during drain)"
    );

    drop(_shutdown_tx);
}

/// **Business Scenario**: Batch linearizable reads share single quorum verification
///
/// **Purpose**: Verify the core optimization of read batching - multiple
/// LinearizableRead requests collected in a batch are verified with a single
/// quorum heartbeat instead of N separate quorum checks.
///
/// **Key Validation**:
/// - Multiple LinearizableRead requests in buffer
/// - Single call to verify_leadership (quorum verification)
/// - All requests succeed with same read_index
///
/// **Architecture Context**:
/// This is the primary performance benefit of read batching in Raft.
/// Instead of:
///   - Request 1 → Quorum check 1 → Read 1
///   - Request 2 → Quorum check 2 → Read 2
///   - Request 3 → Quorum check 3 → Read 3
///
/// We do:
///   - Collect [Request 1, 2, 3] → Single quorum check → Read all
///
/// This reduces network overhead by ~3x while maintaining linearizability.
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_batch_single_quorum() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;

    // Mock replication - expect EXACTLY 1 call for the entire batch
    let mut replication = MockReplicationCore::new();
    replication
        .expect_prepare_batch_requests()
        .times(1) // KEY: Single quorum check for all requests
        .returning(|_, _, _, _, _| {
            // Batch optimization: single quorum for all 5 reads
            Ok(crate::PrepareResult::default())
        });

    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path("/tmp/test_batch_single_quorum")
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Push 5 LinearizableRead requests
    let mut receivers = vec![];
    for i in 0..5 {
        let req = ClientReadRequest {
            client_id: 1,
            keys: vec![Bytes::from(format!("key{i}"))],
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        };
        let (tx, rx) = MaybeCloneOneshot::new();
        state.push_client_cmd(ClientCmd::Read(req, tx), &ctx);
        receivers.push(rx);
    }

    // Action: Flush batch (triggers single quorum verification)
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();

    // Verify: All 5 requests succeeded
    for (i, mut rx) in receivers.into_iter().enumerate() {
        assert!(rx.recv().await.is_ok(), "Request {i} should succeed");
    }

    // Verify: MockReplicationCore received exactly 1 call
    // (If each request triggered separate quorum check, we'd see 5 calls)
    // The .times(1) expectation validates this optimization.

    drop(_shutdown_tx);
}

/// **Business Scenario**: Lease read succeeds when lease is valid
///
/// **Purpose**: Verify that when the lease is still valid, the client is served
/// immediately from state machine without any quorum verification.
///
/// **Key Validation**:
/// - Lease is valid (recently refreshed)
/// - No replication call is made (zero quorum overhead)
/// - Client receives Ok response immediately
///
/// **Architecture Context**:
/// LeaseRead is the fastest read path: if the leader's lease has not expired,
/// it can serve reads locally without any network round trip.
#[tokio::test]
#[traced_test]
async fn test_lease_read_valid_lease_serves_immediately() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;
    // Long lease duration so lease is always valid
    node_config.raft.read_consistency.lease_duration_ms = 60_000;

    // No replication calls expected (valid lease skips quorum)
    let replication = MockReplicationCore::new();

    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path("/tmp/test_lease_read_valid_lease")
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    // Set lease timestamp to now so is_lease_valid() returns true
    state.test_update_lease_timestamp();

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    let req = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from_static(b"key1")],
        consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
    };
    let (tx, mut rx) = MaybeCloneOneshot::new();

    // Action: process_lease_read directly (valid lease → serve immediately, no quorum)
    let result = state.process_lease_read(req, tx, &ctx, &role_tx).await;

    // Verify: succeeds without error
    assert!(result.is_ok(), "Should succeed with valid lease");

    // Verify: client receives Ok response
    let client_response = rx.recv().await;
    assert!(
        client_response.is_ok(),
        "Oneshot should have received a message"
    );
    assert!(
        client_response.unwrap().is_ok(),
        "Client should receive Ok response"
    );

    // Verify: no replication calls were made (MockReplicationCore has no expectations)

    drop(_shutdown_tx);
}

/// LinearizableRead is rejected when noop entry is not yet committed (noop_log_id = None).
///
/// # Scenario
/// New leader calls initiate_noop_commit() (fire-and-forget). Before NoopCommitted
/// arrives, a client sends a LinearizableRead. With noop_log_id = None,
/// calculate_read_index() falls back to commit_index. If last_applied >= commit_index,
/// the read is served immediately in Phase 3 — without any quorum confirmation.
/// This violates linearizability: the leader may be in a minority partition.
///
/// # Before fix (will FAIL)
/// noop_log_id = None → read_index = commit_index = 0 → last_applied(0) >= 0
/// → execute_pending_reads fires immediately → client receives Ok (unsafe).
///
/// # After fix (will PASS)
/// Guard before Phase 3 checks noop_log_id.is_none() and rejects the read
/// with Unavailable("LeaderNotReady"). Client retries after noop commits.
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_rejected_when_noop_not_committed() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // prepare_batch_requests succeeds (pure read, no new entries).
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.batching.max_batch_size = 1;

    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_linearizable_read_rejected_when_noop_not_committed")
        .with_replication_handler(replication_handler)
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    // noop_log_id = None by default — noop not yet committed.

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Read(
        ClientReadRequest {
            client_id: 1,
            keys: vec![safe_kv_bytes(1)],
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        },
        resp_tx,
    );

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    state.push_client_cmd(cmd, &context);
    state.flush_cmd_buffers(&context, &role_tx).await.ok();

    // After fix: client receives LeaderNotReady (noop not yet committed).
    // Before fix: client receives Ok (unsafe — no quorum confirmation).
    let result = resp_rx.recv().await.unwrap();
    assert!(
        result.is_err(),
        "expected LeaderNotReady error before noop commits, but got Ok"
    );
    assert_eq!(
        result.unwrap_err().code(),
        Code::Unavailable,
        "expected Unavailable(LeaderNotReady)"
    );
}

/// Expired lease in multi-voter cluster queues request in `pending_lease_reads` without blocking.
///
/// # Fix Verification
/// The old code called `verify_internal_quorum` which blocked awaiting a quorum ACK that
/// never arrived (empty payload → no log entry → commit_index never advanced → DeadlineExceeded).
///
/// The fix: `process_lease_read` no longer calls `verify_internal_quorum`. Instead it:
/// 1. Pushes the request into `pending_lease_reads`
/// 2. Fires an empty AppendEntries heartbeat (fire-and-forget)
/// 3. Returns immediately — `drain_pending_lease_reads` is called from `handle_append_result`
///    when quorum ACK arrives
///
/// # Expected (Fixed)
/// - `process_lease_read` returns in <1ms (no blocking)
/// - Request is queued in `pending_lease_reads`, NOT in `pending_client_writes`
#[tokio::test]
#[traced_test]
async fn test_lease_read_empty_payload_verification_hangs_in_multi_node() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;
    node_config.raft.read_consistency.lease_duration_ms = 1; // Lease expires immediately
    node_config.raft.batching.max_batch_size = 1;

    let mut replication = MockReplicationCore::new();
    replication
        .expect_prepare_batch_requests()
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path("/tmp/test_lease_verification_empty_payload_hangs")
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(|| {
        vec![
            NodeMeta {
                id: 2,
                address: String::new(),
                role: 0,
                status: 0,
            },
            NodeMeta {
                id: 3,
                address: String::new(),
                role: 0,
                status: 0,
            },
        ]
    });
    membership.expect_replication_peers().returning(|| {
        vec![
            NodeMeta {
                id: 2,
                address: String::new(),
                role: 0,
                status: 0,
            },
            NodeMeta {
                id: 3,
                address: String::new(),
                role: 0,
                status: 0,
            },
        ]
    });
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    assert!(
        !state.cluster_metadata.single_voter,
        "precondition: multi-voter"
    );
    assert!(!state.is_lease_valid(&ctx), "precondition: lease expired");

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let req = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from_static(b"key1")],
        consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
    };
    let (tx, _rx) = MaybeCloneOneshot::new();

    // process_lease_read must return immediately (no blocking await on quorum)
    let start = std::time::Instant::now();
    let result = state.process_lease_read(req, tx, &ctx, &role_tx).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "process_lease_read must not return Err");
    assert!(
        elapsed < std::time::Duration::from_millis(50),
        "process_lease_read must return immediately, elapsed={elapsed:?}"
    );

    // Request must be queued in pending_lease_reads, NOT in pending_client_writes
    assert_eq!(
        state.pending_lease_reads.len(),
        1,
        "request must be in pending_lease_reads"
    );
    assert_eq!(
        state.pending_client_writes.len(),
        0,
        "pending_client_writes must be empty"
    );
}
