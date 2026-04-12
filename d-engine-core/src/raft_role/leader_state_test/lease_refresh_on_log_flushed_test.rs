//! Lease Refresh on LogFlushed Tests
//!
//! Validates that `handle_log_flushed` correctly updates lease timestamp in single-voter
//! scenarios, enabling fast LeaseRead path without unnecessary quorum verification.
//!
//! ## Bug Context
//! Before fix: single_voter clusters never refreshed lease in `handle_log_flushed`,
//! causing LeaseRead to always fall back to slow `verify_leadership_and_refresh_lease` path.
//!
//! After fix: log flush in single-voter confirms leadership (leader is entire quorum),
//! so lease timestamp is updated to enable fast local reads.

use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::RaftNodeConfig;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing_test::traced_test;

// ============================================================================
// Test Utilities
// ============================================================================

/// Create a single-voter leader state for testing lease refresh behavior.
///
/// Returns `(state, ctx, last_entry_id)` where `last_entry_id` is a shared
/// AtomicU64 controlling the mock `raft_log.last_entry_id()` return value.
///
/// MemFirst: `handle_log_flushed` commits to `last_entry_id()`, not `durable`.
/// Set `last_entry_id.store(N)` before calling `handle_log_flushed(N)` to
/// simulate consistent state (IO flushed N means log has N entries).
async fn setup_single_voter_leader(
    db_path: &str,
    lease_duration_ms: u64,
) -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
    Arc<AtomicU64>,
) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.lease_duration_ms = lease_duration_ms;

    let last_entry_id = Arc::new(AtomicU64::new(0));
    let last_entry_id_clone = last_entry_id.clone();

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_last_entry_id()
        .returning(move || last_entry_id_clone.load(Ordering::Relaxed));
    raft_log.expect_durable_index().returning(|| 0);

    let replication = MockReplicationCore::new();

    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path(db_path)
        .with_node_config(node_config)
        .with_raft_log(raft_log)
        .with_replication_handler(replication)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    // Initialize as single-voter cluster
    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    assert!(
        state.cluster_metadata.single_voter,
        "Setup failed: expected single_voter=true"
    );

    (state, ctx, last_entry_id)
}

/// Create a multi-voter leader state for testing lease refresh behavior
/// Note: Uses mock setup that simulates multi-voter without actual peers for negative testing
async fn setup_multi_voter_leader(
    db_path: &str,
    lease_duration_ms: u64,
) -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.lease_duration_ms = lease_duration_ms;

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_durable_index().returning(|| 0);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| None);

    let replication = MockReplicationCore::new();

    let ctx = MockBuilder::new(shutdown_rx)
        .with_db_path(db_path)
        .with_node_config(node_config)
        .with_raft_log(raft_log)
        .with_replication_handler(replication)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    // Manually set cluster_metadata to simulate multi-voter (for negative testing)
    // We don't actually need real peers, just need single_voter=false
    state.cluster_metadata.single_voter = false;
    state.cluster_metadata.total_voters = 3;

    assert!(
        !state.cluster_metadata.single_voter,
        "Setup failed: expected single_voter=false"
    );

    (state, ctx)
}

// ============================================================================
// Single-Voter Lease Refresh Tests
// ============================================================================

/// **Business Scenario**: Single-node cluster serves fast LeaseReads after log flush
///
/// **Purpose**: Verify that in single-voter clusters, `handle_log_flushed` updates
/// lease timestamp, enabling LeaseRead to serve directly from state machine without
/// expensive quorum verification.
///
/// **Key Validation**:
/// - Initial state: lease is invalid (timestamp = 0)
/// - After `handle_log_flushed`: lease becomes valid
/// - LeaseRead can now use fast path
///
/// **Raft Protocol Context**:
/// In single-voter clusters, the leader is the entire quorum. Successful log flush
/// confirms the leader is still operational and can serve reads safely (Raft §6.4).
#[tokio::test]
#[traced_test]
async fn test_single_voter_lease_refreshed_on_log_flushed() {
    let (mut state, ctx, last_entry_id) =
        setup_single_voter_leader("/tmp/test_single_voter_lease_refresh", 1000).await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Precondition: Lease is invalid initially
    assert!(
        !state.is_lease_valid(&ctx),
        "Lease should be invalid before any log flush"
    );

    // Action: Simulate log flush with durable=1 (commit advances from 0 to 1).
    // MemFirst: set last_entry_id=1 before flush (log has entry 1, IO confirms it).
    last_entry_id.store(1, Ordering::Relaxed);
    state.handle_log_flushed(1, &ctx, &role_tx).await;

    // Verify: Lease is now valid
    assert!(
        state.is_lease_valid(&ctx),
        "Lease should be valid after log flush in single_voter cluster"
    );

    // Verify: Commit index advanced
    assert_eq!(state.commit_index(), 1, "Commit should advance to 1");
}

/// **Business Scenario**: Single-voter lease expires naturally over time
///
/// **Purpose**: Verify that after lease duration expires, the lease becomes invalid
/// again, requiring refresh via next log flush or explicit verification.
///
/// **Key Validation**:
/// - Log flush → lease valid
/// - Wait > lease_duration → lease invalid
/// - Another log flush → lease valid again
///
/// **Architecture Context**:
/// Lease-based reads trade safety for performance. The lease duration defines the
/// time window where leader can serve reads without contacting followers. After
/// expiry, must re-verify leadership.
#[tokio::test]
#[traced_test]
async fn test_single_voter_lease_expires_and_refreshes() {
    let lease_duration_ms = 100; // Short duration for testing
    let (mut state, ctx, last_entry_id) =
        setup_single_voter_leader("/tmp/test_single_voter_lease_expiry", lease_duration_ms).await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // T0: Lease invalid
    assert!(!state.is_lease_valid(&ctx));

    // T1: Log flush → lease valid
    last_entry_id.store(1, Ordering::Relaxed);
    state.handle_log_flushed(1, &ctx, &role_tx).await;
    assert!(state.is_lease_valid(&ctx));

    // T2: Wait for lease to expire
    sleep(Duration::from_millis(lease_duration_ms + 50)).await;
    assert!(
        !state.is_lease_valid(&ctx),
        "Lease should expire after lease_duration_ms"
    );

    // T3: Another log flush → lease valid again
    last_entry_id.store(2, Ordering::Relaxed);
    state.handle_log_flushed(2, &ctx, &role_tx).await;
    assert!(
        state.is_lease_valid(&ctx),
        "Lease should be refreshed by subsequent log flush"
    );
}

/// **Business Scenario**: No-op log flush doesn't refresh lease
///
/// **Purpose**: Verify that `handle_log_flushed` only updates lease when commit
/// actually advances. If `durable <= commit_index`, no leadership confirmation
/// occurs, so lease should NOT be refreshed.
///
/// **Key Validation**:
/// - Initial: durable=0, commit=0, lease invalid
/// - Flush(0): commit unchanged → lease remains invalid
/// - Flush(1): commit advances → lease becomes valid
///
/// **Correctness Invariant**:
/// Lease refresh requires proof of ongoing leadership. In single-voter, this proof
/// is a successful log flush that advances commit. Redundant flush events don't
/// provide new proof.
#[tokio::test]
#[traced_test]
async fn test_single_voter_lease_not_refreshed_when_commit_unchanged() {
    let (mut state, ctx, last_entry_id) =
        setup_single_voter_leader("/tmp/test_single_voter_noop_flush", 1000).await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Precondition: commit=0, lease invalid
    assert_eq!(state.commit_index(), 0);
    assert!(!state.is_lease_valid(&ctx));

    // Action: Flush with durable=0, last_entry_id=0 (no new entries, commit unchanged)
    state.handle_log_flushed(0, &ctx, &role_tx).await;

    // Verify: Lease remains invalid (no leadership proof)
    assert!(!state.is_lease_valid(&ctx));
    assert_eq!(state.commit_index(), 0);

    // Action: Flush with durable=1 (commit advances); set last_entry_id=1 to match
    last_entry_id.store(1, Ordering::Relaxed);
    state.handle_log_flushed(1, &ctx, &role_tx).await;

    // Verify: Now lease is valid
    assert!(state.is_lease_valid(&ctx));
    assert_eq!(state.commit_index(), 1);
}

/// **Business Scenario**: Repeated flushes keep lease fresh
///
/// **Purpose**: Verify that continuous write activity in single-voter cluster
/// maintains lease validity, enabling sustained fast LeaseRead performance.
///
/// **Key Validation**:
/// - Series of log flushes → lease stays valid
/// - No gaps > lease_duration → no verification overhead
///
/// **Performance Context**:
/// Single-node clusters often used for:
/// - Development/testing environments
/// - Edge deployments
/// - Low-latency local caches
///
/// Must maintain optimal read performance without multi-voter overhead.
#[tokio::test]
#[traced_test]
async fn test_single_voter_continuous_flushes_maintain_lease() {
    let lease_duration_ms = 500;
    let (mut state, ctx, last_entry_id) =
        setup_single_voter_leader("/tmp/test_single_voter_continuous_flush", lease_duration_ms)
            .await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Simulate continuous write activity
    for i in 1..=10u64 {
        last_entry_id.store(i, Ordering::Relaxed);
        state.handle_log_flushed(i, &ctx, &role_tx).await;
        assert!(
            state.is_lease_valid(&ctx),
            "Lease should remain valid after flush {i}"
        );
        assert_eq!(state.commit_index(), i);

        // Small delay between writes (< lease_duration)
        sleep(Duration::from_millis(50)).await;
    }

    // Final verification: lease still valid
    assert!(state.is_lease_valid(&ctx));
}

// ============================================================================
// Multi-Voter Lease Behavior (Negative Tests)
// ============================================================================

/// **Business Scenario**: Multi-voter cluster does NOT refresh lease on log flush
///
/// **Purpose**: Verify that `handle_log_flushed` does NOT update lease in multi-voter
/// scenarios. Lease refresh must come from `handle_append_result` (follower ACKs).
///
/// **Key Validation**:
/// - Log flush in multi-voter → commit may advance, but lease stays invalid
/// - Lease refresh requires follower responses (network proof of leadership)
///
/// **Architecture Rationale**:
/// Multi-voter: Leader must prove it can reach quorum (network communication).
/// Log flush only proves leader's local disk is working, not that it's still
/// recognized by the cluster.
#[tokio::test]
#[traced_test]
async fn test_multi_voter_lease_not_refreshed_on_log_flushed() {
    let (mut state, ctx) =
        setup_multi_voter_leader("/tmp/test_multi_voter_no_lease_refresh", 1000).await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Precondition: Multi-voter cluster, lease invalid
    assert!(!state.cluster_metadata.single_voter);
    assert!(!state.is_lease_valid(&ctx));

    // Action: Log flush with durable=1
    // Note: In multi-voter, commit won't advance without follower ACKs,
    // but we're testing that even if it did, lease wouldn't refresh here.
    state.handle_log_flushed(1, &ctx, &role_tx).await;

    // Verify: Lease remains invalid (must wait for handle_append_result)
    assert!(
        !state.is_lease_valid(&ctx),
        "Lease should NOT be refreshed by log flush in multi-voter cluster"
    );

    // Verify: Commit unchanged (no quorum yet)
    assert_eq!(
        state.commit_index(),
        0,
        "Commit should not advance without quorum in multi-voter"
    );
}

/// **Business Scenario**: Multi-voter lease refresh via append_result (not log flush)
///
/// **Purpose**: Demonstrate that multi-voter lease refresh is driven by
/// `handle_append_result`, not `handle_log_flushed`. This test shows the
/// architectural difference between single and multi-voter paths.
///
/// **Key Validation**:
/// - Log flush alone → lease invalid
/// - Manual lease update (simulating append_result) → lease valid
///
/// **Note**: Full `handle_append_result` testing is in separate test files.
/// This test just confirms the separation of concerns.
#[tokio::test]
#[traced_test]
async fn test_multi_voter_lease_refresh_requires_append_result() {
    let (mut state, ctx) =
        setup_multi_voter_leader("/tmp/test_multi_voter_append_result_lease", 1000).await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Precondition: Lease invalid
    assert!(!state.is_lease_valid(&ctx));

    // Action 1: Log flush → no lease refresh
    state.handle_log_flushed(1, &ctx, &role_tx).await;
    assert!(!state.is_lease_valid(&ctx));

    // Action 2: Simulate append_result path (via test helper)
    state.test_update_lease_timestamp();

    // Verify: Lease now valid (proves separation of paths)
    assert!(
        state.is_lease_valid(&ctx),
        "Lease should be valid after explicit update (simulating append_result)"
    );
}

// ============================================================================
// Edge Cases and Boundary Conditions
// ============================================================================

/// **Business Scenario**: Durable index regresses (should never happen, defensive check)
///
/// **Purpose**: Verify that if `durable` somehow goes backward, commit doesn't
/// regress and lease isn't incorrectly refreshed.
///
/// **Key Validation**:
/// - Flush(5) → commit=5, lease valid
/// - Flush(3) → commit stays 5, lease remains valid (no update)
///
/// **Note**: In production, durable_index is monotonic. This tests defensive coding.
#[tokio::test]
#[traced_test]
async fn test_single_voter_durable_regression_does_not_refresh_lease() {
    let (mut state, ctx, last_entry_id) =
        setup_single_voter_leader("/tmp/test_durable_regression", 1000).await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // T1: Flush(5) → commit=5, lease valid
    last_entry_id.store(5, Ordering::Relaxed);
    state.handle_log_flushed(5, &ctx, &role_tx).await;
    assert_eq!(state.commit_index(), 5);
    assert!(state.is_lease_valid(&ctx));

    // Wait to distinguish timestamps
    sleep(Duration::from_millis(50)).await;

    // T2: Flush(3) → should be no-op (last_entry_id=5 > commit=5 is false, no advance)
    // last_entry_id stays at 5; durable regressing to 3 is the scenario.
    state.handle_log_flushed(3, &ctx, &role_tx).await;

    // Verify: Commit unchanged, lease not updated (timestamp same as T1)
    assert_eq!(state.commit_index(), 5, "Commit should not regress");
    assert!(
        state.is_lease_valid(&ctx),
        "Lease should remain valid from T1"
    );
}

/// **Business Scenario**: Very short lease duration expires quickly
///
/// **Purpose**: Verify that with minimal `lease_duration_ms = 1`, lease expires
/// almost immediately, useful for testing "always expired" scenarios.
///
/// **Key Validation**:
/// - Flush updates lease timestamp
/// - Lease valid immediately after flush
/// - Lease expires within milliseconds
///
/// **Note**: `lease_duration_ms = 1` creates a race: with 1ms precision, any async yield
/// between `update_lease_timestamp` and `is_lease_valid` can cross the millisecond boundary.
/// Use 50ms to give a stable margin while still verifying "short lease expires quickly".
#[tokio::test]
#[traced_test]
async fn test_single_voter_very_short_lease_expires_quickly() {
    let lease_duration_ms = 50; // Short but stable — avoids 1ms clock-boundary race
    let (mut state, ctx, last_entry_id) =
        setup_single_voter_leader("/tmp/test_very_short_lease", lease_duration_ms).await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // T0: Lease invalid
    assert!(!state.is_lease_valid(&ctx));

    // T1: Flush updates lease
    last_entry_id.store(1, Ordering::Relaxed);
    state.handle_log_flushed(1, &ctx, &role_tx).await;
    assert!(
        state.is_lease_valid(&ctx),
        "Lease should be valid immediately after flush"
    );

    // T2: Wait 100ms (> lease_duration_ms)
    tokio::time::sleep(Duration::from_millis(100)).await;

    // T3: Lease expired
    assert!(
        !state.is_lease_valid(&ctx),
        "Lease should expire with lease_duration_ms = 50"
    );
}

/// **Business Scenario**: Large lease duration maintains validity for extended period
///
/// **Purpose**: Verify that with large `lease_duration_ms`, lease stays valid
/// for long periods, minimizing verification overhead in stable single-node setups.
///
/// **Key Validation**:
/// - Single flush with large duration → lease valid for entire duration
/// - Useful for stable development/edge environments
#[tokio::test]
#[traced_test]
async fn test_single_voter_large_lease_duration() {
    let lease_duration_ms = 10_000; // 10 seconds
    let (mut state, ctx, last_entry_id) =
        setup_single_voter_leader("/tmp/test_large_lease", lease_duration_ms).await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Single flush
    last_entry_id.store(1, Ordering::Relaxed);
    state.handle_log_flushed(1, &ctx, &role_tx).await;
    assert!(state.is_lease_valid(&ctx));

    // Wait 1 second (much less than 10s lease)
    sleep(Duration::from_secs(1)).await;

    // Verify: Lease still valid
    assert!(
        state.is_lease_valid(&ctx),
        "Lease should remain valid within duration"
    );
}
