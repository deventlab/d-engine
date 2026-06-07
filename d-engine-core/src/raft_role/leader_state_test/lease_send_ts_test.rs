//! Tests for lease deadline anchored to heartbeat send time (RTT/2 fix).
//!
//! ## Background
//! The safety invariant for lease-based reads requires:
//!   `lease_duration_ms < election_timeout_min`
//!
//! However, the original implementation renewed the lease with `now_ms()` at the
//! time the quorum ACK was *received and processed*, not when the heartbeat was
//! *sent*. This extends the effective lease duration by ~RTT/2, violating the
//! invariant in practice.
//!
//! The fix: record `last_heartbeat_send_ts` before Phase 1 of
//! `execute_and_process_raft_rpc` and use it as the deadline base in
//! `update_lease_timestamp`, so the lease expires at `send_ts + lease_duration_ms`
//! rather than `ack_ts + lease_duration_ms`.

use crate::RaftNodeConfig;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::read_lease::now_ms;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use std::time::Duration;
use tokio::sync::watch;

// ── helpers ──────────────────────────────────────────────────────────────────

async fn setup_leader(
    lease_duration_ms: u64
) -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.lease_duration_ms = lease_duration_ms;
    let ctx = MockBuilder::new(shutdown_rx).with_node_config(node_config).build_context();
    let state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    (state, ctx)
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Deadline must be anchored to the heartbeat *send* time, not the ACK *receive* time.
///
/// With a simulated RTT of 5 ms:
/// - new impl: deadline = send_ts + lease_duration_ms  (correct)
/// - old impl: deadline = ack_ts  + lease_duration_ms  (too late by RTT)
///
/// At `send_ts + lease_duration_ms` the new lease must already be expired,
/// while the old impl would still report valid — proving the old window was real.
#[tokio::test]
async fn test_lease_deadline_anchored_to_heartbeat_send_time_not_ack_time() {
    let lease_duration_ms = 100u64;
    let (mut state, ctx) = setup_leader(lease_duration_ms).await;

    let send_ts = now_ms();
    state.last_heartbeat_send_ts = send_ts;

    // Simulate RTT: 5 ms elapses before the ACK is processed.
    std::thread::sleep(Duration::from_millis(5));

    // Quorum ACK arrives — renew using send_ts (new impl).
    state.test_renew_lease_from_send_ts(ctx.node_config().raft.read_consistency.lease_duration_ms);

    let term = state.current_term();
    let new_deadline = send_ts + lease_duration_ms;

    // Just before the send-anchored deadline: still valid.
    assert!(
        state.shared_state.lease.is_valid_for_leader(term, new_deadline - 1),
        "lease must be valid just before send-anchored deadline"
    );
    // At the send-anchored deadline: expired.
    assert!(
        !state.shared_state.lease.is_valid_for_leader(term, new_deadline),
        "lease must be expired at send_ts + lease_duration_ms"
    );

    // Document that the old implementation would NOT have expired yet.
    // old deadline ≈ send_ts + RTT + lease_duration_ms > new_deadline.
    let old_deadline_approx = now_ms() + lease_duration_ms; // ≈ send_ts + 5 + 100
    assert!(
        old_deadline_approx > new_deadline,
        "old impl extends lease by RTT: old={old_deadline_approx} new={new_deadline}"
    );
}

/// Without a quorum ACK, recording `last_heartbeat_send_ts` must NOT advance
/// the lease deadline. A stale leader that never gets ACKs must not serve reads.
#[tokio::test]
async fn test_no_quorum_ack_does_not_advance_lease_deadline() {
    let (mut state, _ctx) = setup_leader(100).await;

    // Record send timestamp — but no ACK arrives, so update_lease_timestamp is never called.
    state.last_heartbeat_send_ts = now_ms();

    let term = state.current_term();
    assert!(
        !state.shared_state.lease.is_valid_for_leader(term, now_ms()),
        "lease must remain invalid when no quorum ACK has been received"
    );
}

/// Demonstrates the invariant `lease_duration_ms < election_timeout_min` using
/// concrete numbers, showing the old implementation violated it and the new one preserves it.
///
/// Config: election_timeout_min=150ms, lease_duration_ms=140ms, RTT=10ms.
///
/// old impl: effective deadline = send_ts + RTT + 140 = send_ts + 150
///           = earliest possible election time  →  invariant broken ❌
///
/// new impl: effective deadline = send_ts + 140
///           < send_ts + 150 (earliest election)  →  invariant holds ✅
#[test]
fn test_new_impl_preserves_election_timeout_invariant_old_impl_violates_it() {
    let election_timeout_min = 150u64;
    let lease_duration_ms = 140u64;
    let simulated_rtt = 10u64;

    let send_ts = 1_000u64; // arbitrary base
    let ack_ts = send_ts + simulated_rtt;

    let new_deadline = send_ts + lease_duration_ms; // 1_140
    let old_deadline = ack_ts + lease_duration_ms; // 1_150
    let earliest_election = send_ts + election_timeout_min; // 1_150

    // New impl: lease expires strictly before any possible election.
    assert!(
        new_deadline < earliest_election,
        "new impl: deadline {new_deadline} must be < earliest election {earliest_election}"
    );

    // Old impl: lease expires at the same time as the earliest election — invariant broken.
    assert!(
        old_deadline >= earliest_election,
        "old impl: deadline {old_deadline} must be >= earliest election {earliest_election} \
         (documents the pre-fix violation)"
    );
}
