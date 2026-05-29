//! Tests verifying that `become_follower()` revokes the read lease.
//!
//! Safety invariant: a stepped-down leader must not allow ReadActor to serve
//! stale lease reads. The shared `Arc<ReadLease>` must be invalid immediately
//! after `become_follower()` returns so any concurrent ReadActor check fails.

use std::sync::Arc;

use crate::RaftNodeConfig;
use crate::now_ms;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;

/// become_follower() revokes the shared Arc<ReadLease> so ReadActor immediately
/// returns LeaseInvalid on the very next is_valid() check.
///
/// This pins the safety contract: after step-down the ReadActor fast path is
/// blocked regardless of when (before or after) its thread checks the lease.
#[tokio::test]
async fn test_become_follower_revokes_read_lease() {
    let mut state = LeaderState::<MockTypeConfig>::new(1, RaftNodeConfig::default().into());

    // Clone Arc before step-down to observe the shared lease from the outside
    // (simulates what ReadActor holds).
    let lease = Arc::clone(&state.shared_state.lease);

    // Renew with a generous 60-second deadline.
    state.test_update_lease_timestamp();
    assert!(
        state.is_lease_valid(),
        "precondition: lease must be valid before become_follower()"
    );
    assert!(
        lease.is_valid(now_ms()),
        "precondition: same Arc must also report valid"
    );

    // Transition to follower — must call lease.revoke() atomically.
    let _ = state.become_follower().expect("become_follower must succeed");

    // The shared Arc<ReadLease> must now be invalid.
    assert!(
        !lease.is_valid(now_ms()),
        "become_follower() must revoke the shared Arc<ReadLease>"
    );
}
