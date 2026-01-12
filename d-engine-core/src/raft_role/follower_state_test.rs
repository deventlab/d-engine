use std::sync::Arc;

use crate::RaftEvent;
use crate::raft_role::follower_state::FollowerState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use crate::test_utils::node_config;
use tokio::sync::{mpsc, watch};

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
