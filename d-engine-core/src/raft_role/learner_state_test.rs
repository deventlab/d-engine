use std::sync::Arc;

use crate::RaftEvent;
use crate::raft_role::learner_state::LearnerState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use crate::test_utils::node_config;
use tokio::sync::{mpsc, watch};

/// Test: LearnerState rejects FlushReadBuffer event
///
/// Scenario: Learner node receives FlushReadBuffer event
/// Expected: Returns RoleViolation error (only Leader can handle this event)
#[tokio::test]
async fn test_learner_rejects_flush_read_buffer_event() {
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let ctx = mock_raft_context("/tmp/test_learner_flush", shutdown_rx, None);
    let mut state = LearnerState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Handle FlushReadBuffer event
    let result = state.handle_raft_event(RaftEvent::FlushReadBuffer, &ctx, role_tx.clone()).await;

    // Verify: Returns RoleViolation error
    assert!(
        result.is_err(),
        "Learner should reject FlushReadBuffer event"
    );

    if let Err(e) = result {
        let error_str = format!("{e:?}");
        assert!(
            error_str.contains("RoleViolation"),
            "Error should be RoleViolation, got: {error_str}"
        );
        assert!(
            error_str.contains("Learner"),
            "Error should mention Learner role"
        );
        assert!(
            error_str.contains("Leader"),
            "Error should mention Leader as required role"
        );
    }

    drop(shutdown_tx);
}

/// Test: LearnerState drain_read_buffer returns NotLeader error
///
/// Scenario: Call drain_read_buffer() on Learner
/// Expected: Returns NotLeader error (Learner doesn't buffer reads)
#[tokio::test]
async fn test_learner_drain_read_buffer_returns_error() {
    let mut state =
        LearnerState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_learner_drain")));

    // Action: Call drain_read_buffer()
    let result = state.drain_read_buffer();

    // Verify: Returns NotLeader error
    assert!(
        result.is_err(),
        "Learner drain_read_buffer should return error"
    );

    if let Err(e) = result {
        let error_str = format!("{e:?}");
        assert!(
            error_str.contains("NotLeader"),
            "Error should be NotLeader, got: {error_str}"
        );
    }
}
