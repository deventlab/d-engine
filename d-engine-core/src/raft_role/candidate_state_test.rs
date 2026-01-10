use std::sync::Arc;

use crate::RaftEvent;
use crate::raft_role::candidate_state::CandidateState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
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
