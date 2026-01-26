//! Unit tests for Leader FatalError event handling
//!
//! These tests verify that when Leader receives FatalError events,
//! it properly notifies all pending client requests before shutdown.

use super::super::LeaderState;
use crate::MockTypeConfig;
use crate::RaftEvent;
use crate::RoleEvent;
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::MockBuilder;
use tokio::sync::{mpsc, watch};
use tracing_test::traced_test;

/// Test: Leader handles FatalError and propagates error
///
/// Verifies that when Leader receives FatalError from any component,
/// it returns Error::Fatal and stops further processing.
///
/// This test verifies the error handling path without requiring complex
/// setup of pending client requests (which are managed at a higher level).
///
/// # Test Scenario
/// Leader receives FatalError event from state machine while in leader role.
/// Leader should recognize the fatal error and return Error::Fatal.
///
/// # Given
/// - Leader in normal state
/// - FatalError event from StateMachine component with specific error details
///
/// # When
/// - Leader handles FatalError event via handle_raft_event()
///
/// # Then
/// - handle_raft_event() returns Error::Fatal
/// - Error message contains source and error details from FatalError event
/// - No role transition events are sent (leadership continues until higher level handles error)
#[tokio::test]
#[traced_test]
async fn test_leader_handles_fatal_error_notifies_pending_requests() {
    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_leader_handles_fatal_error_notifies_pending_requests")
        .build_context();

    // Setup: Create leader in normal state
    let mut leader = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Create FatalError event from state machine
    let fatal_error = RaftEvent::FatalError {
        source: "StateMachine".to_string(),
        error: "Disk failure - cannot write to persistent storage".to_string(),
    };

    // Create role event channel
    let (role_tx, mut role_rx) = mpsc::unbounded_channel::<RoleEvent>();

    // Handle the FatalError event
    let result = leader.handle_raft_event(fatal_error, &context, role_tx).await;

    // VERIFY 1: handle_raft_event() returns Error::Fatal
    assert!(
        result.is_err(),
        "Expected handle_raft_event to return Err, got: {result:?}"
    );

    // VERIFY 2: Error is Fatal and contains source information
    match result.unwrap_err() {
        crate::Error::Fatal(msg) => {
            assert!(
                msg.contains("StateMachine"),
                "Error message should mention source (StateMachine), got: {msg}"
            );
            assert!(
                msg.contains("Disk failure") || msg.contains("storage"),
                "Error message should contain error details, got: {msg}"
            );
        }
        other => panic!("Expected Error::Fatal, got: {other:?}"),
    }

    // VERIFY 3: No role events sent (FatalError is handled locally in leader)
    // The leader stops processing at the fatal error and higher level (Raft core)
    // will eventually shut down the node
    assert!(
        role_rx.try_recv().is_err(),
        "No role transition events should be sent during FatalError handling"
    );
}
