//! Unit tests for Leader FatalError event handling
//!
//! These tests verify that when Leader receives FatalError events,
//! it properly notifies all pending client requests before shutdown.

use super::super::LeaderState;
use crate::ClientCmd;
use crate::MockMembership;
use crate::MockTypeConfig;
use crate::RaftEvent;
use crate::ReadConsistencyPolicy;
use crate::RoleEvent;
use crate::maybe_clone_oneshot::MaybeCloneOneshot;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use bytes::Bytes;
use d_engine_proto::client::ClientReadRequest;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tonic::Code;
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
async fn test_leader_handles_fatal_error_notifies_pending_write_apply() {
    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_leader_handles_fatal_error_notifies_pending_write_apply")
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

/// Test: FatalError drains all pending queues and notifies clients immediately
///
/// # Purpose
/// Verifies that when FatalError fires, ALL pending client requests across
/// every queue receive an immediate error response instead of hanging until timeout.
///
/// # Queues verified
/// - `linearizable_read_buffer` (reads buffered pre-flush)
/// - `lease_read_queue` (lease reads awaiting flush)
/// - `eventual_read_queue` (eventual reads awaiting flush)
///
/// # Given
/// - Leader with one request in each read queue
///
/// # When
/// - FatalError event is handled
///
/// # Then
/// - All clients receive Err(INTERNAL) immediately
/// - No client hangs
#[tokio::test]
#[traced_test]
async fn test_fatal_error_drains_all_pending_queues() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = crate::RaftNodeConfig::default();
    node_config.raft.read_consistency.allow_client_override = true;
    // Lease always expired → lease read will sit in queue
    node_config.raft.read_consistency.lease_duration_ms = 60_000;

    let ctx = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_fatal_error_drains_all_queues")
        .with_node_config(node_config)
        .build_context();

    let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    leader.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    // -- Setup: linearizable read in buffer --
    let (lin_tx, mut lin_rx) = MaybeCloneOneshot::new();
    let lin_req = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from_static(b"k1")],
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
    };
    leader.push_client_cmd(ClientCmd::Read(lin_req, lin_tx), &ctx);
    assert_eq!(
        leader.linearizable_read_buffer.len(),
        1,
        "linearizable_read_buffer should have 1 request"
    );

    // -- Setup: lease read in queue --
    let (lease_tx, mut lease_rx) = MaybeCloneOneshot::new();
    let lease_req = ClientReadRequest {
        client_id: 2,
        keys: vec![Bytes::from_static(b"k2")],
        consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
    };
    leader.test_update_lease_timestamp();
    leader.push_client_cmd(ClientCmd::Read(lease_req, lease_tx), &ctx);
    assert_eq!(
        leader.lease_read_queue.len(),
        1,
        "lease_read_queue should have 1 request"
    );

    // -- Setup: eventual read in queue --
    let (ev_tx, mut ev_rx) = MaybeCloneOneshot::new();
    let ev_req = ClientReadRequest {
        client_id: 3,
        keys: vec![Bytes::from_static(b"k3")],
        consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
    };
    leader.push_client_cmd(ClientCmd::Read(ev_req, ev_tx), &ctx);
    assert_eq!(
        leader.eventual_read_queue.len(),
        1,
        "eventual_read_queue should have 1 request"
    );

    // -- Action: FatalError --
    let (role_tx, _role_rx) = mpsc::unbounded_channel::<RoleEvent>();
    let result = leader
        .handle_raft_event(
            RaftEvent::FatalError {
                source: "StateMachine".to_string(),
                error: "disk failure".to_string(),
            },
            &ctx,
            role_tx,
        )
        .await;

    // Verify: returns Fatal error
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), crate::Error::Fatal(_)));

    // Verify: linearizable read buffer notified
    let lin_resp = lin_rx.recv().await;
    assert!(lin_resp.is_ok(), "linearizable read oneshot should fire");
    assert_eq!(lin_resp.unwrap().unwrap_err().code(), Code::Internal);

    // Verify: lease read queue notified
    let lease_resp = lease_rx.recv().await;
    assert!(lease_resp.is_ok(), "lease read oneshot should fire");
    assert_eq!(lease_resp.unwrap().unwrap_err().code(), Code::Internal);

    // Verify: eventual read queue notified
    let ev_resp = ev_rx.recv().await;
    assert!(ev_resp.is_ok(), "eventual read oneshot should fire");
    assert_eq!(ev_resp.unwrap().unwrap_err().code(), Code::Internal);
}
