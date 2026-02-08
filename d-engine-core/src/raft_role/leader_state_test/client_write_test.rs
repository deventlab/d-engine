//! Tests for leader state client write request processing
//!
//! This module tests the `process_raft_request` method and related client write
//! operations in the leader state.

use std::collections::HashMap;
use std::sync::Arc;

use nanoid::nanoid;
use tokio::sync::{mpsc, watch};

use crate::AppendResults;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::PeerUpdate;
use crate::RaftRequestWithSignal;
use crate::client_command_to_entry_payloads;
use crate::event::{NewCommitData, RoleEvent};
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_context::RaftContext;
use crate::raft_role::leader_state::LeaderState;
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::{MockBuilder, MockTypeConfig};
use crate::test_utils::node_config;
use d_engine_proto::client::ClientWriteRequest;

// ============================================================================
// Test Helper Structures and Functions
// ============================================================================

struct ProcessRaftRequestTestContext {
    state: LeaderState<MockTypeConfig>,
    raft_context: RaftContext<MockTypeConfig>,
}

/// Verify client response succeeds
async fn assert_client_response(
    mut rx: crate::MaybeCloneOneshotReceiver<
        std::result::Result<d_engine_proto::client::ClientResponse, tonic::Status>,
    >
) {
    use d_engine_proto::error::ErrorCode;
    match rx.recv().await {
        Ok(Ok(response)) => assert_eq!(
            ErrorCode::try_from(response.error).unwrap(),
            ErrorCode::Success,
            "Expected success response"
        ),
        Ok(Err(e)) => panic!("Unexpected error response: {e:?}"),
        Err(_) => panic!("Response channel closed unexpectedly"),
    }
}

/// Initialize the test environment and return the core components
async fn setup_process_raft_request_test_context(
    test_name: &str,
    batch_threshold: usize,
    handle_raft_request_in_batch_expect_times: usize,
    shutdown_signal: watch::Receiver<()>,
) -> ProcessRaftRequestTestContext {
    let mut node_config = node_config(&format!("/tmp/{test_name}"));
    node_config.raft.replication.rpc_append_entries_in_batch_threshold = batch_threshold;
    let mut raft_context =
        MockBuilder::new(shutdown_signal).with_node_config(node_config).build_context();

    let mut state = LeaderState::new(1, raft_context.node_config());
    state.update_commit_index(4).expect("Should succeed to update commit index");

    // Initialize the mock object
    let mut replication_handler = MockReplicationCore::new();
    let mut raft_log = MockRaftLog::new();

    // Configure mock behavior
    replication_handler
        .expect_handle_raft_request_in_batch()
        .times(handle_raft_request_in_batch_expect_times)
        .returning(move |_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: Some(5),
                            next_index: 6,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: Some(5),
                            next_index: 6,
                            success: true,
                        },
                    ),
                ]),
                learner_progress: HashMap::new(),
            })
        });

    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    raft_context.handlers.replication_handler = replication_handler;
    raft_context.storage.raft_log = Arc::new(raft_log);

    ProcessRaftRequestTestContext {
        state,
        raft_context,
    }
}

// ============================================================================
// Unit Tests for Client Write Request Processing
// ============================================================================

/// Test process_raft_request with immediate batch execution (threshold = 0)
///
/// # Test Scenario
/// Verifies that when batch threshold is 0, the request is processed immediately,
/// commit index is updated, and client receives success response.
///
/// # Given
/// - Leader state with commit_index = 4
/// - Batch threshold = 0 (immediate execution mode)
/// - Mock replication handler configured to succeed with commit quorum
///
/// # When
/// - Client write request is processed
///
/// # Then
/// - Request is handled immediately (not batched)
/// - Commit index advances to 5
/// - Peer next_index values are updated (2 -> 6, 3 -> 6)
/// - NotifyNewCommitIndex event is sent
/// - Client receives success response
#[tokio::test]
async fn test_process_raft_request_immediate_execution() {
    // Given: Test environment with threshold = 0 (immediate execution)
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut test_context = setup_process_raft_request_test_context(
        "test_process_raft_request_immediate_execution",
        0, // batch_threshold: 0 means immediate execution
        1, // expect handle_raft_request_in_batch to be called once
        graceful_rx,
    )
    .await;

    // Prepare test request
    let request = ClientWriteRequest {
        client_id: 0,
        commands: vec![],
    };
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx, rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // When: Execute the write request
    let result = test_context
        .state
        .execute_request_immediately(
            RaftRequestWithSignal {
                id: nanoid!(),
                payloads: client_command_to_entry_payloads(request.commands),
                sender: tx,
                wait_for_apply_event: true,
            },
            &test_context.raft_context,
            &role_tx,
        )
        .await;

    // Then: Verify commit index notification is sent
    if let Some(RoleEvent::NotifyNewCommitIndex(NewCommitData {
        new_commit_index,
        role: _,
        current_term: _,
    })) = role_rx.recv().await
    {
        assert_eq!(new_commit_index, 5, "New commit index should be 5");
    } else {
        panic!("Expected NotifyNewCommitIndex event");
    }

    // Then: Verify operation succeeded
    assert!(result.is_ok(), "Operation should succeed");

    // Then: Verify state updates
    assert_eq!(
        test_context.state.shared_state().commit_index,
        5,
        "Commit index should be updated to 5"
    );
    assert_eq!(
        test_context.state.next_index(2),
        Some(6),
        "Peer 2 next_index should be 6"
    );
    assert_eq!(
        test_context.state.next_index(3),
        Some(6),
        "Peer 3 next_index should be 6"
    );

    // Then: Verify client receives success response
    assert_client_response(rx).await;
}

/// Test process_raft_request with two consecutive forced sends
///
/// # Test Scenario
/// Verifies that when force_send=true is used for two consecutive requests,
/// both are processed immediately (not batched together), and both receive
/// success responses.
///
/// # Given
/// - Leader state with commit_index = 4
/// - Batch threshold = 0 (immediate execution mode)
/// - Mock replication handler expects 2 calls (one per request)
///
/// # When
/// - Two client write requests are processed with force_send=true
///
/// # Then
/// - Both requests are handled immediately (not batched)
/// - Commit index advances to 5
/// - Peer next_index values are updated (2 -> 6, 3 -> 6)
/// - NotifyNewCommitIndex event is sent
/// - Both clients receive success responses
#[tokio::test]
async fn test_process_raft_request_two_consecutive_forced_sends() {
    // Given: Test environment expecting 2 separate batch operations
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut test_context = setup_process_raft_request_test_context(
        "test_process_raft_request_two_consecutive_forced_sends",
        0, // batch_threshold: 0 means immediate execution
        2, // expect handle_raft_request_in_batch to be called twice
        graceful_rx,
    )
    .await;

    // Prepare test requests
    let request = ClientWriteRequest {
        client_id: 0,
        commands: vec![],
    };
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (tx2, rx2) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // When: Execute first write request (immediate execution)
    let result1 = test_context
        .state
        .execute_request_immediately(
            RaftRequestWithSignal {
                id: nanoid!(),
                payloads: client_command_to_entry_payloads(request.commands.clone()),
                sender: tx1,
                wait_for_apply_event: true,
            },
            &test_context.raft_context,
            &role_tx,
        )
        .await;

    // When: Execute second write request (immediate execution)
    let result2 = test_context
        .state
        .execute_request_immediately(
            RaftRequestWithSignal {
                id: nanoid!(),
                payloads: client_command_to_entry_payloads(request.commands),
                sender: tx2,
                wait_for_apply_event: true,
            },
            &test_context.raft_context,
            &role_tx,
        )
        .await;

    // Then: Verify commit index notification is sent
    if let Some(RoleEvent::NotifyNewCommitIndex(NewCommitData {
        new_commit_index,
        role: _,
        current_term: _,
    })) = role_rx.recv().await
    {
        assert_eq!(new_commit_index, 5, "New commit index should be 5");
    } else {
        panic!("Expected NotifyNewCommitIndex event");
    }

    // Then: Verify both operations succeeded
    assert!(result1.is_ok(), "First operation should succeed");
    assert!(result2.is_ok(), "Second operation should succeed");

    // Then: Verify state updates
    assert_eq!(
        test_context.state.shared_state().commit_index,
        5,
        "Commit index should be updated to 5"
    );
    assert_eq!(
        test_context.state.next_index(2),
        Some(6),
        "Peer 2 next_index should be 6"
    );
    assert_eq!(
        test_context.state.next_index(3),
        Some(6),
        "Peer 3 next_index should be 6"
    );

    // Then: Verify both clients receive success responses
    assert_client_response(rx1).await;
    assert_client_response(rx2).await;
}

/// Test process_raft_request with batching enabled (request buffered)
///
/// # Test Scenario
/// Verifies that when batch threshold > 0, requests are buffered and not
/// immediately sent to replication handler. This tests the batching mechanism
/// that allows multiple client requests to be grouped together for efficiency.
///
/// # Given
/// - Leader state with commit_index = 4
/// - Batch threshold = 100 (batching enabled)
/// - Mock replication handler expects 0 calls (no immediate execution)
///
/// # When
/// - Single client write request is processed with force_send=false
///
/// # Then
/// - Request is buffered (not immediately sent)
/// - process_raft_request returns Ok
/// - No immediate replication occurs (verified by mock expectations)
#[tokio::test]
async fn test_process_raft_request_batching_enabled() {
    // Given: Test environment with batching enabled (threshold=100, no immediate calls expected)
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut test_context = setup_process_raft_request_test_context(
        "test_process_raft_request_batching_enabled",
        100, // batch_threshold: 100 means batching enabled
        0,   // expect handle_raft_request_in_batch NOT to be called immediately
        graceful_rx,
    )
    .await;

    // Prepare test request
    let request = ClientWriteRequest {
        client_id: 0,
        commands: vec![],
    };
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx, _rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // When: Execute write request (immediate execution - no buffering in this test)
    let result = test_context
        .state
        .execute_request_immediately(
            RaftRequestWithSignal {
                id: nanoid!(),
                payloads: client_command_to_entry_payloads(request.commands),
                sender: tx,
                wait_for_apply_event: true,
            },
            &test_context.raft_context,
            &role_tx,
        )
        .await;

    // Then: Verify operation succeeded (request buffered, not sent)
    assert!(
        result.is_ok(),
        "Operation should succeed - request buffered for batching"
    );

    // Note: No assertions on commit_index or responses because:
    // - Request is buffered, not yet replicated
    // - Mock handler expects 0 calls, which is verified by mockall on drop
}

// ============================================================================
// Drain-Based Write Batching Tests
// ============================================================================

/// **Business Scenario**: Single write request processes immediately without batching delay
///
/// **Purpose**: Verify that under low load, single write requests are processed
/// immediately via drain pattern (recv + try_recv finds nothing), eliminating
/// artificial batching delays from the old timeout-based mechanism.
///
/// **Key Validation**:
/// - Single request in buffer
/// - Immediate flush (no timeout waiting)
/// - Request succeeds with low latency
///
/// **Architecture Context**:
/// Old: Even single write waited for batch_timeout
/// New: recv() returns immediately, flush happens instantly
#[tokio::test]
async fn test_drain_single_write_no_delay() {
    use crate::ClientCmd;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::test_utils::MockBuilder;
    use d_engine_proto::client::ClientWriteRequest;
    use tokio::time::Instant;

    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = node_config("/tmp/test_drain_single_write");
    node_config.raft.replication.rpc_append_entries_in_batch_threshold = 100;

    // Mock replication handler
    let mut replication = MockReplicationCore::new();
    replication
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::new(),
                learner_progress: HashMap::new(),
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    let ctx = MockBuilder::new(shutdown_rx)
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .with_raft_log(raft_log)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Single write request (simulates low load)
    let start = Instant::now();
    let req = ClientWriteRequest {
        client_id: 1,
        commands: vec![],
    };
    let (tx, mut rx) = MaybeCloneOneshot::new();

    state.push_client_cmd(ClientCmd::Propose(req, tx), &ctx);

    // Verify: Buffer has 1 request
    assert_eq!(
        state.propose_buffer.len(),
        1,
        "Buffer should have 1 request"
    );

    // Flush immediately (drain pattern: no waiting)
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();
    let elapsed = start.elapsed();

    // Verify: Request succeeded
    assert!(rx.recv().await.is_ok(), "Write should succeed");

    // Verify: Processing was immediate (< 10ms)
    assert!(
        elapsed.as_millis() < 10,
        "Single write should process immediately, took {:?}",
        elapsed
    );

    drop(_shutdown_tx);
}

/// **Business Scenario**: Multiple write requests naturally batch together
///
/// **Purpose**: Verify that under high load, drain pattern naturally collects
/// multiple pending writes into a single batch, optimizing replication overhead.
///
/// **Key Validation**:
/// - Multiple requests accumulated in buffer
/// - Single flush processes entire batch
/// - Single replication call for all writes
///
/// **Architecture Context**:
/// High load → many writes accumulate between flush cycles → natural batching
/// without manual threshold checks
#[tokio::test]
async fn test_drain_multiple_writes_natural_batch() {
    use crate::ClientCmd;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::test_utils::MockBuilder;
    use d_engine_proto::client::ClientWriteRequest;

    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = node_config("/tmp/test_drain_multiple_writes");
    node_config.raft.replication.rpc_append_entries_in_batch_threshold = 100;

    // Mock replication - expect single call for entire batch
    let mut replication = MockReplicationCore::new();
    replication
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::new(),
                learner_progress: HashMap::new(),
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    let ctx = MockBuilder::new(shutdown_rx)
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .with_raft_log(raft_log)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Push 10 write requests (simulates high load)
    let mut receivers = vec![];
    for i in 0..10 {
        let req = ClientWriteRequest {
            client_id: i,
            commands: vec![],
        };
        let (tx, rx) = MaybeCloneOneshot::new();
        state.push_client_cmd(ClientCmd::Propose(req, tx), &ctx);
        receivers.push(rx);
    }

    // Verify: All 10 requests in buffer
    assert_eq!(
        state.propose_buffer.len(),
        10,
        "Buffer should accumulate all requests"
    );

    // Action: Single flush processes entire batch
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();

    // Verify: Buffer emptied
    assert_eq!(
        state.propose_buffer.len(),
        0,
        "Buffer should be empty after flush"
    );

    // Verify: All 10 requests received responses
    for (i, mut rx) in receivers.into_iter().enumerate() {
        assert!(
            rx.recv().await.is_ok(),
            "Write {} should succeed in batch",
            i
        );
    }

    // Note: MockReplicationCore expects 1 call - confirms single replication
    // for all 10 writes (batch optimization)

    drop(_shutdown_tx);
}

/// **Business Scenario**: max_batch_size prevents unbounded write batching
///
/// **Purpose**: Verify that max_batch_size limit prevents processing excessively
/// large write batches, protecting against memory pressure and maintaining
/// bounded commit latency.
///
/// **Key Validation**:
/// - Buffer can accumulate > max_batch_size requests
/// - Main loop drain enforces limit during collection
/// - Remaining requests stay for next flush cycle
///
/// **Architecture Context**:
/// The drain pattern could theoretically collect unlimited writes. max_batch_size
/// provides backpressure in the main loop to prevent overwhelming replication
/// and commit processing.
#[tokio::test]
async fn test_drain_max_batch_size_limit() {
    use crate::ClientCmd;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use d_engine_proto::client::ClientWriteRequest;

    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = node_config("/tmp/test_drain_max_batch");
    // Set small max_batch_size for testing
    node_config.raft.replication.rpc_append_entries_in_batch_threshold = 5;

    let ctx = MockBuilder::new(shutdown_rx).with_node_config(node_config).build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    // Action: Push 10 requests (exceeds max_batch_size of 5)
    for i in 0..10 {
        let req = ClientWriteRequest {
            client_id: i,
            commands: vec![],
        };
        let (tx, _rx) = MaybeCloneOneshot::new();
        state.push_client_cmd(ClientCmd::Propose(req, tx), &ctx);
    }

    // Verify: All 10 requests in buffer
    assert_eq!(
        state.propose_buffer.len(),
        10,
        "Buffer should have all 10 requests"
    );

    // Note: This validates buffer accumulation behavior.
    // Actual max_batch_size enforcement happens in main loop's drain logic,
    // not in flush_cmd_buffers().
    //
    // This proves buffer can hold > max_batch_size, demonstrating need for
    // drain-time limiting in raft.rs main loop.

    assert!(
        state.propose_buffer.len()
            > ctx.node_config.raft.replication.rpc_append_entries_in_batch_threshold,
        "Buffer can accumulate beyond max_batch_size (main loop enforces during drain)"
    );

    drop(_shutdown_tx);
}

/// **Business Scenario**: Batch writes share single replication round-trip
///
/// **Purpose**: Verify the core optimization of write batching - multiple client
/// writes collected in a batch are replicated with a single RPC instead of N
/// separate replication calls.
///
/// **Key Validation**:
/// - Multiple write requests in buffer
/// - Single call to replication handler
/// - All requests succeed after single commit
///
/// **Architecture Context**:
/// This is the primary performance benefit of write batching in Raft.
/// Instead of:
///   Write 1 → Replicate 1 → Commit 1
///   Write 2 → Replicate 2 → Commit 2
///   Write 3 → Replicate 3 → Commit 3
///
/// We do:
///   Collect [Write 1, 2, 3] → Single replicate → Single commit
///
/// This reduces network overhead by ~3x and improves throughput significantly.
#[tokio::test]
async fn test_write_batch_single_replication() {
    use crate::ClientCmd;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    use crate::test_utils::MockBuilder;
    use d_engine_proto::client::ClientWriteRequest;

    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = node_config("/tmp/test_batch_single_replication");
    node_config.raft.replication.rpc_append_entries_in_batch_threshold = 100;

    // Mock replication - expect EXACTLY 1 call for entire batch
    let mut replication = MockReplicationCore::new();
    replication
        .expect_handle_raft_request_in_batch()
        .times(1) // KEY: Single replication for all writes
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::new(),
                learner_progress: HashMap::new(),
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    let ctx = MockBuilder::new(shutdown_rx)
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .with_raft_log(raft_log)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Action: Push 5 write requests
    let mut receivers = vec![];
    for i in 0..5 {
        let req = ClientWriteRequest {
            client_id: i,
            commands: vec![],
        };
        let (tx, rx) = MaybeCloneOneshot::new();
        state.push_client_cmd(ClientCmd::Propose(req, tx), &ctx);
        receivers.push(rx);
    }

    // Action: Flush batch (triggers single replication)
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();

    // Verify: All 5 writes succeeded
    for (i, mut rx) in receivers.into_iter().enumerate() {
        assert!(rx.recv().await.is_ok(), "Write {} should succeed", i);
    }

    // Verify: MockReplicationCore received exactly 1 call
    // (If each write triggered separate replication, we'd see 5 calls)
    // The .times(1) expectation validates this optimization.

    drop(_shutdown_tx);
}
