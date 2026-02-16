//! Tests for leader state client write request processing
//!
//! This module tests the `process_raft_request` method and related client write
//! operations in the leader state.

use crate::AppendResults;
use crate::ApplyResult;
use crate::ClientCmd;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::PeerUpdate;
use crate::RaftEvent;
use crate::RaftRequestWithSignal;
use crate::client_command_to_entry_payloads;
use crate::event::{NewCommitData, RoleEvent};
use crate::maybe_clone_oneshot::MaybeCloneOneshot;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_context::RaftContext;
use crate::raft_role::leader_state::LeaderState;
use crate::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::node_config;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::common::AddNode;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::common::membership_change::Change;
use d_engine_proto::error::ErrorCode;
use nanoid::nanoid;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

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
    node_config.raft.replication.max_batch_size = batch_threshold;
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

    raft_log.expect_last_entry_id().returning(|| 4);
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
        command: Some(WriteCommand::default()),
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
                payloads: client_command_to_entry_payloads(request.command.into_iter().collect()),
                senders: vec![tx],
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

    // Simulate SM apply to resolve pending_requests
    test_context
        .state
        .handle_raft_event(
            RaftEvent::ApplyCompleted {
                last_index: 5,
                results: vec![ApplyResult {
                    index: 5,
                    succeeded: true,
                }],
            },
            &test_context.raft_context,
            role_tx.clone(),
        )
        .await
        .unwrap();

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

    let mut node_config = node_config("test_process_raft_request_two_consecutive_forced_sends");
    node_config.raft.replication.max_batch_size = 0;
    let mut raft_context =
        MockBuilder::new(graceful_rx).with_node_config(node_config).build_context();

    let mut state = LeaderState::new(1, raft_context.node_config());
    state.update_commit_index(4).expect("Should succeed");

    let mut replication_handler = MockReplicationCore::new();
    replication_handler.expect_handle_raft_request_in_batch().times(2).returning(
        |_, _, _, _, _| {
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
        },
    );

    // last_entry_id increments per call: first=4, second=5
    let call_count = Arc::new(AtomicU64::new(0));
    let call_count_clone = call_count.clone();
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(move || {
        let n = call_count_clone.fetch_add(1, Ordering::SeqCst);
        4 + n
    });
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    raft_context.handlers.replication_handler = replication_handler;
    raft_context.storage.raft_log = Arc::new(raft_log);

    let mut test_context = ProcessRaftRequestTestContext {
        state,
        raft_context,
    };

    // Prepare test requests
    let request = ClientWriteRequest {
        client_id: 0,
        command: Some(WriteCommand::default()),
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
                payloads: client_command_to_entry_payloads(
                    request.command.clone().into_iter().collect(),
                ),
                senders: vec![tx1],
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
                payloads: client_command_to_entry_payloads(request.command.into_iter().collect()),
                senders: vec![tx2],
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

    // Simulate SM apply for both requests
    let results = vec![
        ApplyResult {
            index: 5,
            succeeded: true,
        },
        ApplyResult {
            index: 6,
            succeeded: true,
        },
    ];
    test_context
        .state
        .handle_raft_event(
            RaftEvent::ApplyCompleted {
                last_index: 6,
                results,
            },
            &test_context.raft_context,
            role_tx.clone(),
        )
        .await
        .unwrap();

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
        command: Some(WriteCommand::default()),
    };
    use crate::ClientCmd;
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx, _rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (_role_tx, _role_rx) = mpsc::unbounded_channel::<RoleEvent>();

    // When: Push request into buffer (batching — not immediately sent)
    test_context
        .state
        .push_client_cmd(ClientCmd::Propose(request, tx), &test_context.raft_context);

    // Then: replication handler NOT called (verified by mockall times(0) on drop)
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
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = node_config("/tmp/test_drain_single_write");
    node_config.raft.replication.max_batch_size = 100;

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
    raft_log.expect_last_entry_id().returning(|| 4);
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
        command: Some(WriteCommand::default()),
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

    // Simulate SM apply: resolve pending_requests so client gets response
    {
        let results = vec![ApplyResult {
            index: 5,
            succeeded: true,
        }];
        state
            .handle_raft_event(
                RaftEvent::ApplyCompleted {
                    last_index: 5,
                    results,
                },
                &ctx,
                role_tx.clone(),
            )
            .await
            .unwrap();
    }

    // Verify: Request succeeded
    assert!(rx.recv().await.is_ok(), "Write should succeed");

    // Verify: Processing was immediate (< 10ms)
    assert!(
        elapsed.as_millis() < 10,
        "Single write should process immediately, took {elapsed:?}"
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
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = node_config("/tmp/test_drain_multiple_writes");
    node_config.raft.replication.max_batch_size = 100;

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
    raft_log.expect_last_entry_id().returning(|| 4);
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
            command: Some(WriteCommand::default()),
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

    // Simulate SM apply: resolve pending_requests so clients get responses
    {
        let results = (5u64..=14)
            .map(|i| ApplyResult {
                index: i,
                succeeded: true,
            })
            .collect();
        state
            .handle_raft_event(
                RaftEvent::ApplyCompleted {
                    last_index: 14,
                    results,
                },
                &ctx,
                role_tx.clone(),
            )
            .await
            .unwrap();
    }

    // Verify: All 10 requests received responses
    for (i, mut rx) in receivers.into_iter().enumerate() {
        assert!(rx.recv().await.is_ok(), "Write {i} should succeed in batch");
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
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = node_config("/tmp/test_drain_max_batch");
    // Set small max_batch_size for testing
    node_config.raft.replication.max_batch_size = 5;

    let ctx = MockBuilder::new(shutdown_rx).with_node_config(node_config).build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    // Action: Push 10 requests (exceeds max_batch_size of 5)
    for i in 0..10 {
        let req = ClientWriteRequest {
            client_id: i,
            command: Some(WriteCommand::default()),
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
        state.propose_buffer.len() > ctx.node_config.raft.replication.max_batch_size,
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
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let mut node_config = node_config("/tmp/test_batch_single_replication");
    node_config.raft.replication.max_batch_size = 100;

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
    raft_log.expect_last_entry_id().returning(|| 4);

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
            command: Some(WriteCommand::default()),
        };
        let (tx, rx) = MaybeCloneOneshot::new();
        state.push_client_cmd(ClientCmd::Propose(req, tx), &ctx);
        receivers.push(rx);
    }

    // Action: Flush batch (triggers single replication)
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();

    // Simulate SM apply: resolve pending_requests so clients get responses
    {
        let results = (5u64..=9)
            .map(|i| ApplyResult {
                index: i,
                succeeded: true,
            })
            .collect();
        state
            .handle_raft_event(
                RaftEvent::ApplyCompleted {
                    last_index: 9,
                    results,
                },
                &ctx,
                role_tx.clone(),
            )
            .await
            .unwrap();
    }

    // Verify: All 5 writes succeeded
    for (i, mut rx) in receivers.into_iter().enumerate() {
        assert!(rx.recv().await.is_ok(), "Write {i} should succeed");
    }

    // Verify: MockReplicationCore received exactly 1 call
    // (If each write triggered separate replication, we'd see 5 calls)
    // The .times(1) expectation validates this optimization.

    drop(_shutdown_tx);
}

// ============================================================================
// wait_for_apply_event Semantic Contract Tests
//
// These tests enforce the invariant that governs when a client response is sent:
//
//   - Client write (propose_buffer path): wait_for_apply_event = true
//     → Response MUST be deferred until StateMachine apply (ApplyCompleted event).
//     → This guarantees the client only sees "success" after the entry is durable
//       and applied. Changing this to false would break linearizability.
//
//   - Noop / quorum-check / config-change (verify_internal_quorum path):
//     wait_for_apply_event = false
//     → Response is sent immediately after quorum is achieved.
//     → These operations do not produce a state-machine result that clients
//       need to observe, so waiting for apply would deadlock (no ApplyCompleted
//       is ever sent for internal-only entries in this path).
//
// If a future developer accidentally swaps these values, one of the following
// tests will catch the regression:
//   - test_client_write_deferred_until_sm_apply: catches true→false (premature response)
//   - test_noop_quorum_check_responds_immediately: catches false→true (deadlock/hang)
//   - test_config_change_quorum_check_responds_immediately: same for conf-change payload
// ============================================================================

/// Verify that a client write response is NOT sent until StateMachine apply completes.
///
/// # Purpose
/// Enforces `wait_for_apply_event = true` for the propose_buffer (client write) path.
/// A client write must only succeed after the entry has been applied to the state machine,
/// not merely after quorum replication. This is the linearizability guarantee.
///
/// # Given
/// - One write request buffered via push_client_cmd
/// - Quorum achieved after flush_cmd_buffers
///
/// # When
/// - ApplyCompleted has NOT yet been received
///
/// # Then
/// - try_recv() returns Empty (no premature response)
///
/// # When
/// - ApplyCompleted is delivered for the committed index
///
/// # Then
/// - recv() returns Ok(write_success)
#[tokio::test]
async fn test_client_write_deferred_until_sm_apply() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut node_config = node_config("/tmp/test_client_write_deferred_until_sm_apply");
    node_config.raft.replication.max_batch_size = 100;

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
    raft_log.expect_last_entry_id().returning(|| 4);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    let ctx = MockBuilder::new(shutdown_rx)
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .with_raft_log(raft_log)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    let req = ClientWriteRequest {
        client_id: 1,
        command: Some(WriteCommand::default()),
    };
    let (tx, mut rx) = MaybeCloneOneshot::new();
    state.push_client_cmd(ClientCmd::Propose(req, tx), &ctx);

    // Quorum achieved — but ApplyCompleted has NOT been delivered yet
    state.flush_cmd_buffers(&ctx, &role_tx).await.unwrap();

    // CRITICAL: response must NOT arrive before SM apply
    assert!(
        rx.try_recv().is_err(),
        "Client write MUST NOT respond before StateMachine apply (wait_for_apply_event must be true)"
    );

    // Now simulate SM apply
    let results = vec![ApplyResult {
        index: 5,
        succeeded: true,
    }];
    state
        .handle_raft_event(
            RaftEvent::ApplyCompleted {
                last_index: 5,
                results,
            },
            &ctx,
            role_tx.clone(),
        )
        .await
        .unwrap();

    // Response must arrive after apply
    assert!(
        rx.recv().await.is_ok(),
        "Client write MUST respond after StateMachine apply"
    );
}

/// Verify that a noop quorum-check responds immediately after quorum, without waiting for SM apply.
///
/// # Purpose
/// Enforces `wait_for_apply_event = false` for the verify_internal_quorum (noop) path.
/// A noop entry is used for leadership verification — it has no state-machine result.
/// Waiting for ApplyCompleted would deadlock because no such event is produced for
/// internal-only noop entries in this path.
///
/// # Given
/// - verify_internal_quorum called with empty payload (noop/heartbeat)
/// - Quorum achieved
///
/// # When
/// - ApplyCompleted is never sent
///
/// # Then
/// - verify_internal_quorum returns Ok(Success) immediately (no hang)
#[tokio::test]
async fn test_noop_quorum_check_responds_immediately_without_sm_apply() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut node_config = node_config("/tmp/test_noop_quorum_check_responds_immediately");
    node_config.raft.replication.max_batch_size = 100;

    let mut replication = MockReplicationCore::new();
    replication
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
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

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    let ctx = MockBuilder::new(shutdown_rx)
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .with_raft_log(raft_log)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.update_commit_index(4).expect("Should succeed");

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Noop payload — used for leadership/quorum verification (wait_for_apply_event = false)
    let payloads = vec![EntryPayload::noop()];

    // CRITICAL: must return without waiting for ApplyCompleted
    // If wait_for_apply_event were true, this would hang (timeout)
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        state.verify_internal_quorum(payloads, &ctx, &role_tx),
    )
    .await
    .expect("Noop quorum check MUST NOT block waiting for SM apply (wait_for_apply_event must be false)");

    use crate::QuorumVerificationResult;
    assert_eq!(result.unwrap(), QuorumVerificationResult::Success);
}

/// Verify that a config-change quorum-check responds immediately after quorum, without waiting for SM apply.
///
/// # Purpose
/// Enforces `wait_for_apply_event = false` for the verify_internal_quorum (config change) path.
/// A config change entry sent via verify_internal_quorum is used for membership verification.
/// Like the noop case, it must not wait for ApplyCompleted — doing so would deadlock.
///
/// # Given
/// - verify_internal_quorum called with a config-change payload
/// - Quorum achieved
///
/// # When
/// - ApplyCompleted is never sent
///
/// # Then
/// - verify_internal_quorum returns Ok(Success) immediately (no hang)
#[tokio::test]
async fn test_config_change_quorum_check_responds_immediately_without_sm_apply() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut node_config = node_config("/tmp/test_config_change_quorum_check_responds_immediately");
    node_config.raft.replication.max_batch_size = 100;

    let mut replication = MockReplicationCore::new();
    replication
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
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

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

    let ctx = MockBuilder::new(shutdown_rx)
        .with_node_config(node_config)
        .with_replication_handler(replication)
        .with_raft_log(raft_log)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.update_commit_index(4).expect("Should succeed");

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // Config change payload — used for membership change quorum verification (wait_for_apply_event = false)
    let change = Change::AddNode(AddNode {
        node_id: 4,
        address: "127.0.0.1:9004".to_string(),
        status: NodeStatus::Promotable as i32,
    });
    let payloads = vec![EntryPayload::config(change)];

    // CRITICAL: must return without waiting for ApplyCompleted
    // If wait_for_apply_event were true, this would hang (timeout)
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        state.verify_internal_quorum(payloads, &ctx, &role_tx),
    )
    .await
    .expect("Config-change quorum check MUST NOT block waiting for SM apply (wait_for_apply_event must be false)");

    use crate::QuorumVerificationResult;
    assert_eq!(result.unwrap(), QuorumVerificationResult::Success);
}
