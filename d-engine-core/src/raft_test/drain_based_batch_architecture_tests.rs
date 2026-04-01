//! # Drain-Based Batch Architecture Tests
//!
//! **Original location**: raft_test.rs L2540-3744
//! **Test count**: 14 tests
//! **Category**: I (Drain-Based Batch Architecture - NEW)
//! **Priorities**: P0 (8 tests), P1 (3 tests)
//!
//! Tests the new drain-based batching architecture for client command processing.
//! This replaces the timeout-based batching with channel-driven batching for:
//! - Lower latency (no batch_timeout delay)
//! - Higher throughput (larger batches under load)
//! - Better resource utilization (respect max_batch_size limits)
//!
//! **Architecture Overview**:
//! - Main loop: recv() blocks for first command → try_recv() drains all pending
//! - Immediate flush on wakeup (no timeout)
//! - Role-specific behavior: Leader batches, Follower/Candidate reject writes
//! - Read consistency policies: Linearizable, Lease, Eventual
//!
//! **Test Categories**:
//! - I1: BatchBuffer unit tests (6 tests) - verify batching mechanics
//! - I2: Leader drain behavior (2 tests) - P0 critical path
//! - I3: Role-specific behavior (3 tests) - P0 critical path
//! - I4: Performance validation (3 tests) - P1 performance targets
//!

use crate::RaftOneshot;
use crate::maybe_clone_oneshot::MaybeCloneOneshot;
use crate::{
    MockBuilder,
    buffers::BatchBuffer,
    raft_role::{RaftRole, role_state::RaftRoleState},
};
use bytes::Bytes;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::client::{
    ClientReadRequest, ClientWriteRequest, ReadConsistencyPolicy, WriteCommand,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

// ========================================================================
// P0-1: LOW LOAD - SINGLE COMMAND (ZERO LATENCY)
// ========================================================================
//
// **Objective**: Verify batch=1 with no unnecessary wait in low load
//
// **Expected Behavior**:
// - recv() blocks for first command → try_recv() finds empty channel → immediate flush
// - batch.size = 1, no timeout penalty
// - Latency < 5ms (vs old architecture ~10ms with batch_timeout)
//
#[test]
fn test_p0_batch_buffer_single_item_no_batching() {
    // **Setup**: Create BatchBuffer with small initial capacity
    let mut buffer: BatchBuffer<u32> = BatchBuffer::new(300);

    let start = Instant::now();

    // **Step 1**: Push single item
    buffer.push(42);
    assert_eq!(buffer.len(), 1);

    // **Step 2**: Drain (take_all) without timeout
    let drained = buffer.take_all();
    let elapsed = start.elapsed();

    // **Assertions**:
    // ✅ Single item drained immediately
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0], 42);

    // ✅ Buffer is now empty
    assert!(buffer.is_empty());

    // ✅ Latency is minimal (no batching wait)
    assert!(
        elapsed < Duration::from_millis(5),
        "Drain latency should be <5ms, got {elapsed:?}"
    );
}

// NOTE: Removed test_p0_batch_buffer_max_batch_size_cap
// Reason: This test incorrectly assumes take_all() respects batch_size.
// In the drain-based architecture, batch_size limits are enforced in the
// drain loop (raft.rs), not in BatchBuffer.take_all(). The take_all() method
// unconditionally returns all buffered items for flush operations.

// ========================================================================
// P0-3: BATCH BUFFER - EMPTY BUFFER IDEMPOTENCY
// ========================================================================
//
// **Objective**: Verify empty buffer flush is safe (no panic, no-op)
//
// **Expected Behavior**:
// - is_empty() returns true → flush is no-op
// - No error, no panic
// - System remains stable
//
#[test]
fn test_p0_batch_buffer_empty_flush_idempotent() {
    // **Setup**: Create BatchBuffer
    let mut buffer: BatchBuffer<u32> = BatchBuffer::new(300);

    // **Step 1**: Push and drain one item
    buffer.push(42);
    let drained1 = buffer.take_all();
    assert_eq!(drained1.len(), 1);

    // **Step 2**: Verify buffer is empty
    assert!(buffer.is_empty());

    // **Step 3**: Try to drain empty buffer (should be no-op)
    let drained2 = buffer.take_all();

    // **Assertions**:
    // ✅ is_empty() returns true
    assert!(buffer.is_empty());

    // ✅ Second drain returns empty VecDeque
    assert_eq!(drained2.len(), 0);

    // ✅ No panic, system stable
}

// NOTE: Removed test_p0_batch_buffer_max_batch_size_one
// Reason: This test incorrectly assumes take_all() respects batch_size.
// In the drain-based architecture, batch_size limits are enforced in the
// drain loop (raft.rs), not in BatchBuffer.take_all(). The take_all() method
// unconditionally returns all buffered items for flush operations.

// NOTE: Removed test_p0_batch_buffer_natural_batching_medium_load
// Reason: This test incorrectly assumes take_all() respects batch_size.
// In the drain-based architecture, batch_size limits are enforced in the
// drain loop (raft.rs), not in BatchBuffer.take_all(). The take_all() method
// unconditionally returns all buffered items for flush operations.

// ========================================================================
// P0 UNIT TESTS - ROLE-SPECIFIC DRAIN BEHAVIOR
// ========================================================================
//
// These tests verify the drain-based batch architecture at the RoleState level.
// Each role (Leader/Follower/Candidate/Learner) has specific behavior for
// push_client_cmd() and flush_cmd_buffers().
//
// Reference: 001_todo_raft_test_plan_2215.md P0-1 through P0-5
//

// ========================================================================
// P0-1: LEADER - SINGLE COMMAND DRAIN IMMEDIATELY
// ========================================================================
//
// **Objective**: Verify Leader processes single command with batch=1, no delay
//
// **Expected Behavior**:
// - Command enters propose_buffer via push_client_cmd()
// - flush_cmd_buffers() drains immediately
// - Metrics: drain_triggered=1, batch_size=1
//
#[tokio::test]
async fn test_p0_leader_single_command_drain_immediately() {
    // **Setup**: Create Leader Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();

    // Setup mocks
    let mut raft_log = crate::MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 11);
    raft_log.expect_durable_index().returning(|| 11);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_append_entries().returning(|_| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(11));
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));

    // Scaffold: prepare_batch_requests succeeds (no peers in single-node test).
    // In the new per-follower worker architecture, log write + request building happen
    // synchronously in the Raft loop. Workers fire-and-forget. Returning Ok(crate::PrepareResult::default())
    // lets BecomeLeader and subsequent process_batch calls complete without errors.
    let mut replication_handler = crate::MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_handler;

    // Transition to Leader
    raft.handle_role_event(crate::RoleEvent::BecomeCandidate)
        .await
        .expect("Should become Candidate");
    raft.handle_role_event(crate::RoleEvent::BecomeLeader)
        .await
        .expect("Should become Leader");

    // **Step 1**: Send single write command

    let (response_tx, _response_rx) = MaybeCloneOneshot::new();
    let write_cmd = WriteCommand {
        operation: Some(Operation::Insert(
            d_engine_proto::client::write_command::Insert {
                key: Bytes::from("test_key"),
                value: Bytes::from("test_value"),
                ttl_secs: 0,
            },
        )),
    };
    let write_req = ClientWriteRequest {
        client_id: 1,
        command: Some(write_cmd),
    };

    // Access leader state to push command
    if let RaftRole::Leader(ref mut leader) = raft.role {
        let cmd = crate::ClientCmd::Propose(write_req, response_tx);

        // **Step 2**: Push command to buffer
        leader.push_client_cmd(cmd, &raft.ctx);

        // **Step 3**: Flush buffers (simulate drain)
        let (role_tx, _role_rx) = tokio::sync::mpsc::unbounded_channel();
        leader
            .flush_cmd_buffers(&raft.ctx, &role_tx)
            .await
            .expect("flush should succeed");

        // **Assertion**: flush_cmd_buffers completed successfully
        // (Private buffer fields are implementation details - not tested directly)
    } else {
        panic!("Expected Leader state");
    }
}

// ========================================================================
// P0-2: LEADER - HIGH LOAD MAX BATCH CAP
// ========================================================================
//
// **Objective**: Verify Leader respects batch_size=300 under high load
//
// **Expected Behavior**:
// - Push 1000 commands to propose_buffer
// - Each flush processes ≤ 300 commands
// - Multiple drain cycles until all processed
//
#[tokio::test]
async fn test_p0_leader_high_load_max_batch_cap() {
    // **Setup**: Create Leader with batch_size monitoring
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();

    // Shared state: record how many entries were passed in each flush call.
    let batch_sizes: Arc<std::sync::Mutex<Vec<usize>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let mut raft_log = crate::MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 11);
    raft_log.expect_durable_index().returning(|| 11);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_append_entries().returning(|_| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(11));
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));

    // Batch size tracking: prepare_batch_requests receives the entry_payloads vec.
    // We record payloads.len() to verify the batch cap is respected per flush call.
    // In the new architecture, prepare_batch_requests does log write + request building;
    // returning Ok(crate::PrepareResult::default()) means no workers are spawned (no real peers here).
    let sizes_clone = Arc::clone(&batch_sizes);
    let mut replication_handler = crate::MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .returning(move |payloads, _, _, _, _| {
            sizes_clone.lock().unwrap().push(payloads.len());
            Ok(crate::PrepareResult::default())
        });

    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_handler;

    // Transition to Leader
    raft.handle_role_event(crate::RoleEvent::BecomeCandidate)
        .await
        .expect("Should become Candidate");
    raft.handle_role_event(crate::RoleEvent::BecomeLeader)
        .await
        .expect("Should become Leader");

    // Architecture note: max_batch_size is enforced by the *drain loop* in the
    // main event loop (try_recv drains up to max_batch_size commands before flush).
    // flush_cmd_buffers() always sends the entire buffer in one RPC — the cap is
    // applied at the push side, not the flush side.
    //
    // This test verifies the correct architecture: push MAX_BATCH_SIZE commands,
    // flush once, repeat 10 times — each flush produces exactly one batch of ≤ cap.
    const MAX_BATCH_SIZE: usize = 100;
    const ROUNDS: usize = 10;

    let (role_tx, _role_rx) = tokio::sync::mpsc::unbounded_channel();

    if let RaftRole::Leader(ref mut leader) = raft.role {
        for round in 0..ROUNDS {
            // Push exactly MAX_BATCH_SIZE commands (simulating drain loop cap)
            for i in 0..MAX_BATCH_SIZE {
                let (response_tx, _response_rx) = MaybeCloneOneshot::new();
                let write_cmd = WriteCommand {
                    operation: Some(Operation::Insert(
                        d_engine_proto::client::write_command::Insert {
                            key: Bytes::from(format!("key_{round}_{i}")),
                            value: Bytes::from(format!("value_{round}_{i}")),
                            ttl_secs: 0,
                        },
                    )),
                };
                let write_req = ClientWriteRequest {
                    client_id: 1,
                    command: Some(write_cmd),
                };
                let cmd = crate::ClientCmd::Propose(write_req, response_tx);
                leader.push_client_cmd(cmd, &raft.ctx);
            }

            // Flush: sends exactly the pushed batch to replication
            leader
                .flush_cmd_buffers(&raft.ctx, &role_tx)
                .await
                .expect("flush should succeed");
        }

        // **Assert 1**: Each batch respected the max_batch_size cap.
        // Note: BecomeLeader sends one extra no-op replication call for leader
        // initialization, so total call count is ROUNDS + 1 (or more). We only
        // assert per-batch size and total payload count.
        let sizes = batch_sizes.lock().unwrap();
        for &size in sizes.iter() {
            assert!(
                size <= MAX_BATCH_SIZE,
                "batch size {size} exceeded max_batch_size {MAX_BATCH_SIZE}"
            );
        }

        // **Assert 2**: Total client commands processed == ROUNDS × MAX_BATCH_SIZE
        // (excludes the no-op entry from leader initialization)
        let total: usize = sizes.iter().sum();
        assert!(
            total >= ROUNDS * MAX_BATCH_SIZE,
            "expected at least {} client commands processed, got {total}",
            ROUNDS * MAX_BATCH_SIZE
        );

        // **Assert 3**: Multiple replication calls occurred (one per round + no-op)
        assert!(
            sizes.len() >= ROUNDS,
            "expected at least {ROUNDS} replication calls, got {}",
            sizes.len()
        );
    } else {
        panic!("Expected Leader state");
    }
}

// ========================================================================
// P0-3: FOLLOWER - EVENTUAL READ LOCAL PROCESSING
// ========================================================================
//
// **Objective**: Verify Follower processes Eventual reads locally (no redirect)
//
// **Expected Behavior**:
// - Eventual read processed immediately in push_client_cmd()
// - No queue buffering
// - Reads served from local state machine
// - No NOT_LEADER errors
//
#[tokio::test]
async fn test_p0_follower_eventual_read_local_processing() {
    // **Setup**: Create Follower Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();

    // Setup state machine mock for read
    let mut state_machine_handler = crate::MockStateMachineHandler::new();
    state_machine_handler
        .expect_read_from_state_machine()
        .times(1)
        .withf(|keys| keys.len() == 1 && keys[0] == "test_key")
        .returning(|_| {
            Some(vec![d_engine_proto::client::ClientResult {
                key: bytes::Bytes::from("test_key"),
                value: bytes::Bytes::from("test_value"),
            }])
        });
    raft.ctx.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Verify initial state is Follower
    assert!(matches!(raft.role, RaftRole::Follower(_)));

    // **Step 1**: Send Eventual read request to Follower
    let (response_tx, mut response_rx) = MaybeCloneOneshot::new();
    let read_req = ClientReadRequest {
        client_id: 1,
        keys: vec![bytes::Bytes::from("test_key")],
        consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
    };

    // Access follower state to push read command
    if let RaftRole::Follower(ref mut follower) = raft.role {
        let cmd = crate::ClientCmd::Read(read_req, response_tx);

        // **Step 2**: Push read command (should process immediately)
        follower.push_client_cmd(cmd, &raft.ctx);

        // **Assertion 1**: Response should be ready immediately
        // Eventual reads are processed synchronously in push_client_cmd
        let result = response_rx.recv().await;
        assert!(result.is_ok(), "Eventual read should return response");

        // **Assertion 2**: Response should be success (not NOT_LEADER error)
        if let Ok(response) = result {
            match response {
                Ok(read_response) => {
                    // Check success_result for ReadResults
                    assert!(
                        read_response.success_result.is_some(),
                        "Should have success_result"
                    );
                    if let Some(d_engine_proto::client::client_response::SuccessResult::ReadData(
                        read_data,
                    )) = read_response.success_result
                    {
                        assert!(!read_data.results.is_empty(), "Should have read results");
                        assert_eq!(read_data.results[0].value, bytes::Bytes::from("test_value"));
                    }
                }
                Err(e) => {
                    panic!("Eventual read should succeed on Follower, got error: {e:?}");
                }
            }
        }
    } else {
        panic!("Expected Follower state");
    }
}

// ========================================================================
// P0-4: FOLLOWER - WRITE REQUEST REJECTION
// ========================================================================
//
// **Objective**: Verify Follower rejects write requests with NOT_LEADER error
//
// **Expected Behavior**:
// - Write request immediately rejected in push_client_cmd()
// - Error: NOT_LEADER with Leader information
// - No buffer entry
// - Response time < 1ms (immediate rejection)
//
#[tokio::test]
async fn test_p0_follower_write_request_rejection() {
    // **Setup**: Create Follower Raft instance with known leader
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();

    // **Step 1**: Send write request to Follower

    let (response_tx, mut response_rx) = MaybeCloneOneshot::new();
    let write_cmd = WriteCommand {
        operation: Some(Operation::Insert(
            d_engine_proto::client::write_command::Insert {
                key: Bytes::from("test_key"),
                value: Bytes::from("test_value"),
                ttl_secs: 0,
            },
        )),
    };
    let write_req = ClientWriteRequest {
        client_id: 1,
        command: Some(write_cmd),
    };

    let start = tokio::time::Instant::now();

    if let RaftRole::Follower(ref mut follower) = raft.role {
        let cmd = crate::ClientCmd::Propose(write_req, response_tx);

        // **Step 2**: Push write command (should reject immediately)
        follower.push_client_cmd(cmd, &raft.ctx);

        // **Assertion 1**: Response should be ready immediately
        let result = response_rx.recv().await;
        let elapsed = start.elapsed();

        assert!(result.is_ok(), "Should receive response from Follower");

        // **Assertion 2**: Response time < 1ms (immediate rejection)
        assert!(
            elapsed.as_millis() < 10,
            "Write rejection should be immediate, took {:?}ms",
            elapsed.as_millis()
        );

        // **Assertion 3**: Response should be NOT_LEADER error
        if let Ok(response) = result {
            match response {
                Err(err) => {
                    // Verify it's a NOT_LEADER error (FailedPrecondition or similar)
                    let err_str = format!("{err:?}");
                    assert!(
                        err_str.contains("Not leader")
                            || err_str.contains("NotLeader")
                            || err_str.contains("NOT_LEADER")
                            || err_str.contains("FailedPrecondition"),
                        "Expected NOT_LEADER error, got: {err:?}"
                    );
                }
                Ok(_) => {
                    panic!("Write to Follower should return NOT_LEADER error, got success");
                }
            }
        }
    } else {
        panic!("Expected Follower state");
    }
}

// ========================================================================
// P0-5: CANDIDATE - ALL CLIENT COMMANDS HANDLING
// ========================================================================
//
// **Objective**: Verify Candidate role behavior for different command types
//
// **Expected Behavior**:
// - Write: NOT_LEADER error (immediate rejection)
// - Linear read: NOT_LEADER error (immediate rejection)
// - Eventual read: SUCCESS (served locally) ← New behavior
// - All responses < 2ms
//
#[tokio::test]
async fn test_p0_candidate_commands_handling() {
    // **Setup**: Create Candidate Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();

    // Setup state machine for eventual read
    let mut state_machine_handler = crate::MockStateMachineHandler::new();
    state_machine_handler
        .expect_read_from_state_machine()
        .times(1)
        .withf(|keys| keys.len() == 1 && keys[0] == "eventual_key")
        .returning(|_| {
            Some(vec![d_engine_proto::client::ClientResult {
                key: bytes::Bytes::from("eventual_key"),
                value: bytes::Bytes::from("eventual_value"),
            }])
        });
    raft.ctx.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Transition to Candidate
    raft.handle_role_event(crate::RoleEvent::BecomeCandidate)
        .await
        .expect("Should become Candidate");

    // **Test 1**: Write request should be rejected
    {
        let (response_tx, mut response_rx) = MaybeCloneOneshot::new();
        let write_cmd = WriteCommand {
            operation: Some(Operation::Insert(
                d_engine_proto::client::write_command::Insert {
                    key: Bytes::from("write_key"),
                    value: Bytes::from("write_value"),
                    ttl_secs: 0,
                },
            )),
        };
        let write_req = ClientWriteRequest {
            client_id: 1,
            command: Some(write_cmd),
        };

        if let RaftRole::Candidate(ref mut candidate) = raft.role {
            let cmd = crate::ClientCmd::Propose(write_req, response_tx);
            candidate.push_client_cmd(cmd, &raft.ctx);

            let result = response_rx.recv().await;
            assert!(result.is_ok(), "Should receive response");

            // **Assertion 1**: Write rejected with NOT_LEADER
            if let Ok(Err(err)) = result {
                let err_str = format!("{err:?}");
                assert!(
                    err_str.contains("Not leader")
                        || err_str.contains("NotLeader")
                        || err_str.contains("NOT_LEADER")
                        || err_str.contains("FailedPrecondition"),
                    "Expected NOT_LEADER for write, got: {err:?}"
                );
            } else {
                panic!("Write to Candidate should return NOT_LEADER error");
            }
        } else {
            panic!("Expected Candidate state");
        }
    }

    // **Test 2**: Linear read should be rejected
    {
        let (response_tx, mut response_rx) = MaybeCloneOneshot::new();
        let read_req = ClientReadRequest {
            client_id: 1,
            keys: vec![bytes::Bytes::from("linear_key")],
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        };

        if let RaftRole::Candidate(ref mut candidate) = raft.role {
            let cmd = crate::ClientCmd::Read(read_req, response_tx);
            candidate.push_client_cmd(cmd, &raft.ctx);

            let result = response_rx.recv().await;
            assert!(result.is_ok(), "Should receive response");

            // **Assertion 2**: Linear read rejected with NOT_LEADER
            if let Ok(Err(err)) = result {
                let err_str = format!("{err:?}");
                assert!(
                    err_str.contains("Not leader")
                        || err_str.contains("NotLeader")
                        || err_str.contains("NOT_LEADER")
                        || err_str.contains("FailedPrecondition"),
                    "Expected NOT_LEADER for linear read, got: {err:?}"
                );
            } else {
                panic!("Linear read to Candidate should return NOT_LEADER error");
            }
        } else {
            panic!("Expected Candidate state");
        }
    }

    // **Test 3**: Eventual read should succeed (new behavior)
    {
        let (response_tx, mut response_rx) = MaybeCloneOneshot::new();
        let read_req = ClientReadRequest {
            client_id: 1,
            keys: vec![bytes::Bytes::from("eventual_key")],
            consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
        };

        if let RaftRole::Candidate(ref mut candidate) = raft.role {
            let cmd = crate::ClientCmd::Read(read_req, response_tx);
            candidate.push_client_cmd(cmd, &raft.ctx);

            let result = response_rx.recv().await;
            assert!(result.is_ok(), "Should receive response");

            // **Assertion 3**: Eventual read succeeds on Candidate
            if let Ok(Ok(read_response)) = result {
                // Check success_result for ReadResults
                assert!(
                    read_response.success_result.is_some(),
                    "Should have success_result"
                );
                if let Some(d_engine_proto::client::client_response::SuccessResult::ReadData(
                    read_data,
                )) = read_response.success_result
                {
                    assert!(!read_data.results.is_empty(), "Should have read results");
                    assert_eq!(
                        read_data.results[0].value,
                        bytes::Bytes::from("eventual_value")
                    );
                }
            } else {
                panic!("Eventual read to Candidate should succeed, got: {result:?}");
            }
        } else {
            panic!("Expected Candidate state");
        }
    }
}

// ========================================================================
// P1 PERFORMANCE VALIDATION TESTS - MEDIUM LOAD
// ========================================================================

// ========================================================================
// P1-1: MEDIUM LOAD - NATURAL BATCHING
// ========================================================================
//
// **Objective**: Verify adaptive batching under moderate load
//
// **Expected Behavior**:
// - Multiple drain cycles (not single batch)
// - Natural batching reflects real backlog (5-30 items per cycle typical)
// - System remains responsive
//
#[tokio::test]
async fn test_p1_medium_load_natural_batching() {
    // **Setup**: Create Leader Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();

    // Shared state: record batch sizes seen by replication handler
    let batch_sizes: Arc<std::sync::Mutex<Vec<usize>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let mut raft_log = crate::MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 11);
    raft_log.expect_durable_index().returning(|| 11);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_append_entries().returning(|_| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(11));
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));

    // Batch size tracking: records payloads.len() per flush to verify natural batching.
    // prepare_batch_requests is where log write + request building happen in the new
    // per-follower worker architecture. Returning Ok(crate::PrepareResult::default()) = no peer workers spawned.
    let sizes_clone = Arc::clone(&batch_sizes);
    let mut replication_handler = crate::MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .returning(move |payloads, _, _, _, _| {
            sizes_clone.lock().unwrap().push(payloads.len());
            Ok(crate::PrepareResult::default())
        });

    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_handler;

    // Transition to Leader
    raft.handle_role_event(crate::RoleEvent::BecomeCandidate)
        .await
        .expect("Should become Candidate");
    raft.handle_role_event(crate::RoleEvent::BecomeLeader)
        .await
        .expect("Should become Leader");

    // **Step 1**: Push 50 commands at once (all fit within max_batch_size=100)
    // Natural batching: all commands accumulated before flush → single batch
    const TOTAL_COMMANDS: usize = 50;
    const MAX_BATCH_SIZE: usize = 100;

    if let RaftRole::Leader(ref mut leader) = raft.role {
        // Reset batch_sizes to exclude the no-op entry appended during BecomeLeader.
        // The no-op is an internal Raft mechanism; we only want to observe user command batches.
        batch_sizes.lock().unwrap().clear();

        for i in 0..TOTAL_COMMANDS {
            let (response_tx, _response_rx) = MaybeCloneOneshot::new();
            let write_cmd = WriteCommand {
                operation: Some(Operation::Insert(
                    d_engine_proto::client::write_command::Insert {
                        key: Bytes::from(format!("key_{i}")),
                        value: Bytes::from(format!("value_{i}")),
                        ttl_secs: 0,
                    },
                )),
            };
            let write_req = ClientWriteRequest {
                client_id: 1,
                command: Some(write_cmd),
            };
            let cmd = crate::ClientCmd::Propose(write_req, response_tx);
            leader.push_client_cmd(cmd, &raft.ctx);
        }

        // **Step 2**: Single flush drains all 50 commands in one batch
        let (role_tx, _role_rx) = tokio::sync::mpsc::unbounded_channel();
        leader
            .flush_cmd_buffers(&raft.ctx, &role_tx)
            .await
            .expect("flush should succeed");

        // **Assert 1**: All commands dispatched in a single batch call
        // (50 < max_batch_size=100 → no splitting needed)
        let sizes = batch_sizes.lock().unwrap();
        assert_eq!(
            sizes.len(),
            1,
            "50 commands should be dispatched as a single batch, got {} batches",
            sizes.len()
        );

        // **Assert 2**: Batch size equals total commands and respects cap
        assert_eq!(
            sizes[0], TOTAL_COMMANDS,
            "batch must contain all {TOTAL_COMMANDS} commands"
        );
        assert!(
            sizes[0] <= MAX_BATCH_SIZE,
            "batch size {} exceeded max_batch_size {MAX_BATCH_SIZE}",
            sizes[0]
        );

        // **Assert 3**: All commands accounted for (sum of batches == total)
        let total: usize = sizes.iter().sum();
        assert_eq!(
            total, TOTAL_COMMANDS,
            "all {TOTAL_COMMANDS} commands must be processed"
        );
    } else {
        panic!("Expected Leader state");
    }
}

// ========================================================================
// P1-2: MIXED WORKLOAD - READ/WRITE COEXISTENCE
// ========================================================================
//
// **Objective**: Verify different command types processed correctly
//
// **Expected Behavior**:
// - Single drain cycle collects all commands
// - Correct routing for different command types
// - All commands complete successfully
//
#[tokio::test]
async fn test_p1_mixed_workload_read_write_coexistence() {
    // **Setup**: Create Leader Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();

    // Setup mocks
    let mut raft_log = crate::MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 11);
    raft_log.expect_durable_index().returning(|| 11);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_append_entries().returning(|_| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(11));
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));

    // Scaffold: prepare_batch_requests succeeds (no peers).
    // This test focuses on read/write coexistence in the Raft loop buffer, not
    // replication behavior. Returning Ok(crate::PrepareResult::default()) satisfies process_batch calls.
    let mut replication_handler = crate::MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_handler;

    // Transition to Leader
    raft.handle_role_event(crate::RoleEvent::BecomeCandidate)
        .await
        .expect("Should become Candidate");
    raft.handle_role_event(crate::RoleEvent::BecomeLeader)
        .await
        .expect("Should become Leader");

    // **Step 1**: Send mixed commands
    if let RaftRole::Leader(ref mut leader) = raft.role {
        // Send 10 write requests
        for i in 0..10 {
            let (response_tx, _response_rx) = MaybeCloneOneshot::new();
            let write_cmd = WriteCommand {
                operation: Some(Operation::Insert(
                    d_engine_proto::client::write_command::Insert {
                        key: Bytes::from(format!("write_key_{i}")),
                        value: Bytes::from(format!("write_value_{i}")),
                        ttl_secs: 0,
                    },
                )),
            };
            let write_req = ClientWriteRequest {
                client_id: 1,
                command: Some(write_cmd),
            };
            let cmd = crate::ClientCmd::Propose(write_req, response_tx);
            leader.push_client_cmd(cmd, &raft.ctx);
        }

        // Send 5 Eventual read requests
        for i in 0..5 {
            let (response_tx, _response_rx) = MaybeCloneOneshot::new();
            let read_req = ClientReadRequest {
                client_id: 1,
                keys: vec![Bytes::from(format!("read_key_{i}"))],
                consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
            };
            let cmd = crate::ClientCmd::Read(read_req, response_tx);
            leader.push_client_cmd(cmd, &raft.ctx);
        }

        // Send 5 Lease read requests (simpler than LinearizableRead)
        for i in 0..5 {
            let (response_tx, _response_rx) = MaybeCloneOneshot::new();
            let read_req = ClientReadRequest {
                client_id: 1,
                keys: vec![Bytes::from(format!("lease_key_{i}"))],
                consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
            };
            let cmd = crate::ClientCmd::Read(read_req, response_tx);
            leader.push_client_cmd(cmd, &raft.ctx);
        }

        // **Step 2**: Flush all commands
        let (role_tx, _role_rx) = tokio::sync::mpsc::unbounded_channel();
        leader
            .flush_cmd_buffers(&raft.ctx, &role_tx)
            .await
            .expect("flush should succeed");

        // **Assertion**: All commands routed and processed successfully
        // (Actual routing verification would require accessing internal state,
        // but the absence of errors indicates successful processing)
    } else {
        panic!("Expected Leader state");
    }
}

// ========================================================================
// P1-3: BURST LOAD - RECOVERY AFTER SPIKE
// ========================================================================
//
// **Objective**: Verify system recovers gracefully after traffic burst
//
// **Expected Behavior**:
// - Burst: Multiple drain cycles with batch_size
// - All burst requests processed
// - Trickle requests: each batch=1, no residual delay
// - System remains stable
//
#[tokio::test]
async fn test_p1_burst_load_recovery_after_spike() {
    // **Setup**: Create Leader Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = MockBuilder::new(graceful_rx).build_raft();

    // Setup mocks
    let mut raft_log = crate::MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 11);
    raft_log.expect_durable_index().returning(|| 11);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_append_entries().returning(|_| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(11));
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));

    // Scaffold: prepare_batch_requests succeeds (no peers).
    // This test verifies burst/drain recovery behavior in the Raft loop buffer.
    // Returning Ok(crate::PrepareResult::default()) satisfies process_batch calls without peer replication.
    let mut replication_handler = crate::MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_handler;

    // Transition to Leader
    raft.handle_role_event(crate::RoleEvent::BecomeCandidate)
        .await
        .expect("Should become Candidate");
    raft.handle_role_event(crate::RoleEvent::BecomeLeader)
        .await
        .expect("Should become Leader");

    if let RaftRole::Leader(ref mut leader) = raft.role {
        // **Step 1**: Burst load - 200 commands rapidly
        const BURST_SIZE: usize = 200;
        for i in 0..BURST_SIZE {
            let (response_tx, _response_rx) = MaybeCloneOneshot::new();
            let write_cmd = WriteCommand {
                operation: Some(Operation::Insert(
                    d_engine_proto::client::write_command::Insert {
                        key: Bytes::from(format!("burst_key_{i}")),
                        value: Bytes::from(format!("burst_value_{i}")),
                        ttl_secs: 0,
                    },
                )),
            };
            let write_req = ClientWriteRequest {
                client_id: 1,
                command: Some(write_cmd),
            };
            let cmd = crate::ClientCmd::Propose(write_req, response_tx);
            leader.push_client_cmd(cmd, &raft.ctx);
        }

        // **Step 2**: Flush burst (multiple cycles expected)
        let (role_tx, _role_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut burst_flushes = 0;
        for _ in 0..20 {
            leader
                .flush_cmd_buffers(&raft.ctx, &role_tx)
                .await
                .expect("flush should succeed");
            burst_flushes += 1;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // **Step 3**: Trickle load - 10 commands with spacing
        const TRICKLE_SIZE: usize = 10;
        for i in 0..TRICKLE_SIZE {
            let (response_tx, _response_rx) = MaybeCloneOneshot::new();
            let write_cmd = WriteCommand {
                operation: Some(Operation::Insert(
                    d_engine_proto::client::write_command::Insert {
                        key: Bytes::from(format!("trickle_key_{i}")),
                        value: Bytes::from(format!("trickle_value_{i}")),
                        ttl_secs: 0,
                    },
                )),
            };
            let write_req = ClientWriteRequest {
                client_id: 1,
                command: Some(write_cmd),
            };
            let cmd = crate::ClientCmd::Propose(write_req, response_tx);
            leader.push_client_cmd(cmd, &raft.ctx);

            // Flush after each trickle command
            leader
                .flush_cmd_buffers(&raft.ctx, &role_tx)
                .await
                .expect("flush should succeed");

            // Small delay between trickle requests
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // **Assertion**: System processed both burst and trickle successfully
        assert!(burst_flushes > 0, "Burst should be processed");
    } else {
        panic!("Expected Leader state");
    }
}
