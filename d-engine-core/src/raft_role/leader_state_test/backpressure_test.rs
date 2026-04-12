use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::ReadConsistencyPolicy;
use d_engine_proto::client::WriteCommand;
use tokio::sync::watch;

use crate::BackpressureConfig;
use crate::ClientCmd;
use crate::MaybeCloneOneshot;
use crate::RaftOneshot;
use crate::raft_role::leader_state::LeaderState;
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;

/// Test: Leader rejects write requests when buffer is full
///
/// Scenario:
/// - Configure backpressure: max_pending_writes = 2
/// - Push 2 write requests (should succeed)
/// - Push 3rd write request (should be rejected with RESOURCE_EXHAUSTED)
#[tokio::test]
async fn test_backpressure_write_limit_enforcement() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_backpressure_write", shutdown_rx, None);

    // Configure strict write backpressure
    let mut config = (*ctx.node_config).clone();
    config.raft.backpressure = BackpressureConfig {
        max_pending_writes: 2,
        max_pending_reads: 100,
    };
    ctx.node_config = std::sync::Arc::new(config);

    let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    // Push first two writes - should succeed
    for i in 1..=2 {
        let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
        let cmd = ClientCmd::Propose(
            ClientWriteRequest {
                client_id: i,
                command: Some(WriteCommand::default()),
            },
            resp_tx,
        );
        leader.push_client_cmd(cmd, &ctx);
    }

    assert_eq!(
        leader.propose_buffer.len(),
        2,
        "Should have 2 pending writes"
    );

    // Push third write - should be rejected
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Propose(
        ClientWriteRequest {
            client_id: 3,
            command: Some(WriteCommand::default()),
        },
        resp_tx,
    );
    leader.push_client_cmd(cmd, &ctx);

    // Verify rejection
    let response = resp_rx.recv().await.unwrap();
    let err = response.unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::ResourceExhausted,
        "Should reject with ResourceExhausted"
    );
    assert_eq!(
        leader.propose_buffer.len(),
        2,
        "Buffer should still have 2 items"
    );
}

/// Test: Leader rejects linearizable read requests when buffer is full
///
/// Scenario:
/// - Configure backpressure: max_pending_reads = 3
/// - Push 3 linearizable read requests (should succeed)
/// - Push 4th linearizable read request (should be rejected)
#[tokio::test]
async fn test_backpressure_read_limit_enforcement() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_backpressure_read", shutdown_rx, None);

    // Configure strict read backpressure
    let mut config = (*ctx.node_config).clone();
    config.raft.backpressure = BackpressureConfig {
        max_pending_writes: 100,
        max_pending_reads: 3,
    };
    ctx.node_config = std::sync::Arc::new(config);

    let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    // Push first three reads - should succeed
    for i in 1..=3 {
        let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
        let cmd = ClientCmd::Read(
            ClientReadRequest {
                client_id: i,
                consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
                keys: vec![],
            },
            resp_tx,
        );
        leader.push_client_cmd(cmd, &ctx);
    }

    assert_eq!(
        leader.linearizable_read_buffer.len(),
        3,
        "Should have 3 pending reads"
    );

    // Push fourth read - should be rejected
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Read(
        ClientReadRequest {
            client_id: 4,
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
            keys: vec![],
        },
        resp_tx,
    );
    leader.push_client_cmd(cmd, &ctx);

    // Verify rejection
    let response = resp_rx.recv().await.unwrap();
    let err = response.unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::ResourceExhausted,
        "Should reject with ResourceExhausted"
    );
    assert_eq!(
        leader.linearizable_read_buffer.len(),
        3,
        "Buffer should still have 3 items"
    );
}

/// Test: Backpressure disabled when configured to 0 (unlimited)
///
/// Scenario:
/// - Configure backpressure: max_pending_writes = 0 (unlimited)
/// - Push many write requests (all should succeed)
#[tokio::test]
async fn test_backpressure_unlimited_when_zero() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_backpressure_unlimited", shutdown_rx, None);

    // Configure unlimited backpressure
    let mut config = (*ctx.node_config).clone();
    config.raft.backpressure = BackpressureConfig {
        max_pending_writes: 0, // Unlimited
        max_pending_reads: 0,  // Unlimited
    };
    ctx.node_config = std::sync::Arc::new(config);

    let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    // Push many writes - all should succeed
    for i in 1..=100 {
        let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
        let cmd = ClientCmd::Propose(
            ClientWriteRequest {
                client_id: i,
                command: Some(WriteCommand::default()),
            },
            resp_tx,
        );
        leader.push_client_cmd(cmd, &ctx);
    }

    assert_eq!(
        leader.propose_buffer.len(),
        100,
        "All 100 writes should be accepted"
    );
}

/// Test: Write and read limits are independent
///
/// Scenario:
/// - Configure: max_pending_writes = 2, max_pending_reads = 10
/// - Fill write buffer (2 items) - should reject new writes
/// - Read buffer still has space - should accept new reads
#[tokio::test]
async fn test_backpressure_write_and_read_independent() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_backpressure_independent", shutdown_rx, None);

    // Configure asymmetric backpressure
    let mut config = (*ctx.node_config).clone();
    config.raft.backpressure = BackpressureConfig {
        max_pending_writes: 2,
        max_pending_reads: 10,
    };
    ctx.node_config = std::sync::Arc::new(config);

    let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    // Fill write buffer
    for i in 1..=2 {
        let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
        let cmd = ClientCmd::Propose(
            ClientWriteRequest {
                client_id: i,
                command: Some(WriteCommand::default()),
            },
            resp_tx,
        );
        leader.push_client_cmd(cmd, &ctx);
    }

    // Writes should be rejected
    let (write_tx, mut write_rx) = MaybeCloneOneshot::new();
    let write_cmd = ClientCmd::Propose(
        ClientWriteRequest {
            client_id: 99,
            command: Some(WriteCommand::default()),
        },
        write_tx,
    );
    leader.push_client_cmd(write_cmd, &ctx);

    let write_response = write_rx.recv().await.unwrap();
    let write_err = write_response.unwrap_err();
    assert_eq!(
        write_err.code(),
        tonic::Code::ResourceExhausted,
        "Write should be rejected with ResourceExhausted"
    );

    // Reads should still be accepted
    let (read_tx, _read_rx) = MaybeCloneOneshot::new();
    let read_cmd = ClientCmd::Read(
        ClientReadRequest {
            client_id: 99,
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
            keys: vec![],
        },
        read_tx,
    );
    leader.push_client_cmd(read_cmd, &ctx);

    // Read should be buffered
    assert_eq!(
        leader.linearizable_read_buffer.len(),
        1,
        "Read should be buffered"
    );
}

/// Test: LeaseRead and EventualConsistency reads also respect backpressure
///
/// Scenario:
/// - Configure: max_pending_reads = 2
/// - Test that all read policies (Linearizable, Lease, Eventual) respect the limit
#[tokio::test]
async fn test_backpressure_all_read_policies() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_backpressure_all_reads", shutdown_rx, None);

    // Configure strict read backpressure
    let mut config = (*ctx.node_config).clone();
    config.raft.backpressure = BackpressureConfig {
        max_pending_writes: 100,
        max_pending_reads: 2,
    };
    ctx.node_config = std::sync::Arc::new(config);

    let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    // Test LeaseRead backpressure
    for i in 1..=2 {
        let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
        let cmd = ClientCmd::Read(
            ClientReadRequest {
                client_id: i,
                consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
                keys: vec![],
            },
            resp_tx,
        );
        leader.push_client_cmd(cmd, &ctx);
    }

    assert_eq!(leader.lease_read_queue.len(), 2);

    // Third LeaseRead should be rejected
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Read(
        ClientReadRequest {
            client_id: 3,
            consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
            keys: vec![],
        },
        resp_tx,
    );
    leader.push_client_cmd(cmd, &ctx);

    let response = resp_rx.recv().await.unwrap();
    let err = response.unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::ResourceExhausted,
        "Should reject with ResourceExhausted"
    );

    // Clear lease queue
    leader.lease_read_queue.clear();

    // Test EventualConsistency backpressure
    for i in 1..=2 {
        let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
        let cmd = ClientCmd::Read(
            ClientReadRequest {
                client_id: i,
                consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
                keys: vec![],
            },
            resp_tx,
        );
        leader.push_client_cmd(cmd, &ctx);
    }

    assert_eq!(leader.eventual_read_queue.len(), 2);

    // Third EventualConsistency read should be rejected
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Read(
        ClientReadRequest {
            client_id: 3,
            consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
            keys: vec![],
        },
        resp_tx,
    );
    leader.push_client_cmd(cmd, &ctx);

    let response = resp_rx.recv().await.unwrap();
    let err = response.unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::ResourceExhausted,
        "Should reject with ResourceExhausted"
    );
}

// ============================================================================
// BackpressureMetrics Unit Tests
// ============================================================================

/// BackpressureMetrics::record_rejection with enabled=true: write and read paths.
/// Verifies both branches complete without panic when metrics recording is active.
#[test]
fn test_backpressure_metrics_record_rejection_enabled() {
    use crate::raft_role::leader_state::BackpressureMetrics;
    let m = BackpressureMetrics::new(1, true, 1);
    // Both calls must not panic
    m.record_rejection(true); // write rejection
    m.record_rejection(false); // read rejection
}

/// BackpressureMetrics::record_rejection with enabled=false: both paths are no-ops.
#[test]
fn test_backpressure_metrics_record_rejection_disabled() {
    use crate::raft_role::leader_state::BackpressureMetrics;
    let m = BackpressureMetrics::new(1, false, 1);
    m.record_rejection(true);
    m.record_rejection(false);
}

/// BackpressureMetrics::record_buffer_utilization with enabled=true: sampling logic.
/// sample_rate=1 means every call records; verifies write and read labels both work.
#[test]
fn test_backpressure_metrics_record_buffer_utilization_enabled() {
    use crate::raft_role::leader_state::BackpressureMetrics;
    let m = BackpressureMetrics::new(1, true, 1);
    m.record_buffer_utilization(0.0, true); // write, empty buffer
    m.record_buffer_utilization(0.5, true); // write, half full
    m.record_buffer_utilization(1.0, true); // write, full
    m.record_buffer_utilization(0.5, false); // read, half full
}

/// BackpressureMetrics::record_buffer_utilization with enabled=false: no-op.
#[test]
fn test_backpressure_metrics_record_buffer_utilization_disabled() {
    use crate::raft_role::leader_state::BackpressureMetrics;
    let m = BackpressureMetrics::new(1, false, 1);
    m.record_buffer_utilization(0.5, true);
    m.record_buffer_utilization(0.5, false);
}

/// BackpressureMetrics: sample_rate>1 skips most recordings (counter-based sampling).
/// With sample_rate=3, only every 3rd call records; all calls must not panic.
#[test]
fn test_backpressure_metrics_sampling_rate() {
    use crate::raft_role::leader_state::BackpressureMetrics;
    let m = BackpressureMetrics::new(1, true, 3);
    for i in 0..10 {
        m.record_buffer_utilization(i as f64 / 10.0, i % 2 == 0);
    }
}
