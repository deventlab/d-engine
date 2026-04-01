//! Tests for per-follower ReplicationWorker snapshot handling.
//!
//! When the leader routes a peer to `ReplicationTask::Snapshot` (because
//! the peer's next_index is below the purge boundary), the worker must:
//!   1. Initiate a snapshot transfer.
//!   2. Set `snapshot_in_progress = true` while transferring.
//!   3. Skip subsequent `Snapshot` tasks that arrive while in progress.
//!   4. Skip `Append` tasks that arrive while snapshot is in progress
//!      (they are meaningless until the peer has the snapshot base).
//!   5. After transfer completes, emit `RoleEvent::SnapshotPushCompleted`.

use std::sync::Arc;
use std::time::Duration;

use d_engine_proto::common::{EntryPayload, NodeRole::Follower, NodeStatus};
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::storage::SnapshotMetadata;
use futures::StreamExt;
use tokio::sync::{mpsc, watch};
use tracing_test::traced_test;

use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockStateMachine;
use crate::MockTransport;
use crate::RaftRequestWithSignal;
use crate::event::RoleEvent;
use crate::maybe_clone_oneshot::{MaybeCloneOneshot, RaftOneshot};
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::{MockTypeConfig, mock_raft_context};

// ── helpers ──────────────────────────────────────────────────────────────────

fn two_peer_membership() -> MockMembership<MockTypeConfig> {
    let peers = vec![
        NodeMeta {
            id: 2,
            address: String::new(),
            status: NodeStatus::Active as i32,
            role: Follower.into(),
        },
        NodeMeta {
            id: 3,
            address: String::new(),
            status: NodeStatus::Active as i32,
            role: Follower.into(),
        },
    ];
    let peers2 = peers.clone();
    let mut m = MockMembership::new();
    m.expect_is_single_node_cluster().returning(|| false);
    m.expect_voters().returning(move || peers.clone());
    m.expect_replication_peers().returning(move || peers2.clone());
    m
}

fn stub_append_request() -> AppendEntriesRequest {
    AppendEntriesRequest::default()
}

fn stub_snapshot_metadata() -> SnapshotMetadata {
    use d_engine_proto::common::LogId;
    SnapshotMetadata {
        last_included: Some(LogId { term: 2, index: 10 }),
        ..Default::default()
    }
}

fn state_machine_with_snapshot() -> MockStateMachine {
    use bytes::Bytes;
    use d_engine_proto::common::LogId;
    let mut sm = MockStateMachine::new();
    sm.expect_start().returning(|| Ok(()));
    sm.expect_stop().returning(|| Ok(()));
    sm.expect_is_running().returning(|| true);
    sm.expect_get().returning(|_| Ok(None));
    sm.expect_entry_term().returning(|_| None);
    sm.expect_apply_chunk().returning(|_| Ok(vec![]));
    sm.expect_len().returning(|| 0);
    sm.expect_update_last_applied().returning(|_| ());
    sm.expect_last_applied().return_const(LogId::default());
    sm.expect_persist_last_applied().returning(|_| Ok(()));
    sm.expect_update_last_snapshot_metadata().returning(|_| Ok(()));
    // Return a valid snapshot so Phase 6 fires the Snapshot task to the worker.
    sm.expect_snapshot_metadata().returning(|| Some(stub_snapshot_metadata()));
    sm.expect_persist_last_snapshot_metadata().returning(|_| Ok(()));
    sm.expect_apply_snapshot_from_file().returning(|_, _| Ok(()));
    sm.expect_generate_snapshot_data()
        .returning(|_, _| Ok(Bytes::copy_from_slice(&[0u8; 32])));
    sm.expect_save_hard_state().returning(|| Ok(()));
    sm.expect_flush().returning(|| Ok(()));
    sm
}

fn one_entry_batch() -> std::collections::VecDeque<RaftRequestWithSignal> {
    let (tx, _rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let req = RaftRequestWithSignal {
        id: "test".into(),
        payloads: vec![EntryPayload::command(bytes::Bytes::from_static(b"cmd"))],
        senders: vec![tx],
        wait_for_apply_event: false,
    };
    std::collections::VecDeque::from(vec![req])
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Worker routes snapshot task: when `prepare_batch_requests` returns a
/// snapshot target, the worker receives `ReplicationTask::Snapshot` and
/// emits `RoleEvent::SnapshotPushCompleted` once the transfer finishes.
///
/// # Scenario
/// - `prepare_batch_requests` returns snapshot_targets = [2], append_requests = []
/// - Expected: `RoleEvent::SnapshotPushCompleted { peer_id: 2, success: true }` arrives
///
/// # Before fix (will FAIL)
/// `ReplicationTask` doesn't exist; worker only handles AppendEntries.
///
/// # After fix (will PASS)
/// Worker handles `ReplicationTask::Snapshot` and emits SnapshotPushCompleted.
#[tokio::test]
#[traced_test]
async fn test_worker_handles_snapshot_task_and_emits_completed_event() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_worker_handles_snapshot_task_and_emits_completed_event",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());
    ctx.storage.state_machine = Arc::new(state_machine_with_snapshot());

    // prepare_batch_requests returns a snapshot target for peer 2, no append requests.
    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(crate::replication::PrepareResult {
                append_requests: vec![],
                snapshot_targets: vec![2],
            })
        });

    // Transport: no AppendEntries should be sent for peer 2.
    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport.expect_send_append_request().never();
    // Snapshot transfer via transport (send_snapshot is the new method).
    transport.expect_send_snapshot().times(1).returning(|_, _, _, _, _| Ok(()));
    // Worker opens bidi stream at startup (new behavior in #345)
    transport.expect_open_replication_stream().returning(|_, _, _| {
        let (tx, _rx) = tokio::sync::mpsc::channel(128);
        let stream = futures::stream::empty().boxed();
        Ok(crate::ReplicationStream {
            sender: tx,
            receiver: stream,
        })
    });
    ctx.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 15);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &role_tx, &ctx).await.unwrap();

    // Allow worker task to run and complete the (mock) snapshot transfer.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let event = role_rx
        .try_recv()
        .expect("SnapshotPushCompleted should arrive after snapshot transfer");
    assert!(
        matches!(
            event,
            RoleEvent::SnapshotPushCompleted {
                peer_id: 2,
                success: true
            }
        ),
        "expected SnapshotPushCompleted for peer 2, got {event:?}"
    );
}

/// Worker skips a second snapshot task while one is already in progress.
///
/// # Scenario
/// - Two consecutive heartbeats both return snapshot_targets = [2]
/// - First snapshot is slow (simulated by delay in mock)
/// - Expected: `transport.send_snapshot` called exactly once, not twice
///
/// # Before fix (will FAIL)
/// Worker doesn't have `snapshot_in_progress` flag; would initiate two transfers.
///
/// # After fix (will PASS)
/// Worker sets `snapshot_in_progress = true` and skips the duplicate.
#[tokio::test]
#[traced_test]
async fn test_worker_skips_duplicate_snapshot_while_in_progress() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_worker_skips_duplicate_snapshot_while_in_progress",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());
    ctx.storage.state_machine = Arc::new(state_machine_with_snapshot());

    // Both batches return snapshot target for peer 2.
    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(2)
        .returning(|_, _, _, _, _| {
            Ok(crate::replication::PrepareResult {
                append_requests: vec![],
                snapshot_targets: vec![2],
            })
        });

    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport.expect_send_append_request().never();
    // send_snapshot must be called EXACTLY ONCE despite two snapshot tasks arriving.
    transport.expect_send_snapshot().times(1).returning(|_, _, _, _, _| {
        // Simulate a slow snapshot transfer so the second task arrives during it.
        std::thread::sleep(Duration::from_millis(50));
        Ok(())
    });
    // Worker opens bidi stream at startup (new behavior in #345)
    transport.expect_open_replication_stream().returning(|_, _, _| {
        let (tx, _rx) = tokio::sync::mpsc::channel(128);
        let stream = futures::stream::empty().boxed();
        Ok(crate::ReplicationStream {
            sender: tx,
            receiver: stream,
        })
    });
    ctx.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 15);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // First batch: snapshot task sent to worker (transfer begins).
    state.process_batch(one_entry_batch(), &role_tx, &ctx).await.unwrap();
    // Second batch arrives before first snapshot completes.
    state.process_batch(one_entry_batch(), &role_tx, &ctx).await.unwrap();

    // Allow worker time to finish processing.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Assertion is enforced by MockTransport: send_snapshot called exactly once.
    // If called twice, mockall panics on drop — no explicit assert needed here.
}

/// Worker skips AppendEntries tasks that arrive while snapshot is in progress.
///
/// # Scenario
/// - First batch: snapshot task for peer 2 (slow transfer)
/// - Second batch: append task for peer 2 (arrives during snapshot)
/// - Expected: `transport.send_append_request` NOT called during snapshot
///
/// # Before fix (will FAIL)
/// Worker has no `snapshot_in_progress` guard; would send AppendEntries
/// with stale prev_log indices, causing conflict.
///
/// # After fix (will PASS)
/// Worker gates Append tasks behind `snapshot_in_progress` flag.
#[tokio::test]
#[traced_test]
async fn test_worker_skips_append_while_snapshot_in_progress() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_worker_skips_append_while_snapshot_in_progress",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());
    ctx.storage.state_machine = Arc::new(state_machine_with_snapshot());

    // First call: snapshot; second call: normal append for peer 2.
    let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let call_count_clone = call_count.clone();
    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(2)
        .returning(move |_, _, _, _, _| {
            let n = call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                Ok(crate::replication::PrepareResult {
                    append_requests: vec![],
                    snapshot_targets: vec![2],
                })
            } else {
                Ok(crate::replication::PrepareResult {
                    append_requests: vec![(2, stub_append_request())],
                    snapshot_targets: vec![],
                })
            }
        });

    let mut transport = MockTransport::<MockTypeConfig>::new();
    // Snapshot runs (slow).
    transport.expect_send_snapshot().times(1).returning(|_, _, _, _, _| {
        std::thread::sleep(Duration::from_millis(80));
        Ok(())
    });
    // AppendEntries must NOT be called while snapshot is in progress.
    transport.expect_send_append_request().never();
    // Worker opens bidi stream at startup (new behavior in #345)
    transport.expect_open_replication_stream().returning(|_, _, _| {
        let (tx, _rx) = tokio::sync::mpsc::channel(128);
        let stream = futures::stream::empty().boxed();
        Ok(crate::ReplicationStream {
            sender: tx,
            receiver: stream,
        })
    });
    ctx.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 15);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // First batch: snapshot task.
    state.process_batch(one_entry_batch(), &role_tx, &ctx).await.unwrap();
    // Second batch: append task arrives while snapshot is still running.
    state.process_batch(one_entry_batch(), &role_tx, &ctx).await.unwrap();

    // Wait for snapshot to finish.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // MockTransport enforces send_append_request was never called.
}
