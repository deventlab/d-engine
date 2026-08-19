//! Tests for per-follower ReplicationWorker snapshot handling.
//!
//! When the leader routes a peer to `ReplicationTask::Snapshot` (because
//! the peer's next_index is below the purge boundary), the worker must:
//!   1. Initiate a snapshot transfer.
//!   2. Set `snapshot_in_progress = true` while transferring.
//!   3. Skip subsequent `Snapshot` tasks that arrive while in progress.
//!   4. Skip `Append` tasks that arrive while snapshot is in progress
//!      (they are meaningless until the peer has the snapshot base).
//!   5. After transfer completes, emit `InternalEvent::SnapshotPushCompleted`.

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
use crate::SnapshotApplyResult;
use crate::event::InternalEvent;
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
    sm.expect_apply_chunk().returning(|_| Ok(vec![]));
    sm.expect_len().returning(|| 0);
    sm.expect_update_last_applied().returning(|_| ());
    sm.expect_last_applied().return_const(LogId::default());
    sm.expect_persist_last_applied().returning(|_| Ok(()));
    sm.expect_update_last_snapshot_metadata().returning(|_| Ok(()));
    // Return a valid snapshot so Phase 6 fires the Snapshot task to the worker.
    sm.expect_snapshot_metadata().returning(|| Some(stub_snapshot_metadata()));
    sm.expect_persist_last_snapshot_metadata().returning(|_| Ok(()));
    sm.expect_apply_snapshot_from_file().returning(|_, _| {
        Ok(SnapshotApplyResult::Applied {
            last_included: LogId::default(),
        })
    });
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
/// emits `InternalEvent::SnapshotPushCompleted` once the transfer finishes.
///
/// # Scenario
/// - `prepare_batch_requests` returns snapshot_targets = [2], append_requests = []
/// - Expected: `InternalEvent::SnapshotPushCompleted { peer_id: 2, success: true }` arrives
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

    let mut transport = MockTransport::<MockTypeConfig>::new();
    // Snapshot transfer via transport (send_snapshot is the new method).
    transport.expect_send_snapshot().times(1).returning(|_, _, _, _, _, _| Ok(()));
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

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    // Allow worker task to run and complete the (mock) snapshot transfer.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let event = internal_event_rx
        .try_recv()
        .expect("SnapshotPushCompleted should arrive after snapshot transfer");
    assert!(
        matches!(
            event,
            InternalEvent::SnapshotPushCompleted {
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
/// - Also asserts the worker-side dedup branch (leader_state.rs ~2075) logs its
///   invariant-violation warning: with no `.await` between the two `process_batch`
///   calls below, the leader's Phase-6 in-flight check (which reads the same Arc)
///   never gets a chance to observe the flag before the second dispatch — both
///   `Snapshot` tasks reach the worker's channel, and it's the worker's own
///   fallback dedup that catches the duplicate, not the leader's new check.
///   (The leader's own check is covered separately, deterministically, by
///   `test_phase6_skips_redispatch_when_snapshot_already_in_flight` below.)
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
    // send_snapshot must be called EXACTLY ONCE despite two snapshot tasks arriving.
    transport.expect_send_snapshot().times(1).returning(|_, _, _, _, _, _| {
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

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // First batch: snapshot task sent to worker (transfer begins).
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    // Second batch arrives before first snapshot completes.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    // Allow worker time to finish processing.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Assertion is enforced by MockTransport: send_snapshot called exactly once.
    // If called twice, mockall panics on drop — no explicit assert needed here.

    // The duplicate task still reached the worker (see doc comment above) — confirm
    // the dedup branch treats it as an invariant violation (#25), not routine debug noise.
    assert!(
        logs_contain("Invariant violation: worker received duplicate Snapshot task"),
        "worker-side dedup hit must be logged as a warning, not silently at debug level"
    );
}

/// Leader observes the worker's `snapshot_push_in_progress` flag in real time,
/// through the same `Arc` — not a synced copy that only updates after
/// `InternalEvent::SnapshotPushCompleted` round-trips back.
///
/// # Scenario
/// - `send_snapshot` blocks for 80ms, simulating an in-flight transfer.
/// - Shortly after dispatch, `snapshot_push_in_progress_for_test(2)` must already
///   read `true` — proving the leader's `ReplicationWorkerHandle` and the worker's
///   own flag are the same memory, not two states synced by a message.
/// - After the transfer finishes, the flag must clear back to `false`.
///
/// Needs `flavor = "multi_thread"`: the mock's `send_snapshot` blocks the thread via
/// `std::thread::sleep` (matching the other mocks in this file). On the default
/// current-thread runtime that would freeze the *entire* runtime for the transfer's
/// duration, including this test's own `tokio::time::sleep` — making it impossible to
/// observe the flag mid-transfer. A second worker thread lets the transfer block one
/// thread while the test's timer progresses on another.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[traced_test]
async fn test_leader_handle_observes_worker_snapshot_flag_in_real_time() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_leader_handle_observes_worker_snapshot_flag_in_real_time",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());
    ctx.storage.state_machine = Arc::new(state_machine_with_snapshot());

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

    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport.expect_send_snapshot().times(1).returning(|_, _, _, _, _, _| {
        std::thread::sleep(Duration::from_millis(80));
        Ok(())
    });
    // `pending()` keeps the receiver alive (no EOF) — unlike `empty()`, this avoids
    // the worker's reconnect loop churning continuously, which would otherwise make
    // the timing assertions below unreliable (same reasoning as
    // `test_worker_skips_append_while_snapshot_in_progress` below).
    transport.expect_open_replication_stream().returning(|_, _, _| {
        let (tx, _rx) = tokio::sync::mpsc::channel(128);
        let stream = futures::stream::pending().boxed();
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

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    // Give the worker a moment to dequeue the Snapshot task and flip the flag —
    // well before the 80ms mock transfer finishes.
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        state.snapshot_push_in_progress_for_test(2),
        "leader's handle must observe the worker's in-flight flag directly, \
         without waiting for SnapshotPushCompleted"
    );

    // Wait past the transfer duration — flag must clear once the worker finishes.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !state.snapshot_push_in_progress_for_test(2),
        "flag must clear once the transfer completes"
    );
}

/// Leader's Phase-6 dispatch skips re-sending a `Snapshot` task when it already
/// observes `snapshot_push_in_progress == true` — independent of the worker's own
/// fallback dedup (leader_state.rs ~2075, covered by
/// `test_worker_skips_duplicate_snapshot_while_in_progress`).
///
/// # Scenario
/// - A worker handle for peer 2 is preset with `snapshot_push_in_progress = true`
///   (via the test-only injector), with no real transfer running.
/// - `process_batch` runs with peer 2 in `snapshot_targets`.
/// - Expected: `transport.send_snapshot` is never called — the leader's own check
///   (leader_state.rs ~3355) must skip dispatch before the task ever reaches a worker.
#[tokio::test]
async fn test_phase6_skips_redispatch_when_snapshot_already_in_flight() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_phase6_skips_redispatch_when_snapshot_already_in_flight",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());
    ctx.storage.state_machine = Arc::new(state_machine_with_snapshot());

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

    let mut transport = MockTransport::<MockTypeConfig>::new();
    // Must NEVER be called — the leader-side check must skip dispatch entirely.
    transport.expect_send_snapshot().times(0);
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
    // Pretend a transfer is already running for peer 2 — no real worker task needed.
    state.set_snapshot_push_in_progress_for_test(2, true);

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    // Give a (wrongly dispatched) task a chance to run, if the check had failed.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Assertion is enforced by MockTransport: send_snapshot's times(0) expectation
    // panics on drop if it was ever called.
}

/// Routing a peer to a snapshot push sets its replication state to `Probe`, not
/// left at a stale `Replicate` from before it fell behind. Trusting a leftover
/// `Replicate` after a fresh snapshot install would let the optimistic `next_index`
/// advance (leader_state.rs ~3311) run ahead without a real post-snapshot ACK.
///
/// # Scenario
/// - Peer 2 starts as `Replicate` (it was healthy before suddenly falling behind).
/// - `process_batch` routes peer 2 to `snapshot_targets`.
/// - Expected: `peer_replication_state(2)` is `Probe` after dispatch.
#[tokio::test]
async fn test_phase6_snapshot_dispatch_sets_probe_not_replicate() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_phase6_snapshot_dispatch_sets_probe_not_replicate",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());
    ctx.storage.state_machine = Arc::new(state_machine_with_snapshot());

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

    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport.expect_send_snapshot().times(1).returning(|_, _, _, _, _, _| Ok(()));
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
    state.set_peer_replication_state(2, crate::role_state::PeerReplicationState::Replicate);

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    assert_eq!(
        state.peer_replication_state.get(&2),
        Some(&crate::role_state::PeerReplicationState::Probe),
        "routing to a snapshot push must downgrade a stale Replicate to Probe"
    );
}

/// Worker skips AppendEntries tasks that arrive while snapshot is in progress.
///
/// # Scenario
/// - First batch: snapshot task for peer 2 (slow transfer)
/// - Second batch: append task for peer 2 (arrives during snapshot)
/// - Expected: no AppendEntries pushed while snapshot is in progress
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
                    append_requests: vec![(2, stub_append_request(), 1)],
                    snapshot_targets: vec![],
                })
            }
        });

    // Observe the bidi stream: the worker must NOT push AppendEntries into it
    // while a snapshot is in progress.
    let (append_tx, mut append_rx) = tokio::sync::mpsc::channel::<AppendEntriesRequest>(128);

    let mut transport = MockTransport::<MockTypeConfig>::new();
    // Snapshot runs (slow).
    transport.expect_send_snapshot().times(1).returning(|_, _, _, _, _, _| {
        std::thread::sleep(Duration::from_millis(80));
        Ok(())
    });
    // Worker opens bidi stream at startup (new behavior in #345). `pending()` keeps
    // the receiver alive (no EOF), so no reconnect happens mid-test.
    transport.expect_open_replication_stream().returning(move |_, _, _| {
        let stream = futures::stream::pending().boxed();
        Ok(crate::ReplicationStream {
            sender: append_tx.clone(),
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

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // First batch: snapshot task.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    // Second batch: append task arrives while snapshot is still running.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    // Wait for snapshot to finish.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The worker must have skipped the append task during the snapshot — nothing
    // should have been pushed into the bidi stream.
    assert!(
        append_rx.try_recv().is_err(),
        "worker must skip AppendEntries while snapshot is in progress"
    );
}
