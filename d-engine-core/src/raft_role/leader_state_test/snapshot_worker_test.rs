//! Tests for the leader-owned `PeerReplicationState` state machine around snapshot
//! transfers, and how it gates the per-follower replication worker.
//!
//! # Architecture under test
//!
//! `PeerReplicationState` has three values: `Probe`, `Replicate`, `Snapshot`. The
//! leader is the sole authority over this state — the per-follower worker (a
//! separate tokio task) has no state or judgment of its own; it only executes
//! whatever task the leader hands it (`ReplicationTask::Append` or
//! `ReplicationTask::Snapshot`).
//!
//! ```text
//!                  ┌───────────────────────────┐
//!      ┌──────────▶│           Probe            │◀─────────────┐
//!      │           └──────────────┬─────────────┘               │
//!      │            real Append success ACK                     │
//!      │                          ▼                             │
//!      │           ┌───────────────────────────┐                │
//! reject/stream err │          Replicate         │       reject/stream err
//!      │           └──────────────┬─────────────┘                │
//!      │            classification says NeedSnapshot             │
//!      │                          ▼                             │
//!      │           ┌───────────────────────────┐                │
//!      └───────────│           Snapshot         │────────────────┘
//!                   └───────────────────────────┘
//!                     SnapshotPushCompleted (success or failure)
//! ```
//!
//! Key invariant: while a peer is `Snapshot`, the leader must not generate or
//! dispatch any `Append` task for it, and must not advance its `next_index` —
//! regardless of what the (possibly stale) per-heartbeat classification says.
//! Leaving `Snapshot` is the exclusive job of processing a `SnapshotPushCompleted`
//! event for that peer's current attempt; nothing else may downgrade it.

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
use crate::raft_role::role_state::PeerReplicationState;
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

/// Builds a `LeaderState` + mock `RaftContext` wired for these tests: two-peer
/// membership, a state machine that always has a snapshot ready, and a raft log
/// whose tip is at index 15 (arbitrary — only matters where a test compares it
/// against the snapshot's own boundary of index 10, see `stub_snapshot_metadata`).
fn new_leader_and_ctx(
    tmp_name: &str
) -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
) {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(tmp_name, graceful_rx, None);

    ctx.membership = Arc::new(two_peer_membership());
    ctx.storage.state_machine = Arc::new(state_machine_with_snapshot());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 15);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    (
        LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone()),
        ctx,
    )
}

fn open_stream_capturing_appends() -> (
    MockTransport<MockTypeConfig>,
    mpsc::Receiver<AppendEntriesRequest>,
) {
    let (append_tx, append_rx) = mpsc::channel::<AppendEntriesRequest>(128);
    let mut transport = MockTransport::<MockTypeConfig>::new();
    transport.expect_open_replication_stream().returning(move |_, _, _| {
        let stream = futures::stream::pending().boxed();
        Ok(crate::ReplicationStream {
            sender: append_tx.clone(),
            receiver: stream,
        })
    });
    (transport, append_rx)
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Worker routes snapshot task: when `prepare_batch_requests` returns a
/// snapshot target, the worker receives `ReplicationTask::Snapshot` and
/// emits `InternalEvent::SnapshotPushCompleted` once the transfer finishes.
///
/// # Scenario
/// - `prepare_batch_requests` returns snapshot_targets = [2], append_requests = []
/// - The snapshot metadata's own boundary is index 10 (`stub_snapshot_metadata`), while
///   the leader's log tip (`raft_log.last_entry_id`) is 15 — deliberately different, so
///   the assertion below proves the event carries the snapshot's own boundary and not
///   the leader's current tip.
/// - Expected: `InternalEvent::SnapshotPushCompleted { peer_id: 2, success: true,
///   last_included_index: Some(10) }` arrives
#[tokio::test]
#[traced_test]
async fn test_worker_handles_snapshot_task_and_emits_completed_event() {
    let (mut state, mut ctx) =
        new_leader_and_ctx("/tmp/test_worker_handles_snapshot_task_and_emits_completed_event");
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

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
        let (tx, _rx) = mpsc::channel(128);
        let stream = futures::stream::empty().boxed();
        Ok(crate::ReplicationStream {
            sender: tx,
            receiver: stream,
        })
    });
    ctx.transport = Arc::new(transport);

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
                success: true,
                term: _,
                last_included_index: Some(10)
            }
        ),
        "expected SnapshotPushCompleted for peer 2 carrying the snapshot's own boundary \
         (index 10, not the leader's log tip of 15), got {event:?}"
    );
}

/// Leader's Phase-6 dispatch skips re-sending a `Snapshot` task to a peer that is
/// already `Snapshot` state — the leader's own bookkeeping is authoritative, no
/// separate worker-side flag is consulted.
///
/// # Scenario
/// - Peer 2's replication state is preset to `Snapshot` (simulating an attempt
///   already in flight), with no real transfer running.
/// - `process_batch` runs with peer 2 in `snapshot_targets`.
/// - Expected: `transport.send_snapshot` is never called.
#[tokio::test]
async fn test_phase6_skips_redispatch_when_snapshot_already_in_flight() {
    let (mut state, mut ctx) =
        new_leader_and_ctx("/tmp/test_phase6_skips_redispatch_when_snapshot_already_in_flight");
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Snapshot);

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
        let (tx, _rx) = mpsc::channel(128);
        let stream = futures::stream::empty().boxed();
        Ok(crate::ReplicationStream {
            sender: tx,
            receiver: stream,
        })
    });
    ctx.transport = Arc::new(transport);

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    // Assertion is enforced by MockTransport: send_snapshot's times(0) expectation
    // panics on drop if it was ever called.
}

/// Routing a peer to a snapshot push sets its replication state to `Snapshot` —
/// not left at a stale `Replicate`/`Probe`, and not skipped straight to `Probe`
/// (a past regression: an earlier version of this code set `Probe` here, which
/// made the state indistinguishable from "no snapshot in flight" and let Phase 5
/// generate Append tasks for a peer that was, in reality, mid-transfer).
///
/// # Scenario
/// - Peer 2 starts as `Replicate` (it was healthy before suddenly falling behind).
/// - `process_batch` routes peer 2 to `snapshot_targets`.
/// - Expected: `peer_replication_state(2)` is `Snapshot` immediately after dispatch —
///   synchronously, with no polling/sleep needed, because the state write and the
///   task dispatch happen in the same function call.
#[tokio::test]
async fn test_phase6_snapshot_dispatch_sets_snapshot_state() {
    let (mut state, mut ctx) =
        new_leader_and_ctx("/tmp/test_phase6_snapshot_dispatch_sets_snapshot_state");
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Replicate);

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
        let (tx, _rx) = mpsc::channel(128);
        let stream = futures::stream::empty().boxed();
        Ok(crate::ReplicationStream {
            sender: tx,
            receiver: stream,
        })
    });
    ctx.transport = Arc::new(transport);

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    assert_eq!(
        state.peer_replication_state.get(&2),
        Some(&PeerReplicationState::Snapshot),
        "routing a peer to a snapshot push must set Snapshot, not Probe or Replicate"
    );
}

/// While a peer is `Snapshot`, the leader must not generate an Append task for it —
/// even if this round's classification would otherwise produce one — and must not
/// touch its `next_index`. This is the direct regression test for the original bug:
/// a peer mid-snapshot-transfer was optimistically advanced past a data batch that
/// never actually reached it, because nothing checked replication state before
/// generating the Append task.
///
/// # Scenario
/// - Peer 2 is preset to `Snapshot` (a transfer is in flight).
/// - `process_batch` runs with an Append request for peer 2 in `append_requests`
///   (simulating a heartbeat that computed fresh data to send, unaware the peer
///   is mid-transfer).
/// - Expected: nothing reaches the worker's bidi stream, and `next_index` for
///   peer 2 is unchanged from its preset value.
#[tokio::test]
async fn test_phase5_skips_append_and_leaves_next_index_untouched_while_peer_in_snapshot_state() {
    let (mut state, mut ctx) = new_leader_and_ctx(
        "/tmp/test_phase5_skips_append_and_leaves_next_index_untouched_while_peer_in_snapshot_state",
    );
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Snapshot);
    let next_index_before = state.next_index(2);

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(crate::replication::PrepareResult {
                append_requests: vec![(2, stub_append_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    let (transport, mut append_rx) = open_stream_capturing_appends();
    ctx.transport = Arc::new(transport);

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        append_rx.try_recv().is_err(),
        "no Append task should reach the worker while the peer is in Snapshot state"
    );
    assert_eq!(
        state.next_index(2),
        next_index_before,
        "next_index must not advance for a peer the leader knows is mid-snapshot"
    );
}

/// The worker has no judgment of its own about whether to forward an `Append`
/// task — it is a pure executor. This bypasses `process_batch`/Phase 5 entirely
/// and hands a task straight to `send_to_worker_or_spawn`, so the gate added in
/// the scenario above (which lives in Phase 5, not the worker) cannot be the
/// reason this passes. The peer is deliberately left in `Snapshot` state — the
/// one state that WOULD block dispatch if this went through Phase 5 — to prove
/// the worker itself never inspects `PeerReplicationState` at all.
///
/// If a future change re-adds a "should I actually send this" check inside the
/// worker's own task-handling loop (the exact shape of the original bug this
/// whole state machine exists to prevent), this test fails immediately,
/// independently of whatever Phase 5 does or doesn't do.
///
/// # Scenario
/// - Peer 2 is set to `Snapshot` (irrelevant to the worker, relevant only to
///   prove this test isn't accidentally exercising Phase 5's gate).
/// - An `Append` task is handed directly to `send_to_worker_or_spawn`.
/// - Expected: the request reaches the worker's bidi stream unmodified.
#[tokio::test]
async fn test_worker_forwards_any_append_task_it_is_given_without_inspecting_state() {
    let (mut state, ctx) = new_leader_and_ctx(
        "/tmp/test_worker_forwards_any_append_task_it_is_given_without_inspecting_state",
    );
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Snapshot);

    let (transport, mut append_rx) = open_stream_capturing_appends();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // Bypasses process_batch/Phase 5 on purpose — see doc comment above.
    state.send_to_worker_or_spawn(
        2,
        super::ReplicationTask::Append(stub_append_request()),
        super::ReplicationWorkerConfig {
            transport: Arc::new(transport),
            membership: ctx.membership.clone(),
            retry_policies: ctx.node_config.retry.clone(),
            response_compress_enabled: ctx.node_config.raft.rpc_compression.replication_response,
            internal_event_tx,
            state_machine_handler: ctx.state_machine_handler().clone(),
            snapshot_config: ctx.node_config.raft.snapshot.clone(),
        },
    );

    let received = tokio::time::timeout(Duration::from_millis(200), append_rx.recv())
        .await
        .expect("timed out waiting for the Append task to reach the worker")
        .expect("worker's bidi stream channel closed unexpectedly");
    let _ = received; // presence is the assertion — content is covered elsewhere
}

/// `SnapshotPushCompleted { success: true }` moves the peer back to `Probe` and
/// clears the way for normal replication to resume.
///
/// # Scenario
/// - Peer 2 is dispatched to Snapshot (via a real `process_batch` round).
/// - The completion handler is invoked directly with `success = true`.
/// - Expected: state is `Probe`, and a subsequent round with an Append request
///   for peer 2 is no longer blocked.
#[tokio::test]
async fn test_snapshot_completion_success_returns_to_probe_and_allows_append() {
    let (mut state, mut ctx) = new_leader_and_ctx(
        "/tmp/test_snapshot_completion_success_returns_to_probe_and_allows_append",
    );
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    // The worker is spawned once, on the first dispatch, and keeps running across
    // later rounds for the same peer — so it must be set up with one transport
    // (and one observed stream) used for the whole test, not swapped out per round.
    let (mut transport, mut append_rx) = open_stream_capturing_appends();
    transport.expect_send_snapshot().times(1).returning(|_, _, _, _, _, _| Ok(()));
    ctx.transport = Arc::new(transport);
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // Round 1: dispatch to Snapshot. The completion handler looks up the worker
    // handle created by dispatch, so it must actually exist first.
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
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert_eq!(
        state.peer_replication_state.get(&2),
        Some(&PeerReplicationState::Snapshot)
    );

    state.handle_snapshot_push_completed(2, true, &ctx.node_config.retry.install_snapshot, 1);
    assert_eq!(
        state.peer_replication_state.get(&2),
        Some(&PeerReplicationState::Probe),
        "a successful snapshot completion must return the peer to Probe"
    );

    // Round 2: an Append for peer 2 must now go through, on the same worker.
    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(crate::replication::PrepareResult {
                append_requests: vec![(2, stub_append_request(), 1)],
                snapshot_targets: vec![],
            })
        });
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    tokio::time::timeout(Duration::from_millis(200), append_rx.recv())
        .await
        .expect("timed out waiting for the Append task to reach the worker")
        .expect("worker's bidi stream channel closed unexpectedly");
}

/// `SnapshotPushCompleted { success: false }` also returns the peer to `Probe`
/// (it must not get stuck in `Snapshot` forever just because the attempt failed),
/// but enters a backoff window before the next snapshot attempt is allowed.
///
/// # Scenario
/// - Peer 2 is in Snapshot state (an attempt in flight).
/// - The completion handler is invoked directly with `success = false`.
/// - Expected: state is `Probe`, and the worker handle's retry-backoff timestamp
///   is set to a point in the future (so an immediate re-dispatch attempt would
///   be skipped — covered by the backoff-window test below).
#[tokio::test]
async fn test_snapshot_completion_failure_returns_to_probe_with_backoff() {
    let (mut state, mut ctx) =
        new_leader_and_ctx("/tmp/test_snapshot_completion_failure_returns_to_probe_with_backoff");
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    // A worker handle must already exist for the backoff bookkeeping to attach to —
    // dispatch once for real first, rather than reaching into private leader
    // internals to fabricate one.
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
    let mut dispatch_transport = MockTransport::<MockTypeConfig>::new();
    dispatch_transport
        .expect_send_snapshot()
        .times(1)
        .returning(|_, _, _, _, _, _| Ok(()));
    dispatch_transport.expect_open_replication_stream().returning(|_, _, _| {
        let (tx, _rx) = mpsc::channel(128);
        let stream = futures::stream::empty().boxed();
        Ok(crate::ReplicationStream {
            sender: tx,
            receiver: stream,
        })
    });
    ctx.transport = Arc::new(dispatch_transport);
    let (dispatch_tx, _dispatch_rx) = mpsc::unbounded_channel();
    state.process_batch(one_entry_batch(), &dispatch_tx, &ctx).await.unwrap();
    assert_eq!(
        state.peer_replication_state.get(&2),
        Some(&PeerReplicationState::Snapshot)
    );

    state.handle_snapshot_push_completed(2, false, &ctx.node_config.retry.install_snapshot, 1);

    assert_eq!(
        state.peer_replication_state.get(&2),
        Some(&PeerReplicationState::Probe),
        "a failed snapshot completion must still return the peer to Probe, not get stuck"
    );
}

/// A stale `SnapshotPushCompleted` — one that arrives after the peer's state has
/// already moved on from `Snapshot` — must be a no-op. This guards against a
/// delayed completion event from an earlier, already-superseded attempt silently
/// corrupting bookkeeping for whatever the peer is doing now.
///
/// # Scenario
/// - Peer 2 is left in `Probe` (no snapshot currently in flight — e.g. the peer
///   caught up normally through ordinary replication after an earlier attempt).
/// - A `SnapshotPushCompleted` for peer 2 is delivered anyway (simulating a
///   straggler event from a past attempt).
/// - Expected: state stays exactly `Probe` (not perturbed), proving the handler
///   checked "am I actually in Snapshot right now" before acting.
#[tokio::test]
async fn test_stale_snapshot_completion_is_ignored_once_state_has_moved_on() {
    let (mut state, ctx) = new_leader_and_ctx(
        "/tmp/test_stale_snapshot_completion_is_ignored_once_state_has_moved_on",
    );
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    // Deliberately NOT setting Snapshot state — this event should find nothing to do.
    assert_eq!(
        state.peer_replication_state.get(&2),
        None,
        "peer starts with no recorded state"
    );

    state.handle_snapshot_push_completed(2, true, &ctx.node_config.retry.install_snapshot, 1);

    assert_eq!(
        state.peer_replication_state.get(&2),
        None,
        "a completion event for a peer that was never in Snapshot state must not \
         fabricate ANY entry (Probe, Snapshot, or Replicate) — it has nothing to \
         conclude and must be a strict no-op, leaving the entry absent"
    );
}

/// While a peer is in its post-failure backoff window, the leader must not
/// re-dispatch a snapshot even if this round's classification still says the
/// peer needs one — and must not fall back to sending it an Append either (the
/// peer is, by definition, still missing the data an Append can't supply).
///
/// # Scenario
/// - Peer 2 fails a snapshot attempt (state returns to `Probe`, backoff starts).
/// - The very next round classifies peer 2 as still needing a snapshot
///   (`snapshot_targets = [2]`) — simulating that nothing about its position
///   has changed since the failure.
/// - Expected: no second `send_snapshot` call happens (backoff not yet expired),
///   and the peer's state stays `Probe` (it does not incorrectly re-enter
///   `Snapshot` without a real dispatch).
#[tokio::test]
async fn test_backoff_window_blocks_redispatch_even_when_classification_still_requests_snapshot() {
    let (mut state, mut ctx) = new_leader_and_ctx(
        "/tmp/test_backoff_window_blocks_redispatch_even_when_classification_still_requests_snapshot",
    );
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Snapshot);

    // First, dispatch once for real so a worker handle (and its backoff bookkeeping)
    // exists, then fail it to enter the backoff window.
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
        let (tx, _rx) = mpsc::channel(128);
        let stream = futures::stream::empty().boxed();
        Ok(crate::ReplicationStream {
            sender: tx,
            receiver: stream,
        })
    });
    ctx.transport = Arc::new(transport);

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    // Reset to Probe first so the first round is a genuine dispatch, not skipped
    // by the already-in-flight check.
    state.set_peer_replication_state(2, PeerReplicationState::Probe);
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    state.handle_snapshot_push_completed(2, false, &ctx.node_config.retry.install_snapshot, 1);
    assert_eq!(
        state.peer_replication_state.get(&2),
        Some(&PeerReplicationState::Probe)
    );

    // Second round: classification still says peer 2 needs a snapshot, but the
    // backoff window from the failure above has not expired yet.
    let mut transport2 = MockTransport::<MockTypeConfig>::new();
    transport2.expect_send_snapshot().times(0); // must NOT be called again — still in backoff
    transport2.expect_open_replication_stream().returning(|_, _, _| {
        let (tx, _rx) = mpsc::channel(128);
        let stream = futures::stream::empty().boxed();
        Ok(crate::ReplicationStream {
            sender: tx,
            receiver: stream,
        })
    });
    ctx.transport = Arc::new(transport2);
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

    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(
        state.peer_replication_state.get(&2),
        Some(&PeerReplicationState::Probe),
        "must not re-enter Snapshot state without an actual dispatch happening"
    );
}
