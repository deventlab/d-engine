//! Test for the missing per-peer in-flight gate during `PeerReplicationState::Probe`.
//!
//! Background (see `446-expert-q-probe-backpressure-fix-8020.md` in the product-design repo):
//! every reference Raft implementation (etcd/raft, tikv/raft-rs, openraft) limits a `Probe`-state
//! peer to at most one outstanding (unacknowledged) `AppendEntries` request. d-engine's
//! `PeerReplicationState::Probe`/`Replicate` only controls whether `next_index` is optimistically
//! advanced before sending — it never checks whether the peer already has a request in flight.
//! Left unchecked, the leader keeps re-sending `prev_log_index=0` probes to a peer whose first
//! attempt hasn't been acknowledged yet, and each one forces the follower to wipe and rebuild its
//! entire log (`buffered_raft_log::reset`), which is the root cause of the throughput collapse
//! this ticket investigated.
//!
//! This test is intentionally RED until the in-flight gate is implemented in
//! `execute_and_process_raft_rpc` (Phase 5, `leader_state.rs`). It does not assert *how* the gate
//! is implemented — only the externally observable contract: a peer with one unacknowledged
//! request must not receive a second one.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use d_engine_proto::common::{Entry, EntryPayload, NodeRole::Follower, NodeStatus};
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::replication::{
    AppendEntriesRequest, AppendEntriesResponse, ConflictResult, append_entries_response,
};
use tokio::sync::{mpsc, watch};
use tracing_test::traced_test;

use crate::MockMembership;
use crate::MockRaftLog;
use crate::RaftRequestWithSignal;
use crate::event::InternalEvent;
use crate::maybe_clone_oneshot::{MaybeCloneOneshot, RaftOneshot};
use crate::network::PeerUpdate;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::{PeerReplicationState, RaftRoleState};
use crate::test_utils::mock::{MockTypeConfig, mock_raft_context};

/// Two-voter membership (peers 2 & 3) so the cluster is multi-voter — a single-voter leader
/// short-circuits Phase 5 entirely (no peer work to gate), which would make this test vacuous.
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

/// A minimal `AppendEntriesRequest` stub — its contents don't matter, only whether Phase 5
/// forwards a request to the peer's worker channel at all.
fn stub_request() -> AppendEntriesRequest {
    AppendEntriesRequest::default()
}

/// A non-empty probe (one entry) — used where the empty-heartbeat vs non-empty-probe
/// distinction matters for the gate: a heartbeat must neither block nor set the gate.
fn stub_probe_request() -> AppendEntriesRequest {
    AppendEntriesRequest {
        entries: vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }],
        ..AppendEntriesRequest::default()
    }
}

/// A single one-entry write batch, matching the shape `process_batch` expects.
fn one_entry_batch() -> VecDeque<RaftRequestWithSignal> {
    let (tx, _rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let req = RaftRequestWithSignal {
        id: "test".into(),
        payloads: vec![EntryPayload::command(Bytes::from_static(b"cmd"))],
        senders: vec![tx],
        wait_for_apply_event: false,
    };
    VecDeque::from(vec![req])
}

/// Scenario (release direction — the half the gate must also get right):
/// - Batch 1 is dispatched to peer 2: its first `Probe`, correctly limited to one outstanding
///   request.
/// - The follower answers that probe with a CONFLICT (reject), not a success. A reject is not
///   evidence the peer is caught up, so `update_peer_index`'s conflict branch retreats
///   `next_index` to the conflict hint and leaves peer 2 in `Probe`.
/// - Batch 2 is processed afterwards. The response to batch 1 has *arrived*, so peer 2 no longer
///   has an outstanding request and the gate must release: the corrected probe must be
///   dispatched.
///
/// # Expected
/// `Probe` means "at most one unacknowledged `AppendEntries` at a time" (etcd/raft
/// `MsgAppFlowPaused`, openraft `Inflight::is_none()`), not "at most one ever". A response must
/// re-arm the gate, never latch it shut: a latched `Probe` peer receives nothing further — not
/// even heartbeats, since they share this dispatch path — while the frozen follower times out
/// into candidacy and the leader cannot reach quorum.
///
/// # Current behavior (why this test is RED)
/// The gate is set on dispatch and only cleared by `handle_peer_stream_error` (a bidi stream
/// disconnect). A peer whose probe was rejected stays `Probe` with the latch closed forever, so
/// batch 2 dispatches nothing.
#[tokio::test]
#[traced_test]
async fn test_probe_peer_dispatches_next_probe_after_reject() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_probe_peer_dispatches_next_probe_after_reject",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());

    // Both batches offer a request for peer 2, so every dispatch decision is Phase 5's own.
    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(2)
        .returning(|_, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_probe_request(), 1)],
                snapshot_targets: vec![],
            })
        });
    ctx.handlers
        .replication_handler
        .expect_handle_conflict_response()
        .returning(|_, _, _, _| {
            Ok(PeerUpdate {
                match_index: None,
                next_index: 1,
                success: false,
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    let (task_tx, mut task_rx) = mpsc::unbounded_channel();
    state.replication_workers.insert(
        2,
        super::ReplicationWorkerHandle {
            task_tx,
            snapshot_failure_count: 0,
            snapshot_next_retry_at: None,
        },
    );

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

    // Batch 1: peer 2's first probe — nothing outstanding, so it must be dispatched.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(
        task_rx.try_recv().is_ok(),
        "first batch must reach peer 2's worker — it had no outstanding request"
    );

    // Peer 2 rejects that probe: the leader retreats next_index and keeps the peer in `Probe`,
    // because a reject says nothing about the peer being caught up.
    let reject = AppendEntriesResponse {
        node_id: 2,
        term: 1,
        result: Some(append_entries_response::Result::Conflict(ConflictResult {
            conflict_term: None,
            conflict_index: Some(1),
        })),
    };
    state
        .handle_append_result(2, Ok(reject), &ctx, &internal_event_tx)
        .await
        .unwrap();
    assert_eq!(
        state.peer_replication_state(2),
        PeerReplicationState::Probe,
        "a rejected probe must leave the peer in `Probe` — it is not caught up"
    );

    // Batch 2: the response to batch 1 already arrived, so the gate must release and the
    // corrected probe must go out.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(
        task_rx.try_recv().is_ok(),
        "the probe was answered (reject), so the peer has nothing in flight — the leader must \
         re-probe with the corrected next_index instead of latching the peer shut forever"
    );
}

/// Scenario:
/// - Peer 2 has a worker whose channel this test holds directly (no real transport/network
///   involved), so what actually got dispatched can be checked synchronously — no dependency on
///   background task scheduling, so this test cannot flake on timing.
/// - `prepare_batch_requests` is mocked to unconditionally offer a request for peer 2 on every
///   call, simulating "there's always more to replicate" regardless of ack status — this isolates
///   the assertion to Phase 5's own dispatch decision, which is where the missing gate belongs.
/// - Batch 1 is processed and dispatched — this is correct: peer 2 starts in `Probe` with nothing
///   outstanding, so it must receive its first probe.
/// - Batch 2 is processed *without* `handle_append_result` ever being called for peer 2's first
///   request — i.e. the leader has not (and cannot have) learned whether the first probe was
///   acknowledged. `next_index`/`match_index`/`peer_replication_state` are therefore still exactly
///   what they were after batch 1.
///
/// # Expected (once the fix lands)
/// Batch 2 must NOT produce a second dispatch to peer 2's worker: a `Probe`-state peer with an
/// unacknowledged request in flight must wait for that response (etcd/raft `MsgAppFlowPaused`,
/// openraft `Inflight::is_none()`) before being sent to again.
///
/// # Current behavior (why this test is RED today)
/// `execute_and_process_raft_rpc`'s Phase 5 loop sends to every peer in `append_requests`
/// unconditionally — `PeerReplicationState` only gates whether `next_index` is optimistically
/// advanced beforehand, not whether sending is allowed at all. So batch 2 dispatches anyway.
#[tokio::test]
#[traced_test]
async fn test_probe_peer_with_pending_ack_receives_no_second_dispatch() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_probe_peer_with_pending_ack_receives_no_second_dispatch",
        graceful_rx,
        None,
    );

    ctx.membership = Arc::new(two_peer_membership());

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(2)
        .returning(|_, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_probe_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    // Inject peer 2's worker handle directly and keep the channel's receiver in this test — this
    // is what makes the dispatch count observable synchronously, without any async worker task or
    // transport mock (`send_to_worker_or_spawn` finds this handle and reuses it, so the real
    // worker-spawn path — the only place that would touch `ctx.transport` — is never exercised).
    // `ReplicationWorkerHandle`/`ReplicationTask` are private to `leader_state`, visible here only
    // because this test module nests under it — same access pattern `inject_dead_worker_for_test`
    // already relies on for the sibling `worker_lifecycle_test.rs` file.
    let (task_tx, mut task_rx) = mpsc::unbounded_channel();
    state.replication_workers.insert(
        2,
        super::ReplicationWorkerHandle {
            task_tx,
            snapshot_failure_count: 0,
            snapshot_next_retry_at: None,
        },
    );

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

    // Batch 1: peer 2 starts in `Probe` with nothing in flight — must be dispatched.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(
        task_rx.try_recv().is_ok(),
        "first batch must reach peer 2's worker — it had no outstanding request"
    );

    // Batch 2: peer 2's first request has not been acknowledged (handle_append_result was never
    // called), so it is still awaiting a response.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(
        task_rx.try_recv().is_err(),
        "peer 2 already has an unacknowledged request in flight — a Probe-state peer must not \
         receive a second AppendEntries until the first is acked. See \
         446-expert-q-probe-backpressure-fix-8020.md for the etcd/raft and openraft references."
    );
}

/// Shared setup for the gate open/close tests below: a two-peer cluster with peer 2's worker
/// channel handed to the test, so dispatch is observable synchronously without a real transport.
async fn setup_gate_harness(
    path: &str
) -> (
    crate::raft_context::RaftContext<MockTypeConfig>,
    LeaderState<MockTypeConfig>,
    mpsc::UnboundedReceiver<super::ReplicationTask>,
    mpsc::UnboundedSender<InternalEvent>,
) {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(path, graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    let (task_tx, task_rx) = mpsc::unbounded_channel();
    state.replication_workers.insert(
        2,
        super::ReplicationWorkerHandle {
            task_tx,
            snapshot_failure_count: 0,
            snapshot_next_retry_at: None,
        },
    );

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();
    (ctx, state, task_rx, internal_event_tx)
}

/// An unparseable response (no `result` variant) must still reopen the gate: the request is no
/// longer in flight, even though the leader learned nothing usable from it. Latching here would
/// freeze the peer exactly like the reject case.
#[tokio::test]
#[traced_test]
async fn test_probe_peer_dispatches_next_probe_after_unparseable_response() {
    let (mut ctx, mut state, mut task_rx, internal_event_tx) =
        setup_gate_harness("/tmp/test_probe_peer_dispatches_next_probe_after_unparseable_response")
            .await;

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(2)
        .returning(|_, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_probe_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(task_rx.try_recv().is_ok(), "first probe must be dispatched");

    state
        .handle_append_result(
            2,
            Ok(AppendEntriesResponse {
                node_id: 2,
                term: 1,
                result: None,
            }),
            &ctx,
            &internal_event_tx,
        )
        .await
        .unwrap();

    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(
        task_rx.try_recv().is_ok(),
        "an unparseable response still resolves the outstanding probe — the leader must re-probe"
    );
}

/// An empty (heartbeat) dispatch must NOT set the gate, otherwise a heartbeat would occupy the
/// "probe in flight" slot and block the next real probe.
#[tokio::test]
#[traced_test]
async fn test_heartbeat_does_not_latch_gate() {
    let (mut ctx, mut state, mut task_rx, internal_event_tx) =
        setup_gate_harness("/tmp/test_heartbeat_does_not_latch_gate").await;

    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(2)
        .returning(move |_, _, _, _, _| {
            let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let req = if n == 0 {
                stub_request()
            } else {
                stub_probe_request()
            };
            Ok(crate::PrepareResult {
                append_requests: vec![(2, req, 1)],
                snapshot_targets: vec![],
            })
        });

    // Batch 1 is an empty heartbeat — it must be dispatched but leave the gate open.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(task_rx.try_recv().is_ok(), "heartbeat must be dispatched");

    // Batch 2 is a real probe — it must not be blocked by a latch the heartbeat never set.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(
        task_rx.try_recv().is_ok(),
        "an empty heartbeat must not latch the gate — the following probe must go out"
    );
}

/// While a non-empty probe is in flight, an empty heartbeat must still be dispatched: the gate
/// throttles probes only, never heartbeats — this is the liveness backstop that unfreezes a peer
/// whose probe response was lost or unparseable.
#[tokio::test]
#[traced_test]
async fn test_heartbeat_bypasses_gate_while_probe_in_flight() {
    let (mut ctx, mut state, mut task_rx, internal_event_tx) =
        setup_gate_harness("/tmp/test_heartbeat_bypasses_gate_while_probe_in_flight").await;

    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(2)
        .returning(move |_, _, _, _, _| {
            let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let req = if n == 0 {
                stub_probe_request()
            } else {
                stub_request()
            };
            Ok(crate::PrepareResult {
                append_requests: vec![(2, req, 1)],
                snapshot_targets: vec![],
            })
        });

    // Batch 1 is a non-empty probe — it latches the gate.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(task_rx.try_recv().is_ok(), "first probe must be dispatched");

    // Batch 2 is an empty heartbeat — it must bypass the gate and still be dispatched.
    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(
        task_rx.try_recv().is_ok(),
        "a heartbeat must bypass the probe gate — it is the liveness backstop, not a probe"
    );
}

/// A stale-term response belongs to an older request, not the outstanding probe, so it must NOT
/// reopen the gate. Liveness is instead recovered by the heartbeat backstop (see the bypass test),
/// mirroring etcd's `MaybeUpdate(n <= Match)` early-return-without-resume.
#[tokio::test]
#[traced_test]
async fn test_stale_term_response_keeps_gate_latched() {
    let (mut ctx, mut state, mut task_rx, internal_event_tx) =
        setup_gate_harness("/tmp/test_stale_term_response_keeps_gate_latched").await;

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(2)
        .returning(|_, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_probe_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(task_rx.try_recv().is_ok(), "first probe must be dispatched");

    // term 0 < leader_term 1 → stale, ignored without clearing the gate.
    state
        .handle_append_result(
            2,
            Ok(AppendEntriesResponse {
                node_id: 2,
                term: 0,
                result: None,
            }),
            &ctx,
            &internal_event_tx,
        )
        .await
        .unwrap();

    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(
        task_rx.try_recv().is_err(),
        "a stale-term response must not reopen the gate — the outstanding probe is still in flight"
    );
}
