//! Test for the missing per-peer in-flight gate during `PeerReplicationState::Probe`.
//!
//! Background (see `446-expert-q-probe-backpressure-fix-8020.md` in the product-design repo):
//! every reference Raft implementation limits a `Probe`-state
//! peer to at most one outstanding (unacknowledged) `AppendEntries` request. d-engine's
//! `PeerReplicationState::Probe`/`Replicate` only controls whether `next_index` is optimistically
//! advanced before sending — it never checks whether the peer already has a request in flight.
//! Left unchecked, the leader keeps re-sending `prev_log_index=0` probes to a peer whose first
//! attempt hasn't been acknowledged yet, and each one forces the follower to wipe and rebuild its
//! entire log (`RaftLogCore::reset`), which is the root cause of the throughput collapse
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
use crate::test_utils::MetricsCapture;
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
/// `Probe` means "at most one unacknowledged `AppendEntries` at a time", not "at most one ever". A response must
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
        .returning(|_, _, _, _, _, _| {
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
/// unacknowledged request in flight must wait for that response before being sent to again.
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
        .returning(|_, _, _, _, _, _| {
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
        .returning(|_, _, _, _, _, _| {
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
        .returning(move |_, _, _, _, _, _| {
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
        .returning(move |_, _, _, _, _, _| {
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
        .returning(|_, _, _, _, _, _| {
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

/// Completes the story `test_stale_term_response_keeps_gate_latched` deliberately stops short
/// of: a stale response must not release the slot, but the *real* (term-matching) response for
/// the same outstanding request must still release it once it arrives. Without this, "stale
/// responses don't release" could be (mis)implemented as "nothing ever releases".
///
/// # Scenario
/// - Batch 1 dispatched to peer 2 — gate latches (Probe, window=1).
/// - A stale-term response arrives (term 0 < leader_term 1) — ignored, gate stays latched
///   (same setup as the sibling test above).
/// - The *real* response for that outstanding probe arrives (term 1, a reject) — this is the
///   response Phase 5 actually sent the probe to elicit, so it must release the slot.
/// - Batch 2 must now dispatch — proves the slot was released by the real response, not
///   permanently stuck after the stale one was ignored.
#[tokio::test]
#[traced_test]
async fn test_real_response_after_stale_still_releases_the_gate() {
    let (mut ctx, mut state, mut task_rx, internal_event_tx) =
        setup_gate_harness("/tmp/test_real_response_after_stale_still_releases_the_gate").await;

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(2)
        .returning(|_, _, _, _, _, _| {
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

    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(task_rx.try_recv().is_ok(), "first probe must be dispatched");
    assert_eq!(
        state.in_flight_count(2),
        1,
        "setup: probe dispatch must occupy the one Probe-window slot"
    );

    // Stale response: term 0 < leader_term 1 — must not release the slot.
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
    assert_eq!(
        state.in_flight_count(2),
        1,
        "a stale-term response must not release the slot"
    );

    // The real response to the original probe: term matches, a reject (conflict) — this must
    // release the slot regardless of accept/reject, because it genuinely resolves the request
    // Phase 5 was tracking.
    state
        .handle_append_result(
            2,
            Ok(AppendEntriesResponse {
                node_id: 2,
                term: 1,
                result: Some(append_entries_response::Result::Conflict(ConflictResult {
                    conflict_term: None,
                    conflict_index: Some(1),
                })),
            }),
            &ctx,
            &internal_event_tx,
        )
        .await
        .unwrap();
    assert_eq!(
        state.in_flight_count(2),
        0,
        "the real (term-matching) response must release the slot the stale one could not"
    );

    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(
        task_rx.try_recv().is_ok(),
        "the slot was released by the real response, so the corrected probe must go out"
    );
}

/// `record_in_flight` must never fire for a heartbeat dispatch, in `Replicate` state exactly
/// as much as in `Probe` state — the "heartbeats occupy zero window slots" contract is
/// state-independent. Without this, repeated heartbeats to an idle Replicate-state peer would
/// silently fill its window (default 256) and eventually start gating genuine data sends, even
/// though nothing was ever un-acknowledged.
///
/// # Scenario
/// - Peer 2 in `Replicate` state (window = configured `max_inflight_append_requests`).
/// - Ten consecutive heartbeat batches (empty entries) are dispatched.
/// - `in_flight_count` must remain 0 throughout — heartbeats never occupied a slot.
#[tokio::test]
#[traced_test]
async fn test_heartbeat_in_replicate_state_does_not_occupy_window() {
    let (mut ctx, mut state, mut task_rx, internal_event_tx) =
        setup_gate_harness("/tmp/test_heartbeat_in_replicate_state_does_not_occupy_window").await;

    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 1);

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(10)
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                // Empty entries: build_append_request's natural heartbeat fallback shape.
                append_requests: vec![(2, stub_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    for i in 0..10 {
        state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
        assert!(
            task_rx.try_recv().is_ok(),
            "heartbeat #{i} must still be dispatched"
        );
        assert_eq!(
            state.in_flight_count(2),
            0,
            "heartbeat #{i} must not occupy a window slot in Replicate state"
        );
    }
}

/// Ledger invariant for any "dispatched but lost" path (stream torn down, send buffer full,
/// or a future drop-on-unreachable): Phase 5 writes `next_index` and `in_flight` *before* the
/// request leaves the leader, so recovering from a lost request must rewind both.
///
/// # Scenario
/// - Peer 2 in `Replicate`, `match_index = 5`. One data request (1 entry, effective next 6) is
///   dispatched: `next_index` advances optimistically to 7 and `in_flight` becomes 1.
/// - The request is lost: `handle_peer_stream_error(2)`.
/// - Expected: `Probe`, `in_flight = 0`, `next_index = match_index + 1 = 6`, so the same range is
///   offered again instead of being skipped.
#[tokio::test]
#[traced_test]
async fn test_lost_dispatch_rewinds_next_index_and_in_flight() {
    let (mut ctx, mut state, mut task_rx, internal_event_tx) =
        setup_gate_harness("/tmp/test_lost_dispatch_rewinds_next_index_and_in_flight").await;

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_probe_request(), 6)],
                snapshot_targets: vec![],
            })
        });

    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.match_index.insert(2, 5);
    state.next_index.insert(2, 6);

    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(
        task_rx.try_recv().is_ok(),
        "the data request must be dispatched"
    );
    assert_eq!(
        state.next_index.get(&2).copied(),
        Some(7),
        "setup: Replicate advances next_index optimistically at dispatch"
    );
    assert_eq!(
        state.in_flight_count(2),
        1,
        "setup: dispatch occupies one slot"
    );

    state.handle_peer_stream_error(2);

    assert_eq!(state.peer_replication_state(2), PeerReplicationState::Probe);
    assert_eq!(
        state.in_flight_count(2),
        0,
        "the lost request's slot must be released, or the gate stays closed"
    );
    assert_eq!(
        state.next_index.get(&2).copied(),
        Some(6),
        "next_index must rewind to match_index + 1 so the lost range is offered again"
    );
}

/// Two-peer harness: both peers' worker channels are held by the test, so each peer's dispatch is
/// observable independently.
async fn setup_two_worker_harness(
    path: &str
) -> (
    crate::raft_context::RaftContext<MockTypeConfig>,
    LeaderState<MockTypeConfig>,
    mpsc::UnboundedReceiver<super::ReplicationTask>,
    mpsc::UnboundedReceiver<super::ReplicationTask>,
    mpsc::UnboundedSender<InternalEvent>,
) {
    let (ctx, mut state, task_rx2, internal_event_tx) = setup_gate_harness(path).await;
    let (task_tx3, task_rx3) = mpsc::unbounded_channel();
    state.replication_workers.insert(
        3,
        super::ReplicationWorkerHandle {
            task_tx: task_tx3,
            snapshot_failure_count: 0,
            snapshot_next_retry_at: None,
        },
    );
    (ctx, state, task_rx2, task_rx3, internal_event_tx)
}

/// One peer's closed window must not block another peer's dispatch.
///
/// # Scenario
/// - Peer 2 is `Probe` (window 1) with its one probe unanswered; peer 3 is `Replicate`.
/// - Every batch offers a data request to both peers.
/// - Expected: batch 1 reaches both; batch 2 is withheld from peer 2 (window full) but still
///   reaches peer 3.
#[tokio::test]
#[traced_test]
async fn test_gated_peer_does_not_block_other_peer() {
    let (mut ctx, mut state, mut rx2, mut rx3, internal_event_tx) =
        setup_two_worker_harness("/tmp/test_gated_peer_does_not_block_other_peer").await;

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(2)
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_probe_request(), 1), (3, stub_probe_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    state.set_peer_replication_state(3, PeerReplicationState::Replicate);

    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(rx2.try_recv().is_ok(), "batch 1 must reach peer 2");
    assert!(rx3.try_recv().is_ok(), "batch 1 must reach peer 3");

    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(
        rx2.try_recv().is_err(),
        "peer 2's window is full, batch 2 must be withheld from it"
    );
    assert!(
        rx3.try_recv().is_ok(),
        "peer 2 being gated must not stop peer 3 from receiving batch 2"
    );
}

/// A stream failure on one peer must leave the other peer's ledger untouched.
///
/// # Scenario
/// - Peer 2 and peer 3 are both `Replicate` with a request in flight; `next_index` recorded.
/// - `handle_peer_stream_error(2)`.
/// - Expected: peer 2 is demoted and cleared; peer 3 keeps its state, in-flight count and
///   `next_index`.
#[tokio::test]
#[traced_test]
async fn test_stream_error_on_one_peer_leaves_other_peer_untouched() {
    let (_ctx, mut state, _rx2, _rx3, _tx) =
        setup_two_worker_harness("/tmp/test_stream_error_on_one_peer_leaves_other_untouched").await;

    for peer in [2_u32, 3] {
        state.set_peer_replication_state(peer, PeerReplicationState::Replicate);
        state.match_index.insert(peer, 5);
        state.next_index.insert(peer, 9);
        state.record_in_flight(peer, 6);
    }

    state.handle_peer_stream_error(2);

    assert_eq!(state.peer_replication_state(2), PeerReplicationState::Probe);
    assert_eq!(state.in_flight_count(2), 0);
    assert_eq!(
        state.peer_replication_state(3),
        PeerReplicationState::Replicate,
        "peer 3 must not be demoted by peer 2's failure"
    );
    assert_eq!(
        state.in_flight_count(3),
        1,
        "peer 3's in-flight slot must survive"
    );
    assert_eq!(
        state.next_index.get(&3).copied(),
        Some(9),
        "peer 3's next_index must not be rewound"
    );
}

/// End-to-end window behavior in `Replicate`: several requests may be in flight without any ACK
/// (not serialized to one), and the dispatch that would exceed the window is withheld.
///
/// # Scenario
/// - `max_inflight_append_requests = 3`, peer 2 in `Replicate`, every batch offers a data request.
/// - Batches 1..3 are all dispatched with no response in between (in flight 1, 2, 3).
/// - Batch 4 would be the fourth outstanding request: it must be withheld, in flight stays 3.
#[tokio::test]
#[traced_test]
async fn test_replicate_dispatches_up_to_window_then_withholds() {
    let (mut ctx, mut state, mut task_rx, internal_event_tx) =
        setup_gate_harness("/tmp/test_replicate_dispatches_up_to_window_then_withholds").await;

    let mut cfg = (*ctx.node_config).clone();
    cfg.raft.replication.max_inflight_append_requests = 3;
    state.node_config = Arc::new(cfg);

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(4)
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_probe_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 1);

    for expected in 1..=3usize {
        state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
        assert!(
            task_rx.try_recv().is_ok(),
            "request {expected} must be dispatched without waiting for an ACK"
        );
        assert_eq!(state.in_flight_count(2), expected);
    }

    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    assert!(
        task_rx.try_recv().is_err(),
        "the fourth outstanding request exceeds the window and must be withheld"
    );
    assert_eq!(
        state.in_flight_count(2),
        3,
        "a withheld request must not occupy a slot"
    );
}

/// Window=3, four dispatch attempts: the first three are recorded (occupancy before each
/// dispatch is 0,1,2, one entry each), the fourth is withheld and is counted as gated but
/// must not add a histogram sample (it never dispatched).
#[tokio::test]
#[traced_test]
async fn test_dispatch_metrics_record_occupancy_entries_and_gated_count() {
    let capture = MetricsCapture::new();
    let _guard = metrics::set_default_local_recorder(&capture);

    let (mut ctx, mut state, mut task_rx, internal_event_tx) =
        setup_gate_harness("/tmp/test_dispatch_metrics_record_occupancy").await;

    let mut cfg = (*ctx.node_config).clone();
    cfg.raft.replication.max_inflight_append_requests = 3;
    state.node_config = Arc::new(cfg);

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(4)
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_probe_request(), 1)],
                snapshot_targets: vec![],
            })
        });

    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 1);

    for _ in 0..4 {
        state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    }
    assert!(task_rx.try_recv().is_ok(), "sanity: dispatches happened");

    assert_eq!(
        capture.histogram("core.raft.peer.in_flight_at_dispatch", &[("peer_id", "2")]),
        vec![0.0, 1.0, 2.0],
        "occupancy is sampled before each of the 3 real dispatches; the withheld one adds nothing"
    );
    assert_eq!(
        capture.histogram("core.raft.replication.entries_per_request", &[]),
        vec![1.0, 1.0, 1.0],
        "one sample per real dispatch, equal to the request's entry count"
    );
    assert_eq!(
        capture.counter("core.raft.peer.dispatch_gated_total", &[("peer_id", "2")]),
        1,
        "exactly the fourth attempt was withheld by the window"
    );
}

/// A heartbeat (empty entries) occupies no window slot, so it must neither be counted as
/// gated when the window is full nor contribute to the dispatch histograms.
#[tokio::test]
#[traced_test]
async fn test_heartbeat_emits_no_dispatch_metrics_even_when_window_full() {
    let capture = MetricsCapture::new();
    let _guard = metrics::set_default_local_recorder(&capture);

    let (mut ctx, mut state, _task_rx, internal_event_tx) =
        setup_gate_harness("/tmp/test_heartbeat_emits_no_dispatch_metrics").await;

    let mut cfg = (*ctx.node_config).clone();
    cfg.raft.replication.max_inflight_append_requests = 1;
    state.node_config = Arc::new(cfg);

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _, _| {
            Ok(crate::PrepareResult {
                append_requests: vec![(2, AppendEntriesRequest::default(), 1)],
                snapshot_targets: vec![],
            })
        });

    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 1);
    state.record_in_flight(2, 1);
    assert!(state.should_gate_by_inflight(2), "setup: window is full");

    state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();

    assert_eq!(
        capture.counter("core.raft.peer.dispatch_gated_total", &[("peer_id", "2")]),
        0,
        "a heartbeat bypasses the gate, so it is not a gated dispatch"
    );
    assert!(
        capture
            .histogram("core.raft.peer.in_flight_at_dispatch", &[("peer_id", "2")])
            .is_empty(),
        "heartbeats are not window dispatches"
    );
    assert!(
        capture.histogram("core.raft.replication.entries_per_request", &[]).is_empty(),
        "an empty heartbeat must not skew the entries-per-request distribution"
    );
}

/// The two config gauges are the reference line for reading occupancy / batch-size
/// distributions; they must carry the values the leader was actually built with.
#[tokio::test]
#[traced_test]
async fn test_config_gauges_reflect_configured_values() {
    let capture = MetricsCapture::new();
    let _guard = metrics::set_default_local_recorder(&capture);

    let (_graceful_tx, graceful_rx) = watch::channel(());
    let ctx = mock_raft_context("/tmp/test_config_gauges_reflect_values", graceful_rx, None);
    let mut cfg = (*ctx.node_config).clone();
    cfg.raft.replication.max_inflight_append_requests = 7;
    cfg.raft.replication.append_entries_max_entries_per_replication = 42;
    cfg.raft.replication.replication_send_queue_capacity = 900;

    let _state = LeaderState::<MockTypeConfig>::new(1, Arc::new(cfg));

    assert_eq!(
        capture.gauge("core.raft.config.max_inflight_append_requests", &[]),
        Some(7.0)
    );
    assert_eq!(
        capture.gauge(
            "core.raft.config.append_entries_max_entries_per_replication",
            &[]
        ),
        Some(42.0)
    );
    assert_eq!(
        capture.gauge("core.raft.config.replication_send_queue_capacity", &[]),
        Some(900.0)
    );
}

/// Real elections build the leader via `From<&CandidateState>`, not `LeaderState::new`,
/// so the config gauges must be set on that path too or they never reach a running cluster.
#[tokio::test]
#[traced_test]
async fn test_config_gauges_are_set_when_leader_is_built_from_candidate() {
    let capture = MetricsCapture::new();
    let _guard = metrics::set_default_local_recorder(&capture);

    let (_graceful_tx, graceful_rx) = watch::channel(());
    let ctx = mock_raft_context("/tmp/test_config_gauges_from_candidate", graceful_rx, None);
    let mut cfg = (*ctx.node_config).clone();
    cfg.raft.replication.max_inflight_append_requests = 7;
    cfg.raft.replication.append_entries_max_entries_per_replication = 42;
    cfg.raft.replication.replication_send_queue_capacity = 900;

    let candidate =
        crate::raft_role::candidate_state::CandidateState::<MockTypeConfig>::new(1, Arc::new(cfg));
    let _leader = LeaderState::<MockTypeConfig>::from(&candidate);

    assert_eq!(
        capture.gauge("core.raft.config.max_inflight_append_requests", &[]),
        Some(7.0),
        "production builds the leader via From<&CandidateState>; gauge must be set there"
    );
    assert_eq!(
        capture.gauge(
            "core.raft.config.append_entries_max_entries_per_replication",
            &[]
        ),
        Some(42.0)
    );
    assert_eq!(
        capture.gauge("core.raft.config.replication_send_queue_capacity", &[]),
        Some(900.0)
    );
}
