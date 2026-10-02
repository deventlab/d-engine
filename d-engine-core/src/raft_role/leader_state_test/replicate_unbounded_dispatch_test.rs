//! Characterizes `LeaderState`'s Phase 5 dispatch loop in isolation: it has NO in-flight limit of
//! its own for `Replicate` state (unlike `Probe`, see `probe_backpressure_test.rs`) and applies
//! `next_index = effective_next_index + entries.len()` to whatever `prepare_batch_requests` hands
//! it, on every batch, unconditionally.
//!
//! Background (full root-cause chain: `replication-worker-backpressure-deadlock-expert-q.md` and
//! `446-replication-stall-case-study-2026-09-28.md` in the product-design repo): a real deadlock
//! reproduced on embedded-bench (100K writes, 3-node localhost) traced to `ReplicationHandler`
//! (`retrieve_to_be_synced_logs_for_peers` / `build_append_request`) repeatedly re-offering the
//! same un-acked range — 8955 rebuilds for 28 real acks in one capture — which starved the peer's
//! own worker task of scheduling (CPU spent re-fetching/re-cloning the same entries, not network
//! I/O) and spiraled into a permanent stall.
//!
//! **Important scoping correction**: the fix belongs in `ReplicationHandler`, upstream of this
//! layer — it must stop *offering* a duplicate un-acked range at all (checked before touching the
//! log, mirroring `raft-rs`'s `Progress::is_paused()` ahead of `maybe_send_append`'s entry fetch).
//! Once that lands, `build_append_request` naturally falls back to an empty (heartbeat) request
//! for a peer with nothing new to offer (`entries_per_peer.remove(..).unwrap_or_default()`), and
//! Phase 5's existing formula correctly no-ops on that (`entries.len() == 0` advances nothing) —
//! **no change needed here**. This test's assertions (Phase 5 forwards whatever it's given,
//! unconditionally) describe a real but *different* property than the bug, and should stay GREEN
//! even after the real fix lands — it is not the regression test for the fix. That test belongs
//! next to `ReplicationHandler` (`replication_handler_test/`) once the in-flight representation
//! is implemented — this file's mocked `prepare_batch_requests` bypasses that logic entirely, by
//! design, to isolate Phase 5's own (correct, by-design) "trust the input" contract.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use d_engine_proto::common::{Entry, EntryPayload, NodeRole::Follower, NodeStatus};
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::replication::AppendEntriesRequest;
use tokio::sync::{mpsc, watch};
use tracing_test::traced_test;

use crate::MockMembership;
use crate::MockRaftLog;
use crate::RaftRequestWithSignal;
use crate::event::InternalEvent;
use crate::maybe_clone_oneshot::{MaybeCloneOneshot, RaftOneshot};
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::{PeerReplicationState, RaftRoleState};
use crate::test_utils::mock::{MockTypeConfig, mock_raft_context};

/// Two-voter membership (peers 2 & 3) — same shape as `probe_backpressure_test.rs`'s, duplicated
/// here rather than shared: these helpers are private to their own test module.
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

/// A non-empty (non-heartbeat) request — one entry, so `next_index` advances by exactly 1 per
/// dispatch, making the "raced ahead by ROUNDS" assertion below exact, not just "some drift".
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

/// 50 back-to-back batches, peer 2 in `Replicate`, `handle_append_result` never called for it —
/// simulating a worker that accepted every task into its (unbounded) queue but has delivered and
/// had acknowledged literally none of them, exactly what a worker wedged inside a full
/// bounded-128 `stream_sender.send().await` looks like from the leader's side.
#[tokio::test]
#[traced_test]
async fn test_replicate_peer_dispatch_and_next_index_are_unbounded_without_any_ack() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_replicate_peer_dispatch_and_next_index_are_unbounded_without_any_ack",
        graceful_rx,
        None,
    );
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    // Peer 2's worker channel held directly by the test — dispatch is observable synchronously,
    // no real transport/worker task involved (same pattern as probe_backpressure_test.rs).
    let (task_tx, mut task_rx) = mpsc::unbounded_channel();
    state.replication_workers.insert(
        2,
        super::ReplicationWorkerHandle {
            task_tx,
            snapshot_failure_count: 0,
            snapshot_next_retry_at: None,
        },
    );

    // The steady state a peer settles into after its first real ACK — where this bug lives.
    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 1);

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

    const ROUNDS: u64 = 50;
    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(ROUNDS as usize)
        .returning(|_, _, leader_state_snapshot, _, _, _| {
            // Read the CURRENT next_index for peer 2, same as the real
            // `prepare_batch_requests` does via `leader_state_snapshot.next_index` — this is
            // what makes next_index compound round over round below, faithfully reproducing
            // the real feedback loop instead of a fixed stand-in value.
            let effective_next_index =
                leader_state_snapshot.next_index.get(&2).copied().unwrap_or(1);
            Ok(crate::PrepareResult {
                append_requests: vec![(2, stub_probe_request(), effective_next_index)],
                snapshot_targets: vec![],
            })
        });

    for _ in 0..ROUNDS {
        state.process_batch(one_entry_batch(), &internal_event_tx, &ctx).await.unwrap();
    }

    let mut dispatched = 0u64;
    while task_rx.try_recv().is_ok() {
        dispatched += 1;
    }
    assert_eq!(
        dispatched, ROUNDS,
        "a Replicate-state peer accepted every one of {ROUNDS} un-acked dispatches — there is no \
         in-flight limit at all for Replicate (unlike Probe), so a wedged worker never causes \
         back-pressure on the dispatch loop"
    );

    // stub_probe_request() carries 1 entry, so next_index advances by 1 per round purely from
    // dispatch — zero acknowledgments were ever processed.
    assert_eq!(
        state.next_index.get(&2),
        Some(&(1 + ROUNDS)),
        "next_index advanced by one dispatch's worth of entries on every single round with zero \
         real acknowledgments — the leader's belief about this peer's progress has no bound tying \
         it to what has actually been confirmed delivered"
    );
}
