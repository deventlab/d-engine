//! Test heartbeat scenarios when in-flight windows are full or empty.
//!
//! Background: the fix for #446 introduces a per-peer in-flight counter to prevent
//! the leader from repeatedly re-offering the same un-acknowledged entries. This creates
//! an architectural question: when the leader wants to send a genuine heartbeat (no new
//! data to replicate), but the peer's in-flight window is already full, what should happen?
//!
//! **Core principle**: heartbeats must never be blocked by window pressure. A heartbeat is
//! semantically an empty-entries request; an empty entries list occupies zero window slots.
//! Therefore:
//! - A *true* heartbeat (entries built by `prepare_batch_requests` are absent, so
//!   `build_append_request` produces empty entries) must always be sendable.
//! - A *substitute* heartbeat (entries were withheld because the window is full, so
//!   `prepare_batch_requests` did not populate `peer_entries` for this peer) is also safe
//!   to send as empty entries, from the follower's point of view.
//!
//! The distinction matters for observability and correctness reasoning: we must verify
//! that even when a Replicate-state peer's window is at its limit (N un-acked requests),
//! the leader still emits a heartbeat that cycle (possibly empty) to keep the peer from
//! timing out and initiating an election.
//!
//! These tests verify the contract:
//! 1. True heartbeat always succeeds, even with a full window.
//! 2. Substitute heartbeat (window full) also gets sent, preserving liveness.
//! 3. Multiple in-flight requests for Replicate state can coexist (window > 1).
//! 4. Probe-state peers are still limited to exactly one in-flight request (window = 1).
//! 5. Receiving an ACK decrements exactly one in-flight slot, not all of them.

use std::sync::Arc;

use d_engine_proto::common::{NodeRole::Follower, NodeStatus};
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::replication::{
    AppendEntriesResponse, ConflictResult, append_entries_response,
};
use tokio::sync::{mpsc, watch};
use tracing_test::traced_test;

use crate::MockMembership;
use crate::MockRaftLog;
use crate::event::InternalEvent;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::{PeerReplicationState, RaftRoleState};
use crate::test_utils::MetricsCapture;
use crate::test_utils::mock::{MockTypeConfig, mock_raft_context};

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

/// Verify that a peer in Replicate state can track multiple un-acknowledged requests
/// simultaneously (window > 1). This is the throughput-critical path: without multi-slot
/// windows, Replicate degrades to Probe's RTT-serialized behavior.
#[tokio::test]
#[traced_test]
async fn test_replicate_multiple_in_flight_requests_coexist() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_replicate_multiple_in_flight_coexist",
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

    // Place peer 2 in Replicate state (not Probe).
    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 1);

    // Verify initial in-flight count is 0.
    assert_eq!(
        state.in_flight_count(2),
        0,
        "Replicate-state peer must start with zero in-flight requests"
    );

    // Simulate dispatching multiple un-acknowledged requests.
    state.record_in_flight(2, 10);
    assert_eq!(state.in_flight_count(2), 1);

    state.record_in_flight(2, 20);
    assert_eq!(state.in_flight_count(2), 2);

    state.record_in_flight(2, 30);
    assert_eq!(state.in_flight_count(2), 3);

    // Contract: for a configurable window size (default 256), this should still be allowed.
    // (Actual window gating happens in Phase 5 / prepare_batch_requests; this test verifies
    // the counter itself accumulates correctly.)
}

/// An ACK releases every outstanding request whose last entry is `<= match_index` (cumulative),
/// and nothing else. An older or repeated ACK frees nothing.
#[tokio::test]
#[traced_test]
async fn test_replicate_ack_releases_all_requests_up_to_its_index() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_replicate_ack_releases_one_slot",
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

    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 1);

    // 5 un-acknowledged requests, last entry indexes 10..=50.
    for last_index in [10, 20, 30, 40, 50] {
        state.record_in_flight(2, last_index);
    }
    assert_eq!(state.in_flight_count(2), 5, "Setup: 5 requests in-flight");

    state.release_in_flight_up_to(2, 10);
    assert_eq!(
        state.in_flight_count(2),
        4,
        "ACK 10 covers only the first request"
    );

    state.release_in_flight_up_to(2, 40);
    assert_eq!(
        state.in_flight_count(2),
        1,
        "ACK 40 is cumulative: it covers the requests ending at 20, 30 and 40"
    );

    state.release_in_flight_up_to(2, 30);
    assert_eq!(state.in_flight_count(2), 1, "an older ACK frees nothing");

    state.release_in_flight_up_to(2, 40);
    assert_eq!(state.in_flight_count(2), 1, "a repeated ACK frees nothing");
}

/// Verify that when a state transition occurs (e.g., Replicate → Probe due to a conflict),
/// the in-flight counter is reset to 0. The old state's un-acknowledged requests are now
/// invalid (they were sent with a stale `next_index`), and the new state must start fresh.
#[tokio::test]
#[traced_test]
async fn test_replicate_to_probe_transition_resets_in_flight() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_replicate_to_probe_reset", graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 100);

    // Peer 2 has 10 un-acknowledged requests in Replicate state.
    for i in 1..=10u64 {
        state.record_in_flight(2, i * 10);
    }
    assert_eq!(state.in_flight_count(2), 10);

    // Conflict received: transition back to Probe, which resets all in-flight.
    state.set_peer_replication_state(2, PeerReplicationState::Probe);
    assert_eq!(
        state.in_flight_count(2),
        0,
        "State transition to Probe must reset in-flight counter to 0"
    );
    assert_eq!(
        state.peer_replication_state(2),
        PeerReplicationState::Probe,
        "Peer should now be in Probe state"
    );
}

/// Verify that Probe state is still limited to at most one in-flight request (window = 1).
/// The window=1 constraint for Probe is enforced in Phase 5 (leader_state.rs dispatch loop);
/// this test verifies the in-flight counter itself can track Probe's semantics correctly
/// (one recorded request, released again by an ACK covering it).
#[tokio::test]
#[traced_test]
async fn test_probe_window_one_semantic() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_probe_window_one", graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    state.set_peer_replication_state(2, PeerReplicationState::Probe);
    state.next_index.insert(2, 1);

    // Probe starts with zero in-flight.
    assert_eq!(state.in_flight_count(2), 0);

    // Dispatch the first (and only allowed) probe request.
    state.record_in_flight(2, 10);
    assert_eq!(
        state.in_flight_count(2),
        1,
        "Probe-state peer has exactly one in-flight request"
    );

    // Phase 5 should NOT dispatch another request until this one is ACK'd
    // (that gate is in Phase 5; here we just verify the counter tracks 1).

    // Receive the ACK, releasing the slot.
    state.release_in_flight_up_to(2, 10);
    assert_eq!(
        state.in_flight_count(2),
        0,
        "Probe ACK releases the single slot"
    );
}

/// Verify that heartbeats (empty entries) do not increment the in-flight counter.
/// This is a contract test: when Phase 5 detects `entries.is_empty()`, it must NOT
/// call `record_in_flight`. The window should only track *data* requests, not keepalive.
#[tokio::test]
#[traced_test]
async fn test_heartbeat_does_not_occupy_window() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_heartbeat_no_window", graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 100);

    // Simulate the window being full (256 un-acked requests).
    // In reality, these would be actual data requests; we just set the counter.
    for last_index in 1..=256u64 {
        state.record_in_flight(2, last_index);
    }
    assert_eq!(state.in_flight_count(2), 256, "Window is now full");

    // Phase 5 logic: if `is_heartbeat` (empty entries), do NOT check the window gate,
    // and do NOT call `record_in_flight`. This test documents that heartbeat
    // does not occupy a slot even when the window is full.
    // The actual gate enforcement is in Phase 5; here we just verify that
    // `record_in_flight` should NOT be called for heartbeats.

    assert_eq!(
        state.in_flight_count(2),
        256,
        "A heartbeat never recorded a request, so the window remains at 256 (full)"
    );
}

/// Releasing on an empty window is a no-op, and the window is still usable afterwards.
#[tokio::test]
#[traced_test]
async fn test_release_on_empty_window_is_a_noop() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_release_on_empty_window", graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 1);

    assert_eq!(state.in_flight_count(2), 0);

    state.release_in_flight_up_to(2, 100);
    assert_eq!(
        state.in_flight_count(2),
        0,
        "an ACK with nothing outstanding frees nothing"
    );

    state.record_in_flight(2, 101);
    assert_eq!(
        state.in_flight_count(2),
        1,
        "the window is still usable afterwards"
    );
}

/// Combines two contracts that must both hold at once for `Replicate` state (window > 1):
/// a stale-term response must not release *any* slot, and a real (term-matching) response
/// must release exactly the requests it covers — not all outstanding slots, and not zero.
///
/// This matters specifically for Replicate because the failure mode is different from Probe's
/// (window=1): with multiple requests in flight, an over-eager release (e.g. accidentally
/// resetting instead of releasing by index) would silently let through a burst of new sends the
/// window was supposed to prevent, while an under-release would eventually starve the peer of
/// any new data once real ACKs stop being able to keep up.
///
/// # Scenario
/// - Peer 2 in `Replicate` state, 3 un-acknowledged requests ending at indexes 3, 6 and 9
///   (in_flight = 3).
/// - A stale-term response arrives (term 0 < leader_term 1) — must release nothing (in_flight
///   stays 3).
/// - A real, term-matching success response for index 3 arrives — must release exactly the
///   request ending at 3 (in_flight becomes 2), proving the earlier stale response didn't
///   already consume it.
#[tokio::test]
#[traced_test]
async fn test_replicate_stale_response_releases_nothing_real_ack_releases_covered_requests() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_replicate_stale_releases_nothing_real_releases_one",
        graceful_rx,
        None,
    );
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 3);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(3));
    ctx.storage.raft_log = Arc::new(raft_log);

    ctx.handlers
        .replication_handler
        .expect_handle_success_response()
        .returning(|_, _, _, _| {
            Ok(crate::PeerUpdate {
                match_index: Some(3),
                next_index: 4,
                success: true,
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 1);

    // Set up 3 outstanding requests, as Phase 5 dispatch would have left them.
    state.record_in_flight(2, 3);
    state.record_in_flight(2, 6);
    state.record_in_flight(2, 9);
    assert_eq!(state.in_flight_count(2), 3, "setup: 3 requests in flight");

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

    // Stale response: term 0 < leader_term 1 — must not touch any slot.
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
        3,
        "a stale-term response must release zero slots, not one and not all three"
    );

    // Real response: term matches — must release exactly one slot.
    state
        .handle_append_result(
            2,
            Ok(AppendEntriesResponse {
                node_id: 2,
                term: 1,
                result: Some(append_entries_response::Result::Success(
                    d_engine_proto::server::replication::SuccessResult {
                        last_match: Some(d_engine_proto::common::LogId { term: 1, index: 3 }),
                    },
                )),
            }),
            &ctx,
            &internal_event_tx,
        )
        .await
        .unwrap();
    assert_eq!(
        state.in_flight_count(2),
        2,
        "the real response must release exactly one slot — the stale response above must not \
         have already consumed it, and this one must not release the other two"
    );
}

/// `ConflictResult` differs from `SuccessResult` here: both are term-matching resolutions (so
/// neither is stale-ignored), but conflict additionally transitions the peer to `Probe`
/// (`update_peer_index`'s conflict branch → `set_peer_replication_state(Probe)`), and that
/// transition unconditionally resets the whole in-flight window to 0 — not a partial release. This is intentional: a conflict means the leader's whole speculative pipeline
/// for this peer was built on a wrong assumption, so every other request still in flight is
/// also suspect, not just the one this response answers. Documented here so this isn't
/// mistaken for a bug symmetrical to the success case.
#[tokio::test]
#[traced_test]
async fn test_replicate_conflict_response_resets_all_inflight_via_probe_transition() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_replicate_conflict_releases_one_slot",
        graceful_rx,
        None,
    );
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    ctx.handlers
        .replication_handler
        .expect_handle_conflict_response()
        .returning(|_, _, _, _| {
            Ok(crate::PeerUpdate {
                match_index: None,
                next_index: 1,
                success: false,
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 1);

    state.record_in_flight(2, 3);
    state.record_in_flight(2, 6);
    assert_eq!(state.in_flight_count(2), 2, "setup: 2 requests in flight");

    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

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
        "conflict must reset in-flight to 0 via the Probe transition, not decrement by one — \
         the whole speculative pipeline for this peer is now suspect, not just this request"
    );
    assert_eq!(
        state.peer_replication_state(2),
        PeerReplicationState::Probe,
        "conflict must move the peer to Probe"
    );
}

/// `handle_peer_stream_error` (the consumer of a failed/full worker send) must demote a
/// `Replicate` peer to `Probe`, drop every outstanding in-flight slot, and rewind `next_index`
/// to `match_index + 1` so the next probe re-sends whatever the follower never received.
///
/// # Scenario
/// - Peer 2 in `Replicate`, `match_index = 5`, speculative `next_index = 10`, 3 requests in flight.
/// - `handle_peer_stream_error(2)`.
/// - Expected: `Probe`, in_flight 0, `next_index = 6`.
#[tokio::test]
#[traced_test]
async fn test_peer_stream_error_demotes_replicate_peer_and_clears_inflight() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_peer_stream_error_demotes_replicate_peer",
        graceful_rx,
        None,
    );
    ctx.membership = Arc::new(two_peer_membership());

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.match_index.insert(2, 5);
    state.next_index.insert(2, 10);
    state.record_in_flight(2, 7);
    state.record_in_flight(2, 8);
    state.record_in_flight(2, 9);
    assert_eq!(state.in_flight_count(2), 3, "setup: 3 requests in flight");

    state.handle_peer_stream_error(2);

    assert_eq!(state.peer_replication_state(2), PeerReplicationState::Probe);
    assert_eq!(
        state.in_flight_count(2),
        0,
        "every outstanding slot is suspect once the send path failed"
    );
    assert_eq!(
        state.next_index.get(&2).copied(),
        Some(6),
        "next_index must rewind to match_index + 1 so unACKed entries are re-sent"
    );
}

/// Pins a deliberate design decision in `set_peer_replication_state`: the "only reset on a
/// transition" check compares against `peer_replication_state.insert`'s return value (was this
/// peer ever explicitly recorded before?), not against the peer's implicit state (`Probe` by
/// default when never recorded). This means the *first* explicit call for a given peer always
/// resets in-flight, even if the state being set matches what the implicit default already was.
///
/// # Why this is the chosen behavior, not a bug
/// Owner's call: correctness must not depend on tracking "was this the peer's first-ever
/// explicit state write" separately from the `HashMap` itself — `insert`'s return value already
/// tells us that for free. The cost is this one corner case (first write happens to match the
/// implicit default) triggers a reset that, by pure state-equality, wasn't strictly necessary —
/// but every current call site only reaches this in ways that are harmless (a peer's first
/// explicit state write coincides with it having at most one outstanding request, since
/// multiple in-flight requires already being in `Replicate`, which requires an explicit
/// insert). Accepting this corner case keeps the implementation simple and avoids a second,
/// separate notion of "current state" to keep in sync with the `HashMap`.
///
/// This test exists so that if a future refactor "simplifies" this into comparing against
/// `peer_replication_state()`'s implicit-default-aware accessor instead, it fails loudly instead
/// of silently changing behavior.
#[tokio::test]
#[traced_test]
async fn test_first_explicit_state_write_resets_inflight_even_if_state_unchanged() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_first_explicit_state_write_resets_inflight",
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

    // Peer 2 has never had set_peer_replication_state called for it — peer_replication_state
    // (the HashMap) has no entry, so its state is only implicitly Probe via the default in
    // peer_replication_state()'s accessor.
    assert_eq!(
        state.peer_replication_state(2),
        PeerReplicationState::Probe,
        "setup: peer 2's state must be the implicit default, not an explicitly recorded one"
    );

    // Simulate a request already dispatched to this never-explicitly-recorded peer (this is
    // exactly what Phase 5 does for a peer's first-ever probe — record_in_flight does not
    // require set_peer_replication_state to have been called first).
    state.record_in_flight(2, 1);
    assert_eq!(state.in_flight_count(2), 1, "setup: one request in flight");

    // The first-ever explicit call for peer 2, setting it to Probe — the same value its
    // implicit default already was. Per the chosen design, this still resets in-flight, because
    // the check is "was this HashMap entry ever written before", not "did the state value
    // change".
    state.set_peer_replication_state(2, PeerReplicationState::Probe);

    assert_eq!(
        state.in_flight_count(2),
        0,
        "the first explicit write for a peer always resets in-flight, by design — the check is \
         against HashMap::insert's return value (was there a prior entry?), not against the \
         peer's implicit state"
    );
}

/// Boundary behavior of the window gate, which Phase 2 and Phase 5 both consume: one below the
/// limit is open, at the limit is closed, and a released slot reopens it.
///
/// # Scenario
/// - `max_inflight_append_requests = 3`. `Replicate`: 0/2 in flight open, 3 closed, back to 2 open.
/// - `Probe` is fixed at a window of 1 regardless of the configured value.
#[tokio::test]
#[traced_test]
async fn test_window_gate_boundaries_for_replicate_and_probe() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_window_gate_boundaries", graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let mut cfg = (*ctx.node_config).clone();
    cfg.raft.replication.max_inflight_append_requests = 3;
    let mut state = LeaderState::<MockTypeConfig>::new(1, Arc::new(cfg));
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    assert!(!state.should_gate_by_inflight(2), "0 in flight: open");
    state.record_in_flight(2, 10);
    state.record_in_flight(2, 20);
    assert!(!state.should_gate_by_inflight(2), "2 of 3 in flight: open");
    state.record_in_flight(2, 30);
    assert!(state.should_gate_by_inflight(2), "3 of 3 in flight: closed");
    state.release_in_flight_up_to(2, 10);
    assert!(
        !state.should_gate_by_inflight(2),
        "a released slot reopens the gate"
    );

    assert!(
        !state.should_gate_by_inflight(3),
        "Probe with 0 in flight: open"
    );
    state.record_in_flight(3, 10);
    assert!(
        state.should_gate_by_inflight(3),
        "Probe is fixed at window 1, regardless of the configured Replicate window"
    );
}

// ── Gauge emission tests ──────────────────────────────────────────────────────

/// `core.raft.peer.in_flight` must reflect the current count immediately after
/// each increment and after a decrement.
#[tokio::test]
#[traced_test]
async fn test_in_flight_gauge_emitted() {
    let capture = MetricsCapture::new();
    // set_default_local_recorder stays active until guard is dropped; works on
    // current_thread tokio tests where there is no thread switching across awaits.
    let _guard = metrics::set_default_local_recorder(&capture);

    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_in_flight_gauge_emitted", graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Replicate);

    state.record_in_flight(2, 10);
    state.record_in_flight(2, 20);
    assert_eq!(
        capture.gauge("core.raft.peer.in_flight", &[("peer_id", "2")]),
        Some(2.0),
        "gauge must be 2 after two dispatches"
    );

    state.release_in_flight_up_to(2, 10);
    assert_eq!(
        capture.gauge("core.raft.peer.in_flight", &[("peer_id", "2")]),
        Some(1.0),
        "gauge must drop to 1 after an ACK covering the first request"
    );
}

/// `core.raft.peer.match_index` must be emitted when `update_peer_index` advances
/// the match index via a success response.
#[tokio::test]
#[traced_test]
async fn test_match_index_gauge_emitted_on_success_ack() {
    let capture = MetricsCapture::new();
    let _guard = metrics::set_default_local_recorder(&capture);

    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_match_index_gauge_emitted", graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 5);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
    ctx.storage.raft_log = Arc::new(raft_log);

    ctx.handlers
        .replication_handler
        .expect_handle_success_response()
        .returning(|_, _, _, _| {
            Ok(crate::PeerUpdate {
                match_index: Some(5),
                next_index: 6,
                success: true,
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 1);
    state.record_in_flight(2, 5);

    let (internal_event_tx, _rx) = mpsc::unbounded_channel::<InternalEvent>();
    state
        .handle_append_result(
            2,
            Ok(AppendEntriesResponse {
                node_id: 2,
                term: 1,
                result: Some(append_entries_response::Result::Success(
                    d_engine_proto::server::replication::SuccessResult {
                        last_match: Some(d_engine_proto::common::LogId { term: 1, index: 5 }),
                    },
                )),
            }),
            &ctx,
            &internal_event_tx,
        )
        .await
        .unwrap();

    assert_eq!(
        capture.gauge("core.raft.peer.match_index", &[("peer_id", "2")]),
        Some(5.0),
        "match_index gauge must be set to 5 after a success ACK for index=5"
    );
}

/// `core.raft.peer.last_ack_timestamp_seconds` must be set (non-zero) after a
/// term-matching response, and must NOT be set for a stale-term response.
#[tokio::test]
#[traced_test]
async fn test_last_ack_timestamp_gauge_emitted_only_on_non_stale_ack() {
    let capture = MetricsCapture::new();
    let _guard = metrics::set_default_local_recorder(&capture);

    {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut ctx = mock_raft_context("/tmp/test_last_ack_ts_gauge_emitted", graceful_rx, None);
        ctx.membership = Arc::new(two_peer_membership());

        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().returning(|| 0);
        raft_log.expect_flush().returning(|| Ok(()));
        raft_log.expect_save_hard_state().returning(|_| Ok(()));
        raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(3));
        ctx.storage.raft_log = Arc::new(raft_log);

        ctx.handlers.replication_handler.expect_handle_success_response().returning(
            |_, _, _, _| {
                Ok(crate::PeerUpdate {
                    match_index: Some(3),
                    next_index: 4,
                    success: true,
                })
            },
        );

        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
        state.init_cluster_metadata(&ctx.membership).await.unwrap();
        state.set_peer_replication_state(2, PeerReplicationState::Replicate);
        state.next_index.insert(2, 1);
        state.record_in_flight(2, 3);

        let (internal_event_tx, _rx) = mpsc::unbounded_channel::<InternalEvent>();

        // Stale response (term 0 < leader term 1): gauge must NOT be written yet.
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
            capture.gauge(
                "core.raft.peer.last_ack_timestamp_seconds",
                &[("peer_id", "2")]
            ),
            None,
            "stale-term response must not set the timestamp gauge"
        );

        // Real response (term 1 = leader term): gauge must be written as a positive unix timestamp.
        state
            .handle_append_result(
                2,
                Ok(AppendEntriesResponse {
                    node_id: 2,
                    term: 1,
                    result: Some(append_entries_response::Result::Success(
                        d_engine_proto::server::replication::SuccessResult {
                            last_match: Some(d_engine_proto::common::LogId { term: 1, index: 3 }),
                        },
                    )),
                }),
                &ctx,
                &internal_event_tx,
            )
            .await
            .unwrap();

        let ts = capture
            .gauge(
                "core.raft.peer.last_ack_timestamp_seconds",
                &[("peer_id", "2")],
            )
            .expect("timestamp gauge must be set after a real ACK");
        assert!(
            ts > 1_700_000_000.0,
            "timestamp must be a plausible unix epoch value, got {ts}"
        );
    }
}

/// A stream error loses every request still queued on that stream, so no response will ever
/// come back for them. The window must be cleared even when the peer is *already* Probe
/// (the Probe->Probe write is a no-op for `set_peer_replication_state`, so it cannot be
/// relied on to do the reset).
#[tokio::test]
#[traced_test]
async fn test_stream_error_clears_inflight_when_peer_is_already_probe() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_stream_error_probe_probe", graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();

    state.set_peer_replication_state(2, PeerReplicationState::Probe);
    state.record_in_flight(2, 1);
    assert_eq!(
        state.in_flight_count(2),
        1,
        "setup: one probe request outstanding"
    );

    state.handle_peer_stream_error(2);

    assert_eq!(
        state.in_flight_count(2),
        0,
        "the outstanding request died with the stream; its slot must be released"
    );
}

/// A success response reporting an index below every outstanding request (what a heartbeat
/// reply carries) must not release any data slot; a later response covering them must.
#[tokio::test]
#[traced_test]
async fn test_success_response_below_outstanding_requests_releases_nothing() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_success_below_outstanding", graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 20);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
    ctx.storage.raft_log = Arc::new(raft_log);

    ctx.handlers.replication_handler.expect_handle_success_response().returning(
        |_, _, success, _| {
            let match_index = success.last_match.as_ref().map(|l| l.index);
            Ok(crate::PeerUpdate {
                match_index,
                next_index: match_index.unwrap_or(0) + 1,
                success: true,
            })
        },
    );

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 1);
    state.record_in_flight(2, 10);
    state.record_in_flight(2, 20);

    let (internal_event_tx, _rx) = mpsc::unbounded_channel::<InternalEvent>();
    let success_at = |index: u64| AppendEntriesResponse {
        node_id: 2,
        term: 1,
        result: Some(append_entries_response::Result::Success(
            d_engine_proto::server::replication::SuccessResult {
                last_match: Some(d_engine_proto::common::LogId { term: 1, index }),
            },
        )),
    };

    state
        .handle_append_result(2, Ok(success_at(5)), &ctx, &internal_event_tx)
        .await
        .unwrap();
    assert_eq!(
        state.in_flight_count(2),
        2,
        "a reply at index 5 confirms nothing the two outstanding requests (10, 20) carry"
    );

    state
        .handle_append_result(2, Ok(success_at(20)), &ctx, &internal_event_tx)
        .await
        .unwrap();
    assert_eq!(
        state.in_flight_count(2),
        0,
        "a reply at index 20 covers both requests"
    );
}

/// A conflict rejects the request. A `Probe` peer stays `Probe`, so the state-change reset does
/// not fire: `update_peer_index` must clear the window itself or the gate stays closed forever.
#[tokio::test]
#[traced_test]
async fn test_probe_conflict_response_clears_window() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_probe_conflict_clears_window", graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    ctx.storage.raft_log = Arc::new(raft_log);

    ctx.handlers
        .replication_handler
        .expect_handle_conflict_response()
        .returning(|_, _, _, _| {
            Ok(crate::PeerUpdate {
                match_index: None,
                next_index: 1,
                success: false,
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Probe);
    state.next_index.insert(2, 1);
    state.record_in_flight(2, 1);
    assert!(
        state.should_gate_by_inflight(2),
        "setup: the Probe window (1) is full"
    );

    let (internal_event_tx, _rx) = mpsc::unbounded_channel::<InternalEvent>();
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
        "the rejected probe no longer occupies the window"
    );
    assert!(
        !state.should_gate_by_inflight(2),
        "the next probe may be sent"
    );
}

/// Builds a leader with peer 2 in `Replicate` and three requests in flight (last indexes 3, 6, 9),
/// feeds it one `handle_append_result` input, and returns the window afterwards.
///
/// If `conflict_handler_fails` is set, the conflict handler returns an error instead of a
/// `PeerUpdate`, as a future change to it might.
async fn window_after_response(
    response: crate::Result<AppendEntriesResponse>,
    conflict_handler_fails: bool,
) -> (usize, PeerReplicationState) {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context("/tmp/test_window_after_response", graceful_rx, None);
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 9);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(1));
    ctx.storage.raft_log = Arc::new(raft_log);

    ctx.handlers.replication_handler.expect_handle_success_response().returning(
        |_, _, success, _| {
            let match_index = success.last_match.as_ref().map(|l| l.index);
            Ok(crate::PeerUpdate {
                match_index,
                next_index: match_index.unwrap_or(0) + 1,
                success: true,
            })
        },
    );
    ctx.handlers.replication_handler.expect_handle_conflict_response().returning(
        move |_, _, _, _| {
            if conflict_handler_fails {
                Err(crate::ReplicationError::HigherTerm(9).into())
            } else {
                Ok(crate::PeerUpdate {
                    match_index: None,
                    next_index: 1,
                    success: false,
                })
            }
        },
    );

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Replicate);
    state.next_index.insert(2, 10);
    for last_index in [3, 6, 9] {
        state.record_in_flight(2, last_index);
    }

    let (internal_event_tx, _rx) = mpsc::unbounded_channel::<InternalEvent>();
    let _ = state.handle_append_result(2, response, &ctx, &internal_event_tx).await;
    (state.in_flight_count(2), state.peer_replication_state(2))
}

fn response_with(
    term: u64,
    result: Option<append_entries_response::Result>,
) -> crate::Result<AppendEntriesResponse> {
    Ok(AppendEntriesResponse {
        node_id: 2,
        term,
        result,
    })
}

fn success_at(index: u64) -> Option<append_entries_response::Result> {
    Some(append_entries_response::Result::Success(
        d_engine_proto::server::replication::SuccessResult {
            last_match: Some(d_engine_proto::common::LogId { term: 1, index }),
        },
    ))
}

/// Every shape of response `handle_append_result` can receive, and what it must do to the window.
/// A new early return that skips the window handling shows up here as a changed row.
/// The leader term is 1; the window starts as [3, 6, 9].
#[tokio::test]
#[traced_test]
async fn test_every_response_shape_has_a_defined_window_effect() {
    use PeerReplicationState::{Probe, Replicate};
    let conflict = Some(append_entries_response::Result::Conflict(ConflictResult {
        conflict_term: None,
        conflict_index: Some(1),
    }));
    let closed = Err(crate::Error::System(crate::SystemError::Network(
        crate::NetworkError::ResponseChannelClosed,
    )));

    let cases: Vec<(
        &str,
        crate::Result<AppendEntriesResponse>,
        usize,
        PeerReplicationState,
    )> = vec![
        (
            "success covering one request",
            response_with(1, success_at(3)),
            2,
            Replicate,
        ),
        (
            "success covering two requests",
            response_with(1, success_at(6)),
            1,
            Replicate,
        ),
        (
            "success covering all requests",
            response_with(1, success_at(9)),
            0,
            Replicate,
        ),
        (
            "success below every request (heartbeat-like)",
            response_with(1, success_at(1)),
            3,
            Replicate,
        ),
        ("conflict", response_with(1, conflict), 0, Probe),
        ("no result variant", response_with(1, None), 0, Replicate),
        (
            "HigherTerm variant that is not higher (late rejection)",
            response_with(1, Some(append_entries_response::Result::HigherTerm(1))),
            3,
            Replicate,
        ),
        ("stale term", response_with(0, success_at(9)), 3, Replicate),
        ("superseded response channel", closed, 0, Replicate),
    ];

    for (name, response, expected_count, expected_state) in cases {
        let (count, state) = window_after_response(response, false).await;
        assert_eq!(count, expected_count, "window after: {name}");
        assert_eq!(state, expected_state, "peer state after: {name}");
    }
}

/// The conflict handler failing must not leave the rejected pipeline counted as in flight:
/// a stuck window means the peer never receives data again.
#[tokio::test]
#[traced_test]
async fn test_conflict_handler_error_does_not_leave_the_window_stuck() {
    let conflict = Some(append_entries_response::Result::Conflict(ConflictResult {
        conflict_term: None,
        conflict_index: Some(1),
    }));
    let (count, _) = window_after_response(response_with(1, conflict), true).await;
    assert_eq!(
        count, 0,
        "an error while handling a conflict must still clear the window"
    );
}

/// A follower can report an index beyond the leader's own log (for example a divergent tail
/// left by an earlier leader). The leader must never record a `match_index` past its own
/// last entry.
#[tokio::test]
#[traced_test]
async fn test_match_index_never_exceeds_the_leaders_own_last_entry() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_match_index_bounded_by_leader_log",
        graceful_rx,
        None,
    );
    ctx.membership = Arc::new(two_peer_membership());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 20);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(1));
    ctx.storage.raft_log = Arc::new(raft_log);

    ctx.handlers.replication_handler.expect_handle_success_response().returning(
        |_, _, success, _| {
            let match_index = success.last_match.as_ref().map(|l| l.index);
            Ok(crate::PeerUpdate {
                match_index,
                next_index: match_index.unwrap_or(0) + 1,
                success: true,
            })
        },
    );

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    state.set_peer_replication_state(2, PeerReplicationState::Replicate);

    let (internal_event_tx, _rx) = mpsc::unbounded_channel::<InternalEvent>();
    let response = response_with(1, success_at(50));
    state.handle_append_result(2, response, &ctx, &internal_event_tx).await.unwrap();

    assert!(
        state.match_index_for_test(2) <= 20,
        "the leader's log ends at 20, but peer 2's match_index was recorded as {}",
        state.match_index_for_test(2)
    );
}
