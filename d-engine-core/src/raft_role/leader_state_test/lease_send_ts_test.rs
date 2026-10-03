//! Tests for lease deadline anchored to heartbeat send time (RTT/2 fix).
//!
//! ## Background
//! The safety invariant for lease-based reads requires:
//!   `lease_duration_ms < election_timeout_min`
//!
//! However, the original implementation renewed the lease with `now_ms()` at the
//! time the quorum ACK was *received and processed*, not when the heartbeat was
//! *sent*. This extends the effective lease duration by ~RTT/2, violating the
//! invariant in practice.
//!
//! The fix: record `last_heartbeat_send_ts` before Phase 1 of
//! `execute_and_process_raft_rpc` and use it as the deadline base in
//! `update_lease_timestamp`, so the lease expires at `send_ts + lease_duration_ms`
//! rather than `ack_ts + lease_duration_ms`.

use crate::RaftNodeConfig;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::read_lease::now_ms;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use std::time::Duration;
use tokio::sync::watch;

// ── helpers ──────────────────────────────────────────────────────────────────

async fn setup_leader(
    lease_duration_ms: u64
) -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.lease_duration_ms = lease_duration_ms;
    let ctx = MockBuilder::new(shutdown_rx).with_node_config(node_config).build_context();
    let state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    (state, ctx)
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Deadline must be anchored to the heartbeat *send* time, not the ACK *receive* time.
///
/// With a simulated RTT of 5 ms:
/// - new impl: deadline = send_ts + lease_duration_ms  (correct)
/// - old impl: deadline = ack_ts  + lease_duration_ms  (too late by RTT)
///
/// At `send_ts + lease_duration_ms` the new lease must already be expired,
/// while the old impl would still report valid — proving the old window was real.
#[tokio::test]
async fn test_lease_deadline_anchored_to_heartbeat_send_time_not_ack_time() {
    let lease_duration_ms = 100u64;
    let (mut state, ctx) = setup_leader(lease_duration_ms).await;
    // Lease is only published once this term's noop is committed (Raft §8).
    state.on_noop_committed(&ctx).unwrap();

    let send_ts = now_ms();
    state.last_heartbeat_send_ts = send_ts;

    // Simulate RTT: 5 ms elapses before the ACK is processed.
    std::thread::sleep(Duration::from_millis(5));

    // Quorum ACK arrives — renew using send_ts (new impl).
    let applied = state.noop_log_id.expect("noop committed in setup above");
    state.test_renew_lease_from_send_ts(
        ctx.node_config().raft.read_consistency.lease_duration_ms,
        applied,
    );

    let term = state.current_term();
    let new_deadline = send_ts + lease_duration_ms;

    // Just before the send-anchored deadline: still valid.
    assert!(
        state.shared_state.read_lease.is_valid_for_leader(term, new_deadline - 1),
        "lease must be valid just before send-anchored deadline"
    );
    // At the send-anchored deadline: expired.
    assert!(
        !state.shared_state.read_lease.is_valid_for_leader(term, new_deadline),
        "lease must be expired at send_ts + lease_duration_ms"
    );

    // Document that the old implementation would NOT have expired yet.
    // old deadline ≈ send_ts + RTT + lease_duration_ms > new_deadline.
    let old_deadline_approx = now_ms() + lease_duration_ms; // ≈ send_ts + 5 + 100
    assert!(
        old_deadline_approx > new_deadline,
        "old impl extends lease by RTT: old={old_deadline_approx} new={new_deadline}"
    );
}

/// Without a quorum ACK, recording `last_heartbeat_send_ts` must NOT advance
/// the lease deadline. A stale leader that never gets ACKs must not serve reads.
#[tokio::test]
async fn test_no_quorum_ack_does_not_advance_lease_deadline() {
    let (mut state, _ctx) = setup_leader(100).await;

    // Record send timestamp — but no ACK arrives, so update_lease_timestamp is never called.
    state.last_heartbeat_send_ts = now_ms();

    let term = state.current_term();
    assert!(
        !state.shared_state.read_lease.is_valid_for_leader(term, now_ms()),
        "lease must remain invalid when no quorum ACK has been received"
    );
}

/// Demonstrates the invariant `lease_duration_ms < election_timeout_min` using
/// concrete numbers, showing the old implementation violated it and the new one preserves it.
///
/// Config: election_timeout_min=150ms, lease_duration_ms=140ms, RTT=10ms.
///
/// old impl: effective deadline = send_ts + RTT + 140 = send_ts + 150
///           = earliest possible election time  →  invariant broken ❌
///
/// new impl: effective deadline = send_ts + 140
///           < send_ts + 150 (earliest election)  →  invariant holds ✅
#[test]
fn test_new_impl_preserves_election_timeout_invariant_old_impl_violates_it() {
    let election_timeout_min = 150u64;
    let lease_duration_ms = 140u64;
    let simulated_rtt = 10u64;

    let send_ts = 1_000u64; // arbitrary base
    let ack_ts = send_ts + simulated_rtt;

    let new_deadline = send_ts + lease_duration_ms; // 1_140
    let old_deadline = ack_ts + lease_duration_ms; // 1_150
    let earliest_election = send_ts + election_timeout_min; // 1_150

    // New impl: lease expires strictly before any possible election.
    assert!(
        new_deadline < earliest_election,
        "new impl: deadline {new_deadline} must be < earliest election {earliest_election}"
    );

    // Old impl: lease expires at the same time as the earliest election — invariant broken.
    assert!(
        old_deadline >= earliest_election,
        "old impl: deadline {old_deadline} must be >= earliest election {earliest_election} \
         (documents the pre-fix violation)"
    );
}

/// A new leader must not publish a lease to readers before its own noop is committed
/// (Raft §8).
///
/// Scenario:
/// - Node is elected; the previous leader had committed entry X, but this node's state
///   machine has not applied X yet (it only learns the commit point once its noop commits).
/// - A quorum ACK arrives before the noop is committed.
///
/// Expected:
/// - The lease stays invalid for readers (`ReadActor` only looks at this lease).
///   Otherwise a LeaseRead right after the election can return a value older than a write
///   the previous leader already acknowledged to its client.
/// - Once the noop is committed, the next quorum ACK makes the lease valid.
#[tokio::test]
async fn test_lease_not_published_to_readers_before_noop_committed() {
    let lease_duration_ms = 1000u64;
    let (mut state, ctx) = setup_leader(lease_duration_ms).await;
    assert!(
        state.noop_log_id.is_none(),
        "precondition: freshly elected leader has not committed its noop"
    );

    state.last_heartbeat_send_ts = now_ms();
    // Applied position is irrelevant here: there is no noop yet to compare against.
    state.test_renew_lease_from_send_ts(lease_duration_ms, u64::MAX);

    assert!(
        !state.shared_state.read_lease.is_valid(now_ms()),
        "lease must not be visible to readers before the noop is committed"
    );

    state.on_noop_committed(&ctx).unwrap();
    let applied = state.noop_log_id.unwrap();
    state.last_heartbeat_send_ts = now_ms();
    state.test_renew_lease_from_send_ts(lease_duration_ms, applied);

    assert!(
        state.shared_state.read_lease.is_valid(now_ms()),
        "lease must become valid on the first quorum ACK after the noop is committed"
    );
}

/// A new leader must not publish a lease until its state machine has applied up to the
/// noop (Raft §8).
///
/// Scenario:
/// - Noop is at index 101 and already committed (a quorum ACK arrived).
/// - The state machine has only applied up to 100, so an entry the previous leader committed
///   (say X=1 at index 100) may or may not be applied yet; the entries before the noop are
///   not guaranteed to be visible to a local read.
///
/// Expected:
/// - Quorum ACK with `last_applied = 100`: lease stays invalid for readers (they fall back
///   to the Raft loop, which waits for `last_applied >= read_index`).
/// - Quorum ACK with `last_applied = 101`: lease becomes valid.
#[tokio::test]
async fn test_lease_not_published_to_readers_before_noop_applied() {
    let lease_duration_ms = 1000u64;
    let (mut state, _ctx) = setup_leader(lease_duration_ms).await;
    let noop_index = 101u64;
    state.noop_log_id = Some(noop_index);

    state.last_heartbeat_send_ts = now_ms();
    state.test_renew_lease_from_send_ts(lease_duration_ms, noop_index - 1);

    assert!(
        !state.shared_state.read_lease.is_valid(now_ms()),
        "lease must not be visible to readers while last_applied < noop index"
    );

    state.last_heartbeat_send_ts = now_ms();
    state.test_renew_lease_from_send_ts(lease_duration_ms, noop_index);

    assert!(
        state.shared_state.read_lease.is_valid(now_ms()),
        "lease must become valid once last_applied reaches the noop index"
    );
}

// ============================================================================
// Write admission: a leader no quorum has answered for a while stops accepting NEW writes
//
// Not required for safety (reads are guarded by the lease and by quorum confirmation), but an
// isolated leader would otherwise keep queueing writes that can never commit and keep telling
// clients "I am the leader". openraft's approach: while the leader has no recent quorum
// acknowledgement it rejects new writes (clients look for another leader); there is no role
// change, no term change and no election, so brief network jitter costs a few rejected writes
// instead of a leadership change.
//
// The decision uses `last_quorum_contact_ms` (set at election and on every quorum ACK): writes
// are admitted while it is no older than `election_timeout_min`, evaluated once per write batch
// when the batch is flushed. A new leader therefore has one window to collect its first ACK
// (the noop's). The longer `write_admission_election_timeout_multiple` x `election_timeout_max`
// is the CheckQuorum step-down limit, not the write rejection.
// ============================================================================

const WRITE_ADMISSION_TEST_LEASE_MS: u64 = 10;
const WRITE_ADMISSION_TEST_ELECTION_MAX_MS: u64 = 40;
/// Silence long enough to be well past both the lease and the step-down limit (multiple 2).
const WRITE_ADMISSION_TEST_WINDOW_MS: u64 = WRITE_ADMISSION_TEST_ELECTION_MAX_MS * 2;

async fn write_admission_leader() -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
) {
    // No heartbeat during the test: only write admission is under test.
    write_admission_leader_with_heartbeat(60_000).await
}

async fn write_admission_leader_with_heartbeat(
    heartbeat_ms: u64
) -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let mut node_config = RaftNodeConfig::default();
    node_config.raft.read_consistency.lease_duration_ms = WRITE_ADMISSION_TEST_LEASE_MS;
    node_config.raft.election.election_timeout_min = WRITE_ADMISSION_TEST_ELECTION_MAX_MS / 2;
    node_config.raft.election.election_timeout_max = WRITE_ADMISSION_TEST_ELECTION_MAX_MS;
    node_config.raft.replication.rpc_append_entries_clock_in_ms = heartbeat_ms;
    let mut ctx = MockBuilder::new(shutdown_rx).with_node_config(node_config).build_context();

    // Enough mocks for an ACCEPTED write batch to be processed (nothing is sent to peers).
    let mut replication_handler = crate::MockReplicationCore::new();
    replication_handler
        .expect_prepare_batch_requests()
        .returning(|_, _, _, _, _, _| Ok(crate::PrepareResult::default()));
    let mut raft_log = crate::MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 4);
    ctx.handlers.replication_handler = replication_handler;
    ctx.storage.raft_log = std::sync::Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.update_commit_index(4).expect("commit index");
    state.cluster_metadata.single_voter = false;
    state.cluster_metadata.total_voters = 3;
    (state, ctx)
}

/// A quorum ACK for a heartbeat sent just now (the noop is already applied).
fn quorum_acks_now(state: &mut LeaderState<MockTypeConfig>) {
    state.noop_log_id = Some(1);
    state.last_heartbeat_send_ts = now_ms();
    state.test_renew_lease_from_send_ts(WRITE_ADMISSION_TEST_LEASE_MS, 1);
}

fn sleep_ms(ms: u64) {
    std::thread::sleep(Duration::from_millis(ms));
}

/// Offers one client write and flushes the batch, as the Raft loop does. Returns `Some(error
/// code)` when the write was answered at once (rejected), `None` when it was accepted (the answer
/// comes later, after commit).
async fn offer_write(
    state: &mut LeaderState<MockTypeConfig>,
    ctx: &crate::RaftContext<MockTypeConfig>,
) -> Option<crate::client::ErrorCode> {
    use crate::client::ClientWriteRequest;
    use crate::maybe_clone_oneshot::{MaybeCloneOneshot, RaftOneshot};

    let (resp_tx, mut resp_rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    state.push_client_cmd(
        crate::ClientCmd::Propose(
            ClientWriteRequest {
                client_id: 1,
                command: Some(bytes::Bytes::from_static(b"write")),
            },
            resp_tx,
        ),
        ctx,
    );
    let (internal_event_tx, _internal_event_rx) = tokio::sync::mpsc::unbounded_channel();
    state.flush_cmd_buffers(ctx, &internal_event_tx).await.unwrap();

    match tokio::time::timeout(Duration::from_millis(1), resp_rx.recv()).await {
        Err(_) => None,
        Ok(answer) => Some(
            answer
                .expect("channel open")
                .expect("rejection is a client response, not a transport error")
                .error,
        ),
    }
}

/// Test: a leader whose quorum acknowledgements stopped rejects NEW writes with `NotLeader`.
///
/// Scenario:
/// - 3-node leader got a quorum ACK, then the network cut it off for longer than the window.
///
/// Expected:
/// - The write is answered `NotLeader` at once; it is not queued (it could never commit).
/// - The leader does not step down: same term, still a leader.
#[tokio::test]
async fn test_leader_rejects_new_writes_when_quorum_acks_stop() {
    let (mut state, ctx) = write_admission_leader().await;
    quorum_acks_now(&mut state);
    let term_before = state.current_term();
    sleep_ms(WRITE_ADMISSION_TEST_WINDOW_MS * 2);

    let rejection = offer_write(&mut state, &ctx).await;

    assert_eq!(rejection, Some(crate::client::ErrorCode::NotLeader));
    assert_eq!(state.current_term(), term_before);
}

/// Test: a leader that keeps getting quorum ACKs accepts writes.
#[tokio::test]
async fn test_leader_accepts_writes_while_quorum_acks_continue() {
    let (mut state, ctx) = write_admission_leader().await;
    for _ in 0..4 {
        quorum_acks_now(&mut state);
        sleep_ms(WRITE_ADMISSION_TEST_WINDOW_MS / 4);
    }
    quorum_acks_now(&mut state);

    assert_eq!(offer_write(&mut state, &ctx).await, None);
}

/// Test: admission reopens as soon as a quorum ACK arrives.
#[tokio::test]
async fn test_leader_accepts_writes_again_as_soon_as_a_quorum_acks() {
    let (mut state, ctx) = write_admission_leader().await;
    quorum_acks_now(&mut state);
    sleep_ms(WRITE_ADMISSION_TEST_WINDOW_MS * 2);
    assert_eq!(
        offer_write(&mut state, &ctx).await,
        Some(crate::client::ErrorCode::NotLeader),
        "precondition: writes are rejected"
    );

    quorum_acks_now(&mut state);

    assert_eq!(
        offer_write(&mut state, &ctx).await,
        None,
        "the partition healed"
    );
}

/// Test: a single-voter leader is its own quorum and always accepts writes, with or without ACKs.
#[tokio::test]
async fn test_single_voter_leader_always_accepts_writes() {
    let (mut state, ctx) = write_admission_leader().await;
    state.cluster_metadata.single_voter = true;
    state.cluster_metadata.total_voters = 1;
    sleep_ms(WRITE_ADMISSION_TEST_WINDOW_MS * 2);

    assert_eq!(offer_write(&mut state, &ctx).await, None);
}

/// Test: a new leader counts its election as the last quorum contact, so it accepts client
/// writes while its noop is still replicating; without any ACK it rejects them once
/// `election_timeout_min` has passed, and the first ACK reopens admission.
///
/// Expected:
/// - Accepted right after the election, rejected after the window with no ACK, accepted again
///   after the first quorum ACK. The leader's own noop is an internal write and never goes
///   through admission.
#[tokio::test]
async fn test_new_leader_accepts_writes_for_one_window_then_needs_a_quorum_ack() {
    let (mut state, ctx) = write_admission_leader().await;

    assert_eq!(
        offer_write(&mut state, &ctx).await,
        None,
        "a new leader must take writes while its noop is replicating"
    );

    sleep_ms(WRITE_ADMISSION_TEST_WINDOW_MS);

    assert_eq!(
        offer_write(&mut state, &ctx).await,
        Some(crate::client::ErrorCode::NotLeader),
        "no quorum ever answered this leader and the window is over"
    );

    quorum_acks_now(&mut state);

    assert_eq!(offer_write(&mut state, &ctx).await, None);
}

// ============================================================================
// CheckQuorum step-down: a leader no quorum has answered for
// `write_admission_election_timeout_multiple` x `election_timeout_max` steps down (same term).
// Writes are rejected much earlier (see above); this is the backstop that hands leadership over.
// ============================================================================

/// Runs one leader tick and returns the internal events it produced.
async fn tick_once(
    state: &mut LeaderState<MockTypeConfig>,
    ctx: &crate::RaftContext<MockTypeConfig>,
) -> Vec<crate::InternalEvent> {
    let (internal_event_tx, mut internal_event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (raft_tx, _raft_rx) = tokio::sync::mpsc::channel(1);
    state.tick(&internal_event_tx, &raft_tx, ctx).await.expect("tick");
    let mut events = vec![];
    while let Ok(event) = internal_event_rx.try_recv() {
        events.push(event);
    }
    events
}

fn became_follower(events: &[crate::InternalEvent]) -> bool {
    events.iter().any(|e| matches!(e, crate::InternalEvent::BecomeFollower(_)))
}

/// Test: no quorum ACK for longer than the limit makes the leader step down.
///
/// Expected:
/// - The tick emits `BecomeFollower`.
#[tokio::test]
async fn test_leader_steps_down_when_no_quorum_ack_beyond_the_limit() {
    let (mut state, ctx) = write_admission_leader().await;
    quorum_acks_now(&mut state);
    sleep_ms(WRITE_ADMISSION_TEST_WINDOW_MS * 2);

    let events = tick_once(&mut state, &ctx).await;

    assert!(
        became_follower(&events),
        "expected BecomeFollower, got: {events:?}"
    );
}

/// Test: silence past the read lease but inside the limit does not step the leader down.
///
/// Expected:
/// - The limit is `multiple x election_timeout_max`, not the (much shorter) lease: jitter must
///   not cost a leadership change.
#[tokio::test]
async fn test_leader_stays_when_silence_is_inside_the_limit() {
    let (mut state, ctx) = write_admission_leader().await;
    quorum_acks_now(&mut state);
    sleep_ms(WRITE_ADMISSION_TEST_LEASE_MS * 3);

    let events = tick_once(&mut state, &ctx).await;

    assert!(
        !became_follower(&events),
        "must not step down on short silence, got: {events:?}"
    );
}

/// Test: the limit follows `write_admission_election_timeout_multiple`.
///
/// Expected:
/// - Silence of 1.5 x election_timeout_max: tolerated with multiple 2, steps down with multiple 1.
#[tokio::test]
async fn test_step_down_limit_follows_the_configured_multiple() {
    let silence_ms = WRITE_ADMISSION_TEST_ELECTION_MAX_MS * 3 / 2;
    let mut stepped_down = vec![];
    for multiple in [2, 1] {
        let (mut state, mut ctx) = write_admission_leader().await;
        let mut node_config = (*ctx.node_config).clone();
        node_config.raft.backpressure.write_admission_election_timeout_multiple = multiple;
        ctx.node_config = std::sync::Arc::new(node_config);
        state.node_config = ctx.node_config.clone();
        quorum_acks_now(&mut state);
        sleep_ms(silence_ms);
        stepped_down.push(became_follower(&tick_once(&mut state, &ctx).await));
    }

    assert_eq!(
        stepped_down,
        vec![false, true],
        "multiple 2 tolerates, multiple 1 steps down"
    );
}

/// Test: a single-voter leader is its own quorum and never steps down for lack of ACKs.
#[tokio::test]
async fn test_single_voter_leader_never_steps_down_for_lack_of_acks() {
    let (mut state, ctx) = write_admission_leader().await;
    state.cluster_metadata.single_voter = true;
    state.cluster_metadata.total_voters = 1;
    sleep_ms(WRITE_ADMISSION_TEST_WINDOW_MS * 2);

    let events = tick_once(&mut state, &ctx).await;

    assert!(!became_follower(&events), "got: {events:?}");
}

/// Test: after a step-down tick the leader's next tick deadline is in the future.
///
/// The Raft loop is a `biased` select with the tick branch first. The step-down tick returns
/// before the heartbeat code re-arms the replication timer, so if the deadline stays in the
/// past the tick fires again at once, forever, and the branch that processes the
/// `BecomeFollower` event (and every client request) is never polled: the leader never leaves.
///
/// Expected:
/// - `BecomeFollower` is emitted AND `next_deadline()` is later than now.
#[tokio::test]
async fn test_step_down_tick_does_not_leave_the_tick_branch_ready() {
    let (mut state, ctx) = write_admission_leader_with_heartbeat(5).await;
    quorum_acks_now(&mut state);
    // Silent past the step-down limit, and past the replication deadline that fires the tick.
    sleep_ms(WRITE_ADMISSION_TEST_WINDOW_MS * 2);

    let events = tick_once(&mut state, &ctx).await;

    assert!(
        became_follower(&events),
        "precondition: the leader must step down, got: {events:?}"
    );
    assert!(
        state.next_deadline() > tokio::time::Instant::now(),
        "the tick branch must not stay ready after a step-down, or the biased Raft loop never \
         processes BecomeFollower and never answers a client"
    );
}
