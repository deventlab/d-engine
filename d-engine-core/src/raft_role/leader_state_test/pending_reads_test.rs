//! Tests for `pending_reads` (LinearizableRead) queue — Bug #381 fix validation.
//!
//! ## Background
//!
//! Before the fix, a multi-voter leader served LinearizableRead immediately in
//! Phase 3 when `last_applied >= read_index`, with zero quorum confirmation.
//! A partitioned leader could serve stale data (Jepsen linearizability violation).
//!
//! After the fix, multi-voter reads are *always* queued in `pending_reads` and
//! drained only via two safe paths:
//!
//! - **Path A** (`handle_append_result`): explicit quorum ACK confirms current leadership.
//! - **Path B** (`handle_apply_completed`): SM apply implies commit, which implies quorum.
//!
//! ## Test matrix
//!
//! | # | Test | What it guards |
//! |---|------|---------------|
//! | T1 | `test_multi_voter_fast_path_linear_read_is_queued` | Phase 3 no longer fast-paths multi-voter |
//! | T2 | `test_pending_reads_drained_by_quorum_ack` | Path A — quorum ACK drains the queue |
//! | T3 | `test_pending_reads_slow_path_drained_by_apply_completed` | Path B — SM apply drains the queue |
//! | T4 | `test_pending_reads_expired_by_tick` | `tick()` sends DeadlineExceeded on timeout |
//! | T5 | `test_pending_reads_cleared_on_stepdown` | `drain_read_buffer()` clears queue on step-down |

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use crate::client::ClientReadRequest;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::SuccessResult;
use d_engine_proto::server::replication::append_entries_response;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tonic::Code;
use tracing_test::traced_test;

use crate::ClientCmd;
use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::MockStateMachine;
use crate::PeerUpdate;
use crate::RaftNodeConfig;
use crate::ReadConsistencyPolicy;
use crate::convert::safe_kv_bytes;
use crate::event::RaftEvent;
use crate::maybe_clone_oneshot::{MaybeCloneOneshot, RaftOneshot};
use crate::raft_role::leader_state::{LeaderState, PendingReadBatch};
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_builder::mock_state_machine;

// ============================================================================
// Shared helpers
// ============================================================================

/// Build a LinearizableRead client command wired to the given sender.
fn linear_read_cmd(
    sender: crate::MaybeCloneOneshotSender<
        std::result::Result<crate::client::ClientResponse, tonic::Status>,
    >
) -> ClientCmd {
    ClientCmd::Read(
        ClientReadRequest {
            client_id: 1,
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead),
            keys: vec![safe_kv_bytes(1)],
        },
        sender,
    )
}

/// Build a `SuccessResult` AppendEntriesResponse for peer 2 at `(term, match_index)`.
fn quorum_ack(
    term: u64,
    match_index: u64,
) -> AppendEntriesResponse {
    AppendEntriesResponse {
        node_id: 2,
        term,
        result: Some(append_entries_response::Result::Success(SuccessResult {
            last_match: Some(LogId {
                term,
                index: match_index,
            }),
        })),
    }
}

/// Build a `MockStateMachine` that reports `last_applied = index`.
/// Inherits all other expectations from the default mock.
fn sm_with_last_applied(index: u64) -> MockStateMachine {
    let mut sm = mock_state_machine();
    // Override the default `return_const(LogId::default())` set by mock_state_machine().
    // mockall resolves expectations in reverse registration order, so the new
    // expectation (with no .times() bound) shadows the earlier one.
    sm.expect_last_applied().return_const(LogId { index, term: 1 });
    sm
}

type MultiVoterFixture = (
    LeaderState<MockTypeConfig>,
    crate::raft_context::RaftContext<MockTypeConfig>,
    mpsc::UnboundedSender<crate::event::RoleEvent>,
    mpsc::UnboundedReceiver<crate::event::RoleEvent>,
);

/// Set up a 3-voter cluster (leader=1, peers=2,3) ready for LinearizableRead tests.
///
/// Caller supplies `replication` and `raft_log` because each test needs different
/// expectations on them.  The state machine's `last_applied` is set to `sm_last_applied`.
async fn setup_multi_voter(
    path: &str,
    replication: MockReplicationCore<MockTypeConfig>,
    raft_log: MockRaftLog,
    sm_last_applied: u64,
) -> MultiVoterFixture {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut node_config = RaftNodeConfig::default();
    node_config.raft.batching.max_batch_size = 1;
    node_config.raft.general_raft_timeout_duration_in_ms = 2_000;

    let context = MockBuilder::new(graceful_rx)
        .with_db_path(path)
        .with_replication_handler(replication)
        .with_raft_log(raft_log)
        .with_state_machine(sm_with_last_applied(sm_last_applied))
        .with_node_config(node_config)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    let peers: Vec<NodeMeta> = (2u32..=3)
        .map(|id| NodeMeta {
            id,
            address: String::new(),
            status: NodeStatus::Active as i32,
            role: Follower.into(),
        })
        .collect();

    let mut membership = MockMembership::new();
    let peers_voters = peers.clone();
    membership.expect_voters().returning(move || peers_voters.clone());
    membership.expect_replication_peers().returning(move || peers.clone());
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    let (role_tx, role_rx) = mpsc::unbounded_channel();
    (state, context, role_tx, role_rx)
}

/// Build a `MockRaftLog` that returns `Some(match_index)` for every
/// `calculate_majority_matched_index` call — satisfies quorum.
fn raft_log_with_quorum(match_index: u64) -> MockRaftLog {
    let mut log = MockRaftLog::new();
    log.expect_last_entry_id().returning(|| 0);
    log.expect_durable_index().returning(|| 0);
    log.expect_last_log_id().returning(|| None);
    log.expect_flush().returning(|| Ok(()));
    log.expect_load_hard_state().returning(|| Ok(None));
    log.expect_save_hard_state().returning(|_| Ok(()));
    log.expect_close().returning(|| ());
    log.expect_calculate_majority_matched_index()
        .returning(move |_, _, _| Some(match_index));
    log
}

/// Build a minimal `MockRaftLog` with no quorum (default: returns None for majority).
fn raft_log_no_quorum() -> MockRaftLog {
    // Reuse the default mock which has calculate_majority_matched_index → None.
    crate::test_utils::mock::mock_raft_builder::mock_raft_log()
}

// ============================================================================
// T1 — Phase 3 must NOT serve multi-voter reads immediately (Bug #381 core)
// ============================================================================

/// **Bug #381 regression guard**: a multi-voter leader must NOT serve a
/// LinearizableRead in Phase 3 even when `last_applied >= read_index`.
///
/// ## Setup
/// - 3-voter cluster (leader=1, peers=2,3)
/// - `commit_index = 0`, `noop_log_id = Some(0)` → `read_index = 0`
/// - `ctx.state_machine().last_applied().index = 0` (default mock)
/// - Precondition for fast-path: `last_applied(0) >= read_index(0)` is true
///
/// ## Expected behavior after fix
/// The read is queued in `pending_reads[0]` and the client channel has no
/// response yet.  The read will only be served after quorum confirmation
/// arrives via `handle_append_result` (Path A).
///
/// ## Failure before fix
/// Phase 3 calls `execute_pending_reads` immediately, sending a response
/// without contacting any peer — linearizability is violated under partition.
#[tokio::test]
#[traced_test]
async fn test_multi_voter_fast_path_linear_read_is_queued() {
    let mut replication = MockReplicationCore::new();
    replication
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let (mut state, context, role_tx, _role_rx) = setup_multi_voter(
        "/tmp/test_multi_voter_fast_path_linear_read_is_queued",
        replication,
        raft_log_no_quorum(),
        0, // last_applied = 0, matches read_index = 0 → fast-path condition
    )
    .await;

    // commit_index = 0, noop_log_id = Some(0) → read_index = max(0,0) = 0
    // last_applied(0) >= read_index(0) → this is the fast-path that used to be a bug
    state.noop_log_id = Some(0);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    state.push_client_cmd(linear_read_cmd(resp_tx), &context);
    state.flush_cmd_buffers(&context, &role_tx).await.expect("flush must succeed");

    // After the fix: read is queued, not served.
    assert_eq!(
        state.pending_reads.len(),
        1,
        "multi-voter fast-path read must be queued in pending_reads, not served immediately"
    );

    // Client must not have received a response yet.
    assert!(
        resp_rx.try_recv().is_err(),
        "client must not receive a response before quorum confirmation"
    );
}

// ============================================================================
// T2 — Path A: quorum ACK drains pending_reads (pure-read scenario)
// ============================================================================

/// When a quorum ACK arrives via `handle_append_result`, pending linearizable
/// reads whose `read_index <= last_applied` must be drained and the client
/// sent a success response.
///
/// ## Why Path A is required
/// For a pure LinearizableRead with no concurrent write, `commit_index` does
/// not advance and `handle_apply_completed` never fires.  Without Path A, a
/// read correctly queued by the fix would hang until timeout.
///
/// ## Setup
/// - `pending_reads[0]` injected directly (simulates a queued fast-path read)
/// - `ctx.state_machine().last_applied().index = 0` → drain condition satisfied
/// - `raft_log.calculate_majority_matched_index` → `Some(0)` → `quorum_confirmed`
///
/// ## What changes after the fix
/// `handle_append_result` adds a drain loop after `drain_pending_lease_reads`.
/// This test will FAIL before that code is added (pending_reads never emptied).
#[tokio::test]
#[traced_test]
async fn test_pending_reads_drained_by_quorum_ack() {
    let mut replication = MockReplicationCore::new();
    replication.expect_handle_success_response().returning(|_, _, _, _| {
        Ok(PeerUpdate {
            match_index: Some(0),
            next_index: 1,
            success: true,
        })
    });

    // quorum_confirmed = calculate_majority_matched_index(...).is_some()
    let (mut state, context, role_tx, _role_rx) = setup_multi_voter(
        "/tmp/test_pending_reads_drained_by_quorum_ack",
        replication,
        raft_log_with_quorum(0),
        0, // last_applied = 0
    )
    .await;

    // Inject a queued read directly — avoids coupling to T1's flush path.
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let mut requests = VecDeque::new();
    requests.push_back((
        ClientReadRequest {
            client_id: 1,
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead),
            keys: vec![safe_kv_bytes(1)],
        },
        resp_tx,
    ));
    state.pending_reads.insert(
        0, // read_index = 0, already <= last_applied(0)
        PendingReadBatch {
            deadline: Instant::now() + Duration::from_secs(5),
            requests,
        },
    );
    assert_eq!(state.pending_reads.len(), 1, "precondition: read is queued");

    // Simulate quorum ACK from peer 2.
    // Majority = 2/3: self + peer 2 is sufficient.
    state
        .handle_append_result(2, Ok(quorum_ack(1, 0)), &context, &role_tx)
        .await
        .expect("handle_append_result must succeed");

    // Path A drain: pending_reads must be empty after quorum confirmation.
    assert_eq!(
        state.pending_reads.len(),
        0,
        "pending_reads must be drained after quorum ACK"
    );

    // Client must receive a successful response.
    let result = resp_rx.recv().await.unwrap();
    assert!(
        result.is_ok(),
        "client must receive Ok after quorum ACK drains pending_reads"
    );
}

// ============================================================================
// T3 — Path B: SM apply drains pending_reads (slow path, read+write concurrent)
// ============================================================================

/// When the state machine applies entries up to `last_index`, all pending reads
/// with `read_index <= last_index` must be served via `handle_apply_completed`.
///
/// ## Scenario
/// - `read_index = 5`, `last_applied = 0 < 5` at enqueue time (slow path).
/// - A concurrent write commits and the SM applies up to index 5.
/// - `handle_apply_completed(5)` fires and drains `pending_reads[5]`.
///
/// ## Why Path B is safe
/// SM apply requires commit to have advanced, which requires quorum.
/// The quorum guarantee is implicit in the commit→apply chain.
///
/// ## Regression coverage
/// This test ensures the slow-path drain is not accidentally broken while
/// fixing the fast-path (Bug #381 change 1).
#[tokio::test]
#[traced_test]
async fn test_pending_reads_slow_path_drained_by_apply_completed() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_pending_reads_slow_path_drained_by_apply_completed")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Inject a read queued at read_index=5 (SM not yet caught up at enqueue time).
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let mut requests = VecDeque::new();
    requests.push_back((
        ClientReadRequest {
            client_id: 1,
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead),
            keys: vec![safe_kv_bytes(1)],
        },
        resp_tx,
    ));
    state.pending_reads.insert(
        5,
        PendingReadBatch {
            deadline: Instant::now() + Duration::from_secs(5),
            requests,
        },
    );
    assert_eq!(state.pending_reads.len(), 1, "precondition: read is queued");

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // SM applies entries up to index 5 — this is the commit chain completing.
    state
        .handle_apply_completed(5, vec![], &context, &role_tx)
        .await
        .expect("handle_apply_completed must succeed");

    // Path B drain: read_index(5) <= last_index(5) → must be served.
    assert_eq!(
        state.pending_reads.len(),
        0,
        "pending_reads must be drained by handle_apply_completed"
    );

    let result = resp_rx.recv().await.unwrap();
    assert!(
        result.is_ok(),
        "client must receive Ok after SM applies up to read_index"
    );
}

// ============================================================================
// T4 — Partition scenario: no quorum ACK, no apply → read times out
// ============================================================================

/// A partitioned leader must NEVER serve a queued linearizable read.
/// Reads queued in `pending_reads` without a quorum ACK or SM advance must
/// be cleaned up by `tick()` with `DeadlineExceeded` once their deadline passes.
///
/// ## Simulated scenario (Bug #381)
/// - Leader is isolated in a minority partition.
/// - `commit_index` and `last_applied` are frozen (no new ACKs, no new commits).
/// - A LinearizableRead arrives and is queued with `read_index = frozen_index`.
/// - Neither Path A nor Path B fires.
/// - `tick()` scans for expired entries and delivers `DeadlineExceeded`.
///
/// ## Why this matters
/// Without this drain, clients would block until their own RPC timeout, which
/// is much longer and harder to diagnose.  The bounded-latency guarantee also
/// allows clients to retry against the majority partition quickly.
#[tokio::test]
#[traced_test]
async fn test_pending_reads_partition_scenario_times_out() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_pending_reads_partition_scenario_times_out")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Inject a read with an already-expired deadline to simulate the partition
    // window having elapsed.
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let mut requests = VecDeque::new();
    requests.push_back((
        ClientReadRequest {
            client_id: 1,
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead),
            keys: vec![safe_kv_bytes(1)],
        },
        resp_tx,
    ));
    state.pending_reads.insert(
        10, // frozen read_index — no quorum will ever confirm this
        PendingReadBatch {
            deadline: Instant::now() - Duration::from_secs(1), // already past
            requests,
        },
    );

    let (role_tx, _role_rx) = mpsc::unbounded_channel();
    let (raft_tx, _raft_rx) = mpsc::channel::<RaftEvent>(1);

    state.tick(&role_tx, &raft_tx, &context).await.expect("tick must succeed");

    // tick() must remove the expired entry.
    assert_eq!(
        state.pending_reads.len(),
        0,
        "tick() must evict expired pending_reads entries"
    );

    // Client must receive DeadlineExceeded — not hang forever.
    let result = resp_rx.recv().await.unwrap();
    assert!(result.is_err(), "expired read must produce an error");
    assert_eq!(
        result.unwrap_err().code(),
        Code::DeadlineExceeded,
        "expired pending read must receive DeadlineExceeded"
    );
}

// ============================================================================
// T5 — Step-down drains pending_reads with Unavailable
// ============================================================================

/// When the leader steps down (role change or higher-term discovery),
/// `drain_read_buffer()` must drain all entries in `pending_reads` with
/// `Unavailable` so clients are notified immediately rather than hanging.
///
/// ## Why this matters
/// Without an eager step-down drain, clients would wait until the per-request
/// deadline fires in `tick()`, which could be seconds later.  A fast
/// `Unavailable` lets clients retry against the new leader immediately.
#[tokio::test]
#[traced_test]
async fn test_pending_reads_cleared_on_stepdown() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = MockBuilder::new(graceful_rx)
        .with_db_path("/tmp/test_pending_reads_cleared_on_stepdown")
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Queue two reads at different read_indexes with valid (unexpired) deadlines.
    // Using distinct keys because BTreeMap overwrites on duplicate key.
    let mut senders = vec![];
    for read_index in [5u64, 10u64] {
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        let mut requests = VecDeque::new();
        requests.push_back((
            ClientReadRequest {
                client_id: 1,
                consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead),
                keys: vec![safe_kv_bytes(1)],
            },
            resp_tx,
        ));
        state.pending_reads.insert(
            read_index,
            PendingReadBatch {
                deadline: Instant::now() + Duration::from_secs(30),
                requests,
            },
        );
        senders.push(resp_rx);
    }
    assert_eq!(state.pending_reads.len(), 2, "precondition: 2 reads queued");

    // Simulate role transition — drain_read_buffer is called on step-down.
    state.drain_read_buffer().expect("drain_read_buffer must succeed");

    assert_eq!(
        state.pending_reads.len(),
        0,
        "all pending_reads must be cleared on step-down"
    );

    // Each client must receive Unavailable — not hang.
    for mut rx in senders {
        let result = rx.recv().await.unwrap();
        assert!(
            result.is_err(),
            "step-down must send an error to pending reads"
        );
        assert_eq!(
            result.unwrap_err().code(),
            Code::Unavailable,
            "step-down must deliver Unavailable to pending reads"
        );
    }
}

// ============================================================================
// N1 — Lease fast path: valid lease + SM current → serve immediately (#390)
// ============================================================================

/// A multi-voter leader with a valid lease and an up-to-date state machine must
/// serve a LinearizableRead directly in Phase 3 without queuing or AppendEntries RTT.
///
/// ## Background
/// #390 adds a lease fast path: when `is_lease_valid()` is true and
/// `last_applied >= read_index`, the leader can safely serve the read inline.
/// `is_lease_valid()` proves no higher-term leader exists (Raft §6.4);
/// `last_applied >= read_index` proves the state machine is current.
/// Config validation enforces `lease_duration < election_timeout`, making
/// "lease valid" and "new leader elected" mutually exclusive on the timeline.
///
/// ## Setup
/// - 3-voter cluster (leader=1, peers=2,3)
/// - Lease is valid: `test_update_lease_timestamp()` called immediately before flush
/// - `commit_index=0`, `noop_log_id=Some(0)` → `read_index=0`
/// - `last_applied=0` → `last_applied(0) >= read_index(0)` satisfied
///
/// ## Expected behavior
/// Phase 3 takes the lease fast path: `execute_pending_reads` is called inline.
/// `pending_reads` is empty after flush; client receives a successful response.
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_served_immediately_with_valid_lease_in_multi_voter() {
    let mut replication = MockReplicationCore::new();
    replication
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let (mut state, context, role_tx, _role_rx) = setup_multi_voter(
        "/tmp/test_linearizable_read_served_immediately_with_valid_lease_in_multi_voter",
        replication,
        raft_log_no_quorum(),
        0, // last_applied = 0
    )
    .await;

    state.noop_log_id = Some(0); // read_index = max(commit_index=0, noop=0) = 0

    // Refresh the lease immediately before the flush — simulates steady-state
    // operation where the leader received a quorum ACK within lease_duration_ms.
    state.test_update_lease_timestamp();

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    state.push_client_cmd(linear_read_cmd(resp_tx), &context);
    state.flush_cmd_buffers(&context, &role_tx).await.expect("flush must succeed");

    // Fast path taken: read must NOT be queued.
    assert_eq!(
        state.pending_reads.len(),
        0,
        "valid lease + SM current: read must be served inline, not queued"
    );

    // Client must receive a response immediately (no quorum round-trip needed).
    let result = resp_rx.try_recv();
    assert!(
        result.is_ok(),
        "client must receive a response immediately via lease fast path"
    );
    assert!(
        result.unwrap().is_ok(),
        "lease fast path response must be Ok"
    );
}

// ============================================================================
// N2 — Lease fast path: SM behind → queue despite valid lease (#390)
// ============================================================================

/// A multi-voter leader with a valid lease must still queue a LinearizableRead
/// when `last_applied < read_index` — the state machine has not yet applied all
/// entries up to the read's consistency point.
///
/// ## Why this matters
/// `is_lease_valid()` alone is not sufficient to serve the read safely.
/// The additional `last_applied >= read_index` guard ensures the state machine
/// reflects all committed entries at the time of the read.  Skipping this check
/// would return stale data even with a valid lease.
///
/// ## Setup
/// - 3-voter cluster, valid lease
/// - `noop_log_id = Some(5)` → `read_index = 5`
/// - `last_applied = 0` → `last_applied(0) < read_index(5)` → SM is behind
///
/// ## Expected behavior
/// The lease fast path condition is not fully satisfied.  The read is queued in
/// `pending_reads[5]` and will be drained once the SM applies up to index 5
/// (Path B) or a quorum ACK arrives (Path A).
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_queued_when_sm_behind_despite_valid_lease() {
    let mut replication = MockReplicationCore::new();
    replication
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let (mut state, context, role_tx, _role_rx) = setup_multi_voter(
        "/tmp/test_linearizable_read_queued_when_sm_behind_despite_valid_lease",
        replication,
        raft_log_no_quorum(),
        0, // last_applied = 0 — SM is behind
    )
    .await;

    // read_index = max(commit_index=0, noop=5) = 5; last_applied = 0 < 5
    state.noop_log_id = Some(5);

    // Lease is valid — but SM is behind, so the fast path must not trigger.
    state.test_update_lease_timestamp();

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    state.push_client_cmd(linear_read_cmd(resp_tx), &context);
    state.flush_cmd_buffers(&context, &role_tx).await.expect("flush must succeed");

    // SM not caught up: read must be queued regardless of lease validity.
    assert_eq!(
        state.pending_reads.len(),
        1,
        "read must be queued when last_applied < read_index even with a valid lease"
    );
    assert!(
        resp_rx.try_recv().is_err(),
        "client must not receive a response before SM catches up"
    );
}

// ============================================================================
// N3 — Protocol safety: expired lease → slow path, no stale read (#390)
// ============================================================================

/// A multi-voter leader with an expired (or never-set) lease must NOT serve a
/// LinearizableRead via the lease fast path, even when `last_applied >= read_index`.
///
/// ## Protocol safety argument
/// The lease fast path requires `is_lease_valid()` as leadership proof (Raft §6.4).
/// Without a valid lease, the leader may be in a minority partition: it cannot
/// renew its lease because majority ACKs stopped arriving. Serving the read inline
/// here would violate linearizability (the same bug that #381 fixed for the
/// original code path).
///
/// The safety invariant is enforced by the timeline constraint:
///   `lease_duration < election_timeout - max_clock_drift`
/// When the lease has expired, a new leader *may* already exist and have committed
/// new entries unknown to this node.  The read must wait for explicit quorum
/// confirmation (Path A or B) before being served.
///
/// ## Setup
/// - 3-voter cluster, lease never refreshed (default: expired / None)
/// - `last_applied=0 >= read_index=0` — the fast-path condition for `last_applied`
///   is satisfied, but `is_lease_valid()` is false
///
/// ## Expected behavior
/// The lease fast path is NOT taken.  The read is queued in `pending_reads` and
/// will only be served after quorum confirmation arrives.
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_not_served_when_lease_expired_in_multi_voter() {
    let mut replication = MockReplicationCore::new();
    replication
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let (mut state, context, role_tx, _role_rx) = setup_multi_voter(
        "/tmp/test_linearizable_read_not_served_when_lease_expired_in_multi_voter",
        replication,
        raft_log_no_quorum(),
        0, // last_applied = 0
    )
    .await;

    // read_index = 0, last_applied = 0 → the `last_applied >= read_index` condition
    // is satisfied, but no lease has been set — is_lease_valid() returns false.
    state.noop_log_id = Some(0);
    // Intentionally do NOT call test_update_lease_timestamp() — simulates a
    // partitioned leader that has not received a majority ACK recently.

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    state.push_client_cmd(linear_read_cmd(resp_tx), &context);
    state.flush_cmd_buffers(&context, &role_tx).await.expect("flush must succeed");

    // Expired lease → slow path: read must be queued, not served inline.
    assert_eq!(
        state.pending_reads.len(),
        1,
        "expired lease must not trigger fast path: read must be queued for quorum confirmation"
    );
    assert!(
        resp_rx.try_recv().is_err(),
        "client must not receive a response when lease is expired (partition safety)"
    );
}
