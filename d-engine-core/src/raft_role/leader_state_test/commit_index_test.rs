//! Commit Index Correctness Tests
//!
//! Verifies that commit index decisions always use `durable_index` (crash-safe),
//! never `last_entry_id` (in-memory). Covers both the single-voter fast path and
//! the multi-voter quorum path. Fixed by ticket #329.

use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use crate::event::RoleEvent;
use crate::maybe_clone_oneshot::MaybeCloneOneshot;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_role::leader_state::LeaderState;
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::{MockTypeConfig, mock_raft_context};
use crate::{MockMembership, MockRaftLog, PeerUpdate, RaftRequestWithSignal};
use bytes::Bytes;
use d_engine_proto::common::{EntryPayload, LogId};
use d_engine_proto::server::replication::append_entries_response;
use d_engine_proto::server::replication::{AppendEntriesResponse, SuccessResult};

fn success_response(
    term: u64,
    match_index: u64,
) -> AppendEntriesResponse {
    AppendEntriesResponse {
        node_id: 0,
        term,
        result: Some(append_entries_response::Result::Success(SuccessResult {
            last_match: Some(LogId {
                term,
                index: match_index,
            }),
        })),
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn write_request() -> (
    RaftRequestWithSignal,
    crate::maybe_clone_oneshot::MaybeCloneOneshotReceiver<
        std::result::Result<d_engine_proto::client::ClientResponse, tonic::Status>,
    >,
) {
    use nanoid::nanoid;
    let (tx, rx) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let req = RaftRequestWithSignal {
        id: nanoid!(),
        payloads: vec![EntryPayload::command(Bytes::from_static(b"cmd"))],
        senders: vec![tx],
        wait_for_apply_event: false,
    };
    (req, rx)
}

/// Single-voter cluster setup (no peer nodes → single_voter=true).
async fn setup_single_voter(
    path: &str,
    graceful_rx: watch::Receiver<()>,
    raft_log: MockRaftLog,
) -> (
    LeaderState<MockTypeConfig>,
    crate::raft_context::RaftContext<MockTypeConfig>,
) {
    let mut ctx = mock_raft_context(path, graceful_rx, None);

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new); // no peers → single_voter
    membership.expect_replication_peers().returning(Vec::new);
    ctx.membership = Arc::new(membership);
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    assert!(
        state.cluster_metadata.single_voter,
        "single_voter must be true for this test"
    );

    (state, ctx)
}

// ── single-voter: commit must not advance when entry is not durable ───────────

/// Single-voter leader: commit must not advance when entry is in memory but not yet crash-safe.
///
/// This is the bug introduced before #329: last_entry_id()=1 but durable_index=0.
/// The commit should NOT advance because the entry is not crash-safe yet.
#[tokio::test]
async fn test_single_voter_commit_must_not_advance_before_durable() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1); // entry 1 in memory
    raft_log.expect_durable_index().returning(|| 0); // not yet durable (BUG triggers here)
    raft_log.expect_flush().returning(|| Ok(())); // Phase 4 flushes inline for single_voter

    let (mut state, mut ctx) = setup_single_voter(
        "/tmp/test_single_voter_no_commit_before_durable",
        graceful_rx,
        raft_log,
    )
    .await;

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let (req, _rx) = write_request();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state.process_batch(VecDeque::from(vec![req]), &role_tx, &ctx).await.unwrap();
    // Single-voter: commit driven by handle_log_flushed; durable=0 → no advance yet.

    // CORRECT: commit must NOT advance — entry is in memory but not crash-safe
    assert_eq!(
        state.shared_state().commit_index,
        0,
        "CORRECT: single-voter commit must not advance until durable_index catches up"
    );
    assert!(
        role_rx.try_recv().is_err(),
        "CORRECT: NotifyNewCommitIndex must not fire before durable"
    );
}

// ── single-voter: commit advances once entry is durable ──────────────────────

/// Single-voter leader: commit advances to durable_index after flush completes.
#[tokio::test]
async fn test_single_voter_commit_advances_after_durable() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_durable_index().returning(|| 1); // entry 1 is now crash-safe
    raft_log.expect_flush().returning(|| Ok(())); // Phase 4 flushes inline for single_voter

    let (mut state, mut ctx) = setup_single_voter(
        "/tmp/test_single_voter_commit_after_durable",
        graceful_rx,
        raft_log,
    )
    .await;

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));

    let (req, _rx) = write_request();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state.process_batch(VecDeque::from(vec![req]), &role_tx, &ctx).await.unwrap();
    // Single-voter: commit driven by handle_log_flushed; durable=1 → advances.
    state.handle_log_flushed(1, &ctx, &role_tx).await;

    assert_eq!(
        state.shared_state().commit_index,
        1,
        "single-voter commit must advance to durable_index after flush"
    );
    assert!(
        matches!(role_rx.try_recv(), Ok(RoleEvent::NotifyNewCommitIndex(_))),
        "NotifyNewCommitIndex must fire after commit advances"
    );
}

// ── multi-voter: quorum uses durable_index via calculate_majority_matched_index ─

/// Multi-voter path: quorum calculation (via calculate_majority_matched_index) correctly
/// blocks commit when leader's durable_index=0, even if last_entry_id=1.
///
/// This path was fixed in Step 2 (buffered_raft_log.rs). This test ensures the
/// MockRaftLog-based quorum mock correctly gates the commit index.
#[tokio::test]
async fn test_multi_voter_commit_respects_quorum_result() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut ctx = mock_raft_context("/tmp/test_multi_voter_commit_quorum", graceful_rx, None);

    let mut membership = crate::MockMembership::<MockTypeConfig>::new();
    membership.expect_is_single_node_cluster().returning(|| false);
    membership.expect_voters().returning(|| {
        use d_engine_proto::common::{NodeRole::Follower, NodeStatus};
        use d_engine_proto::server::cluster::NodeMeta;
        vec![
            NodeMeta {
                id: 2,
                address: "".into(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
            NodeMeta {
                id: 3,
                address: "".into(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
        ]
    });
    membership.expect_replication_peers().returning(Vec::new);
    ctx.membership = Arc::new(membership);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_durable_index().returning(|| 0);
    // Quorum blocked: calculate_majority_matched_index returns None (leader durable=0)
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| None);
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    assert!(!state.cluster_metadata.single_voter);

    ctx.handlers
        .replication_handler
        .expect_prepare_batch_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(crate::PrepareResult::default()));
    ctx.handlers
        .replication_handler
        .expect_handle_success_response()
        .returning(|_, _, _, _| {
            Ok(PeerUpdate {
                match_index: Some(1),
                next_index: 2,
                success: true,
            })
        });

    let (req, _rx) = write_request();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state.process_batch(VecDeque::from(vec![req]), &role_tx, &ctx).await.unwrap();
    // Simulate peer responses: both succeed, but calculate_majority_matched_index returns None.
    let resp = success_response(1, 1);
    state.handle_append_result(2, Ok(resp), &ctx, &role_tx).await.unwrap();
    state.handle_append_result(3, Ok(resp), &ctx, &role_tx).await.unwrap();

    assert_eq!(
        state.shared_state().commit_index,
        0,
        "multi-voter commit must not advance when calculate_majority returns None"
    );
    assert!(
        role_rx.try_recv().is_err(),
        "NotifyNewCommitIndex must not fire"
    );
}

// ── handle_log_flushed: Leader commit advances on LogFlushed (#313 P0) ────────

/// Single-voter leader: LogFlushed(1) triggers commit_index to advance to 1.
///
/// P0 scenario: process_batch saw durable=0 → commit didn't advance.
/// When LogFlushed(1) arrives, handle_log_flushed re-evaluates and advances commit.
/// This is the fix for single-node write hangs after #329.
#[tokio::test]
async fn test_leader_single_voter_commit_advances_on_log_flushed() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_durable_index().returning(|| 0); // process_batch sees 0
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| None); // not called for single_voter

    let (mut state, ctx) = setup_single_voter(
        "/tmp/test_leader_log_flushed_single_voter",
        graceful_rx,
        raft_log,
    )
    .await;

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Precondition: commit_index still at 0 (process_batch saw durable=0)
    assert_eq!(state.commit_index(), 0);

    // Action: LogFlushed(1) — entry 1 is now crash-safe
    state.handle_log_flushed(1, &ctx, &role_tx).await;

    // Verify: commit_index advanced to 1
    assert_eq!(
        state.commit_index(),
        1,
        "single-voter commit must advance to durable index after LogFlushed"
    );

    // Verify: NotifyNewCommitIndex event sent to state machine
    assert!(
        matches!(
            role_rx.try_recv().unwrap(),
            RoleEvent::NotifyNewCommitIndex(_)
        ),
        "NotifyNewCommitIndex must fire after commit advances"
    );
}

/// Single-voter leader: LogFlushed with durable <= commit_index is a no-op.
///
/// Guards against spurious re-notification when durable hasn't progressed.
#[tokio::test]
async fn test_leader_single_voter_log_flushed_noop_when_already_committed() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_durable_index().returning(|| 1);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| None);

    let (mut state, ctx) =
        setup_single_voter("/tmp/test_leader_log_flushed_noop", graceful_rx, raft_log).await;

    // Pre-set commit_index to 1 (already committed)
    state.update_commit_index(1).unwrap();

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Action: LogFlushed(1) — durable == commit_index, nothing to do
    state.handle_log_flushed(1, &ctx, &role_tx).await;

    // Verify: commit_index unchanged, no event fired
    assert_eq!(state.commit_index(), 1, "commit_index must not change");
    assert!(
        role_rx.try_recv().is_err(),
        "NotifyNewCommitIndex must NOT fire when durable <= commit_index"
    );
}

// ── pipeline: match_index only advances (max-guard) ──────────────────────────

/// Pipeline replication sends multiple batches without waiting for ACKs.
/// Responses may arrive out of order. match_index must only advance, never retreat.
///
/// T1: response with match=10 → match_index[2] = 10
/// T2: stale response with match=7 → match_index[2] stays 10 (not 7)
/// T3: response with match=12 → match_index[2] = 12
#[tokio::test]
async fn test_update_match_index_only_advances() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut ctx = mock_raft_context(
        "/tmp/test_update_match_index_only_advances",
        graceful_rx,
        None,
    );

    let mut membership = crate::MockMembership::<MockTypeConfig>::new();
    membership.expect_is_single_node_cluster().returning(|| false);
    membership.expect_voters().returning(|| {
        use d_engine_proto::common::{NodeRole::Follower, NodeStatus};
        use d_engine_proto::server::cluster::NodeMeta;
        vec![NodeMeta {
            id: 2,
            address: "".into(),
            status: NodeStatus::Active as i32,
            role: Follower.into(),
        }]
    });
    membership.expect_replication_peers().returning(Vec::new);
    ctx.membership = Arc::new(membership);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 12);
    raft_log.expect_durable_index().returning(|| 12);
    // Quorum: not the focus here — return None so commit doesn't advance.
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| None);
    ctx.storage.raft_log = Arc::new(raft_log);

    // handle_success_response: extract match_index from the success result.
    ctx.handlers.replication_handler.expect_handle_success_response().returning(
        |_, _, success, _| {
            let match_idx = success.last_match.map(|l| l.index).unwrap_or(0);
            Ok(PeerUpdate {
                match_index: Some(match_idx),
                next_index: match_idx + 1,
                success: true,
            })
        },
    );

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    assert!(!state.cluster_metadata.single_voter);

    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // T1: response match=10 → stored
    state
        .handle_append_result(2, Ok(success_response(1, 10)), &ctx, &role_tx)
        .await
        .unwrap();
    assert_eq!(
        state.match_index_for_test(2),
        10,
        "match_index[2] = 10 after first response"
    );

    // T2: out-of-order stale response match=7 → ignored
    state
        .handle_append_result(2, Ok(success_response(1, 7)), &ctx, &role_tx)
        .await
        .unwrap();
    assert_eq!(
        state.match_index_for_test(2),
        10,
        "match_index[2] must not retreat to 7"
    );

    // T3: later response match=12 → advances
    state
        .handle_append_result(2, Ok(success_response(1, 12)), &ctx, &role_tx)
        .await
        .unwrap();
    assert_eq!(
        state.match_index_for_test(2),
        12,
        "match_index[2] = 12 after latest response"
    );
}

// ── learner exclusion from quorum ─────────────────────────────────────────────

/// Mixed cluster setup: 1 voter peer + 1 learner peer (single_voter=false).
///
/// replication_targets = [voter(2), learner(3)]
/// voters()            = [voter(2)]            → total_voters = 2
/// Used to verify learner match_index is excluded from quorum calculation.
async fn setup_voter_with_learner(
    path: &str,
    graceful_rx: watch::Receiver<()>,
    raft_log: MockRaftLog,
) -> (
    LeaderState<MockTypeConfig>,
    crate::raft_context::RaftContext<MockTypeConfig>,
) {
    use d_engine_proto::common::{NodeRole, NodeStatus};
    use d_engine_proto::server::cluster::NodeMeta;

    let mut ctx = mock_raft_context(path, graceful_rx, None);

    let mut membership = MockMembership::new();
    // Only voter(2) → total_voters = 2, quorum = 2
    membership.expect_voters().returning(|| {
        vec![NodeMeta {
            id: 2,
            address: "".into(),
            status: NodeStatus::Active as i32,
            role: NodeRole::Follower as i32,
        }]
    });
    // replication_targets = voter(2) + learner(3)
    membership.expect_replication_peers().returning(|| {
        vec![
            NodeMeta {
                id: 2,
                address: "".into(),
                status: NodeStatus::Active as i32,
                role: NodeRole::Follower as i32,
            },
            NodeMeta {
                id: 3,
                address: "".into(),
                status: NodeStatus::Active as i32,
                role: NodeRole::Learner as i32,
            },
        ]
    });
    ctx.membership = Arc::new(membership);
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    assert!(!state.cluster_metadata.single_voter);

    (state, ctx)
}

/// handle_log_flushed must not include learner match_index in quorum peer list.
///
/// Invariant: only voter match indexes may contribute to commit quorum.
/// Learners replicate entries but must never count toward majority.
///
/// Bug: peer_keys was built from self.match_index.keys(), which includes learner(3).
/// When learner(3) has match_index=4 and voter(2) is absent from match_index,
/// matched_ids=[4] is passed to calculate_majority_matched_index, adding the learner
/// as a quorum member and shifting the majority threshold — blocking a valid commit.
///
/// Fix: filter peer_keys to voter nodes only (exclude learner role from replication_targets).
///
/// Setup:
///   - 2-voter cluster: leader(1), voter(2); learner(3) in replication_targets
///   - voter(2) NOT in match_index (never ACKed)
///   - learner(3) match_index = 4 (fully caught up to commit_index)
///   - commit_index = 4, leader durable = 5
///
/// Expected (after fix): calculate_majority_matched_index receives matched_ids NOT
///   containing 4 (learner); returns Some(5) → commit_index advances to 5.
///
/// Bug behavior: matched_ids=[4] → withf guard fails → test panics → FAILS before fix.
#[tokio::test]
async fn test_handle_log_flushed_excludes_learner_from_quorum() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 5);
    raft_log.expect_durable_index().returning(|| 5);
    // Guard: matched_ids must NOT contain learner(3)'s match value (4).
    // Before fix → ids=[4] → withf returns false → Mockall panics → test FAILS.
    // After fix  → ids=[]  → withf returns true  → returns Some(5) → test PASSES.
    raft_log
        .expect_calculate_majority_matched_index()
        .withf(|_term, _commit, ids| !ids.contains(&4u64))
        .returning(|_, _, _| Some(5));

    let (mut state, ctx) = setup_voter_with_learner(
        "/tmp/test_log_flushed_excludes_learner",
        graceful_rx,
        raft_log,
    )
    .await;

    // learner(3) has caught up to commit_index=4; voter(2) absent from match_index
    state.update_match_index(3, 4).unwrap();
    state.update_commit_index(4).unwrap();

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Action: leader flushes entry 5 to disk
    state.handle_log_flushed(5, &ctx, &role_tx).await;

    assert_eq!(
        state.commit_index(),
        5,
        "commit must advance when learner is correctly excluded from quorum"
    );
    assert!(
        matches!(
            role_rx.try_recv().unwrap(),
            RoleEvent::NotifyNewCommitIndex(_)
        ),
        "NotifyNewCommitIndex must fire after commit advances"
    );
}

/// handle_append_result must not include learner match_index in quorum peer list
/// when a voter ACK triggers quorum re-calculation.
///
/// Invariant: when voter(2) ACKs and we rebuild peer_keys for quorum, only voter
/// match indexes may be included — learner(3)'s prior match_index must be excluded.
///
/// Bug: peer_keys was built from self.match_index.keys(), including learner(3)
/// whose match_index=4 was stored from an earlier replication round.
///
/// Fix: filter peer_keys to voter nodes only.
///
/// Setup:
///   - 2-voter cluster: leader(1), voter(2); learner(3) in replication_targets
///   - learner(3) match_index = 4 (already in map from prior replication)
///   - voter(2) ACKs entry 5 (match=5)
///   - leader durable = 5, commit_index = 4
///
/// Expected (after fix): calculate_majority_matched_index receives matched_ids NOT
///   containing 4 (learner); returns Some(5) → commit_index advances to 5.
///
/// Bug behavior: matched_ids=[5,4] → withf guard fails → test panics → FAILS before fix.
#[tokio::test]
async fn test_handle_append_result_excludes_learner_from_quorum() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 5);
    raft_log.expect_durable_index().returning(|| 5);
    // Guard: matched_ids must NOT contain learner(3)'s match value (4).
    // Before fix → ids=[5,4] → withf returns false → Mockall panics → test FAILS.
    // After fix  → ids=[5]   → withf returns true  → returns Some(5) → test PASSES.
    raft_log
        .expect_calculate_majority_matched_index()
        .withf(|_term, _commit, ids| !ids.contains(&4u64))
        .returning(|_, _, _| Some(5));

    let (mut state, mut ctx) = setup_voter_with_learner(
        "/tmp/test_append_result_excludes_learner",
        graceful_rx,
        raft_log,
    )
    .await;

    ctx.handlers.replication_handler.expect_handle_success_response().returning(
        |_, _, success, _| {
            let match_idx = success.last_match.map(|l| l.index).unwrap_or(0);
            Ok(PeerUpdate {
                match_index: Some(match_idx),
                next_index: match_idx + 1,
                success: true,
            })
        },
    );

    // learner(3) has prior match_index=4 in the map; voter(2) absent
    state.update_match_index(3, 4).unwrap();
    state.update_commit_index(4).unwrap();

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Action: voter(2) ACKs entry 5 — triggers quorum re-calculation
    state
        .handle_append_result(2, Ok(success_response(1, 5)), &ctx, &role_tx)
        .await
        .unwrap();

    assert_eq!(
        state.commit_index(),
        5,
        "commit must advance when learner is correctly excluded from quorum after voter ACK"
    );
    assert!(
        matches!(
            role_rx.try_recv().unwrap(),
            RoleEvent::NotifyNewCommitIndex(_)
        ),
        "NotifyNewCommitIndex must fire after commit advances"
    );
}

// ── multi-voter: LogFlushed triggers quorum re-calculation ────────────────────

/// Multi-voter cluster setup (2 peer nodes → single_voter=false).
/// Returns (state, ctx) where cluster_metadata.single_voter == false.
async fn setup_multi_voter(
    path: &str,
    graceful_rx: watch::Receiver<()>,
    raft_log: MockRaftLog,
) -> (
    LeaderState<MockTypeConfig>,
    crate::raft_context::RaftContext<MockTypeConfig>,
) {
    use d_engine_proto::common::{NodeRole::Follower, NodeStatus};
    use d_engine_proto::server::cluster::NodeMeta;

    let mut ctx = mock_raft_context(path, graceful_rx, None);

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(|| {
        vec![
            NodeMeta {
                id: 2,
                address: "".into(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
            NodeMeta {
                id: 3,
                address: "".into(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
        ]
    });
    membership.expect_replication_peers().returning(Vec::new);
    ctx.membership = Arc::new(membership);
    ctx.storage.raft_log = Arc::new(raft_log);

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
    state.init_cluster_metadata(&ctx.membership).await.unwrap();
    assert!(
        !state.cluster_metadata.single_voter,
        "single_voter must be false for multi-voter tests"
    );

    (state, ctx)
}

/// Multi-voter leader: commit advances when LogFlushed enables quorum.
///
/// **Purpose**: Verify that `handle_log_flushed` re-calculates quorum in multi-voter
/// clusters and advances commit when the leader's durable index catches up to satisfy
/// the majority requirement.
///
/// **Why needed**: This is the multi-voter equivalent of the P0 single-voter fix (#329).
/// Without this, leader writes would hang even when peers have ACKed, because the leader
/// is waiting for its own local flush to complete before the quorum can be satisfied.
///
/// **Scenario**:
/// - 3-node cluster (leader + 2 peers)
/// - Leader appends entry 1, peers ACK immediately (match_index=[1,1])
/// - Leader's durable_index=0 → quorum blocked (need 2/3, have only 2/3 peers but leader not durable)
/// - LogFlushed(1) arrives → leader durable=1 → quorum satisfied ([1,1,1]) → commit advances to 1
#[tokio::test]
async fn test_multi_voter_commit_advances_when_log_flushed_enables_quorum() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_durable_index().returning(|| 1); // LogFlushed completed
    // Quorum satisfied: majority of [leader=1, peer2=1, peer3=1] → commit=1
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(1));

    let (mut state, ctx) = setup_multi_voter(
        "/tmp/test_multi_voter_log_flushed_enables_quorum",
        graceful_rx,
        raft_log,
    )
    .await;

    // Simulate peers already ACKed (match_index pre-populated)
    state.update_match_index(2, 1).unwrap();
    state.update_match_index(3, 1).unwrap();

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Precondition: commit still at 0 (durable_index was 0 during process_batch)
    assert_eq!(state.commit_index(), 0);

    // Action: LogFlushed(1) — leader's entry is now crash-safe, quorum can be calculated
    state.handle_log_flushed(1, &ctx, &role_tx).await;

    // Verify: commit_index advanced to 1 (quorum of [1,1,1] satisfied)
    assert_eq!(
        state.commit_index(),
        1,
        "multi-voter commit must advance when LogFlushed enables quorum"
    );

    // Verify: NotifyNewCommitIndex event sent to state machine
    assert!(
        matches!(
            role_rx.try_recv().unwrap(),
            RoleEvent::NotifyNewCommitIndex(_)
        ),
        "NotifyNewCommitIndex must fire after commit advances"
    );
}

/// Multi-voter leader: LogFlushed does not advance commit when quorum still insufficient.
///
/// **Purpose**: Guard against premature commit advancement — LogFlushed alone is NOT
/// sufficient to commit an entry in multi-voter clusters. The entry must still satisfy
/// the majority replication requirement.
///
/// **Why needed**: Ensures correctness and prevents a single point of failure where the
/// leader incorrectly commits an entry that hasn't been replicated to a majority of nodes.
/// This would violate Raft's safety guarantee.
///
/// **Scenario**:
/// - 3-node cluster (leader + 2 peers)
/// - Leader appends entry 1, but peers have NOT ACKed yet (match_index=[0,0])
/// - LogFlushed(1) arrives → leader durable=1, but quorum=[1,0,0] not satisfied (need 2/3)
/// - Commit must stay at 0 until at least one peer ACKs
#[tokio::test]
async fn test_multi_voter_log_flushed_no_commit_when_quorum_insufficient() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_durable_index().returning(|| 1); // Leader durable
    // Quorum NOT satisfied: majority of [leader=1, peer2=0, peer3=0] → None
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| None);

    let (mut state, ctx) = setup_multi_voter(
        "/tmp/test_multi_voter_log_flushed_quorum_insufficient",
        graceful_rx,
        raft_log,
    )
    .await;

    // Peers have NOT ACKed yet (match_index remains at 0 for both)
    state.update_match_index(2, 0).unwrap();
    state.update_match_index(3, 0).unwrap();

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    // Precondition: commit at 0
    assert_eq!(state.commit_index(), 0);

    // Action: LogFlushed(1) — leader durable, but peers haven't replicated yet
    state.handle_log_flushed(1, &ctx, &role_tx).await;

    // Verify: commit_index must stay at 0 (quorum not satisfied)
    assert_eq!(
        state.commit_index(),
        0,
        "multi-voter commit must NOT advance when quorum insufficient despite LogFlushed"
    );

    // Verify: no commit event should be sent
    assert!(
        role_rx.try_recv().is_err(),
        "NotifyNewCommitIndex must NOT fire when quorum not satisfied"
    );
}
