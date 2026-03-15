//! Commit Index Correctness Tests
//!
//! Verifies that commit index decisions always use `durable_index` (crash-safe),
//! never `last_entry_id` (in-memory). Covers both the single-voter fast path and
//! the multi-voter quorum path. Fixed by ticket #329.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use crate::event::RoleEvent;
use crate::maybe_clone_oneshot::MaybeCloneOneshot;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_role::leader_state::LeaderState;
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::{MockTypeConfig, mock_raft_context};
use crate::{AppendResults, MockMembership, MockRaftLog, PeerUpdate, RaftRequestWithSignal};
use bytes::Bytes;
use d_engine_proto::common::EntryPayload;

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

    let (mut state, mut ctx) = setup_single_voter(
        "/tmp/test_single_voter_no_commit_before_durable",
        graceful_rx,
        raft_log,
    )
    .await;

    ctx.handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::new(), // single voter: no peers
                learner_progress: HashMap::new(),
            })
        });

    let (req, _rx) = write_request();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state.process_batch(VecDeque::from(vec![req]), &role_tx, &ctx).await.unwrap();

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

    let (mut state, mut ctx) = setup_single_voter(
        "/tmp/test_single_voter_commit_after_durable",
        graceful_rx,
        raft_log,
    )
    .await;

    ctx.handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::new(),
                learner_progress: HashMap::new(),
            })
        });

    let (req, _rx) = write_request();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state.process_batch(VecDeque::from(vec![req]), &role_tx, &ctx).await.unwrap();

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
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: Some(1),
                            next_index: 2,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: Some(1),
                            next_index: 2,
                            success: true,
                        },
                    ),
                ]),
                learner_progress: HashMap::new(),
            })
        });

    let (req, _rx) = write_request();
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    state.process_batch(VecDeque::from(vec![req]), &role_tx, &ctx).await.unwrap();

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
