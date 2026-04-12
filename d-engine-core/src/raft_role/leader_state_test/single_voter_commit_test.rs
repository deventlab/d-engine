//! Single-Voter Commit Path Tests
//!
//! Regression tests for the MemFirst single-voter commit path in `handle_log_flushed`.
//!
//! ## Bug History
//! `fix #329` changed `handle_log_flushed` single-voter branch to commit to `durable`
//! instead of `last_entry_id()`. This placed IO thread latency on the commit critical
//! path, causing a ~3x latency regression in 3-node embedded bench (1731µs vs ~566µs).
//!
//! ## MemFirst Single-Voter Invariant
//! `LogFlushed(durable)` is an IO checkpoint. Commit must advance to `last_entry_id()`
//! — not just `durable` — to allow pipelining across IO batch boundaries.
//! This matches the multi-voter path where the leader contributes `last_entry_id()` to
//! quorum (not `durable_index`).

use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tokio::sync::watch;

async fn setup_single_voter(
    last_entry_id_val: u64
) -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
    Arc<AtomicU64>,
) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let last_entry_id = Arc::new(AtomicU64::new(last_entry_id_val));
    let last_entry_id_clone = last_entry_id.clone();

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_last_entry_id()
        .returning(move || last_entry_id_clone.load(Ordering::Relaxed));

    let replication = MockReplicationCore::new();

    let ctx = MockBuilder::new(shutdown_rx)
        .with_raft_log(raft_log)
        .with_replication_handler(replication)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    assert!(state.cluster_metadata.single_voter);

    (state, ctx, last_entry_id)
}

/// MemFirst single-voter: `handle_log_flushed` must commit to `last_entry_id`, not `durable`.
///
/// Simulates: IO batch flushed entries 1-3 (`durable=3`), but entries 4-5 arrived
/// in memory during the flush (`last_entry_id=5`). MemFirst: commit must advance
/// to 5 (all in-memory entries), not stall at 3 (only persisted entries).
///
/// This test FAILS if `handle_log_flushed` uses `durable` for commit
/// (the `fix #329` regression that caused +617µs avg latency in 3-node embedded bench).
#[tokio::test]
async fn test_single_voter_commit_uses_last_entry_id_not_durable() {
    // last_entry_id=5: entries 4-5 arrived in memory during the IO flush of 1-3
    let (mut state, ctx, _last_entry_id) = setup_single_voter(5).await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // IO flushed only entries 1-3 (durable=3), but log has entries 1-5 in memory
    state.handle_log_flushed(3, &ctx, &role_tx).await;

    assert_eq!(
        state.commit_index(),
        5,
        "MemFirst single-voter: commit must use last_entry_id=5, not durable=3. \
         Using durable puts IO latency on the commit critical path."
    );
}

/// After IO catches up (durable == last_entry_id), commit equals last_entry_id.
#[tokio::test]
async fn test_single_voter_commit_when_durable_equals_last_entry_id() {
    let (mut state, ctx, _last_entry_id) = setup_single_voter(5).await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    state.handle_log_flushed(5, &ctx, &role_tx).await;

    assert_eq!(
        state.commit_index(),
        5,
        "commit must advance to last_entry_id=5 when durable=5"
    );
}

/// Pipelining across multiple IO batches: each flush triggers commit to current last_entry_id.
///
/// Simulates rapid writes where IO batches lag behind in-memory log:
/// - Flush 1: IO flushed 1-3, log has 1-7 in memory → commit=7
/// - Flush 2: IO flushed 4-7, log has 1-10 in memory → commit=10
#[tokio::test]
async fn test_single_voter_pipelining_across_io_batches() {
    let (mut state, ctx, last_entry_id) = setup_single_voter(7).await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // IO batch 1: flushed 1-3, memory has 1-7
    state.handle_log_flushed(3, &ctx, &role_tx).await;
    assert_eq!(
        state.commit_index(),
        7,
        "commit must advance to last_entry_id=7"
    );

    // IO batch 2: flushed 4-7, memory now has 1-10
    last_entry_id.store(10, Ordering::Relaxed);
    state.handle_log_flushed(7, &ctx, &role_tx).await;
    assert_eq!(
        state.commit_index(),
        10,
        "commit must advance to last_entry_id=10"
    );
}

/// No-op flush: last_entry_id == commit_index means nothing new to commit.
#[tokio::test]
async fn test_single_voter_no_commit_when_nothing_new() {
    let (mut state, ctx, _last_entry_id) = setup_single_voter(3).await;
    let (role_tx, _role_rx) = mpsc::unbounded_channel();

    // First flush advances commit to 3
    state.handle_log_flushed(3, &ctx, &role_tx).await;
    assert_eq!(state.commit_index(), 3);

    // Second flush with same last_entry_id=3: no new entries → no commit advance
    state.handle_log_flushed(3, &ctx, &role_tx).await;
    assert_eq!(
        state.commit_index(),
        3,
        "commit must not advance when last_entry_id == commit_index"
    );
}
