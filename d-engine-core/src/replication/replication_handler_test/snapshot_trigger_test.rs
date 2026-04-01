//! Tests for snapshot-trigger logic in `prepare_batch_requests`.
//!
//! When a peer's `next_index` falls below the leader's `first_entry_id`
//! (i.e. the required log entries have been purged via snapshot), the leader
//! must NOT send an AppendEntries request with `prev_log_term = 0`.
//! Instead, the peer must be routed to a snapshot transfer.
//!
//! ## Bug reproduced (before fix)
//! `entry_term(prev_log_index)` returns `None` for purged non-boundary entries.
//! `build_append_request` uses `.unwrap_or(0)` → `prev_log_term = 0`.
//! Follower has the entry at a non-zero term → perpetual CONFLICT loop.
//!
//! ## Expected behaviour (after fix)
//! `prepare_batch_requests` detects `peer_next_id < first_entry_id` and returns
//! the peer in `PrepareResult.snapshot_targets` instead of `append_requests`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::ClusterMetadata;
use crate::LeaderStateSnapshot;
use crate::MockRaftLog;
use crate::MockTypeConfig;
use crate::ReplicationCore;
use crate::ReplicationHandler;
use crate::StateSnapshot;
use crate::test_utils::mock_raft_context;
use d_engine_proto::common::NodeRole;
use d_engine_proto::common::NodeRole::Leader;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::NodeMeta;
use tokio::sync::watch;

fn voter(id: u32) -> NodeMeta {
    NodeMeta {
        id,
        address: format!("127.0.0.1:5500{id}"),
        role: NodeRole::Follower.into(),
        status: NodeStatus::Active.into(),
    }
}

/// Peer whose next_index falls below the purge boundary must be routed to
/// snapshot, not AppendEntries.
///
/// # Scenario
/// - Leader log: entries 1-10 purged (first_entry_id = 11, last_entry_id = 15)
/// - Peer 2 next_index = 4  → prev_log_index = 3, which is purged (non-boundary)
/// - Expected: PrepareResult.snapshot_targets = [2]
///             PrepareResult.append_requests does NOT contain peer 2
///
/// # Before fix (will FAIL)
/// `prepare_batch_requests` returns an AppendEntriesRequest for peer 2 with
/// `prev_log_term = 0`, causing an infinite CONFLICT loop on the follower.
///
/// # After fix (will PASS)
/// Peer 2 is detected as needing a snapshot and excluded from append_requests.
#[tokio::test]
async fn test_prepare_batch_requests_routes_lagging_peer_to_snapshot() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_prepare_batch_requests_routes_lagging_peer_to_snapshot",
        graceful_rx,
        None,
    );

    let handler = ReplicationHandler::<MockTypeConfig>::new(1);

    // Leader log: entries 1-10 purged, entries 11-15 in memory.
    // first_entry_id = 11  (the purge boundary)
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 15);
    raft_log.expect_first_entry_id().returning(|| 11); // <── key: purge boundary
    raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
    raft_log.expect_entry_term().returning(|idx| {
        // Only entry 10 (last_purged_index boundary) and 11-15 have terms.
        match idx {
            10 => Some(2), // snapshot boundary
            11..=15 => Some(2),
            _ => None, // 1-9 are purged non-boundary
        }
    });
    ctx.storage.raft_log = Arc::new(raft_log);

    // Peer 2: next_index = 4 → prev_log_index = 3, which is purged.
    let next_index = HashMap::from([(2u32, 4u64)]);

    let result = handler
        .prepare_batch_requests(
            vec![],
            StateSnapshot {
                current_term: 2,
                voted_for: None,
                commit_index: 15,
                role: Leader.into(),
            },
            LeaderStateSnapshot {
                next_index,
                match_index: HashMap::new(),
                noop_log_id: None,
            },
            &ClusterMetadata {
                single_voter: false,
                replication_targets: vec![voter(2)],
                total_voters: 2,
            },
            &ctx,
        )
        .await
        .unwrap();

    // After fix: peer 2 must be in snapshot_targets, not append_requests.
    assert!(
        result.snapshot_targets.contains(&2),
        "peer 2 (next_index=4 < first_entry_id=11) must be routed to snapshot"
    );
    assert!(
        !result.append_requests.iter().any(|(id, _)| *id == 2),
        "peer 2 must NOT receive an AppendEntries when behind purge boundary"
    );
}

/// Peer caught up with the log (next_index >= first_entry_id) must still
/// receive a normal AppendEntries — snapshot routing must not over-trigger.
///
/// # Scenario
/// - Leader: first_entry_id = 11, last = 15
/// - Peer 2: next_index = 12 → caught up, no snapshot needed
/// - Expected: peer 2 in append_requests, NOT in snapshot_targets
#[tokio::test]
async fn test_prepare_batch_requests_caught_up_peer_gets_append_entries() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_prepare_batch_requests_caught_up_peer_gets_append_entries",
        graceful_rx,
        None,
    );

    let handler = ReplicationHandler::<MockTypeConfig>::new(1);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 15);
    raft_log.expect_first_entry_id().returning(|| 11);
    raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
    raft_log.expect_entry_term().returning(|idx| match idx {
        10 => Some(2),
        11..=15 => Some(2),
        _ => None,
    });
    ctx.storage.raft_log = Arc::new(raft_log);

    // Peer 2: next_index = 12 → prev_log_index = 11, which is in memory.
    let next_index = HashMap::from([(2u32, 12u64)]);

    let result = handler
        .prepare_batch_requests(
            vec![],
            StateSnapshot {
                current_term: 2,
                voted_for: None,
                commit_index: 15,
                role: Leader.into(),
            },
            LeaderStateSnapshot {
                next_index,
                match_index: HashMap::new(),
                noop_log_id: None,
            },
            &ClusterMetadata {
                single_voter: false,
                replication_targets: vec![voter(2)],
                total_voters: 2,
            },
            &ctx,
        )
        .await
        .unwrap();

    assert!(
        !result.snapshot_targets.contains(&2),
        "caught-up peer 2 must NOT be routed to snapshot"
    );
    assert!(
        result.append_requests.iter().any(|(id, _)| *id == 2),
        "caught-up peer 2 must receive a normal AppendEntries"
    );
}

/// Mixed cluster: one peer behind boundary (needs snapshot), one caught up
/// (needs AppendEntries). Each peer must be routed correctly.
///
/// # Scenario
/// - first_entry_id = 11
/// - Peer 2: next_index = 4  → snapshot target
/// - Peer 3: next_index = 12 → append target
#[tokio::test]
async fn test_prepare_batch_requests_splits_snapshot_and_append_peers() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut ctx = mock_raft_context(
        "/tmp/test_prepare_batch_requests_splits_snapshot_and_append_peers",
        graceful_rx,
        None,
    );

    let handler = ReplicationHandler::<MockTypeConfig>::new(1);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 15);
    raft_log.expect_first_entry_id().returning(|| 11);
    raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
    raft_log.expect_entry_term().returning(|idx| match idx {
        10 => Some(2),
        11..=15 => Some(2),
        _ => None,
    });
    ctx.storage.raft_log = Arc::new(raft_log);

    let next_index = HashMap::from([
        (2u32, 4u64),  // behind boundary → snapshot
        (3u32, 12u64), // caught up → append
    ]);

    let result = handler
        .prepare_batch_requests(
            vec![],
            StateSnapshot {
                current_term: 2,
                voted_for: None,
                commit_index: 15,
                role: Leader.into(),
            },
            LeaderStateSnapshot {
                next_index,
                match_index: HashMap::new(),
                noop_log_id: None,
            },
            &ClusterMetadata {
                single_voter: false,
                replication_targets: vec![voter(2), voter(3)],
                total_voters: 3,
            },
            &ctx,
        )
        .await
        .unwrap();

    assert!(
        result.snapshot_targets.contains(&2),
        "peer 2 (behind boundary) must be in snapshot_targets"
    );
    assert!(
        !result.append_requests.iter().any(|(id, _)| *id == 2),
        "peer 2 must NOT be in append_requests"
    );
    assert!(
        !result.snapshot_targets.contains(&3),
        "peer 3 (caught up) must NOT be in snapshot_targets"
    );
    assert!(
        result.append_requests.iter().any(|(id, _)| *id == 3),
        "peer 3 must be in append_requests"
    );
}
