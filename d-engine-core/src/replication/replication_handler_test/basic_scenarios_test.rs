//! Basic cluster topology scenarios for ReplicationHandler
//!
//! Tests verify that prepare_batch_requests builds the correct number of
//! replication requests for each cluster topology:
//! - Single-node: no requests (no peers to replicate to)
//! - Two-node: 1 request (one per peer)
//! - Three-node: 2 requests (one per peer)
//! - Five-node: 4 requests (one per peer)

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

/// Single-node cluster builds no replication requests (no peers to replicate to).
///
/// # Scenario
/// - Cluster: 1 voter (no peers, single_voter=true)
/// - Input: Empty command batch
/// - Expected: Empty request list — no replication needed, auto-commit
#[tokio::test]
async fn test_single_voter_builds_no_replication_requests() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_single_voter_builds_no_replication_requests",
        graceful_rx,
        None,
    );
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_first_entry_id().returning(|| 1);
    // next_index map is empty here, so build_append_request defaults every peer's
    // recorded_next_index to 1 -> prev_index=0 -> entry_term(0) is always queried.
    raft_log.expect_entry_term().returning(|_| None);
    context.storage.raft_log = Arc::new(raft_log);

    let result = handler
        .prepare_batch_requests(
            vec![],
            StateSnapshot {
                current_term: 1,
                voted_for: None,
                commit_index: 1,
                role: Leader.into(),
            },
            LeaderStateSnapshot {
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                noop_log_id: None,
            },
            &ClusterMetadata {
                single_voter: true,
                replication_targets: vec![],
                total_voters: 1,
            },
            &context,
            &HashMap::new(),
        )
        .await
        .unwrap();

    assert!(
        result.append_requests.is_empty() && result.snapshot_targets.is_empty(),
        "single voter builds no replication requests"
    );
}

/// Two-node cluster builds one replication request for the single peer.
///
/// # Scenario
/// - Cluster: 1 leader + 1 voter (2 nodes total)
/// - Expected: 1 request built, targeting peer 2
#[tokio::test]
async fn test_two_node_cluster_builds_one_replication_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_two_node_cluster_builds_one_replication_request",
        graceful_rx,
        None,
    );
    let peer2_id = 2u32;
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_first_entry_id().returning(|| 1);
    // next_index map is empty here, so build_append_request defaults every peer's
    // recorded_next_index to 1 -> prev_index=0 -> entry_term(0) is always queried.
    raft_log.expect_entry_term().returning(|_| None);
    context.storage.raft_log = Arc::new(raft_log);

    let result = handler
        .prepare_batch_requests(
            vec![],
            StateSnapshot {
                current_term: 1,
                voted_for: None,
                commit_index: 0,
                role: Leader.into(),
            },
            LeaderStateSnapshot {
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                noop_log_id: None,
            },
            &ClusterMetadata {
                single_voter: false,
                replication_targets: vec![NodeMeta {
                    id: peer2_id,
                    address: "http://127.0.0.1:55001".to_string(),
                    role: NodeRole::Follower.into(),
                    status: NodeStatus::Active.into(),
                }],
                total_voters: 2,
            },
            &context,
            &HashMap::new(),
        )
        .await
        .unwrap();

    assert_eq!(
        result.append_requests.len(),
        1,
        "two-node cluster builds 1 replication request"
    );
    assert_eq!(
        result.append_requests[0].0, peer2_id,
        "request targets peer 2"
    );
}

/// Three-node cluster builds two replication requests, one per peer.
///
/// # Scenario
/// - Cluster: 1 leader + 2 voters (3 nodes total)
/// - Expected: 2 requests (one for peer 2 and one for peer 3)
#[tokio::test]
async fn test_three_node_cluster_builds_two_replication_requests() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_three_node_cluster_builds_two_replication_requests",
        graceful_rx,
        None,
    );
    let peer2_id = 2u32;
    let peer3_id = 3u32;
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_first_entry_id().returning(|| 1);
    // next_index map is empty here, so build_append_request defaults every peer's
    // recorded_next_index to 1 -> prev_index=0 -> entry_term(0) is always queried.
    raft_log.expect_entry_term().returning(|_| None);
    context.storage.raft_log = Arc::new(raft_log);

    let result = handler
        .prepare_batch_requests(
            vec![],
            StateSnapshot {
                current_term: 1,
                voted_for: None,
                commit_index: 0,
                role: Leader.into(),
            },
            LeaderStateSnapshot {
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                noop_log_id: None,
            },
            &ClusterMetadata {
                single_voter: false,
                replication_targets: vec![
                    NodeMeta {
                        id: peer2_id,
                        address: "http://127.0.0.1:55001".to_string(),
                        role: NodeRole::Follower.into(),
                        status: NodeStatus::Active.into(),
                    },
                    NodeMeta {
                        id: peer3_id,
                        address: "http://127.0.0.1:55002".to_string(),
                        role: NodeRole::Follower.into(),
                        status: NodeStatus::Active.into(),
                    },
                ],
                total_voters: 3,
            },
            &context,
            &HashMap::new(),
        )
        .await
        .unwrap();

    assert_eq!(
        result.append_requests.len(),
        2,
        "three-node cluster builds 2 replication requests"
    );
    let peer_ids: Vec<u32> = result.append_requests.iter().map(|(id, _, _)| *id).collect();
    assert!(peer_ids.contains(&peer2_id), "request for peer 2");
    assert!(peer_ids.contains(&peer3_id), "request for peer 3");
}

/// Five-node cluster builds four replication requests, one per peer.
///
/// # Scenario
/// - Cluster: 1 leader + 4 voters (5 nodes total)
/// - Expected: 4 requests (one for each peer)
#[tokio::test]
async fn test_five_node_cluster_builds_four_replication_requests() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_five_node_cluster_builds_four_replication_requests",
        graceful_rx,
        None,
    );
    let peer2_id = 2u32;
    let peer3_id = 3u32;
    let peer4_id = 4u32;
    let peer5_id = 5u32;
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_first_entry_id().returning(|| 1);
    // next_index map is empty here, so build_append_request defaults every peer's
    // recorded_next_index to 1 -> prev_index=0 -> entry_term(0) is always queried.
    raft_log.expect_entry_term().returning(|_| None);
    context.storage.raft_log = Arc::new(raft_log);

    let result = handler
        .prepare_batch_requests(
            vec![],
            StateSnapshot {
                current_term: 1,
                voted_for: None,
                commit_index: 0,
                role: Leader.into(),
            },
            LeaderStateSnapshot {
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                noop_log_id: None,
            },
            &ClusterMetadata {
                single_voter: false,
                replication_targets: vec![
                    NodeMeta {
                        id: peer2_id,
                        address: "http://127.0.0.1:55001".to_string(),
                        role: NodeRole::Follower.into(),
                        status: NodeStatus::Active.into(),
                    },
                    NodeMeta {
                        id: peer3_id,
                        address: "http://127.0.0.1:55002".to_string(),
                        role: NodeRole::Follower.into(),
                        status: NodeStatus::Active.into(),
                    },
                    NodeMeta {
                        id: peer4_id,
                        address: "http://127.0.0.1:55003".to_string(),
                        role: NodeRole::Follower.into(),
                        status: NodeStatus::Active.into(),
                    },
                    NodeMeta {
                        id: peer5_id,
                        address: "http://127.0.0.1:55004".to_string(),
                        role: NodeRole::Follower.into(),
                        status: NodeStatus::Active.into(),
                    },
                ],
                total_voters: 5,
            },
            &context,
            &HashMap::new(),
        )
        .await
        .unwrap();

    assert_eq!(
        result.append_requests.len(),
        4,
        "five-node cluster builds 4 replication requests"
    );
    let peer_ids: Vec<u32> = result.append_requests.iter().map(|(id, _, _)| *id).collect();
    assert!(peer_ids.contains(&peer2_id));
    assert!(peer_ids.contains(&peer3_id));
    assert!(peer_ids.contains(&peer4_id));
    assert!(peer_ids.contains(&peer5_id));
}

/// A window-gated peer must still receive a request, just without entries (liveness), and its
/// position fields must match what an ungated peer at the same position gets.
///
/// # Scenario
/// - Peers 2 and 3 both at `next_index = 1`, one old entry at index 1.
/// - `peer_gating_decisions = {2: true}`.
/// - Expected: two requests; peer 2's has empty entries, peer 3's has one entry; both carry the
///   same `prev_log_index`.
#[tokio::test]
async fn test_gated_peer_still_gets_empty_request_for_liveness() {
    use crate::mock_insert_log_entries;
    use crate::mock_log_entries_exist;

    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_gated_peer_still_gets_empty_request_for_liveness",
        graceful_rx,
        None,
    );
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);

    let old_entries = mock_insert_log_entries(vec![1], 1, 1);
    let mut raft_log = MockRaftLog::new();
    mock_log_entries_exist(&mut raft_log, old_entries);
    raft_log.expect_first_entry_id().returning(|| 1);
    raft_log.expect_entry_term().returning(|_| None);
    context.storage.raft_log = Arc::new(raft_log);

    let peer = |id: u32| NodeMeta {
        id,
        address: format!("http://127.0.0.1:{}", 55000 + id),
        role: NodeRole::Follower.into(),
        status: NodeStatus::Active.into(),
    };

    let result = handler
        .prepare_batch_requests(
            vec![],
            StateSnapshot {
                current_term: 1,
                voted_for: None,
                commit_index: 0,
                role: Leader.into(),
            },
            LeaderStateSnapshot {
                next_index: HashMap::from([(2, 1), (3, 1)]),
                match_index: HashMap::new(),
                noop_log_id: None,
            },
            &ClusterMetadata {
                single_voter: false,
                replication_targets: vec![peer(2), peer(3)],
                total_voters: 3,
            },
            &context,
            &HashMap::from([(2_u32, true)]),
        )
        .await
        .unwrap();

    assert_eq!(result.append_requests.len(), 2, "both peers get a request");
    let req_of = |id: u32| {
        &result
            .append_requests
            .iter()
            .find(|(p, _, _)| *p == id)
            .unwrap_or_else(|| panic!("no request for peer {id}"))
            .1
    };
    assert!(
        req_of(2).entries.is_empty(),
        "the gated peer's request is an empty keepalive"
    );
    assert_eq!(
        req_of(3).entries.len(),
        1,
        "the ungated peer still gets its entry"
    );
    assert_eq!(
        req_of(2).prev_log_index,
        req_of(3).prev_log_index,
        "the keepalive is a consistent request at the same position"
    );
}
