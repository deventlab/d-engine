//! Basic cluster topology scenarios for ReplicationHandler
//!
//! Tests verify correct behavior across different cluster configurations:
//! - Single-node clusters (auto-commit without replication)
//! - Two-node clusters (simple majority)
//! - Three-node clusters (standard quorum)
//! - Five-node clusters (larger quorum calculations)

use crate::AppendResult;
use crate::ClusterMetadata;
use crate::LeaderStateSnapshot;
use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockTransport;
use crate::MockTypeConfig;
use crate::NetworkError;
use crate::ReplicationCore;
use crate::ReplicationHandler;
use crate::StateSnapshot;
use crate::test_utils::mock_raft_context;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole;
use d_engine_proto::common::NodeRole::Leader;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::replication::AppendEntriesResponse;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;

/// Helper: Create mock membership for multi-node cluster
fn create_mock_membership_multi_node() -> MockMembership<MockTypeConfig> {
    let mut membership = MockMembership::new();
    membership.expect_is_single_node_cluster().returning(|| false);
    membership.expect_initial_cluster_size().returning(|| 3);
    membership
}

/// Single-node cluster should achieve quorum immediately without replication.
///
/// # Scenario
/// - Cluster: 1 voter (no peers)
/// - Input: Empty command batch
/// - Expected: Immediate commit (no replication needed)
#[tokio::test]
async fn test_single_voter_achieves_quorum_immediately() {
    // Arrange: Setup single-voter cluster context
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_single_voter_achieves_quorum_immediately",
        graceful_rx,
        None,
    );
    let my_id = 1;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Arrange: Leader state with empty batch
    let commands = Vec::new();
    let state_snapshot = StateSnapshot {
        current_term: 1,
        voted_for: None,
        commit_index: 1,
        role: Leader.into(),
    };
    let leader_state_snapshot = LeaderStateSnapshot {
        next_index: HashMap::new(),
        match_index: HashMap::new(),
        noop_log_id: None,
    };

    // Arrange: Mock raft log
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
    raft_log.expect_entry_term().returning(|_| None);
    context.storage.raft_log = Arc::new(raft_log);

    // Arrange: Mock transport (expects no peer communication)
    let mut transport = MockTransport::new();
    transport.expect_send_append_requests().returning(move |_, _, _, _| {
        Err(NetworkError::EmptyPeerList {
            request_type: "send_vote_requests",
        }
        .into())
    });
    context.transport = Arc::new(transport);

    // Arrange: Mock membership with no replication peers
    let mut membership = create_mock_membership_multi_node();
    membership.expect_replication_peers().returning(Vec::new);
    context.membership = Arc::new(membership);

    // Act: Process empty batch
    let result = handler
        .handle_raft_request_in_batch(
            commands,
            state_snapshot,
            leader_state_snapshot,
            &ClusterMetadata {
                single_voter: true,
                replication_targets: vec![],
                total_voters: 1,
            },
            &context,
        )
        .await;

    // Assert: Quorum achieved without peer responses
    assert!(result.is_ok(), "single voter should succeed immediately");
    let append_result = result.unwrap();
    assert!(
        append_result.commit_quorum_achieved,
        "single voter should auto-commit"
    );
    assert!(
        append_result.peer_updates.is_empty(),
        "no peers to update in single-voter cluster"
    );
}

/// Two-node cluster should achieve quorum when peer responds successfully.
///
/// # Scenario
/// - Cluster: 1 leader + 1 voter
/// - Input: Empty command batch
/// - Expected: Quorum achieved when single peer responds
#[tokio::test]
async fn test_two_node_cluster_achieves_quorum_with_peer_response() {
    // Arrange: Setup two-node cluster context
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_two_node_cluster_achieves_quorum",
        graceful_rx,
        None,
    );
    let my_id = 1;
    let peer2_id = 2;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Arrange: Leader state with empty batch
    let state_snapshot = StateSnapshot {
        current_term: 1,
        voted_for: None,
        commit_index: 0,
        role: Leader.into(),
    };
    let leader_state_snapshot = LeaderStateSnapshot {
        next_index: HashMap::new(),
        match_index: HashMap::new(),
        noop_log_id: None,
    };

    // Arrange: Mock raft log
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().return_const(1_u64);
    raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
    raft_log.expect_entry_term().returning(|_| None);
    context.storage.raft_log = Arc::new(raft_log);

    // Arrange: Mock transport with successful peer response
    let mut transport = MockTransport::new();
    transport.expect_send_append_requests().return_once(move |_, _, _, _| {
        Ok(AppendResult {
            peer_ids: vec![peer2_id].into_iter().collect(),
            responses: vec![Ok(AppendEntriesResponse::success(
                peer2_id,
                1,
                Some(LogId { term: 1, index: 1 }),
            ))],
        })
    });
    context.transport = Arc::new(transport);

    // Act: Process empty batch with 2-node cluster metadata
    let result = handler
        .handle_raft_request_in_batch(
            vec![],
            state_snapshot,
            leader_state_snapshot,
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
        )
        .await
        .unwrap();

    // Assert: Quorum achieved (2/2 = leader + 1 peer)
    assert!(
        result.commit_quorum_achieved,
        "two-node cluster should achieve quorum with peer response"
    );
}

/// Three-node cluster should achieve quorum when both peers respond successfully.
///
/// # Scenario
/// - Cluster: 1 leader + 2 voters
/// - Input: Empty command batch
/// - Expected: Quorum achieved when both peers respond (3/3)
#[tokio::test]
async fn test_three_node_cluster_achieves_quorum_with_all_peers() {
    // Arrange: Setup three-node cluster context
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_three_node_cluster_achieves_quorum",
        graceful_rx,
        None,
    );
    let my_id = 1;
    let peer2_id = 2;
    let peer3_id = 3;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Arrange: Leader state with empty batch
    let state_snapshot = StateSnapshot {
        current_term: 1,
        voted_for: None,
        commit_index: 0,
        role: Leader.into(),
    };
    let leader_state_snapshot = LeaderStateSnapshot {
        next_index: HashMap::new(),
        match_index: HashMap::new(),
        noop_log_id: None,
    };

    // Arrange: Mock raft log
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().return_const(1_u64);
    raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
    raft_log.expect_entry_term().returning(|_| None);
    context.storage.raft_log = Arc::new(raft_log);

    // Arrange: Mock transport with all peers responding successfully
    let mut transport = MockTransport::new();
    transport.expect_send_append_requests().return_once(move |_, _, _, _| {
        Ok(AppendResult {
            peer_ids: vec![peer2_id, peer3_id].into_iter().collect(),
            responses: vec![
                Ok(AppendEntriesResponse::success(
                    peer2_id,
                    1,
                    Some(LogId { term: 1, index: 1 }),
                )),
                Ok(AppendEntriesResponse::success(
                    peer3_id,
                    1,
                    Some(LogId { term: 1, index: 1 }),
                )),
            ],
        })
    });
    context.transport = Arc::new(transport);

    // Act: Process empty batch with 3-node cluster metadata
    let result = handler
        .handle_raft_request_in_batch(
            vec![],
            state_snapshot,
            leader_state_snapshot,
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
        )
        .await
        .unwrap();

    // Assert: Quorum achieved (3/3 = leader + 2 peers)
    assert!(
        result.commit_quorum_achieved,
        "three-node cluster should achieve quorum with all peers responding"
    );
}

/// Five-node cluster should achieve quorum when all four peers respond successfully.
///
/// # Scenario
/// - Cluster: 1 leader + 4 voters
/// - Input: Empty command batch
/// - Expected: Quorum achieved when all 4 peers respond (5/5)
#[tokio::test]
async fn test_five_node_cluster_achieves_quorum_with_all_peers() {
    // Arrange: Setup five-node cluster context
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_five_node_cluster_achieves_quorum",
        graceful_rx,
        None,
    );
    let my_id = 1;
    let peer2_id = 2;
    let peer3_id = 3;
    let peer4_id = 4;
    let peer5_id = 5;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Arrange: Leader state with empty batch
    let state_snapshot = StateSnapshot {
        current_term: 1,
        voted_for: None,
        commit_index: 0,
        role: Leader.into(),
    };
    let leader_state_snapshot = LeaderStateSnapshot {
        next_index: HashMap::new(),
        match_index: HashMap::new(),
        noop_log_id: None,
    };

    // Arrange: Mock raft log
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().return_const(1_u64);
    raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
    raft_log.expect_entry_term().returning(|_| None);
    context.storage.raft_log = Arc::new(raft_log);

    // Arrange: Mock transport with all four peers responding successfully
    let mut transport = MockTransport::new();
    transport.expect_send_append_requests().return_once(move |_, _, _, _| {
        Ok(AppendResult {
            peer_ids: vec![peer2_id, peer3_id, peer4_id, peer5_id].into_iter().collect(),
            responses: vec![
                Ok(AppendEntriesResponse::success(
                    peer2_id,
                    1,
                    Some(LogId { term: 1, index: 1 }),
                )),
                Ok(AppendEntriesResponse::success(
                    peer3_id,
                    1,
                    Some(LogId { term: 1, index: 1 }),
                )),
                Ok(AppendEntriesResponse::success(
                    peer4_id,
                    1,
                    Some(LogId { term: 1, index: 1 }),
                )),
                Ok(AppendEntriesResponse::success(
                    peer5_id,
                    1,
                    Some(LogId { term: 1, index: 1 }),
                )),
            ],
        })
    });
    context.transport = Arc::new(transport);

    // Act: Process empty batch with 5-node cluster metadata
    let result = handler
        .handle_raft_request_in_batch(
            vec![],
            state_snapshot,
            leader_state_snapshot,
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
        )
        .await
        .unwrap();

    // Assert: Quorum achieved (5/5 = leader + 4 peers)
    assert!(
        result.commit_quorum_achieved,
        "five-node cluster should achieve quorum with all peers responding"
    );
}
