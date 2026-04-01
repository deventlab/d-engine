//! Quorum calculation scenarios for ReplicationHandler
//!
//! Tests verify that prepare_batch_requests correctly builds requests for
//! quorum-relevant cluster topologies. Full quorum verification logic
//! (commit advancement, peer_updates, stale/higher-term handling) is
//! covered in leader_state_test/replication_test.rs via handle_append_result.

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
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeRole::Leader;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::NodeMeta;
use tokio::sync::watch;

/// Two-node cluster builds one replication request targeting the peer.
///
/// # Scenario
/// - Cluster: 1 leader + 1 voter (2 nodes total)
/// - Expected: 1 request built for peer 2 with correct term and leader_id
///
/// # Note
/// The quorum verification (commit advancement, peer_updates match_index=3,
/// next_index=4) is tested in replication_test::test_handle_append_result_two_node_quorum_achieved.
#[tokio::test]
async fn test_two_node_cluster_builds_one_peer_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_two_node_cluster_builds_one_peer_request",
        graceful_rx,
        None,
    );
    let my_id = 1u32;
    let peer2_id = 2u32;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 1);
    raft_log.expect_first_entry_id().returning(|| 1);
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
                single_voter: false,
                replication_targets: vec![NodeMeta {
                    id: peer2_id,
                    address: "http://127.0.0.1:55001".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                }],
                total_voters: 2,
            },
            &context,
        )
        .await
        .unwrap();

    assert_eq!(
        result.append_requests.len(),
        1,
        "two-node cluster builds exactly 1 replication request"
    );
    assert_eq!(
        result.append_requests[0].0, peer2_id,
        "request targets peer 2"
    );
    assert_eq!(
        result.append_requests[0].1.term, 1,
        "request carries current term"
    );
    assert_eq!(
        result.append_requests[0].1.leader_id, my_id,
        "request identifies correct leader"
    );
}
