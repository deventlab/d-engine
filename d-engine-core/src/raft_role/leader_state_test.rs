use crate::MockMembership;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::node_config;
use d_engine_proto::server::cluster::NodeMeta;
use std::sync::Arc;

/// Test: init_cluster_metadata single-node  
#[tokio::test]
async fn test_init_cluster_metadata_single_node() {
    let config = Arc::new(node_config("/tmp/test_init_single"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, config);
    
    let mut membership = MockMembership::new();
    membership.expect_is_single_node_cluster().returning(|| true);
    membership.expect_voters().returning(Vec::new);
    
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();
    
    assert!(state.cluster_metadata.is_single_node);
    assert_eq!(state.cluster_metadata.total_voters, 1);
}

/// CRITICAL: Test update_cluster_metadata updates BOTH fields (would catch rejoin bug!)
#[tokio::test]
async fn test_update_cluster_metadata_from_single_to_multi() {
    let config = Arc::new(node_config("/tmp/test_update"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, config);
    
    // Init as single-node
    let mut m1 = MockMembership::new();
    m1.expect_is_single_node_cluster().returning(|| true);
    m1.expect_voters().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(m1)).await.unwrap();
    assert!(state.cluster_metadata.is_single_node);
    
    // Update to 3-node (simulate rejoin scenario)
    let mut m2 = MockMembership::new();
    m2.expect_is_single_node_cluster().returning(|| false);
    m2.expect_voters().returning(|| vec![
        NodeMeta { id: 2, address: "127.0.0.1:9082".into(), role: 0, status: 1 },
        NodeMeta { id: 3, address: "127.0.0.1:9083".into(), role: 0, status: 1 },
    ]);
    state.update_cluster_metadata(&Arc::new(m2)).await.unwrap();
    
    // CRITICAL: Both fields MUST update!
    assert!(!state.cluster_metadata.is_single_node, "BUG: is_single_node not updated!");
    assert_eq!(state.cluster_metadata.total_voters, 3, "BUG: total_voters not updated!");
}
