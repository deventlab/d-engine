use d_engine_core::MockStateMachine;
use tokio::sync::oneshot;
use tracing_test::traced_test;

use super::RaftMembership;
use d_engine_core::ConnectionType;
use d_engine_core::ConsensusError;
use d_engine_core::Error;

use crate::node::RaftTypeConfig;
use d_engine_core::Membership;
use d_engine_core::MembershipError;
use d_engine_core::MockStorageEngine;
use d_engine_core::MockTypeConfig;
use d_engine_core::RaftNodeConfig;
use d_engine_core::ensure_safe_join;
use d_engine_core::test_utils::MockNode;
use d_engine_core::test_utils::MockRpcService;
use d_engine_proto::common::AddNode;
use d_engine_proto::common::BatchRemove;
use d_engine_proto::common::MembershipChange;
use d_engine_proto::common::NodeRole::Candidate;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeRole::Leader;
use d_engine_proto::common::NodeRole::Learner;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::common::PromoteLearner;
use d_engine_proto::common::RemoveNode;
use d_engine_proto::common::membership_change::Change;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::cluster::cluster_conf_update_response::ErrorCode;

// Helper function to create a test instance
pub fn create_test_membership()
-> RaftMembership<RaftTypeConfig<MockStorageEngine, MockStateMachine>> {
    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            address: "127.0.0.1:10000".to_string(),
            role: Leader as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 2,
            address: "127.0.0.1:10000".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10000".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:10000".to_string(),
            role: Learner as i32,
            status: NodeStatus::Promotable.into(),
        },
        NodeMeta {
            id: 5,
            address: "127.0.0.1:10000".to_string(),
            role: Learner as i32,
            status: NodeStatus::Promotable.into(),
        },
    ];
    RaftMembership::<RaftTypeConfig<MockStorageEngine, MockStateMachine>>::new(
        1,
        initial_cluster,
        RaftNodeConfig::default(),
    )
}

#[tokio::test]
#[traced_test]
async fn test_update_single_node_case1() {
    let membership = create_test_membership();

    // Test updating an existing node
    let node_id = 4; //Learner 4
    let new_status = NodeStatus::Promotable;
    let result = membership
        .update_single_node(node_id, |node| {
            node.status = new_status as i32;
            Ok(())
        })
        .await;

    assert!(result.is_ok(), "Failed to update single node");

    // Verify node was updated
    let node_status = membership.get_node_status(node_id).await.unwrap();
    assert_eq!(
        node_status,
        NodeStatus::Promotable,
        "Node status should be updated"
    );
}

#[tokio::test]
#[traced_test]
async fn test_update_single_node_case2() {
    let membership = create_test_membership();

    // Test updating a non-existing node
    let node_id = 10; //none existent node
    let new_status = NodeStatus::Promotable;
    let result = membership
        .update_single_node(node_id, |node| {
            node.status = new_status as i32;
            Ok(())
        })
        .await;

    assert!(result.is_err(), "Should return error");

    // Verify node was created
    assert!(membership.get_node_status(node_id).await.is_none());
}

#[tokio::test]
#[traced_test]
async fn test_update_multiple_nodes_case1() {
    let membership = create_test_membership();
    let nodes = vec![4, 5];

    let new_status = NodeStatus::Promotable;
    let result = membership
        .update_multiple_nodes(&nodes, |node| {
            node.status = new_status as i32;
            Ok(())
        })
        .await;
    assert!(result.is_ok(), "Failed to update multiple nodes");

    // Verify all nodes were updated
    for node_id in nodes {
        let node_status = membership.get_node_status(node_id).await.unwrap();
        assert_eq!(
            node_status,
            NodeStatus::Promotable,
            "All nodes should be Promotable"
        );
    }
}

#[tokio::test]
#[traced_test]
async fn test_update_multiple_nodes_case2() {
    let membership = create_test_membership();
    let nodes = vec![4, 13];

    let new_status = NodeStatus::Promotable;
    let result = membership
        .update_multiple_nodes(&nodes, |node| {
            node.status = new_status as i32;
            Ok(())
        })
        .await;
    assert!(result.is_ok(), "Failed to update mixed nodes");

    // Verify existing node was updated
    let node4_status = membership.get_node_status(4).await.unwrap();
    assert_eq!(
        node4_status,
        NodeStatus::Promotable,
        "Existing node should be updated"
    );

    // Verify new nodes were created
    assert!(!membership.contains_node(13).await, "New node should exist");
}

#[tokio::test]
#[traced_test]
async fn test_replication_peers_case1() {
    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            address: "127.0.0.1:10000".to_string(),
            role: Leader as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 2,
            address: "127.0.0.1:10000".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10000".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:10000".to_string(),
            role: Learner as i32,
            status: NodeStatus::Promotable.into(),
        },
        NodeMeta {
            id: 5,
            address: "127.0.0.1:10000".to_string(),
            role: Learner as i32,
            status: NodeStatus::Promotable.into(),
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig<MockStorageEngine, MockStateMachine>>::new(
        1,
        initial_cluster,
        RaftNodeConfig::default(),
    );
    assert_eq!(membership.replication_peers().await.len(), 3);
}

/// Test: remove_node allows leader removal (Raft protocol compliance)
///
/// Per Raft protocol and industry best practices:
/// - Leader CAN propose to remove itself
/// - remove_node() should NOT block leader removal
/// - Leader will step down after applying the config change
///
/// Related: Issue #200
#[tokio::test]
#[traced_test]
async fn test_remove_node_allows_leader_removal() {
    // Setup 3-node cluster with node 1 as leader
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            address: "127.0.0.1:10001".to_string(),
            role: Leader as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 2,
            address: "127.0.0.1:10002".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10003".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        },
    ];

    let membership = RaftMembership::<RaftTypeConfig<MockStorageEngine, MockStateMachine>>::new(
        1,
        initial_cluster,
        RaftNodeConfig::default(),
    );

    // Verify node 1 exists
    assert!(membership.contains_node(1).await);

    // Remove leader node (should succeed per Raft protocol)
    let result = membership.remove_node(1).await;
    assert!(
        result.is_ok(),
        "remove_node should allow leader removal per Raft protocol"
    );

    // Verify node 1 is removed from membership
    assert!(!membership.contains_node(1).await);

    // Verify cluster still has 2 nodes
    let members = membership.members().await;
    assert_eq!(members.len(), 2);
    assert!(members.iter().all(|n| n.id != 1));
}

#[tokio::test]
#[traced_test]
async fn test_retrieve_cluster_membership_config() {
    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            address: "127.0.0.1:10000".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10000".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:10000".to_string(),
            role: Learner as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 5,
            address: "127.0.0.1:10000".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 6,
            address: "127.0.0.1:10000".to_string(),
            role: Learner as i32,
            status: NodeStatus::Active.into(),
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig<MockStorageEngine, MockStateMachine>>::new(
        1,
        initial_cluster,
        RaftNodeConfig::default(),
    );

    let r = membership.retrieve_cluster_membership_config(None).await;
    assert_eq!(r.nodes.len(), 5);
    assert!(!r.nodes.iter().any(|n| n.role == Leader as i32));
}

#[tokio::test]
#[traced_test]
async fn test_update_cluster_conf_from_leader_case1() {
    // Test AddNode operation
    let membership = RaftMembership::<MockTypeConfig>::new(1, vec![], RaftNodeConfig::default());
    let req = ClusterConfChangeRequest {
        id: 2,
        term: 1,
        version: 0,
        change: Some(MembershipChange {
            change: Some(Change::AddNode(AddNode {
                node_id: 3,
                address: "127.0.0.1:8080".to_string(),
                status: NodeStatus::Promotable as i32,
            })),
        }),
    };

    assert!(
        membership
            .update_cluster_conf_from_leader(
                1,       // my_id
                1,       // my_current_term
                0,       // current_conf_version
                Some(2), // current_leader_id
                &req,
            )
            .await
            .is_ok()
    );
    assert!(membership.contains_node(3).await);
    assert_eq!(membership.get_cluster_conf_version().await, 0); // Version from request
}

#[tokio::test]
#[traced_test]
async fn test_update_cluster_conf_from_leader_case2() {
    // Test RemoveNode operation
    let membership = RaftMembership::<MockTypeConfig>::new(
        1,
        vec![NodeMeta {
            id: 3,
            address: "127.0.0.1:8080".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        }],
        RaftNodeConfig::default(),
    );
    let req = ClusterConfChangeRequest {
        id: 2,
        term: 1,
        version: 1,
        change: Some(MembershipChange {
            change: Some(Change::RemoveNode(RemoveNode { node_id: 3 })),
        }),
    };

    assert!(
        membership
            .update_cluster_conf_from_leader(
                1,       // my_id
                1,       // my_current_term
                0,       // current_conf_version
                Some(2), // current_leader_id
                &req,
            )
            .await
            .is_ok()
    );
    assert!(!membership.contains_node(3).await);
    assert_eq!(membership.get_cluster_conf_version().await, 1);
}

#[tokio::test]
#[traced_test]
async fn test_update_cluster_conf_from_leader_case3() {
    // Test PromoteLearner operation
    let membership = RaftMembership::<MockTypeConfig>::new(
        1,
        vec![NodeMeta {
            id: 3,
            address: "127.0.0.1:8080".to_string(),
            role: Learner as i32,
            status: NodeStatus::Active.into(),
        }],
        RaftNodeConfig::default(),
    );
    let req = ClusterConfChangeRequest {
        id: 2,
        term: 1,
        version: 1,
        change: Some(MembershipChange {
            change: Some(Change::Promote(PromoteLearner {
                node_id: 3,
                status: NodeStatus::Promotable.into(),
            })),
        }),
    };

    let response = membership
        .update_cluster_conf_from_leader(
            1,       // my_id
            1,       // my_current_term
            0,       // current_conf_version
            Some(2), // current_leader_id
            &req,
        )
        .await
        .expect("should succeed");

    assert!(response.success);
    assert_eq!(
        membership.get_role_by_node_id(3).await.unwrap(),
        Follower as i32
    );
    assert_eq!(membership.get_cluster_conf_version().await, 1);
}

#[tokio::test]
#[traced_test]
async fn test_update_cluster_conf_from_leader_case4_conf_invalid_promotion() {
    // Try to promote non-learner node
    let membership = RaftMembership::<MockTypeConfig>::new(
        1,
        vec![NodeMeta {
            id: 3,
            address: "127.0.0.1:8080".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        }],
        RaftNodeConfig::default(),
    );
    let req = ClusterConfChangeRequest {
        id: 2,
        term: 1,
        version: 1,
        change: Some(MembershipChange {
            change: Some(Change::Promote(PromoteLearner {
                node_id: 3,
                status: NodeStatus::Promotable.into(),
            })),
        }),
    };

    let result = membership
        .update_cluster_conf_from_leader(
            1,       // my_id
            1,       // my_current_term
            0,       // current_conf_version
            Some(2), // current_leader_id
            &req,
        )
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::Consensus(ConsensusError::Membership(
            MembershipError::InvalidPromotion { .. }
        ))
    ));
}

#[tokio::test]
#[traced_test]
async fn test_update_cluster_conf_from_leader_case5_conf_missing_change() {
    // Test missing change type
    let membership = RaftMembership::<MockTypeConfig>::new(1, vec![], RaftNodeConfig::default());
    let req = ClusterConfChangeRequest {
        id: 2,
        term: 1,
        version: 0,
        change: None,
    };

    let result = membership
        .update_cluster_conf_from_leader(
            1,       // my_id
            1,       // my_current_term
            0,       // current_conf_version
            Some(2), // current_leader_id
            &req,
        )
        .await;

    println!("Result: {:?}", &result);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::Consensus(ConsensusError::Membership(
            MembershipError::InvalidChangeRequest
        ))
    ));
}

#[tokio::test]
#[traced_test]
async fn test_update_cluster_conf_from_leader_case6_conf_version_mismatch() {
    // Test version mismatch
    let membership = RaftMembership::<MockTypeConfig>::new(1, vec![], RaftNodeConfig::default());
    membership.update_conf_version(5).await; // Set current version to 5

    let req = ClusterConfChangeRequest {
        id: 2,
        term: 1,
        version: 4, // Older version
        change: Some(MembershipChange {
            change: Some(Change::AddNode(AddNode {
                node_id: 3,
                address: "127.0.0.1:8080".to_string(),
                status: NodeStatus::Promotable as i32,
            })),
        }),
    };

    let response = membership
        .update_cluster_conf_from_leader(
            1,       // my_id
            1,       // my_current_term
            5,       // current_conf_version
            Some(2), // current_leader_id
            &req,
        )
        .await
        .expect("should return response");

    assert!(!response.success);
    assert_eq!(response.error_code, ErrorCode::VersionConflict as i32);
}

#[tokio::test]
#[traced_test]
async fn test_batch_remove_nodes() {
    // Setup cluster with multiple nodes
    let initial_nodes = vec![
        NodeMeta {
            id: 2,
            address: "127.0.0.1:10001".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active as i32,
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10002".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active as i32,
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:10003".to_string(),
            role: Learner as i32,
            status: NodeStatus::Promotable as i32,
        },
        NodeMeta {
            id: 5,
            address: "127.0.0.1:10004".to_string(),
            role: Learner as i32,
            status: NodeStatus::Promotable as i32,
        },
    ];

    let membership =
        RaftMembership::<MockTypeConfig>::new(1, initial_nodes, RaftNodeConfig::default());
    membership.update_conf_version(1).await;

    // Create batch removal request
    let req = ClusterConfChangeRequest {
        id: 1, // current leader
        term: 1,
        version: 1,
        change: Some(MembershipChange {
            change: Some(Change::BatchRemove(BatchRemove {
                node_ids: vec![3, 4], // Remove follower and learner
            })),
        }),
    };

    // Execute batch removal
    let response = membership
        .update_cluster_conf_from_leader(
            1,       // current node
            1,       // current term
            1,       // current conf version
            Some(1), // current leader
            &req,
        )
        .await
        .expect("Batch removal should succeed");

    // Verify results
    assert!(response.success);
    assert!(!membership.contains_node(3).await);
    assert!(!membership.contains_node(4).await);
    assert!(membership.contains_node(2).await);
    assert!(membership.contains_node(5).await);
    assert_eq!(membership.get_cluster_conf_version().await, 1);
}

#[tokio::test]
#[traced_test]
async fn test_batch_remove_leader_protection() {
    // Setup cluster with leader
    let initial_nodes = vec![
        NodeMeta {
            id: 1,
            address: "127.0.0.1:10000".to_string(),
            role: Leader as i32,
            status: NodeStatus::Active as i32,
        },
        NodeMeta {
            id: 2,
            address: "127.0.0.1:10001".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active as i32,
        },
    ];

    let membership =
        RaftMembership::<MockTypeConfig>::new(1, initial_nodes, RaftNodeConfig::default());

    // Attempt to remove leader in batch
    let req = ClusterConfChangeRequest {
        id: 1,
        term: 1,
        version: 1,
        change: Some(MembershipChange {
            change: Some(Change::BatchRemove(BatchRemove {
                node_ids: vec![1, 2], // Includes leader
            })),
        }),
    };

    let result = membership.update_cluster_conf_from_leader(1, 1, 0, Some(1), &req).await;

    // Should fail with leader protection error
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::Consensus(ConsensusError::Membership(
            MembershipError::RemoveNodeIsLeader(1)
        ))
    ));
}

#[tokio::test]
#[traced_test]
async fn test_apply_batch_remove_config_change() {
    let membership = RaftMembership::<MockTypeConfig>::new(
        1,
        vec![
            NodeMeta {
                id: 2,
                address: "127.0.0.1:10001".to_string(),
                role: Follower as i32,
                status: NodeStatus::Active as i32,
            },
            NodeMeta {
                id: 3,
                address: "127.0.0.1:10002".to_string(),
                role: Learner as i32,
                status: NodeStatus::Promotable as i32,
            },
            NodeMeta {
                id: 4,
                address: "127.0.0.1:10003".to_string(),
                role: Follower as i32,
                status: NodeStatus::Active as i32,
            },
        ],
        RaftNodeConfig::default(),
    );

    let change = MembershipChange {
        change: Some(Change::BatchRemove(BatchRemove {
            node_ids: vec![2, 3],
        })),
    };

    // Apply config change
    membership
        .apply_config_change(change)
        .await
        .expect("Batch remove should succeed");

    // Verify nodes removed and version updated
    assert!(!membership.contains_node(2).await);
    assert!(!membership.contains_node(3).await);
    assert!(membership.contains_node(4).await);
    assert_eq!(membership.get_cluster_conf_version().await, 1);
}

// Add to existing test series
#[tokio::test]
#[traced_test]
async fn test_update_cluster_conf_from_leader_case7_batch_remove() {
    // Setup cluster
    let initial_nodes = vec![
        NodeMeta {
            id: 2,
            address: "127.0.0.1:10001".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active as i32,
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10002".to_string(),
            role: Learner as i32,
            status: NodeStatus::Promotable as i32,
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:10003".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active as i32,
        },
    ];
    let membership =
        RaftMembership::<MockTypeConfig>::new(1, initial_nodes, RaftNodeConfig::default());
    membership.update_conf_version(1).await;

    // Batch removal request
    let req = ClusterConfChangeRequest {
        id: 1,
        term: 1,
        version: 1,
        change: Some(MembershipChange {
            change: Some(Change::BatchRemove(BatchRemove {
                node_ids: vec![3, 4], // Remove learner and follower
            })),
        }),
    };

    let response = membership
        .update_cluster_conf_from_leader(1, 1, 1, Some(1), &req)
        .await
        .expect("Batch remove should succeed");

    assert!(response.success);
    assert!(!membership.contains_node(3).await);
    assert!(!membership.contains_node(4).await);
    assert!(membership.contains_node(2).await);
    assert_eq!(membership.get_cluster_conf_version().await, 1);
}

#[tokio::test]
#[traced_test]
async fn test_update_cluster_conf_from_leader_case8_batch_remove_nonexistent() {
    // Tests graceful handling of non-existent nodes in batch
    let membership = RaftMembership::<MockTypeConfig>::new(1, vec![], RaftNodeConfig::default());

    let req = ClusterConfChangeRequest {
        id: 1,
        term: 1,
        version: 0,
        change: Some(MembershipChange {
            change: Some(Change::BatchRemove(BatchRemove {
                node_ids: vec![99, 100], // Non-existent nodes
            })),
        }),
    };

    let response = membership
        .update_cluster_conf_from_leader(1, 1, 0, Some(1), &req)
        .await
        .expect("Should succeed with no-op");

    assert!(response.success);
    assert_eq!(membership.get_cluster_conf_version().await, 0);
}

/// This test covers:
/// Filtering for specific roles (Followers + Candidates)
/// Filtering for a single role (Leaders)
/// Empty result case
/// All-items-match case
#[tokio::test]
#[traced_test]
async fn test_get_peers_id_with_condition() {
    // Setup test data
    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            address: "127.0.0.1:10000".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10000".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:10000".to_string(),
            role: Learner as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 5,
            address: "127.0.0.1:10000".to_string(),
            role: Candidate as i32,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 6,
            address: "127.0.0.1:10000".to_string(),
            role: Leader as i32,
            status: NodeStatus::Active.into(),
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig<MockStorageEngine, MockStateMachine>>::new(
        1,
        initial_cluster,
        RaftNodeConfig::default(),
    );

    // Test 1: Filter followers and candidates
    let mut result = membership
        .get_peers_id_with_condition(|role| role == Follower as i32 || role == Candidate as i32)
        .await;
    result.sort_unstable();
    let mut expect = vec![1, 3, 5];
    expect.sort_unstable();
    assert_eq!(result, expect, "Should return follower and candidate IDs");

    // Test 2: Filter leaders only
    let result = membership.get_peers_id_with_condition(|role| role == Leader as i32).await;
    assert_eq!(result, vec![6], "Should return leader ID only");

    // Test 3: Empty result case
    let result = membership.get_peers_id_with_condition(|_| false).await;
    assert!(
        result.is_empty(),
        "Should return empty vector when no matches"
    );
    let mut expect = vec![1, 3, 4, 5, 6];
    expect.sort_unstable();

    // Test 4: All items match
    let mut result = membership.get_peers_id_with_condition(|_| true).await;
    result.sort_unstable();
    assert_eq!(
        result, expect,
        "Should return all IDs when condition is always true"
    );
}

#[cfg(test)]
mod check_cluster_is_ready_test {
    use tokio::sync::oneshot;

    use super::*;
    use d_engine_core::MockStateMachine;
    use d_engine_core::MockStorageEngine;
    use d_engine_core::test_utils::MockNode;
    use d_engine_core::test_utils::MockRpcService;

    /// Case 1: Test all peers are healthy
    #[tokio::test]
    async fn test_check_cluster_is_ready_case1() {
        let peer_ids = vec![2, 3];
        let mut mock_services = Vec::new();

        let mut config = RaftNodeConfig::default();
        config.retry.membership.max_retries = 3;
        config.raft.membership.cluster_healthcheck_probe_service_name =
            "d_engine.server.cluster.ClusterManagementService".to_string();

        // Create membership with 2 peers
        let membership = RaftMembership::<RaftTypeConfig<MockStorageEngine, MockStateMachine>>::new(
            1,
            peer_ids
                .iter()
                .map(|id| NodeMeta {
                    id: *id,
                    address: "127.0.0.1:0".to_string(),
                    role: Follower as i32,
                    status: NodeStatus::Active.into(),
                })
                .collect(),
            config,
        );

        // Create mock services for peers
        for &id in &peer_ids {
            let (tx, rx) = oneshot::channel::<()>();
            let service = MockRpcService::default();
            let (port, addr) = MockNode::mock_listener(service, rx, true).await.unwrap();
            membership.update_node_address(id, format!("127.0.0.1:{port}")).await.unwrap();
            println!("{:?}---", &addr);
            mock_services.push((tx, addr));
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify cluster health
        let result = membership.check_cluster_is_ready().await;
        assert!(result.is_ok(), "Should return OK for healthy cluster");

        // Cleanup
        for (tx, _) in mock_services {
            let _ = tx.send(());
        }
    }

    /// Case 2: Test failed connection to peers
    #[tokio::test]
    async fn test_check_cluster_is_ready_case2() {
        // Create membership with unreachable peer
        let mut config = RaftNodeConfig::default();
        config.retry.membership.max_retries = 1;

        let membership = RaftMembership::<RaftTypeConfig<MockStorageEngine, MockStateMachine>>::new(
            1,
            vec![NodeMeta {
                id: 2,
                address: "127.0.0.1:9999".to_string(), // Invalid port
                role: Follower as i32,
                status: NodeStatus::Active.into(),
            }],
            config,
        );

        let result = membership.check_cluster_is_ready().await;
        assert!(result.is_err(), "Should return error for unreachable peer");
    }

    /// Case 3: Test peer returns unhealthy status
    #[tokio::test]
    async fn test_check_cluster_is_ready_case3() {
        let (tx, rx) = oneshot::channel::<()>();
        let service = MockRpcService::default();
        let (port, _addr) = MockNode::mock_listener(service, rx, false).await.unwrap();

        let mut config = RaftNodeConfig::default();
        config.retry.membership.max_retries = 1;
        let membership = RaftMembership::<RaftTypeConfig<MockStorageEngine, MockStateMachine>>::new(
            1,
            vec![NodeMeta {
                id: 4,
                address: format!("127.0.0.1:{port}",),
                role: Follower as i32,
                status: NodeStatus::Active.into(),
            }],
            config,
        );

        let result = membership.check_cluster_is_ready().await;
        println!("Result: {:?}", &result);
        assert!(result.is_err(), "Should return error for unhealthy peer");

        let _ = tx.send(());
    }

    /// Case 4: Test mixed status responses
    #[tokio::test]
    async fn test_check_cluster_is_ready_case4() {
        let mut mock_services = Vec::new();
        let peer_ids = [5, 6];

        // Create membership with 2 peers
        let mut config = RaftNodeConfig::default();
        config.retry.membership.max_retries = 1;
        let membership = RaftMembership::<RaftTypeConfig<MockStorageEngine, MockStateMachine>>::new(
            1,
            peer_ids
                .iter()
                .map(|id| NodeMeta {
                    id: *id,
                    address: "127.0.0.1:0".to_string(),
                    role: Follower as i32,
                    status: NodeStatus::Active.into(),
                })
                .collect(),
            config,
        );

        // Create mock services with different responses
        for id in peer_ids.iter() {
            let (tx, rx) = oneshot::channel::<()>();
            let service = MockRpcService::default();
            let (port, addr) = MockNode::mock_listener(service, rx, false).await.unwrap();
            membership.update_node_address(*id, format!("127.0.0.1:{port}")).await.unwrap();
            mock_services.push((tx, addr));
        }

        let result = membership.check_cluster_is_ready().await;
        assert!(result.is_err(), "Should return error for unhealthy peer");

        // Cleanup
        for (tx, _) in mock_services {
            let _ = tx.send(());
        }
    }
}

#[cfg(test)]
mod add_learner_test {
    use super::*;

    #[tokio::test]
    async fn test_add_learner_case1() {
        let membership = RaftMembership::<RaftTypeConfig<MockStorageEngine, MockStateMachine>>::new(
            1,
            vec![],
            RaftNodeConfig::default(),
        );

        let result = membership
            .add_learner(2, "127.0.0.1:1234".to_string(), NodeStatus::Promotable)
            .await;
        assert!(result.is_ok(), "Should add learner successfully");

        let replication_members = membership.members().await;
        assert_eq!(replication_members.len(), 1);
        assert_eq!(replication_members[0].id, 2);
        assert_eq!(replication_members[0].address, "127.0.0.1:1234");
        assert_eq!(replication_members[0].role, Learner as i32);
        assert_eq!(replication_members[0].status, NodeStatus::Promotable as i32);
    }

    #[tokio::test]
    async fn test_add_learner_case2() {
        let membership = RaftMembership::<RaftTypeConfig<MockStorageEngine, MockStateMachine>>::new(
            1,
            vec![NodeMeta {
                id: 1,
                address: "127.0.0.1:0".to_string(),
                role: Follower as i32,
                status: NodeStatus::Active.into(),
            }],
            RaftNodeConfig::default(),
        );

        let result = membership
            .add_learner(1, "127.0.0.1:1234".to_string(), NodeStatus::Promotable)
            .await;
        assert!(result.is_err(), "Node is follower");

        let replication_members = membership.members().await;
        assert_eq!(replication_members.len(), 1);
        assert_eq!(replication_members[0].id, 1);
        assert_eq!(replication_members[0].address, "127.0.0.1:0");
        assert_eq!(replication_members[0].role, Follower as i32);
        assert_eq!(replication_members[0].status, NodeStatus::Active as i32);
    }
}

#[tokio::test]
#[traced_test]
async fn test_ensure_safe_join() {
    assert!(ensure_safe_join(1, 0).is_ok());
    assert!(ensure_safe_join(1, 1).is_err());
    assert!(ensure_safe_join(1, 2).is_ok());
    assert!(ensure_safe_join(1, 3).is_err());
    assert!(ensure_safe_join(1, 4).is_ok());
    assert!(ensure_safe_join(1, 5).is_err());
}

#[tokio::test]
#[traced_test]
async fn test_health_monitoring_integration() {
    let mut config = RaftNodeConfig::default();
    config.raft.membership.zombie.threshold = 2; // Set low threshold for testing

    let membership = RaftMembership::<MockTypeConfig>::new(1, vec![], config);

    // Add test node
    membership
        .add_learner(100, "invalid.address".to_string(), NodeStatus::Promotable)
        .await
        .unwrap();

    // Test 1: Record connection failure
    let channel = membership.get_peer_channel(100, ConnectionType::Control).await;
    assert!(channel.is_none());
    assert_eq!(
        membership.health_monitor.failure_counts.get(&100).map(|c| *c),
        Some(1)
    );

    // Test 2: Record second failure (should become zombie candidate)
    membership.get_peer_channel(100, ConnectionType::Control).await;
    let zombies = membership.get_zombie_candidates().await;
    assert_eq!(zombies, vec![100]);

    // Test 3: Record success resets failures
    // Update to valid address (using mock would be better in real impl)
    let (_tx, rx) = oneshot::channel::<()>();
    let service = MockRpcService::default();
    let (port, _addr) = MockNode::mock_listener(service, rx, true).await.unwrap();
    membership.update_node_address(100, format!("127.0.0.1:{port}")).await.unwrap();
    membership.get_peer_channel(100, ConnectionType::Control).await; // Should "succeed"
    assert!(membership.health_monitor.failure_counts.get(&100).is_none());
}

#[cfg(test)]
mod pre_warm_connections_tests {
    use tracing_test::traced_test;

    use super::*;
    use crate::utils::net::address_str;

    use d_engine_proto::common::NodeStatus;
    use d_engine_proto::server::cluster::NodeMeta;

    #[derive(Clone, Copy)]
    pub enum AddressType {
        Success,
        Failed,
    }

    pub async fn create_test_membership(
        nodes: Vec<(u32, AddressType)>
    ) -> (
        RaftMembership<RaftTypeConfig<MockStorageEngine, MockStateMachine>>,
        Vec<oneshot::Sender<()>>,
    ) {
        let mut cluster = Vec::new();
        let mut shutdown_channels = Vec::new();
        for (id, addr_type) in nodes {
            let (address, shutdown_opt) = match addr_type {
                AddressType::Success => {
                    let (addr, tx) = mock_address().await;
                    (addr, Some(tx))
                }
                AddressType::Failed => {
                    // Deliberately invalid or unreachable address
                    ("127.0.0.1:9".to_string(), None)
                }
            };

            cluster.push(NodeMeta {
                id,
                address,
                role: Follower as i32,
                status: NodeStatus::Active.into(),
            });

            if let Some(tx) = shutdown_opt {
                shutdown_channels.push(tx);
            }
        }

        let membership = RaftMembership::<RaftTypeConfig<MockStorageEngine, MockStateMachine>>::new(
            1,
            cluster,
            RaftNodeConfig::default(),
        );
        (membership, shutdown_channels)
    }

    async fn mock_address() -> (String, oneshot::Sender<()>) {
        let (tx, rx) = oneshot::channel::<()>();
        let is_ready = true;
        let mock_service = MockRpcService::default();
        let (_port, addr) =
            d_engine_core::test_utils::MockNode::mock_listener(mock_service, rx, is_ready)
                .await
                .unwrap();

        (address_str(&addr.to_string()), tx)
    }

    #[tokio::test]
    #[traced_test]
    async fn pre_warm_connections_successful() {
        let (membership, _shutdown_tx) =
            create_test_membership(vec![(2, AddressType::Success), (3, AddressType::Success)])
                .await;

        // Execute pre-warm
        membership.pre_warm_connections().await.expect("Should succeed");

        // Verify logs
        assert!(logs_contain("Pre-warmed Control connection to node 2"));
        assert!(logs_contain("Pre-warmed Data connection to node 3"));
        assert!(logs_contain("Pre-warmed Bulk connection to node 2"));
        assert!(!logs_contain("Failed to pre-warm"));
    }

    #[tokio::test]
    #[traced_test]
    async fn pre_warm_connections_partial_failure() {
        let (membership, _shutdown_tx) = create_test_membership(vec![
            (2, AddressType::Success),
            (3, AddressType::Failed),
            (4, AddressType::Success),
        ])
        .await;

        // Execute pre-warm
        membership.pre_warm_connections().await.expect("Should succeed");

        // Verify success logs
        assert!(logs_contain("Pre-warmed Data connection to node 2"));
        assert!(logs_contain("Pre-warmed Bulk connection to node 4"));

        // Verify failure logs
        assert!(logs_contain(
            "Failed to pre-warm Control connection to node 3"
        ));
        assert!(logs_contain("Failed to pre-warm Data connection to node 3"));
        assert!(logs_contain("Failed to pre-warm Bulk connection to node 3"));

        // Verify warning summary
        assert!(logs_contain(
            "Connection pre-warming failed for one or more peers"
        ));
    }

    #[tokio::test]
    #[traced_test]
    async fn pre_warm_connections_no_peers() {
        let (membership, _shutdown_tx) = create_test_membership(vec![]).await;

        membership.pre_warm_connections().await.expect("Should succeed");
        assert!(logs_contain("No replication peers found"));
    }
}

// ================================================================================================
// Tests for Single-Node Cluster Support (Issue #179)
// ================================================================================================

#[cfg(test)]
mod single_node_tests {
    use super::*;

    /// Helper: Create single-node membership
    fn create_single_node_membership()
    -> RaftMembership<RaftTypeConfig<MockStorageEngine, MockStateMachine>> {
        let initial_cluster = vec![NodeMeta {
            id: 1,
            address: "127.0.0.1:9081".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active.into(),
        }];
        RaftMembership::new(1, initial_cluster, RaftNodeConfig::default())
    }

    /// Helper: Create 3-node membership
    fn create_three_node_membership()
    -> RaftMembership<RaftTypeConfig<MockStorageEngine, MockStateMachine>> {
        let initial_cluster = vec![
            NodeMeta {
                id: 1,
                address: "127.0.0.1:9081".to_string(),
                role: Leader as i32,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 2,
                address: "127.0.0.1:9082".to_string(),
                role: Follower as i32,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                address: "127.0.0.1:9083".to_string(),
                role: Follower as i32,
                status: NodeStatus::Active.into(),
            },
        ];
        RaftMembership::new(1, initial_cluster, RaftNodeConfig::default())
    }

    /// Helper: Create 5-node membership
    fn create_five_node_membership()
    -> RaftMembership<RaftTypeConfig<MockStorageEngine, MockStateMachine>> {
        let initial_cluster = vec![
            NodeMeta {
                id: 1,
                address: "127.0.0.1:9081".to_string(),
                role: Leader as i32,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 2,
                address: "127.0.0.1:9082".to_string(),
                role: Follower as i32,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                address: "127.0.0.1:9083".to_string(),
                role: Follower as i32,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 4,
                address: "127.0.0.1:9084".to_string(),
                role: Follower as i32,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 5,
                address: "127.0.0.1:9085".to_string(),
                role: Follower as i32,
                status: NodeStatus::Active.into(),
            },
        ];
        RaftMembership::new(1, initial_cluster, RaftNodeConfig::default())
    }

    #[tokio::test]
    async fn test_initial_cluster_size_single_node() {
        let membership = create_single_node_membership();

        let size = membership.initial_cluster_size().await;

        assert_eq!(size, 1, "Single-node cluster should report size 1");
    }

    #[tokio::test]
    async fn test_initial_cluster_size_three_nodes() {
        let membership = create_three_node_membership();

        let size = membership.initial_cluster_size().await;

        assert_eq!(size, 3, "Three-node cluster should report size 3");
    }

    #[tokio::test]
    async fn test_initial_cluster_size_five_nodes() {
        let membership = create_five_node_membership();

        let size = membership.initial_cluster_size().await;

        assert_eq!(size, 5, "Five-node cluster should report size 5");
    }

    #[tokio::test]
    async fn test_is_single_node_cluster_true() {
        let membership = create_single_node_membership();

        let is_single = membership.is_single_node_cluster().await;

        assert!(is_single, "Single-node cluster should return true");
    }

    #[tokio::test]
    async fn test_is_single_node_cluster_false_three_nodes() {
        let membership = create_three_node_membership();

        let is_single = membership.is_single_node_cluster().await;

        assert!(!is_single, "Three-node cluster should return false");
    }

    #[tokio::test]
    async fn test_is_single_node_cluster_false_five_nodes() {
        let membership = create_five_node_membership();

        let is_single = membership.is_single_node_cluster().await;

        assert!(!is_single, "Five-node cluster should return false");
    }

    #[tokio::test]
    async fn test_initial_cluster_size_immutable_after_add_learner() {
        let membership = create_three_node_membership();

        // Initial size should be 3
        assert_eq!(membership.initial_cluster_size().await, 3);

        // Add a learner (simulate cluster expansion)
        membership
            .add_learner(4, "127.0.0.1:9084".to_string(), NodeStatus::Promotable)
            .await
            .expect("Should succeed");

        // Initial cluster size should still be 3 (immutable)
        assert_eq!(
            membership.initial_cluster_size().await,
            3,
            "initial_cluster_size should remain unchanged after adding learner"
        );

        // Verify the learner was actually added
        assert!(membership.contains_node(4).await, "Node 4 should exist");
    }

    #[tokio::test]
    async fn test_initial_cluster_size_immutable_after_remove_node() {
        let membership = create_three_node_membership();

        // Initial size should be 3
        assert_eq!(membership.initial_cluster_size().await, 3);

        // Remove node 3 (simulate node failure)
        membership.force_remove_node(3).await.expect("Should succeed");

        // Initial cluster size should still be 3 (immutable)
        assert_eq!(
            membership.initial_cluster_size().await,
            3,
            "initial_cluster_size should remain unchanged after removing node"
        );

        // Verify node was actually removed
        assert!(
            !membership.contains_node(3).await,
            "Node 3 should be removed"
        );
    }

    #[tokio::test]
    async fn test_voters_empty_in_single_node_but_cluster_size_correct() {
        let membership = create_single_node_membership();

        // voters() should return empty (excludes self)
        let voters = membership.voters().await;
        assert!(
            voters.is_empty(),
            "voters() should be empty for single-node (excludes self)"
        );

        // But initial_cluster_size() should still be 1
        assert_eq!(
            membership.initial_cluster_size().await,
            1,
            "initial_cluster_size should be 1 even when voters() is empty"
        );

        // is_single_node_cluster() should be true
        assert!(
            membership.is_single_node_cluster().await,
            "is_single_node_cluster should be true"
        );
    }

    #[tokio::test]
    async fn test_network_partition_does_not_affect_cluster_size() {
        let membership = create_three_node_membership();

        // Simulate network partition by removing all other nodes
        membership.force_remove_node(2).await.expect("Should succeed");
        membership.force_remove_node(3).await.expect("Should succeed");

        // voters() should be empty now (simulating partition)
        let voters = membership.voters().await;
        assert!(
            voters.is_empty(),
            "voters() should be empty after partition"
        );

        // But initial_cluster_size should still be 3
        assert_eq!(
            membership.initial_cluster_size().await,
            3,
            "initial_cluster_size should remain 3 even in network partition"
        );

        // is_single_node_cluster() should still return false
        assert!(
            !membership.is_single_node_cluster().await,
            "is_single_node_cluster should return false (was 3-node cluster)"
        );
    }
}
