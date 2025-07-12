use super::RaftMembership;
use crate::ensure_safe_join;
use crate::proto::cluster::cluster_conf_update_response::ErrorCode;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::NodeMeta;
use crate::proto::common::membership_change::Change;
use crate::proto::common::AddNode;
use crate::proto::common::MembershipChange;
use crate::proto::common::NodeStatus;
use crate::proto::common::PromoteLearner;
use crate::proto::common::RemoveNode;
use crate::test_utils::MockTypeConfig;
use crate::ConsensusError;
use crate::Error;
use crate::Membership;
use crate::MembershipError;
use crate::RaftNodeConfig;
use crate::RaftTypeConfig;
use crate::CANDIDATE;
use crate::FOLLOWER;
use crate::LEADER;
use crate::LEARNER;

// Helper function to create a test instance
pub fn create_test_membership() -> RaftMembership<RaftTypeConfig> {
    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            address: "127.0.0.1:10000".to_string(),
            role: LEADER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 2,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:10000".to_string(),
            role: LEARNER,
            status: NodeStatus::Joining.into(),
        },
        NodeMeta {
            id: 5,
            address: "127.0.0.1:10000".to_string(),
            role: LEARNER,
            status: NodeStatus::Joining.into(),
        },
    ];
    RaftMembership::<RaftTypeConfig>::new(1, initial_cluster, RaftNodeConfig::default())
}

#[test]
fn test_update_single_node_case1() {
    let membership = create_test_membership();

    // Test updating an existing node
    let node_id = 4; //Learner 4
    let new_status = NodeStatus::PendingActive;
    let result = membership.update_single_node(node_id, |node| {
        node.status = new_status as i32;
        Ok(())
    });

    assert!(result.is_ok(), "Failed to update single node");

    // Verify node was updated
    let node_status = membership.get_node_status(node_id).unwrap();
    assert_eq!(node_status, NodeStatus::PendingActive, "Node status should be updated");
}

#[test]
fn test_update_single_node_case2() {
    let membership = create_test_membership();

    // Test updating a non-existing node
    let node_id = 10; //none existent node
    let new_status = NodeStatus::PendingActive;
    let result = membership.update_single_node(node_id, |node| {
        node.status = new_status as i32;
        Ok(())
    });

    assert!(result.is_err(), "Should return error");

    // Verify node was created
    assert!(membership.get_node_status(node_id).is_none());
}

#[test]
fn test_update_multiple_nodes_case1() {
    let membership = create_test_membership();
    let nodes = vec![4, 5];

    let new_status = NodeStatus::PendingActive;
    let result = membership.update_multiple_nodes(&nodes, |node| {
        node.status = new_status as i32;
        Ok(())
    });
    assert!(result.is_ok(), "Failed to update multiple nodes");

    // Verify all nodes were updated
    for node_id in nodes {
        let node_status = membership.get_node_status(node_id).unwrap();
        assert_eq!(
            node_status,
            NodeStatus::PendingActive,
            "All nodes should be PendingActive"
        );
    }
}

#[test]
fn test_update_multiple_nodes_case2() {
    let membership = create_test_membership();
    let nodes = vec![4, 13];

    let new_status = NodeStatus::PendingActive;
    let result = membership.update_multiple_nodes(&nodes, |node| {
        node.status = new_status as i32;
        Ok(())
    });
    assert!(result.is_ok(), "Failed to update mixed nodes");

    // Verify existing node was updated
    let node4_status = membership.get_node_status(4).unwrap();
    assert_eq!(
        node4_status,
        NodeStatus::PendingActive,
        "Existing node should be updated"
    );

    // Verify new nodes were created
    assert!(!membership.contains_node(13), "New node should exist");
}

#[test]
fn test_replication_peers_case1() {
    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            address: "127.0.0.1:10000".to_string(),
            role: LEADER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 2,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:10000".to_string(),
            role: LEARNER,
            status: NodeStatus::PendingActive.into(),
        },
        NodeMeta {
            id: 5,
            address: "127.0.0.1:10000".to_string(),
            role: LEARNER,
            status: NodeStatus::Joining.into(),
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster, RaftNodeConfig::default());
    assert_eq!(membership.replication_peers().len(), 3);
}

/// # Case 1: Test old leader id been cleaned up
///
/// ## Setup
/// 1. There is leader configured the membership
/// 2. Try to mark a new leader
///
/// ## Validation criteria
/// 1. new leader is marked as only Leader in membership
#[tokio::test]
async fn test_mark_leader_id_case1() {
    let old_leader_id = 10;
    let new_leader_id = 3;

    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: old_leader_id,
            address: "127.0.0.1:10000".to_string(),
            role: LEADER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:10000".to_string(),
            role: LEARNER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 5,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 6,
            address: "127.0.0.1:10000".to_string(),
            role: LEARNER,
            status: NodeStatus::Active.into(),
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster, RaftNodeConfig::default());

    assert_eq!(membership.current_leader_id(), Some(old_leader_id));
    assert!(membership.mark_leader_id(new_leader_id).is_ok());
    assert_eq!(membership.current_leader_id(), Some(new_leader_id));
}

/// # Case 2: Try to mark an none exist peer as leader will throw Error
///
/// ## Setup
/// 1. Try to mark a none exist member as leader
///
/// ## Validation criteria
/// 1. mark_leader_id returns Error
#[tokio::test]
async fn test_mark_leader_id_case2() {
    let new_leader_id = 100;

    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:10000".to_string(),
            role: LEARNER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 5,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 6,
            address: "127.0.0.1:10000".to_string(),
            role: LEARNER,
            status: NodeStatus::Active.into(),
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster, RaftNodeConfig::default());

    assert!(membership.mark_leader_id(new_leader_id).is_err());
    assert_eq!(membership.current_leader_id(), None);
}

#[test]
fn test_retrieve_cluster_membership_config() {
    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:10000".to_string(),
            role: LEARNER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 5,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 6,
            address: "127.0.0.1:10000".to_string(),
            role: LEARNER,
            status: NodeStatus::Active.into(),
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster, RaftNodeConfig::default());

    let r = membership.retrieve_cluster_membership_config();
    assert_eq!(r.nodes.len(), 5);
    assert!(!r.nodes.iter().any(|n| n.role == LEADER));
}

#[tokio::test]
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
            })),
        }),
    };

    assert!(membership
        .update_cluster_conf_from_leader(
            1,       // my_id
            1,       // my_current_term
            0,       // current_conf_version
            Some(2), // current_leader_id
            &req,
        )
        .await
        .is_ok());
    assert!(membership.contains_node(3));
    assert_eq!(membership.get_cluster_conf_version(), 0); // Version from request
}

#[tokio::test]
async fn test_update_cluster_conf_from_leader_case2() {
    // Test RemoveNode operation
    let membership = RaftMembership::<MockTypeConfig>::new(
        1,
        vec![NodeMeta {
            id: 3,
            address: "127.0.0.1:8080".to_string(),
            role: FOLLOWER,
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

    assert!(membership
        .update_cluster_conf_from_leader(
            1,       // my_id
            1,       // my_current_term
            0,       // current_conf_version
            Some(2), // current_leader_id
            &req,
        )
        .await
        .is_ok());
    assert!(!membership.contains_node(3));
    assert_eq!(membership.get_cluster_conf_version(), 1);
}

#[tokio::test]
async fn test_update_cluster_conf_from_leader_case3() {
    // Test PromoteLearner operation
    let membership = RaftMembership::<MockTypeConfig>::new(
        1,
        vec![NodeMeta {
            id: 3,
            address: "127.0.0.1:8080".to_string(),
            role: LEARNER,
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
                status: NodeStatus::PendingActive.into(),
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
    assert_eq!(membership.get_role_by_node_id(3).unwrap(), FOLLOWER);
    assert_eq!(membership.get_cluster_conf_version(), 1);
}

#[tokio::test]
async fn test_update_cluster_conf_from_leader_case4_conf_invalid_promotion() {
    // Try to promote non-learner node
    let membership = RaftMembership::<MockTypeConfig>::new(
        1,
        vec![NodeMeta {
            id: 3,
            address: "127.0.0.1:8080".to_string(),
            role: FOLLOWER,
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
                status: NodeStatus::PendingActive.into(),
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
        Error::Consensus(ConsensusError::Membership(MembershipError::InvalidPromotion { .. }))
    ));
}

#[tokio::test]
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
        Error::Consensus(ConsensusError::Membership(MembershipError::InvalidChangeRequest))
    ));
}

#[tokio::test]
async fn test_update_cluster_conf_from_leader_case6_conf_version_mismatch() {
    // Test version mismatch
    let membership = RaftMembership::<MockTypeConfig>::new(1, vec![], RaftNodeConfig::default());
    membership.update_conf_version(5); // Set current version to 5

    let req = ClusterConfChangeRequest {
        id: 2,
        term: 1,
        version: 4, // Older version
        change: Some(MembershipChange {
            change: Some(Change::AddNode(AddNode {
                node_id: 3,
                address: "127.0.0.1:8080".to_string(),
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

/// This test covers:
/// Filtering for specific roles (Followers + Candidates)
/// Filtering for a single role (Leaders)
/// Empty result case
/// All-items-match case
#[test]
fn test_get_peers_id_with_condition() {
    // Setup test data
    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:10000".to_string(),
            role: LEARNER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 5,
            address: "127.0.0.1:10000".to_string(),
            role: CANDIDATE,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 6,
            address: "127.0.0.1:10000".to_string(),
            role: LEADER,
            status: NodeStatus::Active.into(),
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster, RaftNodeConfig::default());

    // Test 1: Filter followers and candidates
    let mut result = membership.get_peers_id_with_condition(|role| role == FOLLOWER || role == CANDIDATE);
    result.sort_unstable();
    let mut expect = vec![1, 3, 5];
    expect.sort_unstable();
    assert_eq!(result, expect, "Should return follower and candidate IDs");

    // Test 2: Filter leaders only
    let result = membership.get_peers_id_with_condition(|role| role == LEADER);
    assert_eq!(result, vec![6], "Should return leader ID only");

    // Test 3: Empty result case
    let result = membership.get_peers_id_with_condition(|_| false);
    assert!(result.is_empty(), "Should return empty vector when no matches");
    let mut expect = vec![1, 3, 4, 5, 6];
    expect.sort_unstable();

    // Test 4: All items match
    let mut result = membership.get_peers_id_with_condition(|_| true);
    result.sort_unstable();
    assert_eq!(result, expect, "Should return all IDs when condition is always true");
}

#[cfg(test)]
mod check_cluster_is_ready_test {
    use tokio::sync::oneshot;

    use super::*;
    use crate::test_utils::enable_logger;
    use crate::test_utils::MockNode;
    use crate::test_utils::MockRpcService;
    use crate::test_utils::MOCK_MEMBERSHIP_PORT_BASE;

    /// Case 1: Test all peers are healthy
    #[tokio::test]
    async fn test_check_cluster_is_ready_case1() {
        enable_logger();
        let peer_ids = vec![2, 3];
        let mut mock_services = Vec::new();

        // Create membership with 2 peers
        let membership = RaftMembership::<RaftTypeConfig>::new(
            1,
            peer_ids
                .iter()
                .map(|id| NodeMeta {
                    id: *id,
                    address: format!("127.0.0.1:{}", MOCK_MEMBERSHIP_PORT_BASE + *id),
                    role: FOLLOWER,
                    status: NodeStatus::Active.into(),
                })
                .collect(),
            RaftNodeConfig::default(),
        );

        // Create mock services for peers
        for &id in &peer_ids {
            let (tx, rx) = oneshot::channel::<()>();
            let service = MockRpcService::default();
            let port = MOCK_MEMBERSHIP_PORT_BASE + id;
            let addr = MockNode::mock_listener(service, port as u64, rx, true).await.unwrap();
            mock_services.push((tx, addr));
        }

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
        enable_logger();

        // Create membership with unreachable peer
        let mut config = RaftNodeConfig::default();
        config.retry.membership.max_retries = 1;

        let membership = RaftMembership::<RaftTypeConfig>::new(
            1,
            vec![NodeMeta {
                id: 2,
                address: "127.0.0.1:9999".to_string(), // Invalid port
                role: FOLLOWER,
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
        enable_logger();
        let (tx, rx) = oneshot::channel::<()>();
        let service = MockRpcService::default();
        let port = MOCK_MEMBERSHIP_PORT_BASE + 4;
        let _addr = MockNode::mock_listener(service, port as u64, rx, false).await.unwrap();

        let mut config = RaftNodeConfig::default();
        config.retry.membership.max_retries = 1;
        let membership = RaftMembership::<RaftTypeConfig>::new(
            1,
            vec![NodeMeta {
                id: 4,
                address: format!("127.0.0.1:{}", port),
                role: FOLLOWER,
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
        enable_logger();
        let mut mock_services = Vec::new();
        let peer_ids = [5, 6];

        // Create membership with 2 peers
        let mut config = RaftNodeConfig::default();
        config.retry.membership.max_retries = 1;
        let membership = RaftMembership::<RaftTypeConfig>::new(
            1,
            peer_ids
                .iter()
                .map(|id| NodeMeta {
                    id: *id,
                    address: format!("127.0.0.1:{}", MOCK_MEMBERSHIP_PORT_BASE + *id),
                    role: FOLLOWER,
                    status: NodeStatus::Active.into(),
                })
                .collect(),
            config,
        );

        // Create mock services with different responses
        for &id in peer_ids.iter() {
            let (tx, rx) = oneshot::channel::<()>();
            let service = MockRpcService::default();
            let port = MOCK_MEMBERSHIP_PORT_BASE + id;
            let addr = MockNode::mock_listener(service, port as u64, rx, false).await.unwrap();
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
        let membership = RaftMembership::<RaftTypeConfig>::new(1, vec![], RaftNodeConfig::default());

        let result = membership.add_learner(2, "127.0.0.1:1234".to_string());
        assert!(result.is_ok(), "Should add learner successfully");

        let replication_members = membership.members();
        assert_eq!(replication_members.len(), 1);
        assert_eq!(replication_members[0].id, 2);
        assert_eq!(replication_members[0].address, "127.0.0.1:1234");
        assert_eq!(replication_members[0].role, LEARNER);
        assert_eq!(replication_members[0].status, NodeStatus::PendingActive as i32);
    }

    #[tokio::test]
    async fn test_add_learner_case2() {
        let membership = RaftMembership::<RaftTypeConfig>::new(
            1,
            vec![NodeMeta {
                id: 1,
                address: "127.0.0.1:0".to_string(),
                role: FOLLOWER,
                status: NodeStatus::Active.into(),
            }],
            RaftNodeConfig::default(),
        );

        let result = membership.add_learner(1, "127.0.0.1:1234".to_string());
        assert!(result.is_err(), "Node is follower");

        let replication_members = membership.members();
        assert_eq!(replication_members.len(), 1);
        assert_eq!(replication_members[0].id, 1);
        assert_eq!(replication_members[0].address, "127.0.0.1:0");
        assert_eq!(replication_members[0].role, FOLLOWER);
        assert_eq!(replication_members[0].status, NodeStatus::Active as i32);
    }
}

#[test]
fn test_ensure_safe_join() {
    assert!(ensure_safe_join(1, 0).is_ok());
    assert!(ensure_safe_join(1, 1).is_err());
    assert!(ensure_safe_join(1, 2).is_ok());
    assert!(ensure_safe_join(1, 3).is_err());
    assert!(ensure_safe_join(1, 4).is_ok());
    assert!(ensure_safe_join(1, 5).is_err());
}
