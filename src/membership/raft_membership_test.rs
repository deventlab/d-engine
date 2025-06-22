use super::RaftMembership;
use crate::proto::cluster::cluster_conf_change_request::Change;
use crate::proto::cluster::cluster_conf_update_response::ErrorCode;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::NodeMeta;
use crate::proto::common::AddNode;
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
        change: Some(Change::AddNode(AddNode {
            node_id: 3,
            address: "127.0.0.1:8080".to_string(),
        })),
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
        change: Some(Change::RemoveNode(RemoveNode { node_id: 3 })),
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
        change: Some(Change::PromoteLearner(PromoteLearner {
            node_id: 3,
            status: NodeStatus::PendingActive.into(),
        })),
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
        change: Some(Change::PromoteLearner(PromoteLearner {
            node_id: 3,
            status: NodeStatus::PendingActive.into(),
        })),
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
    membership.update_cluster_conf_from_leader_version(5); // Set current version to 5

    let req = ClusterConfChangeRequest {
        id: 2,
        term: 1,
        version: 4, // Older version
        change: Some(Change::AddNode(AddNode {
            node_id: 3,
            address: "127.0.0.1:8080".to_string(),
        })),
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
