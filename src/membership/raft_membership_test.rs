use tokio::sync::oneshot;
use tokio::sync::watch;

use super::RaftMembership;
use crate::cluster::is_follower;
use crate::proto::ClusteMembershipChangeRequest;
use crate::proto::ClusterMembership;
use crate::proto::NodeMeta;
use crate::test_utils::mock_raft_context;
use crate::test_utils::MockNode;
use crate::test_utils::MOCK_MEMBERSHIP_PORT_BASE;
use crate::Membership;
use crate::PeerChannelsFactory;
use crate::RaftTypeConfig;
use crate::RpcPeerChannels;
use crate::CANDIDATE;
use crate::FOLLOWER;
use crate::LEADER;
use crate::LEARNER;

/// # Case 1: Retrieve followers from cluster membership successfully;
///
/// ## Setup
/// 1. PeerChannels has already connected with two followers
/// 2. Membership config is ready with two followers
///
/// ## Validation criteria
/// 1. result len is 2
#[tokio::test]
async fn test_get_peers_address_with_role_condition_case1() {
    let db_root_dir = "/tmp/test_get_get_peers_address_with_role_condition_case1";
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context(db_root_dir, graceful_rx, None);

    // We are validation two followers
    let f1 = 3;
    let f2 = 5;

    // Step 1: prepare connected channels
    let port = MOCK_MEMBERSHIP_PORT_BASE + 20;
    let (_tx, rx) = oneshot::channel::<()>();
    let address = MockNode::simulate_mock_service_without_reps(port, rx, true)
        .await
        .expect("should succeed");
    let peer_channels: RpcPeerChannels = PeerChannelsFactory::create(1, context.settings());
    peer_channels.set_peer_channel(f1, address.clone());
    peer_channels.set_peer_channel(f2, address.clone());

    // Step 2: prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 2,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: CANDIDATE,
        },
        NodeMeta {
            id: f1,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 4,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEARNER,
        },
        NodeMeta {
            id: f2,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 6,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEARNER,
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster);

    let result = membership.get_peers_address_with_role_condition(&peer_channels.channels, is_follower);

    assert_eq!(result.len(), 2);
}

/// # Case 2: Retrieve followers from cluster membership successfully;
///
/// ## Setup
/// 1. PeerChannels has only connected with one follower
/// 2. Membership config is ready with two followers
///
/// ## Validation criteria
/// 1. result len is 1
#[tokio::test]
async fn test_get_peers_address_with_role_condition_case2() {
    let db_root_dir = "/tmp/test_get_get_peers_address_with_role_condition_case2";
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context(db_root_dir, graceful_rx, None);

    // We are validation two followers
    let f1 = 3;
    let f2 = 5;

    // Step 1: prepare connected channels
    let port = MOCK_MEMBERSHIP_PORT_BASE + 21;
    let (_tx, rx) = oneshot::channel::<()>();
    let address = MockNode::simulate_mock_service_without_reps(port, rx, true)
        .await
        .expect("should succeed");
    let peer_channels: RpcPeerChannels = PeerChannelsFactory::create(1, context.settings());
    peer_channels.set_peer_channel(f1, address.clone());

    // Step 2: prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 2,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: CANDIDATE,
        },
        NodeMeta {
            id: f1,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 4,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEARNER,
        },
        NodeMeta {
            id: f2,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 6,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEARNER,
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster);

    let result = membership.get_peers_address_with_role_condition(&peer_channels.channels, is_follower);

    assert_eq!(result.len(), 1);
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
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEADER,
        },
        NodeMeta {
            id: 3,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 4,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEARNER,
        },
        NodeMeta {
            id: 5,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 6,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEARNER,
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster);

    assert_eq!(membership.current_leader(), Some(old_leader_id));
    assert!(membership.mark_leader_id(new_leader_id).is_ok());
    assert_eq!(membership.current_leader(), Some(new_leader_id));
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
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 3,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 4,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEARNER,
        },
        NodeMeta {
            id: 5,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 6,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEARNER,
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster);

    assert!(membership.mark_leader_id(new_leader_id).is_err());
    assert_eq!(membership.current_leader(), None);
}

#[test]
fn test_retrieve_cluster_membership_config() {
    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 3,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 4,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEARNER,
        },
        NodeMeta {
            id: 5,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 6,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEARNER,
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster);

    let r = membership.retrieve_cluster_membership_config();
    assert_eq!(r.nodes.len(), 5);
    assert!(!r.nodes.iter().any(|n| n.role == LEADER));
}

/// # Case 1: Test cluster conf update from Leader
///     with Error, my term is higher than request one
#[tokio::test]
async fn test_update_cluster_conf_from_leader_case1() {
    let my_term = 10;

    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 3,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 4,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
    ];

    // Prepare request
    let request = ClusteMembershipChangeRequest {
        id: 3,
        term: my_term - 1,
        version: 1,
        cluster_membership: Some(ClusterMembership {
            nodes: initial_cluster.clone(),
        }),
    };
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster);

    assert!(membership
        .update_cluster_conf_from_leader(my_term, &request)
        .await
        .is_err());
}

/// # Case 2: Test cluster conf update from Leader
///     with Error, because my cluster conf version is higher than request one
///
/// # Setup:
/// 1. my term is equal with request one
/// 2. my cluster conf version is higher than request one
#[tokio::test]
async fn test_update_cluster_conf_from_leader_case2() {
    let my_term = 10;

    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 3,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 4,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
    ];

    // Prepare request
    let request_cluster_conf_version = 1;
    let request = ClusteMembershipChangeRequest {
        id: 3,
        term: my_term,
        version: request_cluster_conf_version,
        cluster_membership: Some(ClusterMembership {
            nodes: initial_cluster.clone(),
        }),
    };
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster);

    // Prepare my cluster conf version is higher than request one
    membership.update_cluster_conf_version(request_cluster_conf_version + 1);
    assert!(membership
        .update_cluster_conf_from_leader(my_term, &request)
        .await
        .is_err());
}

/// # Case 3: Test cluster conf update from Leader
///     with Ok()
///
/// # Setup:
/// 1. my term is equal with request one
/// 2. my cluster conf version is smaller than request one
#[tokio::test]
async fn test_update_cluster_conf_from_leader_case3() {
    let my_term = 10;

    // Prepare cluster membership
    let initial_cluster = vec![
        NodeMeta {
            id: 1,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 3,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 4,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
    ];

    // Prepare request
    let request_cluster_conf_version = 2;
    let request = ClusteMembershipChangeRequest {
        id: 3,
        term: my_term,
        version: request_cluster_conf_version,
        cluster_membership: Some(ClusterMembership {
            nodes: initial_cluster.clone(),
        }),
    };
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster);

    // Prepare my cluster conf version is higher than request one
    membership.update_cluster_conf_version(request_cluster_conf_version - 1);
    assert!(membership
        .update_cluster_conf_from_leader(my_term, &request)
        .await
        .is_ok());
    assert_eq!(membership.get_cluster_conf_version(), request_cluster_conf_version);
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
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 3,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 4,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEARNER,
        },
        NodeMeta {
            id: 5,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: CANDIDATE,
        },
        NodeMeta {
            id: 6,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: LEADER,
        },
    ];
    let membership = RaftMembership::<RaftTypeConfig>::new(1, initial_cluster);

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
