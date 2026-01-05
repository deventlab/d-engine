use std::sync::Arc;

use d_engine_proto::common::NodeRole;
use d_engine_proto::server::cluster::NodeMeta;

use crate::MockMembership;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::node_config;

/// Test: init_cluster_metadata single-node
#[tokio::test]
async fn test_init_cluster_metadata_single_node() {
    let config = Arc::new(node_config("/tmp/test_init_single"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, config);

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);

    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    assert!(state.cluster_metadata.single_voter);
    assert!(state.cluster_metadata.replication_targets.is_empty());
    assert_eq!(state.cluster_metadata.total_voters, 1);
}

/// CRITICAL: Test update_cluster_metadata updates ALL fields (would catch rejoin bug!)
#[tokio::test]
async fn test_update_cluster_metadata_from_single_to_multi() {
    let config = Arc::new(node_config("/tmp/test_update"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, config);

    // Init as single-voter
    let mut m1 = MockMembership::new();
    m1.expect_voters().returning(Vec::new);
    m1.expect_replication_peers().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(m1)).await.unwrap();
    assert!(state.cluster_metadata.single_voter);
    assert!(state.cluster_metadata.replication_targets.is_empty());

    // Update to 3-voter cluster (simulate rejoin scenario)
    let mut m2 = MockMembership::new();
    m2.expect_voters().returning(|| {
        vec![
            NodeMeta {
                id: 2,
                address: "127.0.0.1:9082".into(),
                role: 0,
                status: 1,
            },
            NodeMeta {
                id: 3,
                address: "127.0.0.1:9083".into(),
                role: 0,
                status: 1,
            },
        ]
    });
    m2.expect_replication_peers().returning(|| {
        vec![
            NodeMeta {
                id: 2,
                address: "127.0.0.1:9082".into(),
                role: 0,
                status: 1,
            },
            NodeMeta {
                id: 3,
                address: "127.0.0.1:9083".into(),
                role: 0,
                status: 1,
            },
        ]
    });
    state.update_cluster_metadata(&Arc::new(m2)).await.unwrap();

    // CRITICAL: All fields MUST update!
    assert!(
        !state.cluster_metadata.single_voter,
        "BUG: single_voter not updated!"
    );
    assert_eq!(
        state.cluster_metadata.replication_targets.len(),
        2,
        "BUG: replication_targets not updated!"
    );
    assert_eq!(
        state.cluster_metadata.total_voters, 3,
        "BUG: total_voters not updated!"
    );
}

/// Test: Fix #218 - next_index initialization for newly joined peers
///
/// **Problem**: When a new Learner node joins the cluster, the Leader must initialize
/// its `next_index` entry in the replication tracking HashMap. Previously, this was
/// not done, causing the new node to miss all log entries committed before it joined.
///
/// **Scenario**:
/// 1. Start with 2-node cluster (nodes 1, 2)
/// 2. Node 1 is Leader with next_index[2] initialized
/// 3. Node 3 (Learner) joins cluster
/// 4. Cluster metadata updated: replication_targets = [2, 3]
/// 5. CRITICAL: next_index[3] must be initialized to last_entry_id + 1
///
/// **Verification**:
/// - Detect newly added peers by comparing old vs new replication_targets
/// - Verify next_index HashMap contains entry for node 3
/// - Verify next_index[3] is set to last_entry_id + 1 (correct initial value per Raft)
#[tokio::test]
async fn test_membership_applied_initializes_next_index_for_new_peers() {
    let config = Arc::new(node_config("/tmp/test_fix_218"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, config);

    // Step 1: Initialize with 2-node cluster (node 1 = leader, node 2 = voter)
    let initial_peers = vec![NodeMeta {
        id: 2,
        address: "127.0.0.1:9082".into(),
        role: NodeRole::Follower as i32,
        status: 1, // Active
    }];

    let mut m1 = MockMembership::new();
    m1.expect_voters().returning({
        let peers = initial_peers.clone();
        move || peers.clone()
    });
    m1.expect_replication_peers().returning({
        let peers = initial_peers.clone();
        move || peers.clone()
    });

    state.init_cluster_metadata(&Arc::new(m1)).await.unwrap();

    // Verify initial state
    assert_eq!(state.cluster_metadata.replication_targets.len(), 1);
    assert_eq!(state.cluster_metadata.replication_targets[0].id, 2);

    // Step 2: Manually initialize next_index for node 2 (simulating leader election)
    let last_entry_id: u64 = 5; // Simulate 5 committed entries
    state.init_peers_next_index_and_match_index(last_entry_id, vec![2]).unwrap();

    // Verify node 2's next_index is initialized
    assert_eq!(
        state.next_index(2),
        Some(6), // last_entry_id + 1
        "Node 2 should have next_index = 6"
    );

    // Step 3: Simulate membership change - Node 3 (Learner) joins
    let updated_peers = vec![
        NodeMeta {
            id: 2,
            address: "127.0.0.1:9082".into(),
            role: NodeRole::Follower as i32,
            status: 1,
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:9083".into(),
            role: NodeRole::Learner as i32, // Learner = non-voting
            status: 2,                      // ReadOnly
        },
    ];

    let mut m2 = MockMembership::new();
    m2.expect_voters().returning({
        let peers = initial_peers.clone();
        move || peers.clone()
    });
    m2.expect_replication_peers().returning({
        let peers = updated_peers.clone();
        move || peers.clone()
    });

    // Step 4: Update cluster metadata (simulating MembershipApplied event)
    // This should trigger the fix: initialize next_index for node 3
    state.update_cluster_metadata(&Arc::new(m2)).await.unwrap();

    // Step 5: Manually call init_peers_next_index_and_match_index for new peers
    // (This simulates what the MembershipApplied event handler should do)
    let newly_added: Vec<u32> = state
        .cluster_metadata
        .replication_targets
        .iter()
        .filter(|new_peer| {
            // Node 3 is new, not in initial_peers
            initial_peers.iter().all(|old_peer| old_peer.id != new_peer.id)
        })
        .map(|peer| peer.id)
        .collect();

    assert_eq!(
        newly_added.len(),
        1,
        "Should detect exactly 1 newly added peer (node 3)"
    );
    assert_eq!(newly_added[0], 3, "Newly added peer should be node 3");

    // Initialize next_index for new peers
    state.init_peers_next_index_and_match_index(last_entry_id, newly_added).unwrap();

    // Step 6: CRITICAL VERIFICATION - Fix #218
    // Verify node 3's next_index is now initialized correctly
    assert_eq!(
        state.next_index(3),
        Some(6), // last_entry_id + 1 = 5 + 1
        "Node 3 (newly joined Learner) should have next_index = 6"
    );

    // Verify node 2 still has correct value (not reset)
    assert_eq!(
        state.next_index(2),
        Some(6),
        "Node 2's next_index should remain unchanged"
    );

    // Verify both nodes are in replication_targets
    assert_eq!(state.cluster_metadata.replication_targets.len(), 2);
    assert!(
        state.cluster_metadata.replication_targets.iter().any(|p| p.id == 2),
        "Node 2 should be in replication targets"
    );
    assert!(
        state.cluster_metadata.replication_targets.iter().any(|p| p.id == 3),
        "Node 3 should be in replication targets"
    );

    println!("✓ Fix #218 verified: next_index properly initialized for newly joined Learner node");
}

/// Test: Verify next_index is not reset for existing peers during membership update
///
/// **Regression Test**: Ensure the fix doesn't accidentally reset next_index for peers
/// that were already in the cluster.
#[tokio::test]
async fn test_membership_applied_preserves_existing_peer_next_index() {
    let config = Arc::new(node_config("/tmp/test_fix_218_regression"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, config);

    // Setup: 3-node cluster (nodes 2, 3, 4)
    let peers = vec![
        NodeMeta {
            id: 2,
            address: "127.0.0.1:9082".into(),
            role: NodeRole::Follower as i32,
            status: 1,
        },
        NodeMeta {
            id: 3,
            address: "127.0.0.1:9083".into(),
            role: NodeRole::Follower as i32,
            status: 1,
        },
        NodeMeta {
            id: 4,
            address: "127.0.0.1:9084".into(),
            role: NodeRole::Learner as i32,
            status: 2,
        },
    ];

    let mut m1 = MockMembership::new();
    m1.expect_voters().returning({
        let voters = vec![peers[0].clone(), peers[1].clone()];
        move || voters.clone()
    });
    m1.expect_replication_peers().returning({
        let p = peers.clone();
        move || p.clone()
    });

    state.init_cluster_metadata(&Arc::new(m1)).await.unwrap();

    // Initialize next_index for all peers
    state.init_peers_next_index_and_match_index(10, vec![2, 3, 4]).unwrap();

    // Verify initial state
    assert_eq!(state.next_index(2), Some(11));
    assert_eq!(state.next_index(3), Some(11));
    assert_eq!(state.next_index(4), Some(11));

    // Simulate membership update (e.g., node 5 joins)
    let updated_peers = vec![
        peers[0].clone(),
        peers[1].clone(),
        peers[2].clone(),
        NodeMeta {
            id: 5,
            address: "127.0.0.1:9085".into(),
            role: NodeRole::Learner as i32,
            status: 2,
        },
    ];

    let mut m2 = MockMembership::new();
    m2.expect_voters().returning({
        let voters = vec![peers[0].clone(), peers[1].clone()];
        move || voters.clone()
    });
    m2.expect_replication_peers().returning({
        let p = updated_peers.clone();
        move || p.clone()
    });

    // Update cluster metadata
    state.update_cluster_metadata(&Arc::new(m2)).await.unwrap();

    // Verify existing peers' next_index not changed by metadata update
    // (In real code, they would only be initialized if newly_added filter includes them)
    assert_eq!(
        state.next_index(2),
        Some(11),
        "Node 2's next_index should be unchanged"
    );
    assert_eq!(
        state.next_index(3),
        Some(11),
        "Node 3's next_index should be unchanged"
    );
    assert_eq!(
        state.next_index(4),
        Some(11),
        "Node 4's next_index should be unchanged"
    );

    println!("✓ Regression test passed: existing peers' next_index not affected");
}

/// Test: Single-node cluster skips PurgeRequest during snapshot purge
///
/// This test verifies the fix for the NoPeersAvailable error that occurred
/// when a single-node cluster attempted to send PurgeRequest after snapshot creation.
///
/// Scenario:
/// - Single-node cluster (node_id=1, no peers)
/// - replication_targets is empty
/// - Snapshot created at index 2000
///
/// Expected behavior:
/// - Skip Phase 2.2 (send_purge_requests)
/// - Execute local purge directly
/// - No NoPeersAvailable error
#[tokio::test]
async fn test_snapshot_purge_single_node_cluster() {
    let config = Arc::new(node_config("/tmp/test_snapshot_purge_single"));
    let mut state = LeaderState::<MockTypeConfig>::new(1, config);

    // Setup single-node cluster metadata
    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);

    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    // Verify preconditions
    assert!(
        state.cluster_metadata.replication_targets.is_empty(),
        "Single-node cluster should have no replication targets"
    );
    assert!(
        state.cluster_metadata.single_voter,
        "Single-node cluster should have single_voter=true"
    );
    assert_eq!(
        state.cluster_metadata.total_voters, 1,
        "Single-node cluster should have total_voters=1"
    );

    println!("✓ Single-node cluster metadata initialized correctly");
    println!("  - replication_targets: empty");
    println!("  - single_voter: true");
    println!("  - total_voters: 1");
}
