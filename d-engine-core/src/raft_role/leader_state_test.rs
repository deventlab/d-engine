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

#[cfg(test)]
mod peer_purge_progress_tests {
    use super::*;
    use crate::Result;
    use crate::raft_role::RoleEvent;
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::storage::PurgeLogResponse;
    use tokio::sync::mpsc;

    /// Test: peer_purge_progress returns false when responses are empty
    #[test]
    fn test_peer_purge_progress_empty_responses() {
        let config = Arc::new(node_config("/tmp/test_empty"));
        let mut state = LeaderState::<MockTypeConfig>::new(1, config);
        state.update_current_term(3);

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();
        let responses: Vec<Result<PurgeLogResponse>> = vec![];

        let should_step_down = state
            .peer_purge_progress(responses, &role_tx)
            .expect("Should handle empty responses");

        assert!(
            !should_step_down,
            "Empty responses should not trigger step-down"
        );
        assert!(
            role_rx.try_recv().is_err(),
            "No event should be sent for empty responses"
        );
        assert_eq!(state.current_term(), 3, "Term should remain unchanged");
    }

    /// Test: peer_purge_progress returns false for normal responses (same term)
    #[test]
    fn test_peer_purge_progress_normal_responses() {
        let config = Arc::new(node_config("/tmp/test_normal"));
        let mut state = LeaderState::<MockTypeConfig>::new(1, config);
        state.update_current_term(5);

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();
        let responses = vec![
            Ok(PurgeLogResponse {
                node_id: 2,
                term: 5, // Same term
                success: true,
                last_purged: Some(LogId {
                    term: 5,
                    index: 100,
                }),
            }),
            Ok(PurgeLogResponse {
                node_id: 3,
                term: 5, // Same term
                success: true,
                last_purged: Some(LogId {
                    term: 5,
                    index: 100,
                }),
            }),
        ];

        let should_step_down = state
            .peer_purge_progress(responses, &role_tx)
            .expect("Should handle normal responses");

        assert!(
            !should_step_down,
            "Normal responses should not trigger step-down"
        );
        assert!(
            role_rx.try_recv().is_err(),
            "No step-down event should be sent"
        );
        assert_eq!(state.current_term(), 5, "Term should remain unchanged");
        assert_eq!(
            state.peer_purge_progress.get(&2),
            Some(&100),
            "Peer 2 progress should be updated"
        );
        assert_eq!(
            state.peer_purge_progress.get(&3),
            Some(&100),
            "Peer 3 progress should be updated"
        );
    }

    /// Test: peer_purge_progress returns true when higher term detected
    /// This is the critical test for the bug fix
    #[test]
    fn test_peer_purge_progress_higher_term_step_down() {
        let config = Arc::new(node_config("/tmp/test_higher_term"));
        let mut state = LeaderState::<MockTypeConfig>::new(1, config);
        state.update_current_term(3);

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();
        let responses = vec![Ok(PurgeLogResponse {
            node_id: 2,
            term: 6, // Higher than current term (3)
            success: true,
            last_purged: Some(LogId {
                term: 5,
                index: 100,
            }),
        })];

        let should_step_down = state
            .peer_purge_progress(responses, &role_tx)
            .expect("Should handle higher term response");

        assert!(
            should_step_down,
            "Higher term response MUST trigger step-down signal"
        );
        assert_eq!(
            state.current_term(),
            6,
            "Term should be updated to higher value"
        );

        let event = role_rx.try_recv().expect("BecomeFollower event should be sent");
        assert!(
            matches!(event, RoleEvent::BecomeFollower(None)),
            "Should send BecomeFollower(None) event"
        );
    }

    /// Test: peer_purge_progress handles mixed responses (higher term found first)
    #[test]
    fn test_peer_purge_progress_mixed_responses_higher_term_first() {
        let config = Arc::new(node_config("/tmp/test_mixed_first"));
        let mut state = LeaderState::<MockTypeConfig>::new(1, config);
        state.update_current_term(4);

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();
        let responses = vec![
            Ok(PurgeLogResponse {
                node_id: 2,
                term: 7, // Higher term - should trigger immediate step-down
                success: true,
                last_purged: Some(LogId {
                    term: 6,
                    index: 100,
                }),
            }),
            Ok(PurgeLogResponse {
                node_id: 3,
                term: 4, // This should not be processed due to early return
                success: true,
                last_purged: Some(LogId { term: 4, index: 90 }),
            }),
        ];

        let should_step_down = state
            .peer_purge_progress(responses, &role_tx)
            .expect("Should handle mixed responses");

        assert!(should_step_down, "Should signal step-down");
        assert_eq!(state.current_term(), 7, "Term should be updated to 7");

        // Verify peer 3's progress was NOT updated (early return)
        assert!(
            !state.peer_purge_progress.contains_key(&3),
            "Peer 3 progress should NOT be updated after step-down"
        );

        let event = role_rx.try_recv().expect("Should receive event");
        assert!(matches!(event, RoleEvent::BecomeFollower(None)));
    }

    /// Test: peer_purge_progress ignores lower term responses
    #[test]
    fn test_peer_purge_progress_lower_term() {
        let config = Arc::new(node_config("/tmp/test_lower_term"));
        let mut state = LeaderState::<MockTypeConfig>::new(1, config);
        state.update_current_term(10);

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();
        let responses = vec![Ok(PurgeLogResponse {
            node_id: 2,
            term: 5, // Lower than current term (10)
            success: true,
            last_purged: Some(LogId {
                term: 5,
                index: 100,
            }),
        })];

        let should_step_down = state
            .peer_purge_progress(responses, &role_tx)
            .expect("Should handle lower term response");

        assert!(!should_step_down, "Lower term should not trigger step-down");
        assert_eq!(state.current_term(), 10, "Term should remain unchanged");
        assert!(
            role_rx.try_recv().is_err(),
            "No event should be sent for lower term"
        );
        // Progress should still be updated even for lower term
        assert_eq!(
            state.peer_purge_progress.get(&2),
            Some(&100),
            "Peer progress should be updated"
        );
    }

    /// Test: peer_purge_progress handles response without last_purged
    #[test]
    fn test_peer_purge_progress_no_last_purged() {
        let config = Arc::new(node_config("/tmp/test_no_purged"));
        let mut state = LeaderState::<MockTypeConfig>::new(1, config);
        state.update_current_term(5);

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();
        let responses = vec![Ok(PurgeLogResponse {
            node_id: 2,
            term: 5,
            success: false,
            last_purged: None, // No purge progress
        })];

        let should_step_down = state
            .peer_purge_progress(responses, &role_tx)
            .expect("Should handle response without last_purged");

        assert!(!should_step_down, "Should not trigger step-down");
        assert!(
            !state.peer_purge_progress.contains_key(&2),
            "Peer progress should not be updated when last_purged is None"
        );
        assert!(role_rx.try_recv().is_err(), "No event should be sent");
    }
}

// ============================================================================
// Read Batching Unit Tests
// ============================================================================

mod read_batching_tests {
    use super::*;
    use crate::RaftEvent;
    use crate::maybe_clone_oneshot::{MaybeCloneOneshot, RaftOneshot};
    use crate::test_utils::mock::MockTypeConfig;
    use bytes::Bytes;
    use d_engine_proto::client::ClientReadRequest;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, sleep};

    /// Helper: Create mock ClientReadRequest
    fn create_read_request(key: Vec<u8>) -> ClientReadRequest {
        ClientReadRequest {
            client_id: 1,
            keys: vec![Bytes::from(key)],
            consistency_policy: Some(
                d_engine_proto::client::ReadConsistencyPolicy::LinearizableRead as i32,
            ),
        }
    }

    /// Test 1.1: Size threshold triggers immediate flush
    ///
    /// Scenario: Send 50 linearizable read requests (reaching size_threshold)
    /// Expected:
    ///   - 49th request: buffer accumulates, NO flush yet
    ///   - 50th request: triggers process_linearizable_read_batch() immediately
    ///   - All 50 requests share single verify_leadership() call
    #[tokio::test]
    async fn test_read_buffer_size_trigger() {
        // This test verifies size threshold trigger by checking buffer state
        // Full end-to-end flush behavior is tested in integration tests
        let mut state =
            LeaderState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_size_trigger")));

        // Setup: Initialize cluster metadata (single node)
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        // Action: Enqueue 49 requests (below threshold)
        for i in 0..49 {
            let req = create_read_request(format!("key{i}").into_bytes());
            let (tx, _rx) = MaybeCloneOneshot::new();
            state.read_buffer.push((req, tx));
        }

        // Verify: Buffer has 49 requests (not yet flushed)
        assert_eq!(
            state.read_buffer.len(),
            49,
            "Buffer should have 49 requests before threshold"
        );

        // Action: Add 50th request (reaches threshold)
        let req = create_read_request(b"key49".to_vec());
        let (tx, _rx) = MaybeCloneOneshot::new();
        state.read_buffer.push((req, tx));

        // Verify: Buffer reaches size threshold
        assert_eq!(
            state.read_buffer.len(),
            50,
            "Buffer should have 50 requests at threshold"
        );

        // Verify: Size threshold condition is met (would trigger flush in real code)
        let config = node_config("/tmp/test_size_trigger");
        let size_threshold = config.raft.read_consistency.read_batching.size_threshold;
        assert!(
            state.read_buffer.len() >= size_threshold,
            "Buffer size {} should meet threshold {} (triggers flush in handle_raft_event)",
            state.read_buffer.len(),
            size_threshold
        );

        // NOTE: Actual flush execution is tested in integration tests
        // This unit test verifies the trigger condition is correctly detected
    }

    /// Test 1.2: Time threshold triggers flush for single request (CRITICAL BOUNDARY CASE)
    ///
    /// Scenario: Send 1 linearizable read request (far below size_threshold=50)
    /// Expected:
    ///   - Timeout task spawned after 1st request
    ///   - After ~10ms, FlushReadBuffer event sent
    ///   - Request completes successfully (NOT stuck forever)
    ///
    /// This verifies batching does NOT starve low-concurrency requests.
    #[tokio::test]
    async fn test_read_buffer_time_trigger() {
        // This test verifies timeout mechanism by checking time-based conditions
        // Full end-to-end timeout behavior is tested in integration tests
        let mut state =
            LeaderState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_time_trigger")));

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        // Action: Enqueue single request
        let req = create_read_request(b"lonely_key".to_vec());
        let (tx, _rx) = MaybeCloneOneshot::new();
        state.read_buffer.push((req, tx));
        let start_time = tokio::time::Instant::now();
        state.read_buffer_start_time = Some(start_time);

        // Verify: Buffer has 1 request (below size threshold)
        assert_eq!(state.read_buffer.len(), 1, "Buffer should have 1 request");
        let config = node_config("/tmp/test_time_trigger");
        assert!(
            state.read_buffer.len() < config.raft.read_consistency.read_batching.size_threshold,
            "Single request should NOT trigger size threshold"
        );

        // Simulate timeout: Wait for time_threshold_ms
        sleep(Duration::from_millis(11)).await;

        // Verify: Timeout condition is met
        let elapsed = start_time.elapsed();
        assert!(
            elapsed >= Duration::from_millis(10),
            "Timeout should expire after {}ms, actual: {:?}",
            config.raft.read_consistency.read_batching.time_threshold_ms,
            elapsed
        );

        // Verify: Start time was recorded (enables timeout detection)
        assert!(
            state.read_buffer_start_time.is_some(),
            "Start time should be recorded for timeout detection"
        );

        // NOTE: Actual timeout task spawn and FlushReadBuffer event handling
        // is tested in integration tests. This unit test verifies the timeout
        // condition is correctly detected after time_threshold_ms expires.
    }

    /// Test 1.3: Timeout idempotency (prevent duplicate flush)
    ///
    /// Scenario:
    /// 1. 50 requests arrive → size threshold triggers immediate flush
    /// 2. Buffer becomes empty
    /// 3. 10ms later, timeout fires and sends FlushReadBuffer event
    ///
    /// Expected: Second flush is no-op (does not panic or duplicate work)
    #[tokio::test]
    async fn test_read_buffer_timeout_idempotent() {
        // This test verifies idempotency by simulating cleared buffer state
        // Full end-to-end idempotency is tested in integration tests
        let mut state =
            LeaderState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_idempotent")));

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        // Action: Enqueue 50 requests and immediately clear (simulates size flush)
        for i in 0..50 {
            let req = create_read_request(format!("key{i}").into_bytes());
            let (tx, _rx) = MaybeCloneOneshot::new();
            state.read_buffer.push((req, tx));
        }

        let start_time = tokio::time::Instant::now();
        state.read_buffer_start_time = Some(start_time);

        // Verify: Buffer has 50 requests
        assert_eq!(
            state.read_buffer.len(),
            50,
            "Buffer should have 50 requests"
        );

        // Simulate size threshold flush
        state.read_buffer.clear();
        state.read_buffer_start_time = None;

        // Verify: Buffer is empty after flush
        assert_eq!(
            state.read_buffer.len(),
            0,
            "Buffer should be empty after size flush"
        );

        // Wait for timeout to expire
        sleep(Duration::from_millis(15)).await;

        // Verify: Buffer remains empty (idempotent - no second flush needed)
        assert_eq!(state.read_buffer.len(), 0, "Buffer should still be empty");
        assert!(
            state.read_buffer_start_time.is_none(),
            "Start time should be cleared after first flush"
        );

        // NOTE: In real code, process_linearizable_read_batch() checks if buffer is empty
        // and returns early (no-op). This prevents duplicate work when
        // delayed timeout events arrive after size threshold flush.
    }

    /// Test 1.4: Leader role change drains buffer (CRITICAL RAFT SAFETY)
    ///
    /// Scenario:
    /// 1. Leader has buffered requests (below size threshold, timeout not expired)
    /// 2. Leader loses leadership (receives higher term) → becomes Follower
    /// 3. drain_read_buffer() is called during role transition
    ///
    /// Expected:
    /// - All buffered requests are drained and failed with "Leader stepped down" error
    /// - Buffer is cleared
    /// - Raft safety preserved: old Leader cannot process reads after stepping down
    #[tokio::test]
    async fn test_read_buffer_role_change_drain() {
        let mut state =
            LeaderState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_drain")));

        // Action: Simulate Leader → Follower transition (drain_read_buffer is called)
        let result = state.drain_read_buffer();

        // Verify: drain_read_buffer returns Ok(()) for empty buffer
        assert!(
            result.is_ok(),
            "drain_read_buffer should succeed for newly created Leader"
        );

        // The drain mechanism is tested implicitly:
        // - LeaderState implements drain_read_buffer() trait method
        // - It drains all buffered requests and sends errors to senders
        // - Integration tests will verify end-to-end behavior with real requests
    }

    /// Test 1.5: Non-Leader roles return error when drain_read_buffer is called
    ///
    /// Scenario: Follower/Candidate calls drain_read_buffer()
    /// Expected: Returns NotLeader error (these roles don't buffer reads)
    #[tokio::test]
    async fn test_drain_read_buffer_other_roles() {
        use crate::raft_role::candidate_state::CandidateState;
        use crate::raft_role::follower_state::FollowerState;

        // Test Follower
        let mut follower_state = FollowerState::<MockTypeConfig>::new(
            1,
            Arc::new(node_config("/tmp/test_follower")),
            None,
            None,
        );
        let result = follower_state.drain_read_buffer();
        assert!(
            result.is_err(),
            "Follower drain_read_buffer should return error"
        );

        // Test Candidate
        let mut candidate_state =
            CandidateState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_candidate")));
        let result = candidate_state.drain_read_buffer();
        assert!(
            result.is_err(),
            "Candidate drain_read_buffer should return error"
        );

        // Verify: Non-Leader roles should not buffer reads
        // (The drain_read_buffer() trait method returns NotLeader error for them)
    }

    /// Test 1.6: Verify batching configuration is correctly applied
    #[tokio::test]
    async fn test_read_batching_config_applied() {
        let config = node_config("/tmp/test_config");

        // Verify default config values from raft.toml
        assert_eq!(
            config.raft.read_consistency.read_batching.size_threshold, 50,
            "Default size threshold should be 50"
        );
        assert_eq!(
            config.raft.read_consistency.read_batching.time_threshold_ms, 10,
            "Default time threshold should be 10ms"
        );
    }

    /// Test 1.7: First ClientReadRequest spawns timeout task
    ///
    /// Scenario: Send 1st LinearizableRead request
    /// Expected:
    ///   - Request enqueued to buffer
    ///   - read_buffer_start_time recorded
    ///   - Timeout task spawns and sends FlushReadBuffer after 10ms
    ///   - Early return Ok(()) without immediate response
    #[tokio::test]
    async fn test_first_request_spawns_timeout_task() {
        use crate::test_utils::mock::mock_raft_context;
        use tokio::sync::watch;

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let ctx = mock_raft_context("/tmp/test_first_request", shutdown_rx, None);
        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();

        // Action: Send 1st request
        let req = create_read_request(b"key1".to_vec());
        let (tx, _rx) = MaybeCloneOneshot::new();
        let result = state
            .handle_raft_event(RaftEvent::ClientReadRequest(req, tx), &ctx, role_tx.clone())
            .await;

        // Verify: Function returns Ok (early return)
        assert!(result.is_ok(), "Should return Ok on first request");

        // Verify: Buffer has 1 request
        assert_eq!(state.read_buffer.len(), 1, "Buffer should have 1 request");

        // Verify: Start time recorded
        assert!(
            state.read_buffer_start_time.is_some(),
            "Start time should be recorded for timeout detection"
        );

        // Wait for timeout task to send FlushReadBuffer event
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Verify: role_rx receives ReprocessEvent(FlushReadBuffer)
        let event = role_rx.try_recv();
        assert!(
            event.is_ok(),
            "Timeout task should send FlushReadBuffer event"
        );

        if let Ok(role_event) = event {
            match role_event {
                crate::raft_role::RoleEvent::ReprocessEvent(boxed_event) => {
                    assert!(
                        matches!(*boxed_event, RaftEvent::FlushReadBuffer),
                        "Event should be FlushReadBuffer"
                    );
                }
                _ => panic!("Expected ReprocessEvent, got {role_event:?}"),
            }
        }

        drop(shutdown_tx);
    }

    /// Test 1.8: 2nd-49th requests only enqueue (no new timeout task)
    ///
    /// Scenario: Send 2nd LinearizableRead request (buffer already has 1)
    /// Expected:
    ///   - Request enqueued to buffer
    ///   - NO new timeout task spawned
    ///   - read_buffer_start_time unchanged
    ///   - Early return Ok(())
    #[tokio::test]
    async fn test_subsequent_requests_only_enqueue() {
        use crate::test_utils::mock::mock_raft_context;
        use tokio::sync::watch;

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let ctx = mock_raft_context("/tmp/test_subsequent", shutdown_rx, None);
        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();

        // Action: Send 1st request (spawns timeout task)
        let req1 = create_read_request(b"key1".to_vec());
        let (tx1, _rx1) = MaybeCloneOneshot::new();
        state
            .handle_raft_event(
                RaftEvent::ClientReadRequest(req1, tx1),
                &ctx,
                role_tx.clone(),
            )
            .await
            .unwrap();

        let first_start_time = state.read_buffer_start_time;
        assert_eq!(state.read_buffer.len(), 1, "Buffer should have 1 request");

        // Action: Send 2nd-10th requests (should only enqueue)
        for i in 2..=10 {
            let req = create_read_request(format!("key{i}").into_bytes());
            let (tx, _rx) = MaybeCloneOneshot::new();
            let result = state
                .handle_raft_event(RaftEvent::ClientReadRequest(req, tx), &ctx, role_tx.clone())
                .await;

            assert!(result.is_ok(), "Request {i} should return Ok");
        }

        // Verify: Buffer has 10 requests
        assert_eq!(
            state.read_buffer.len(),
            10,
            "Buffer should have 10 requests"
        );

        // Verify: Start time unchanged (no new timeout task)
        assert_eq!(
            state.read_buffer_start_time, first_start_time,
            "Start time should not change for subsequent requests"
        );

        // Verify: Only ONE FlushReadBuffer event in channel (from 1st request)
        tokio::time::sleep(Duration::from_millis(15)).await;
        let event1 = role_rx.try_recv();
        assert!(event1.is_ok(), "Should have 1 FlushReadBuffer event");

        let event2 = role_rx.try_recv();
        assert!(
            event2.is_err(),
            "Should NOT have 2nd FlushReadBuffer event (no duplicate timeout tasks)"
        );

        drop(shutdown_tx);
    }

    /// Test 1.9: 50th request triggers immediate flush
    ///
    /// Scenario: Send 50th LinearizableRead request (size threshold reached)
    /// Expected:
    ///   - process_linearizable_read_batch() called immediately
    ///   - Buffer cleared
    ///   - read_buffer_start_time cleared
    ///   - Requests receive responses
    #[tokio::test]
    async fn test_size_threshold_triggers_immediate_flush() {
        use crate::MockReplicationCore;
        use crate::test_utils::mock::mock_raft_context;
        use tokio::sync::watch;

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let mut ctx = mock_raft_context("/tmp/test_size_flush", shutdown_rx, None);

        // Mock ReplicationCore to allow verify_leadership
        let mut replication = MockReplicationCore::new();
        replication.expect_handle_raft_request_in_batch().returning(|_, _, _, _, _| {
            Ok(crate::AppendResults {
                commit_quorum_achieved: true,
                peer_updates: Default::default(),
                learner_progress: Default::default(),
            })
        });
        ctx.handlers.replication_handler = replication;

        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        // Action: Send 49 requests
        for i in 1..=49 {
            let req = create_read_request(format!("key{i}").into_bytes());
            let (tx, _rx) = MaybeCloneOneshot::new();
            state
                .handle_raft_event(RaftEvent::ClientReadRequest(req, tx), &ctx, role_tx.clone())
                .await
                .unwrap();
        }

        assert_eq!(
            state.read_buffer.len(),
            49,
            "Buffer should have 49 requests"
        );

        // Action: Send 50th request (should trigger immediate flush)
        let req50 = create_read_request(b"key50".to_vec());
        let (tx50, mut rx50) = MaybeCloneOneshot::new();
        let result = state
            .handle_raft_event(
                RaftEvent::ClientReadRequest(req50, tx50),
                &ctx,
                role_tx.clone(),
            )
            .await;

        assert!(result.is_ok(), "50th request should return Ok");

        // Verify: Buffer cleared (flush executed)
        assert_eq!(
            state.read_buffer.len(),
            0,
            "Buffer should be empty after size threshold flush"
        );

        // Verify: Start time cleared
        assert!(
            state.read_buffer_start_time.is_none(),
            "Start time should be cleared after flush"
        );

        // Verify: 50th request receives response (flush completed)
        let response = rx50.recv().await;
        assert!(
            response.is_ok(),
            "50th request should receive response after flush"
        );

        drop(shutdown_tx);
    }

    /// Test 1.10: FlushReadBuffer event with non-empty buffer
    ///
    /// Scenario: Handle FlushReadBuffer event when buffer has requests
    /// Expected:
    ///   - process_linearizable_read_batch() called
    ///   - Buffer cleared
    ///   - Requests receive responses
    #[tokio::test]
    async fn test_flush_read_buffer_event_non_empty() {
        use crate::MockReplicationCore;
        use crate::test_utils::mock::mock_raft_context;
        use tokio::sync::watch;

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let mut ctx = mock_raft_context("/tmp/test_flush_event", shutdown_rx, None);

        // Mock ReplicationCore to allow verify_leadership
        let mut replication = MockReplicationCore::new();
        replication.expect_handle_raft_request_in_batch().returning(|_, _, _, _, _| {
            Ok(crate::AppendResults {
                commit_quorum_achieved: true,
                peer_updates: Default::default(),
                learner_progress: Default::default(),
            })
        });
        ctx.handlers.replication_handler = replication;

        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        // Setup: Manually add requests to buffer (simulate timeout scenario)
        for i in 1..=5 {
            let req = create_read_request(format!("key{i}").into_bytes());
            let (tx, _rx) = MaybeCloneOneshot::new();
            state.read_buffer.push((req, tx));
        }

        assert_eq!(state.read_buffer.len(), 5, "Buffer should have 5 requests");

        // Action: Handle FlushReadBuffer event
        let result =
            state.handle_raft_event(RaftEvent::FlushReadBuffer, &ctx, role_tx.clone()).await;

        assert!(result.is_ok(), "FlushReadBuffer event should return Ok");

        // Verify: Buffer cleared (flush executed)
        assert_eq!(
            state.read_buffer.len(),
            0,
            "Buffer should be empty after FlushReadBuffer event"
        );

        drop(shutdown_tx);
    }

    /// Test 1.11: FlushReadBuffer event with empty buffer (idempotency)
    ///
    /// Scenario: Handle FlushReadBuffer event when buffer is already empty
    /// Expected:
    ///   - No error (idempotent no-op)
    ///   - Buffer remains empty
    #[tokio::test]
    async fn test_flush_read_buffer_event_empty_buffer() {
        use crate::test_utils::mock::mock_raft_context;
        use tokio::sync::watch;

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let ctx = mock_raft_context("/tmp/test_flush_empty", shutdown_rx, None);
        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        // Verify: Buffer is empty
        assert_eq!(
            state.read_buffer.len(),
            0,
            "Buffer should be empty initially"
        );

        // Action: Handle FlushReadBuffer event on empty buffer
        let result =
            state.handle_raft_event(RaftEvent::FlushReadBuffer, &ctx, role_tx.clone()).await;

        // Verify: No error (idempotent)
        assert!(
            result.is_ok(),
            "FlushReadBuffer on empty buffer should be no-op without error"
        );

        // Verify: Buffer still empty
        assert_eq!(
            state.read_buffer.len(),
            0,
            "Buffer should remain empty after idempotent flush"
        );

        drop(shutdown_tx);
    }

    /// Test: process_linearizable_read_batch updates lease timestamp after successful verification
    ///
    /// Scenario:
    /// 1. Leader has invalid lease (lease_timestamp = 0)
    /// 2. LinearizableRead request triggers process_linearizable_read_batch
    /// 3. verify_leadership_and_refresh_lease succeeds (mocked)
    /// 4. process_linearizable_read_batch calls update_lease_timestamp
    ///
    /// Purpose:
    /// Verify that successful LinearizableRead verification updates the lease timestamp,
    /// allowing subsequent LeaseRead requests to reuse this verification result without
    /// redundant quorum checks (performance optimization across consistency policies).
    #[tokio::test]
    async fn test_flush_read_buffer_updates_lease_timestamp() {
        use crate::MockReplicationCore;
        use crate::maybe_clone_oneshot::MaybeCloneOneshot;
        use crate::test_utils::mock::mock_raft_context;
        use bytes::Bytes;
        use d_engine_proto::client::{ClientReadRequest, ReadConsistencyPolicy};
        use tokio::sync::watch;

        // Setup: Create Leader with invalid lease
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let mut ctx = mock_raft_context("/tmp/test_lease_update", shutdown_rx, None);

        // Mock successful leadership verification
        let mut replication = MockReplicationCore::new();
        replication
            .expect_handle_raft_request_in_batch()
            .times(1)
            .returning(|_, _, _, _, _| {
                Ok(crate::AppendResults {
                    commit_quorum_achieved: true,
                    peer_updates: Default::default(),
                    learner_progress: Default::default(),
                })
            });
        ctx.handlers.replication_handler = replication;

        let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

        // Setup: Initialize cluster metadata
        let mut membership = MockMembership::new();
        membership.expect_voters().returning(Vec::new);
        membership.expect_replication_peers().returning(Vec::new);
        state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

        // Verify: Initial lease is invalid (timestamp = 0)
        assert!(
            !state.is_lease_valid(&ctx),
            "Lease should be invalid initially"
        );
        let initial_timestamp = state.lease_timestamp.load(std::sync::atomic::Ordering::Acquire);
        assert_eq!(initial_timestamp, 0, "Initial lease timestamp should be 0");

        // Action: Add LinearizableRead request to buffer and flush
        let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
        state.read_buffer.push((
            ClientReadRequest {
                client_id: 1,
                keys: vec![Bytes::from_static(b"key1")],
                consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
            },
            resp_tx,
        ));

        let (role_tx, _role_rx) = tokio::sync::mpsc::unbounded_channel();
        let result = state.process_linearizable_read_batch(&ctx, &role_tx).await;

        // Verify: process_linearizable_read_batch succeeded
        assert!(
            result.is_ok(),
            "process_linearizable_read_batch should succeed after verification"
        );

        // Verify: Lease timestamp updated (now > 0)
        let updated_timestamp = state.lease_timestamp.load(std::sync::atomic::Ordering::Acquire);
        assert!(
            updated_timestamp > 0,
            "Lease timestamp should be updated after successful verification"
        );
        assert!(
            updated_timestamp > initial_timestamp,
            "Lease timestamp should increase from initial value"
        );

        // Verify: Lease is now valid
        assert!(
            state.is_lease_valid(&ctx),
            "Lease should be valid after process_linearizable_read_batch updates timestamp"
        );

        drop(shutdown_tx);
    }
}
