//! Unit tests for Raft leader change notification

#[cfg(test)]
mod leader_change_tests {
    use tokio::sync::mpsc;

    use crate::RoleEvent;
    use crate::test_utils::mock_raft_for_test;

    #[tokio::test]
    async fn test_register_leader_change_listener() {
        // Create test Raft instance
        let mut raft = mock_raft_for_test(1).await;

        // Register listener
        let (tx, mut rx) = mpsc::unbounded_channel();
        raft.register_leader_change_listener(tx);

        // Trigger leader change via notify_leader_change
        raft.notify_leader_change(Some(1), 5);

        // Verify notification received
        let (leader_id, term) = rx.recv().await.expect("Should receive notification");
        assert_eq!(leader_id, Some(1));
        assert_eq!(term, 5);
    }

    #[tokio::test]
    async fn test_multiple_listeners() {
        // Create test Raft instance
        let mut raft = mock_raft_for_test(1).await;

        // Register multiple listeners
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        raft.register_leader_change_listener(tx1);
        raft.register_leader_change_listener(tx2);

        // Trigger leader change
        raft.notify_leader_change(Some(2), 10);

        // Verify both listeners receive notification
        let (leader1, term1) = rx1.recv().await.expect("Listener 1 should receive");
        let (leader2, term2) = rx2.recv().await.expect("Listener 2 should receive");

        assert_eq!(leader1, Some(2));
        assert_eq!(term1, 10);
        assert_eq!(leader2, Some(2));
        assert_eq!(term2, 10);
    }

    #[tokio::test]
    async fn test_become_leader_triggers_notification() {
        // Create test Raft instance
        let mut raft = mock_raft_for_test(1).await;

        // Register listener
        let (tx, mut rx) = mpsc::unbounded_channel();
        raft.register_leader_change_listener(tx);

        // Trigger BecomeLeader event
        raft.handle_role_event(RoleEvent::BecomeLeader)
            .await
            .expect("Should handle BecomeLeader");

        // Verify notification received
        let (leader_id, term) = rx.recv().await.expect("Should receive notification");
        assert_eq!(leader_id, Some(1)); // Node 1 becomes leader
        assert!(term > 0); // Term should be set
    }

    #[tokio::test]
    async fn test_become_follower_triggers_notification() {
        // Create test Raft instance
        let mut raft = mock_raft_for_test(1).await;

        // Register listener
        let (tx, mut rx) = mpsc::unbounded_channel();
        raft.register_leader_change_listener(tx);

        // Trigger BecomeFollower event with known leader
        raft.handle_role_event(RoleEvent::BecomeFollower(Some(2)))
            .await
            .expect("Should handle BecomeFollower");

        // Verify notification received
        let (leader_id, term) = rx.recv().await.expect("Should receive notification");
        assert_eq!(leader_id, Some(2)); // Leader is node 2
        assert!(term > 0);
    }

    #[tokio::test]
    async fn test_become_candidate_no_leader() {
        // Create test Raft instance
        let mut raft = mock_raft_for_test(1).await;

        // Register listener
        let (tx, mut rx) = mpsc::unbounded_channel();
        raft.register_leader_change_listener(tx);

        // Trigger BecomeCandidate event
        raft.handle_role_event(RoleEvent::BecomeCandidate)
            .await
            .expect("Should handle BecomeCandidate");

        // Verify notification received with None (no leader during election)
        let (leader_id, term) = rx.recv().await.expect("Should receive notification");
        assert_eq!(leader_id, None); // No leader during candidate state
        assert!(term > 0);
    }
}
