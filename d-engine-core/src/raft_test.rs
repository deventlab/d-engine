//! Unit tests for Raft leader change notification

#[cfg(test)]
mod leader_change_tests {
    use tokio::sync::mpsc;

    #[test]
    fn test_leader_change_listener_registration() {
        // Test that we can create channels for leader change notifications
        let (tx, mut rx) = mpsc::unbounded_channel::<(Option<u32>, u64)>();

        // Simulate sending a notification
        tx.send((Some(1), 5)).unwrap();

        // Verify we can receive it
        let (leader_id, term) = rx.try_recv().expect("Should receive notification");
        assert_eq!(leader_id, Some(1));
        assert_eq!(term, 5);
    }

    #[test]
    fn test_multiple_listeners() {
        // Test broadcasting to multiple listeners
        let (tx1, mut rx1) = mpsc::unbounded_channel::<(Option<u32>, u64)>();
        let (tx2, mut rx2) = mpsc::unbounded_channel::<(Option<u32>, u64)>();

        // Simulate sending to both
        tx1.send((Some(2), 10)).unwrap();
        tx2.send((Some(2), 10)).unwrap();

        // Verify both receive
        let (leader1, term1) = rx1.try_recv().expect("Listener 1 should receive");
        let (leader2, term2) = rx2.try_recv().expect("Listener 2 should receive");

        assert_eq!(leader1, Some(2));
        assert_eq!(term1, 10);
        assert_eq!(leader2, Some(2));
        assert_eq!(term2, 10);
    }

    #[test]
    fn test_no_leader_notification() {
        // Test sending None for leader_id (candidate state)
        let (tx, mut rx) = mpsc::unbounded_channel::<(Option<u32>, u64)>();

        tx.send((None, 15)).unwrap();

        let (leader_id, term) = rx.try_recv().expect("Should receive notification");
        assert_eq!(leader_id, None);
        assert_eq!(term, 15);
    }

    #[test]
    fn test_channel_closed() {
        // Test that sending fails when receiver is dropped
        let (tx, rx) = mpsc::unbounded_channel::<(Option<u32>, u64)>();

        drop(rx);

        let result = tx.send((Some(1), 5));
        assert!(result.is_err(), "Send should fail when receiver is dropped");
    }
}

#[cfg(test)]
mod leader_discovered_tests {
    use super::super::{Raft, RoleEvent};
    use crate::test_utils::{MockBuilder, MockTypeConfig};
    use tokio::sync::mpsc;
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_leader_discovered_event_handling() {
        // Test that LeaderDiscovered event triggers leader change notification
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

        // Register leader change listener
        let (leader_tx, mut leader_rx) = mpsc::unbounded_channel();
        raft.register_leader_change_listener(leader_tx);

        // Send LeaderDiscovered event
        let leader_id = 3;
        let term = 5;
        raft.handle_role_event(RoleEvent::LeaderDiscovered(leader_id, term))
            .await
            .expect("Should handle LeaderDiscovered");

        // Verify notification was sent
        let (notified_leader, notified_term) =
            leader_rx.try_recv().expect("Should receive leader change notification");
        assert_eq!(notified_leader, Some(leader_id));
        assert_eq!(notified_term, term);
    }

    #[tokio::test]
    async fn test_leader_discovered_no_state_change() {
        // Test that LeaderDiscovered does NOT change node role
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

        let initial_role = raft.role.as_i32();

        // Send LeaderDiscovered event
        raft.handle_role_event(RoleEvent::LeaderDiscovered(3, 5))
            .await
            .expect("Should handle LeaderDiscovered");

        // Verify role unchanged (still Follower)
        assert_eq!(raft.role.as_i32(), initial_role);
    }

    #[tokio::test]
    async fn test_leader_discovered_multiple_listeners() {
        // Test that multiple listeners receive notification
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

        // Register multiple listeners
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        raft.register_leader_change_listener(tx1);
        raft.register_leader_change_listener(tx2);

        // Send LeaderDiscovered event
        let leader_id = 2;
        let term = 10;
        raft.handle_role_event(RoleEvent::LeaderDiscovered(leader_id, term))
            .await
            .expect("Should handle LeaderDiscovered");

        // Verify all listeners receive notification
        let (l1, t1) = rx1.try_recv().expect("Listener 1 should receive");
        let (l2, t2) = rx2.try_recv().expect("Listener 2 should receive");

        assert_eq!(l1, Some(leader_id));
        assert_eq!(t1, term);
        assert_eq!(l2, Some(leader_id));
        assert_eq!(t2, term);
    }

    #[tokio::test]
    async fn test_leader_discovered_no_deduplication() {
        // Test that mpsc channel receives all notifications (no auto-deduplication)
        // Note: If deduplication is needed, consumers should use watch channels
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

        let (leader_tx, mut leader_rx) = mpsc::unbounded_channel();
        raft.register_leader_change_listener(leader_tx);

        // Send same leader multiple times
        raft.handle_role_event(RoleEvent::LeaderDiscovered(2, 5))
            .await
            .expect("Should handle first");
        raft.handle_role_event(RoleEvent::LeaderDiscovered(2, 5))
            .await
            .expect("Should handle second (duplicate)");

        // Should receive both notifications (mpsc does not deduplicate)
        let (l1, t1) = leader_rx.try_recv().expect("Should receive first");
        assert_eq!(l1, Some(2));
        assert_eq!(t1, 5);

        let (l2, t2) = leader_rx.try_recv().expect("Should receive second");
        assert_eq!(l2, Some(2));
        assert_eq!(t2, 5);
    }

    #[test]
    fn test_role_event_leader_discovered_creation() {
        // Test creating LeaderDiscovered event
        let leader_id = 5;
        let term = 20;
        let event = RoleEvent::LeaderDiscovered(leader_id, term);

        // Verify we can match on it
        match event {
            RoleEvent::LeaderDiscovered(id, t) => {
                assert_eq!(id, leader_id);
                assert_eq!(t, term);
            }
            _ => panic!("Should be LeaderDiscovered variant"),
        }
    }
}
