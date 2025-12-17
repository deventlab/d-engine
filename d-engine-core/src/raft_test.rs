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
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_leader_discovered_event_handling() {
        // Test that LeaderDiscovered event triggers leader change notification
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

        // Register leader change listener
        let (leader_tx, mut leader_rx) = watch::channel(None);
        raft.register_leader_change_listener(leader_tx);

        // Send LeaderDiscovered event
        let leader_id = 3;
        let term = 5;
        raft.handle_role_event(RoleEvent::LeaderDiscovered(leader_id, term))
            .await
            .expect("Should handle LeaderDiscovered");

        // Verify notification was sent
        leader_rx.changed().await.expect("Should receive change notification");
        let leader_info = *leader_rx.borrow();
        assert!(leader_info.is_some());
        let info = leader_info.unwrap();
        assert_eq!(info.leader_id, leader_id);
        assert_eq!(info.term, term);
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
        // Test that multiple subscribers can receive notifications via watch::Sender::subscribe()
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

        // Register leader change listener
        let (tx, _rx) = watch::channel(None);
        raft.register_leader_change_listener(tx.clone());

        // Create multiple subscribers
        let mut rx1 = tx.subscribe();
        let mut rx2 = tx.subscribe();

        // Send LeaderDiscovered event
        let leader_id = 2;
        let term = 10;
        raft.handle_role_event(RoleEvent::LeaderDiscovered(leader_id, term))
            .await
            .expect("Should handle LeaderDiscovered");

        // Verify all subscribers receive notification
        rx1.changed().await.expect("Subscriber 1 should receive");
        rx2.changed().await.expect("Subscriber 2 should receive");

        let info1 = (*rx1.borrow()).unwrap();
        let info2 = (*rx2.borrow()).unwrap();

        assert_eq!(info1.leader_id, leader_id);
        assert_eq!(info1.term, term);
        assert_eq!(info2.leader_id, leader_id);
        assert_eq!(info2.term, term);
    }

    #[tokio::test]
    async fn test_leader_discovered_with_deduplication() {
        // Test that watch channel automatically deduplicates identical notifications
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

        let (leader_tx, mut leader_rx) = watch::channel(None);
        raft.register_leader_change_listener(leader_tx);

        // Send same leader multiple times
        raft.handle_role_event(RoleEvent::LeaderDiscovered(2, 5))
            .await
            .expect("Should handle first");
        raft.handle_role_event(RoleEvent::LeaderDiscovered(2, 5))
            .await
            .expect("Should handle second (duplicate)");

        // Should receive only one notification (watch channel deduplicates)
        leader_rx.changed().await.expect("Should receive first change");
        let info = (*leader_rx.borrow()).unwrap();
        assert_eq!(info.leader_id, 2);
        assert_eq!(info.term, 5);

        // No second change notification because value is identical
        tokio::select! {
            _ = leader_rx.changed() => {
                panic!("Should not receive duplicate notification");
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                // Expected: timeout because no new change
            }
        }
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
