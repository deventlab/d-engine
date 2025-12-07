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
mod notify_leader_elected_tests {
    use super::super::{Raft, RoleEvent};
    use crate::test_utils::{MockBuilder, MockTypeConfig};
    use tokio::sync::mpsc;
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_notify_leader_elected_event_handling() {
        // Test that NotifyLeaderElected event triggers leader change notification
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

        // Register leader change listener
        let (leader_tx, mut leader_rx) = mpsc::unbounded_channel();
        raft.register_leader_change_listener(leader_tx);

        // Send NotifyLeaderElected event
        let leader_id = 3;
        let term = 5;
        raft.handle_role_event(RoleEvent::NotifyLeaderElected(leader_id, term))
            .await
            .expect("Should handle NotifyLeaderElected");

        // Verify notification was sent
        let (notified_leader, notified_term) =
            leader_rx.try_recv().expect("Should receive leader change notification");
        assert_eq!(notified_leader, Some(leader_id));
        assert_eq!(notified_term, term);
    }

    #[tokio::test]
    async fn test_notify_leader_elected_no_state_change() {
        // Test that NotifyLeaderElected does NOT change node role
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

        let initial_role = raft.role.as_i32();

        // Send NotifyLeaderElected event
        raft.handle_role_event(RoleEvent::NotifyLeaderElected(3, 5))
            .await
            .expect("Should handle NotifyLeaderElected");

        // Verify role unchanged (still Follower)
        assert_eq!(raft.role.as_i32(), initial_role);
    }

    #[tokio::test]
    async fn test_notify_leader_elected_multiple_listeners() {
        // Test that multiple listeners receive notification
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

        // Register multiple listeners
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        raft.register_leader_change_listener(tx1);
        raft.register_leader_change_listener(tx2);

        // Send NotifyLeaderElected event
        let leader_id = 2;
        let term = 10;
        raft.handle_role_event(RoleEvent::NotifyLeaderElected(leader_id, term))
            .await
            .expect("Should handle NotifyLeaderElected");

        // Verify all listeners receive notification
        let (l1, t1) = rx1.try_recv().expect("Listener 1 should receive");
        let (l2, t2) = rx2.try_recv().expect("Listener 2 should receive");

        assert_eq!(l1, Some(leader_id));
        assert_eq!(t1, term);
        assert_eq!(l2, Some(leader_id));
        assert_eq!(t2, term);
    }

    #[tokio::test]
    async fn test_notify_leader_elected_with_different_terms() {
        // Test notifications with different terms
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut raft: Raft<MockTypeConfig> = MockBuilder::new(graceful_rx).build_raft();

        let (leader_tx, mut leader_rx) = mpsc::unbounded_channel();
        raft.register_leader_change_listener(leader_tx);

        // Send first notification (term 5, leader 2)
        raft.handle_role_event(RoleEvent::NotifyLeaderElected(2, 5))
            .await
            .expect("Should handle first notification");

        let (l1, t1) = leader_rx.try_recv().expect("Should receive first");
        assert_eq!(l1, Some(2));
        assert_eq!(t1, 5);

        // Send second notification (term 10, leader 3 - re-election)
        raft.handle_role_event(RoleEvent::NotifyLeaderElected(3, 10))
            .await
            .expect("Should handle second notification");

        let (l2, t2) = leader_rx.try_recv().expect("Should receive second");
        assert_eq!(l2, Some(3));
        assert_eq!(t2, 10);
    }
}

#[cfg(test)]
mod follower_leader_discovery_tests {
    use super::super::RoleEvent;
    use tokio::sync::mpsc;

    #[test]
    fn test_role_event_notify_leader_elected_creation() {
        // Test creating NotifyLeaderElected event
        let leader_id = 5;
        let term = 20;
        let event = RoleEvent::NotifyLeaderElected(leader_id, term);

        // Verify we can match on it
        match event {
            RoleEvent::NotifyLeaderElected(id, t) => {
                assert_eq!(id, leader_id);
                assert_eq!(t, term);
            }
            _ => panic!("Should be NotifyLeaderElected variant"),
        }
    }

    #[test]
    fn test_role_event_notify_leader_elected_channel_send() {
        // Test sending NotifyLeaderElected through channel
        let (tx, mut rx) = mpsc::unbounded_channel::<RoleEvent>();

        let leader_id = 3;
        let term = 15;
        tx.send(RoleEvent::NotifyLeaderElected(leader_id, term)).expect("Should send");

        match rx.try_recv().expect("Should receive") {
            RoleEvent::NotifyLeaderElected(id, t) => {
                assert_eq!(id, leader_id);
                assert_eq!(t, term);
            }
            _ => panic!("Should receive NotifyLeaderElected"),
        }
    }

    #[test]
    fn test_role_event_notify_leader_elected_vs_become_follower() {
        // Test difference between NotifyLeaderElected and BecomeFollower
        let (tx, mut rx) = mpsc::unbounded_channel::<RoleEvent>();

        // NotifyLeaderElected: notification without state transition
        tx.send(RoleEvent::NotifyLeaderElected(3, 10))
            .expect("Should send NotifyLeaderElected");

        // BecomeFollower: state transition with optional leader
        tx.send(RoleEvent::BecomeFollower(Some(3))).expect("Should send BecomeFollower");

        // Verify both can be distinguished
        match rx.try_recv().expect("Should receive first") {
            RoleEvent::NotifyLeaderElected(id, term) => {
                assert_eq!(id, 3);
                assert_eq!(term, 10);
            }
            _ => panic!("First should be NotifyLeaderElected"),
        }

        match rx.try_recv().expect("Should receive second") {
            RoleEvent::BecomeFollower(leader_id) => {
                assert_eq!(leader_id, Some(3));
            }
            _ => panic!("Second should be BecomeFollower"),
        }
    }
}
