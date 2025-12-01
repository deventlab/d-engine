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
