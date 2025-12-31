//! Leader election notification channel with guaranteed delivery.
//!
//! This module provides [`LeaderNotifier`], a wrapper around `tokio::sync::watch`
//! that guarantees the channel remains valid for the lifetime of the notifier.

use d_engine_proto::common::LeaderInfo;
use tokio::sync::watch;

/// A notification channel for leader election events.
///
/// This wrapper ensures the underlying watch channel remains valid by holding
/// an internal receiver. Without this, if all receivers are dropped, the sender
/// becomes unable to send notifications.
///
/// # Example
/// ```ignore
/// let notifier = LeaderNotifier::new();
/// raft_core.register_leader_change_listener(notifier.sender());
///
/// // Application can subscribe to leader changes
/// let mut rx = notifier.subscribe();
/// while rx.changed().await.is_ok() {
///     if let Some(info) = rx.borrow().as_ref() {
///         println!("Leader: {} (term {})", info.leader_id, info.term);
///     }
/// }
/// ```
pub struct LeaderNotifier {
    tx: watch::Sender<Option<LeaderInfo>>,
    _rx: watch::Receiver<Option<LeaderInfo>>,
}

impl LeaderNotifier {
    /// Creates a new leader notifier with no leader initially.
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(None);
        Self { tx, _rx: rx }
    }

    /// Returns a sender for Raft core to send leader change notifications.
    ///
    /// This sender can be cloned and registered with Raft core.
    pub fn sender(&self) -> watch::Sender<Option<LeaderInfo>> {
        self.tx.clone()
    }

    /// Subscribe to leader change notifications.
    ///
    /// Returns a new receiver that will be notified whenever the leader changes.
    /// Multiple subscribers can exist simultaneously.
    pub fn subscribe(&self) -> watch::Receiver<Option<LeaderInfo>> {
        self.tx.subscribe()
    }
}

impl Default for LeaderNotifier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_leader_notifier_basic() {
        let notifier = LeaderNotifier::new();
        let sender = notifier.sender();
        let mut rx = notifier.subscribe();

        // Initially no leader
        assert!(rx.borrow().is_none());

        // Send leader election
        let leader_info = LeaderInfo {
            leader_id: 1,
            term: 5,
        };
        sender.send(Some(leader_info)).unwrap();

        // Receiver should get notification
        rx.changed().await.unwrap();
        let received = *rx.borrow();
        assert_eq!(received.unwrap().leader_id, 1);
        assert_eq!(received.unwrap().term, 5);
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let notifier = LeaderNotifier::new();
        let sender = notifier.sender();
        let mut rx1 = notifier.subscribe();
        let mut rx2 = notifier.subscribe();

        let leader_info = LeaderInfo {
            leader_id: 2,
            term: 10,
        };
        sender.send(Some(leader_info)).unwrap();

        // Both receivers should get notification
        rx1.changed().await.unwrap();
        rx2.changed().await.unwrap();

        assert_eq!(rx1.borrow().unwrap().leader_id, 2);
        assert_eq!(rx2.borrow().unwrap().leader_id, 2);
    }
}
