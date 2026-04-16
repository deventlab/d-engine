use tracing_test::traced_test;

use super::*;

#[tokio::test]
#[traced_test]
async fn test_record_failure_increments_count() {
    let (monitor, _rx) = RaftHealthMonitor::new(3);
    let node_id = 42;

    monitor.record_failure(node_id).await;
    monitor.record_failure(node_id).await;
    let count = monitor.failure_counts.get(&node_id).map(|v| *v).unwrap_or(0);
    assert_eq!(count, 2);
}

#[tokio::test]
#[traced_test]
async fn test_record_success_resets_count() {
    let (monitor, _rx) = RaftHealthMonitor::new(3);
    let node_id = 7;

    monitor.record_failure(node_id).await;
    monitor.record_failure(node_id).await;
    monitor.record_success(node_id).await;
    let count = monitor.failure_counts.get(&node_id);
    assert!(count.is_none());
}

#[tokio::test]
#[traced_test]
async fn test_zombie_signal_fires_at_threshold() {
    let (monitor, mut rx) = RaftHealthMonitor::new(2);
    let node1 = 1;
    let node2 = 2;

    // node1: one failure — below threshold, no signal
    monitor.record_failure(node1).await;
    assert!(rx.try_recv().is_err(), "no signal before threshold");

    // node2: one failure — below threshold
    monitor.record_failure(node2).await;
    assert!(rx.try_recv().is_err(), "no signal before threshold");

    // node1: second failure — hits threshold, signal fires
    monitor.record_failure(node1).await;
    let detected = rx.try_recv().expect("signal must fire at threshold");
    assert_eq!(detected, node1);

    // node1: third failure — already past threshold, no duplicate signal
    monitor.record_failure(node1).await;
    assert!(
        rx.try_recv().is_err(),
        "no duplicate signal after threshold"
    );
}

#[tokio::test]
#[traced_test]
async fn test_record_success_clears_failure_count() {
    let (monitor, mut rx) = RaftHealthMonitor::new(2);
    let node_id = 7;

    monitor.record_failure(node_id).await;
    monitor.record_success(node_id).await;

    // After success the counter is gone; one more failure does not cross threshold
    monitor.record_failure(node_id).await;
    assert!(rx.try_recv().is_err(), "counter reset by success");
}

// ============================================================================
// Bug 1 + Bug 3: zombie signal must re-trigger after the first signal is lost
//
// Scenario: ZombieDetected was consumed by a non-leader (no-op) or dropped due
// to channel backpressure.  Because record_failure() only fires at exactly
// new_count == threshold, the counter must be reset to 0 after each signal so
// that the next batch of failures can re-trigger the signal.
//
// Current behaviour (FAIL): counter stays at threshold; new_count > threshold
// forever; no second signal is ever emitted.
// Expected behaviour (PASS after fix): counter resets to 0; two more failures
// cross the threshold again and emit a second signal.
// ============================================================================
#[tokio::test]
#[traced_test]
async fn test_zombie_signal_re_triggers_after_signal_lost() {
    let (monitor, mut rx) = RaftHealthMonitor::new(2);
    let node_id = 1;

    // First threshold crossing — signal emitted and immediately consumed.
    monitor.record_failure(node_id).await;
    monitor.record_failure(node_id).await;
    assert_eq!(rx.try_recv().ok(), Some(node_id), "first signal must fire");

    // Simulate: signal was dropped (non-leader no-op or try_send full).
    // The peer is still failing; the counter should be back at 0 so that
    // two more failures re-cross the threshold.
    monitor.record_failure(node_id).await;
    monitor.record_failure(node_id).await;
    assert_eq!(
        rx.try_recv().ok(),
        Some(node_id),
        "zombie signal must re-trigger after counter reset — peer is still failing"
    );
}

// ============================================================================
// Bug 2: zombie signal must be invalidated when the peer recovers
//
// Scenario: ZombieDetected was queued in the channel, but before the bridge
// task forwards it to the Raft event loop the peer reconnects successfully.
// The bridge task calls is_zombie_valid() to avoid proposing BatchRemove for a
// healthy node.
//
// Expected: is_zombie_valid() returns false after record_success().
// ============================================================================
#[tokio::test]
#[traced_test]
async fn test_zombie_revoked_by_recovery() {
    let (monitor, mut rx) = RaftHealthMonitor::new(2);
    let node_id = 1;

    // Threshold crossed — signal queued in the channel.
    monitor.record_failure(node_id).await;
    monitor.record_failure(node_id).await;
    assert_eq!(rx.try_recv().ok(), Some(node_id), "signal must be queued");

    // Peer recovers before the bridge task forwards the signal.
    monitor.record_success(node_id).await;

    // Bridge task should now drop the stale signal.
    assert!(
        !monitor.is_zombie_valid(node_id),
        "zombie must be revoked by record_success — bridge task must not forward it"
    );
}
