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
// task forwards it to the inbound event loop the peer reconnects successfully.
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

// ============================================================================
// try_send migration (#428): record_failure() must never block the caller on
// channel capacity, and must not silently lose a signal or a retry
// opportunity when the channel is full or the receiver has shut down.
//
// zombie_tx is created with a fixed capacity of 64 (RaftHealthMonitor::new).
// These tests use threshold=1 so every record_failure() call for a distinct
// node_id immediately attempts a send, letting the tests fill/drain the
// channel deterministically without depending on that constant directly.
// ============================================================================

#[tokio::test]
#[traced_test]
async fn test_record_failure_uses_try_send_and_does_not_block() {
    let (monitor, mut rx) = RaftHealthMonitor::new(1);

    // With a synchronous send().await this call would block forever once the
    // channel is full, since nothing is draining rx concurrently. Reaching
    // this point at all (no test timeout) proves try_send is non-blocking.
    for node_id in 0..64u32 {
        monitor.record_failure(node_id).await;
    }

    // Sanity: all 64 signals were queued.
    let mut received = Vec::new();
    while let Ok(node_id) = rx.try_recv() {
        received.push(node_id);
    }
    assert_eq!(received.len(), 64, "all 64 signals must have been queued");
}

#[tokio::test]
#[traced_test]
async fn test_record_failure_channel_full_does_not_reset_counter() {
    let (monitor, mut rx) = RaftHealthMonitor::new(1);

    // Fill the channel to its capacity (64) with 64 distinct nodes, without
    // draining rx, so the next send attempt observes TrySendError::Full.
    for node_id in 0..64u32 {
        monitor.record_failure(node_id).await;
    }

    let overflow_node = 999;
    monitor.record_failure(overflow_node).await;

    // The overflow signal must NOT have been queued (channel was full).
    let queued: Vec<u32> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert!(
        !queued.contains(&overflow_node),
        "signal must not be queued when the channel is full"
    );

    // The counter must NOT have been reset — record_failure() must retry the
    // send on the next failure instead of losing the signal permanently.
    let count = monitor.failure_counts.get(&overflow_node).map(|v| *v);
    assert_eq!(
        count,
        Some(1),
        "counter must be preserved (not reset) when try_send hits Full"
    );
}

#[tokio::test]
#[traced_test]
async fn test_record_failure_retries_send_after_channel_has_space() {
    let (monitor, mut rx) = RaftHealthMonitor::new(1);

    // Fill the channel completely.
    for node_id in 0..64u32 {
        monitor.record_failure(node_id).await;
    }

    let overflow_node = 999;
    // First attempt: channel full, counter preserved (see previous test).
    monitor.record_failure(overflow_node).await;

    // Free up exactly one slot.
    rx.try_recv().expect("channel must have queued signals to drain");

    // Retry: this failure must now succeed in sending, since the counter was
    // never reset and >= threshold still holds.
    monitor.record_failure(overflow_node).await;

    let queued: Vec<u32> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert!(
        queued.contains(&overflow_node),
        "signal for the overflow node must be sent once channel space is available"
    );
    assert_eq!(
        monitor.failure_counts.get(&overflow_node).map(|v| *v),
        Some(0),
        "counter must be reset once the retried send succeeds"
    );
}

#[tokio::test]
#[traced_test]
async fn test_record_failure_closed_receiver_does_not_panic_or_reset() {
    let (monitor, rx) = RaftHealthMonitor::new(1);
    let node_id = 1;

    // Simulate the node shutting down: the bridge task's receiver is dropped
    // while record_failure() may still be in flight from another task.
    drop(rx);

    // Must not panic despite the closed channel.
    monitor.record_failure(node_id).await;

    // Counter is not reset on a Closed send — there's no one left to notify,
    // but record_failure() must still behave predictably rather than panic
    // or silently assume success.
    assert_eq!(
        monitor.failure_counts.get(&node_id).map(|v| *v),
        Some(1),
        "counter must not be reset when the receiver is closed"
    );
}
