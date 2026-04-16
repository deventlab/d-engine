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
