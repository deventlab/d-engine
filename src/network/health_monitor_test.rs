use super::*;

#[tokio::test]
async fn test_record_failure_increments_count() {
    let monitor = RaftHealthMonitor::new(3);
    let node_id = 42;

    monitor.record_failure(node_id).await;
    monitor.record_failure(node_id).await;
    let count = monitor.failure_counts.get(&node_id).map(|v| *v).unwrap_or(0);
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_record_success_resets_count() {
    let monitor = RaftHealthMonitor::new(3);
    let node_id = 7;

    monitor.record_failure(node_id).await;
    monitor.record_failure(node_id).await;
    monitor.record_success(node_id).await;
    let count = monitor.failure_counts.get(&node_id);
    assert!(count.is_none());
}

#[tokio::test]
async fn test_get_zombie_candidates() {
    let monitor = RaftHealthMonitor::new(2);
    let node1 = 1;
    let node2 = 2;

    monitor.record_failure(node1).await;
    monitor.record_failure(node1).await;
    monitor.record_failure(node2).await;
    let zombies = monitor.get_zombie_candidates().await;
    assert!(zombies.contains(&node1));
    assert!(!zombies.contains(&node2));
    assert_eq!(zombies.len(), 1);
}
