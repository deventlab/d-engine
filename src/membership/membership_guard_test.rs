use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::proto::cluster::NodeMeta;
use crate::ConsensusError;
use crate::Error;
use crate::MembershipError;

fn create_test_node(id: u32) -> NodeMeta {
    NodeMeta {
        id,
        address: format!("node-{}.test:8080", id),
        ..Default::default()
    }
}

#[tokio::test]
async fn initial_state_correctly_set() {
    let nodes = vec![create_test_node(1), create_test_node(2)];
    let guard = MembershipGuard::new(nodes.clone(), 100);

    guard
        .blocking_read(|state| {
            assert_eq!(state.nodes.len(), 2);
            assert_eq!(state.nodes.get(&1).unwrap().address, "node-1.test:8080");
            assert_eq!(state.nodes.get(&2).unwrap().address, "node-2.test:8080");
            assert_eq!(state.cluster_conf_version, 100);
        })
        .await;
}

#[tokio::test]
async fn blocking_read_access() {
    let guard = MembershipGuard::new(vec![create_test_node(1)], 1);

    let result = guard
        .blocking_read(|state| state.nodes.get(&1).map(|n| n.address.clone()))
        .await;

    assert_eq!(result, Some("node-1.test:8080".to_string()));
}

#[tokio::test]
async fn blocking_write_updates_state() {
    let guard = MembershipGuard::new(vec![create_test_node(1)], 1);

    guard
        .blocking_write(|state| {
            state.cluster_conf_version = 200;
            state.nodes.insert(2, create_test_node(2));
        })
        .await;

    guard
        .blocking_read(|state| {
            assert_eq!(state.cluster_conf_version, 200);
            assert_eq!(state.nodes.len(), 2);
        })
        .await;
}

#[tokio::test]
async fn update_node_success() {
    let guard = MembershipGuard::new(vec![create_test_node(1)], 1);

    let result = guard
        .update_node(1, |node| {
            node.address = "updated.test:9090".to_string();
        })
        .await;

    assert!(result.is_ok());
    guard
        .blocking_read(|state| {
            assert_eq!(state.nodes.get(&1).unwrap().address, "updated.test:9090");
        })
        .await;
}

#[tokio::test]
async fn update_node_not_found() {
    let guard = MembershipGuard::new(vec![create_test_node(1)], 1);

    let result = guard.update_node(99, |_| {}).await;

    assert!(matches!(
        result.unwrap_err(),
        Error::Consensus(ConsensusError::Membership(MembershipError::NoMetadataFoundForNode {
            node_id: 99
        }))
    ));
}

#[tokio::test]
async fn contains_node_checks_existence() {
    let guard = MembershipGuard::new(vec![create_test_node(1)], 1);

    assert!(guard.contains_node(1).await);
    assert!(!guard.contains_node(2).await);
}

#[tokio::test]
async fn concurrent_read_access() {
    let guard = Arc::new(MembershipGuard::new(vec![create_test_node(1)], 1));
    let mut handles = vec![];

    for _ in 0..10 {
        let guard_clone = Arc::clone(&guard);
        handles.push(tokio::spawn(async move { guard_clone.contains_node(1).await }));
    }

    let results = futures::future::join_all(handles).await;
    for res in results {
        assert!(res.unwrap());
    }
}

#[tokio::test]
async fn write_lock_blocks_other_access() {
    let guard = Arc::new(MembershipGuard::new(vec![create_test_node(1)], 1));
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let guard_clone = Arc::clone(&guard);

    let write_handle = tokio::spawn(async move {
        // Acquire the write lock directly to hold it across async sleep
        let mut inner = guard_clone.inner.write().await;
        tx.send(()).await.unwrap(); // Signal that the lock is acquired

        // Use async sleep to yield control while holding the lock
        tokio::time::sleep(Duration::from_millis(100)).await;
        inner.cluster_conf_version = 2;

        // Lock is released when `inner` is dropped here
    });

    // Give write thread time to acquire lock
    rx.recv().await.unwrap();

    let start = std::time::Instant::now();
    guard.blocking_read(|_| {}).await;
    let elapsed = start.elapsed();

    write_handle.await.unwrap();

    println!("Write completed: {}", elapsed.as_millis());
    // Should have waited at least 90ms for write to complete
    assert!(elapsed >= Duration::from_millis(90));
}
