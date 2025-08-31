use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tracing_test::traced_test;

use super::*;
use crate::proto::cluster::NodeMeta;
use crate::ConsensusError;
use crate::Error;
use crate::MembershipError;

fn create_test_node(id: u32) -> NodeMeta {
    NodeMeta {
        id,
        address: format!("node-{id}.test:8080"),
        ..Default::default()
    }
}

#[tokio::test]
#[traced_test]
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
#[traced_test]
async fn blocking_read_access() {
    let guard = MembershipGuard::new(vec![create_test_node(1)], 1);

    let result = guard
        .blocking_read(|state| state.nodes.get(&1).map(|n| n.address.clone()))
        .await;

    assert_eq!(result, Some("node-1.test:8080".to_string()));
}

#[tokio::test]
#[traced_test]
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
#[traced_test]
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
#[traced_test]
async fn update_node_not_found() {
    let guard = MembershipGuard::new(vec![create_test_node(1)], 1);

    let result = guard.update_node(99, |_| {}).await;

    assert!(matches!(
        result.unwrap_err(),
        Error::Consensus(ConsensusError::Membership(
            MembershipError::NoMetadataFoundForNode { node_id: 99 }
        ))
    ));
}

#[tokio::test]
#[traced_test]
async fn contains_node_checks_existence() {
    let guard = MembershipGuard::new(vec![create_test_node(1)], 1);

    assert!(guard.contains_node(1).await);
    assert!(!guard.contains_node(2).await);
}

#[tokio::test]
#[traced_test]
async fn concurrent_read_access() {
    let guard = Arc::new(MembershipGuard::new(vec![create_test_node(1)], 1));
    let mut handles = vec![];

    for _ in 0..10 {
        let guard_clone = Arc::clone(&guard);
        handles.push(tokio::spawn(
            async move { guard_clone.contains_node(1).await },
        ));
    }

    let results = futures::future::join_all(handles).await;
    for res in results {
        assert!(res.unwrap());
    }
}

#[tokio::test]
#[traced_test]
async fn write_operations_are_serialized() {
    let guard = Arc::new(MembershipGuard::new(vec![create_test_node(1)], 1));
    let (tx_started, rx_started) = oneshot::channel();

    let guard_clone = Arc::clone(&guard);

    let write_handle = tokio::spawn(async move {
        guard_clone
            .blocking_write(|state| {
                // Signal that we've entered the write operation
                tx_started.send(()).unwrap();

                // Simulate long write operation
                let start = std::time::Instant::now();
                while start.elapsed() < Duration::from_millis(100) {
                    std::hint::spin_loop();
                }
                // Update state
                state.cluster_conf_version = 2;
            })
            .await;
    });

    // Start measuring time BEFORE the second write
    let start = std::time::Instant::now();

    // Wait for write operation to start
    rx_started.await.unwrap();

    // Start second write in a separate task
    let second_write_handle = tokio::spawn({
        let guard = Arc::clone(&guard);
        async move {
            guard
                .blocking_write(|state| {
                    state.cluster_conf_version = 3;
                })
                .await;
        }
    });

    // Now wait for second write to complete
    second_write_handle.await.unwrap();
    let elapsed = start.elapsed();
    write_handle.await.unwrap();

    println!("Second write completed in: {}ms", elapsed.as_millis());

    // Verify write serialization - second write should have waited
    assert!(
        elapsed >= Duration::from_millis(90),
        "Expected min 90ms, got {}ms",
        elapsed.as_millis()
    );

    // Verify final state
    let version = guard.blocking_read(|state| state.cluster_conf_version).await;
    assert_eq!(version, 3);
}

#[tokio::test]
#[traced_test]
async fn reads_are_not_blocked_by_writes() {
    let guard = Arc::new(MembershipGuard::new(vec![create_test_node(1)], 1));
    let (tx_started, rx_started) = oneshot::channel();
    let write_handle = tokio::spawn({
        let guard = Arc::clone(&guard);
        async move {
            guard
                .blocking_write(|state| {
                    // Signal that write has started
                    tx_started.send(()).unwrap();

                    // Long operation
                    let start = std::time::Instant::now();
                    while start.elapsed() < Duration::from_millis(100) {
                        std::hint::spin_loop();
                    }
                    state.cluster_conf_version = 2;
                })
                .await;
        }
    });

    // Wait for write to start
    rx_started.await.unwrap();

    // Time 100 reads during the write
    let start = std::time::Instant::now();
    for _ in 0..100 {
        guard
            .blocking_read(|state| {
                assert_eq!(state.cluster_conf_version, 2);
            })
            .await;
    }
    let elapsed = start.elapsed();

    write_handle.await.unwrap();

    let avg_read_time = elapsed.as_micros() as f64 / 100.0;
    println!("Average read time: {avg_read_time:.2}μs");
    assert!(
        avg_read_time < 50.0,
        "Reads should be fast, average was {avg_read_time:.2}μs"
    );

    // Verify write completed
    let version = guard.blocking_read(|state| state.cluster_conf_version).await;
    assert_eq!(version, 2);
}
