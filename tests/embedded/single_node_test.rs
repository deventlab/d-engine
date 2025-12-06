use std::time::Duration;
use tracing_test::traced_test;

use d_engine_server::EmbeddedEngine;

const TEST_DIR: &str = "embedded/single_node";

/// Test single-node EmbeddedEngine basic lifecycle
#[tokio::test]
#[traced_test]
#[cfg(feature = "rocksdb")]
async fn test_single_node_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = format!("./db/{TEST_DIR}");

    // Clean up previous test data
    let _ = tokio::fs::remove_dir_all(&data_dir).await;

    // Start embedded engine with RocksDB
    let engine = EmbeddedEngine::with_rocksdb(&data_dir, None).await?;

    // Wait for node initialization
    engine.ready().await;

    // Single-node should elect itself as leader immediately
    let leader_info = engine.wait_leader(Duration::from_secs(2)).await?;
    assert_eq!(
        leader_info.leader_id, 1,
        "Single node should elect itself as leader"
    );
    assert_eq!(leader_info.term, 1, "First term should be 1");

    // Test basic KV operations
    let client = engine.client();

    client.put(b"test-key".to_vec(), b"test-value".to_vec()).await?;
    let value = client.get(b"test-key".to_vec()).await?;
    assert_eq!(value, Some(b"test-value".to_vec()));

    client.delete(b"test-key".to_vec()).await?;
    let deleted = client.get(b"test-key".to_vec()).await?;
    assert_eq!(deleted, None);

    // Graceful shutdown
    engine.stop().await?;

    Ok(())
}

/// Test leader notification mechanism
#[tokio::test]
#[traced_test]
#[cfg(feature = "rocksdb")]
async fn test_leader_notification() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = format!("./db/{TEST_DIR}_notify");

    let _ = tokio::fs::remove_dir_all(&data_dir).await;

    let engine = EmbeddedEngine::with_rocksdb(&data_dir, None).await?;
    engine.ready().await;

    // Subscribe to leader changes
    let mut leader_rx = engine.leader_notifier();

    // Wait for first leader election event
    tokio::time::timeout(Duration::from_secs(2), leader_rx.changed())
        .await
        .expect("Should receive leader election event")?;

    let leader = leader_rx.borrow().clone();
    assert!(leader.is_some(), "Leader should be elected");

    let leader_info = leader.unwrap();
    assert_eq!(leader_info.leader_id, 1);

    engine.stop().await?;

    Ok(())
}

/// Test data persistence across restarts
#[tokio::test]
#[traced_test]
#[cfg(feature = "rocksdb")]
async fn test_data_persistence() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = format!("./db/{TEST_DIR}_persist");

    let _ = tokio::fs::remove_dir_all(&data_dir).await;

    // First session: write data
    {
        let engine = EmbeddedEngine::with_rocksdb(&data_dir, None).await?;
        engine.ready().await;
        engine.wait_leader(Duration::from_secs(2)).await?;

        engine.client().put(b"persist-key".to_vec(), b"persist-value".to_vec()).await?;
        engine.stop().await?;
    }

    // Second session: verify data still exists
    {
        let engine = EmbeddedEngine::with_rocksdb(&data_dir, None).await?;
        engine.ready().await;
        engine.wait_leader(Duration::from_secs(2)).await?;

        let value = engine.client().get(b"persist-key".to_vec()).await?;
        assert_eq!(
            value,
            Some(b"persist-value".to_vec()),
            "Data should persist across restarts"
        );

        engine.stop().await?;
    }

    Ok(())
}
