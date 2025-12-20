use d_engine_server::api::EmbeddedEngine;
use std::time::Duration;
use tracing_test::traced_test;

#[allow(dead_code)]
const TEST_DIR: &str = "embedded/single_node";

/// Test single-node EmbeddedEngine basic lifecycle
#[tokio::test]
#[cfg(feature = "rocksdb")]
async fn test_single_node_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = format!("./db/{TEST_DIR}");

    // Clean up previous test data
    if tokio::fs::metadata(&data_dir).await.is_ok() {
        tokio::fs::remove_dir_all(&data_dir).await?;
    }

    // Configure single-node cluster via environment variables
    // Safe in test context: tests run in isolated processes
    unsafe {
        std::env::set_var("RAFT__CLUSTER__NODE_ID", "1");
        std::env::set_var("RAFT__CLUSTER__LISTEN_ADDRESS", "127.0.0.1:9001");
    }

    // Start embedded engine with RocksDB
    let engine = EmbeddedEngine::with_rocksdb(&data_dir, None).await?;

    // Clean up environment variables immediately
    unsafe {
        std::env::remove_var("RAFT__CLUSTER__NODE_ID");
        std::env::remove_var("RAFT__CLUSTER__LISTEN_ADDRESS");
    }

    // Single-node should elect itself as leader
    let leader_info = engine.wait_ready(Duration::from_secs(5)).await?;
    assert_eq!(
        leader_info.leader_id, 1,
        "Single node should elect itself as leader"
    );
    // Term may be > 1 due to election timeouts during startup
    assert!(leader_info.term >= 1, "Term should be at least 1");

    // Test basic KV operations
    let client = engine.client();

    let put_result = client.put(b"test-key".to_vec(), b"test-value".to_vec()).await;
    assert!(
        put_result.is_ok(),
        "Put operation failed: {:?}",
        put_result.err()
    );

    // Small delay to ensure data is committed
    tokio::time::sleep(Duration::from_millis(100)).await;

    let value = client.get_linearizable(b"test-key".to_vec()).await?;
    assert_eq!(
        value.as_deref(),
        Some(b"test-value".as_ref()),
        "Get after put should return the value"
    );

    client.delete(b"test-key".to_vec()).await?;

    // Small delay to ensure deletion is committed
    tokio::time::sleep(Duration::from_millis(100)).await;

    let deleted = client.get_linearizable(b"test-key".to_vec()).await?;
    assert_eq!(deleted, None, "Get after delete should return None");

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

    if tokio::fs::metadata(&data_dir).await.is_ok() {
        tokio::fs::remove_dir_all(&data_dir).await?;
    }

    // Configure single-node cluster
    unsafe {
        std::env::set_var("RAFT__CLUSTER__NODE_ID", "1");
        std::env::set_var("RAFT__CLUSTER__LISTEN_ADDRESS", "127.0.0.1:9002");
    }

    let engine = EmbeddedEngine::with_rocksdb(&data_dir, None).await?;

    unsafe {
        std::env::remove_var("RAFT__CLUSTER__NODE_ID");
        std::env::remove_var("RAFT__CLUSTER__LISTEN_ADDRESS");
    }

    // Wait for leader election
    let leader_info = engine.wait_ready(Duration::from_secs(5)).await?;
    assert_eq!(
        leader_info.leader_id, 1,
        "Single node should elect itself as leader"
    );

    // Subscribe to leader changes AFTER election
    let leader_rx = engine.leader_change_notifier();

    // Current value should already show leader elected
    let leader = *leader_rx.borrow();
    assert!(leader.is_some(), "Leader should already be elected");

    engine.stop().await?;

    Ok(())
}

/// Test data persistence across restarts
#[tokio::test]
#[traced_test]
#[cfg(feature = "rocksdb")]
async fn test_data_persistence() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = format!("./db/{TEST_DIR}_persist");

    if tokio::fs::metadata(&data_dir).await.is_ok() {
        tokio::fs::remove_dir_all(&data_dir).await?;
    }

    // Configure single-node cluster
    unsafe {
        std::env::set_var("RAFT__CLUSTER__NODE_ID", "1");
        std::env::set_var("RAFT__CLUSTER__LISTEN_ADDRESS", "127.0.0.1:9003");
    }

    // First session: write data
    {
        let engine = EmbeddedEngine::with_rocksdb(&data_dir, None).await?;
        engine.wait_ready(Duration::from_secs(5)).await?;

        engine.client().put(b"persist-key".to_vec(), b"persist-value".to_vec()).await?;

        // Small delay to ensure data is committed before shutdown
        tokio::time::sleep(Duration::from_millis(100)).await;

        engine.stop().await?;
    }

    // Small delay between sessions
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second session: verify data still exists
    {
        let engine = EmbeddedEngine::with_rocksdb(&data_dir, None).await?;
        engine.wait_ready(Duration::from_secs(5)).await?;

        let value = engine.client().get_linearizable(b"persist-key".to_vec()).await?;
        assert_eq!(
            value.as_deref(),
            Some(b"persist-value".as_ref()),
            "Data should persist across restarts"
        );

        engine.stop().await?;
    }

    // Clean up
    unsafe {
        std::env::remove_var("RAFT__CLUSTER__NODE_ID");
        std::env::remove_var("RAFT__CLUSTER__LISTEN_ADDRESS");
    }

    Ok(())
}
