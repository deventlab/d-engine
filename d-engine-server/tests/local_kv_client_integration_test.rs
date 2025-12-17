//! LocalKvClient Integration Tests
//!
//! Tests for LocalKvClient in embedded mode:
//! - Basic CRUD operations with real Node
//! - Error handling
//! - Concurrent operations

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_server::node::RaftTypeConfig;
use d_engine_server::{FileStateMachine, FileStorageEngine, NodeBuilder};
use tokio::sync::watch;

/// Type alias for our test node
type TestNode = Arc<d_engine_server::Node<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>;

/// Helper to create a test node with LocalKvClient
async fn create_test_node(test_name: &str) -> (TestNode, tokio::sync::watch::Sender<()>) {
    use d_engine_core::ClusterConfig;
    use d_engine_proto::common::{NodeRole, NodeStatus};
    use d_engine_proto::server::cluster::NodeMeta;

    let db_path = PathBuf::from(format!("/tmp/d-engine-test-local-client-{test_name}"));

    // Clean up old test data
    if db_path.exists() {
        std::fs::remove_dir_all(&db_path).ok();
    }

    let storage_engine = Arc::new(
        FileStorageEngine::new(db_path.join("storage")).expect("Failed to create storage engine"),
    );
    let state_machine = Arc::new(
        FileStateMachine::new(db_path.join("state_machine"))
            .await
            .expect("Failed to create state machine"),
    );

    // Create single-node cluster configuration
    let cluster_config = ClusterConfig {
        node_id: 1,
        listen_address: "127.0.0.1:9081".parse().unwrap(),
        initial_cluster: vec![NodeMeta {
            id: 1,
            address: "127.0.0.1:9081".to_string(),
            role: NodeRole::Follower as i32,
            status: NodeStatus::Active as i32,
        }],
        db_root_dir: db_path.clone(),
        log_dir: db_path.join("logs"),
    };

    let (graceful_tx, graceful_rx) = watch::channel(());

    let node = NodeBuilder::from_cluster_config(cluster_config, graceful_rx)
        .storage_engine(storage_engine)
        .state_machine(state_machine)
        .start()
        .await
        .expect("Failed to start node");

    // Clone node for background task
    let node_clone = node.clone();

    // Spawn node's run loop in background
    tokio::spawn(async move {
        if let Err(e) = node_clone.run().await {
            eprintln!("Node run error: {e:?}");
        }
    });

    // Give node time to initialize and become leader
    tokio::time::sleep(Duration::from_secs(2)).await;

    (node, graceful_tx)
}

/// Test: Basic PUT operation via LocalKvClient
#[tokio::test]
async fn test_local_client_put() {
    let (node, _shutdown) = create_test_node("put").await;
    let client = node.local_client();

    let key = b"test_key";
    let value = b"test_value";

    let result = client.put(key, value).await;
    assert!(result.is_ok(), "PUT should succeed: {result:?}");

    println!("✅ LocalKvClient PUT operation succeeded");
}

/// Test: Basic GET operation via LocalKvClient
#[tokio::test]
async fn test_local_client_get() {
    let (node, _shutdown) = create_test_node("get").await;
    let client = node.local_client();

    let key = b"get_test_key";
    let value = b"get_test_value";

    // First PUT the value
    client.put(key, value).await.expect("PUT failed");

    // Give system time to process commit and apply to state machine
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Then GET it back
    let result = client.get_eventual(key).await.expect("GET failed");

    assert!(result.is_some(), "Value should exist");
    assert_eq!(result.unwrap(), Bytes::from_static(value), "Value mismatch");

    println!("✅ LocalKvClient GET operation succeeded");
}

/// Test: GET non-existent key returns None
#[tokio::test]
async fn test_local_client_get_not_found() {
    let (node, _shutdown) = create_test_node("not_found").await;
    let client = node.local_client();

    let key = b"non_existent_key";

    let result = client.get_eventual(key).await.expect("GET should not error");
    assert!(result.is_none(), "Non-existent key should return None");

    println!("✅ LocalKvClient GET not found handled correctly");
}

/// Test: DELETE operation
#[tokio::test]
async fn test_local_client_delete() {
    let (node, _shutdown) = create_test_node("delete").await;
    let client = node.local_client();

    let key = b"delete_test_key";
    let value = b"delete_test_value";

    // PUT, verify, DELETE, verify
    client.put(key, value).await.expect("PUT failed");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let get_result = client.get_eventual(key).await.expect("First GET failed");
    assert!(get_result.is_some(), "Value should exist before delete");

    client.delete(key).await.expect("DELETE failed");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let get_result = client.get_eventual(key).await.expect("Second GET failed");
    assert!(get_result.is_none(), "Value should not exist after delete");

    println!("✅ LocalKvClient DELETE operation succeeded");
}

/// Test: Multiple sequential operations
#[tokio::test]
async fn test_local_client_sequential_ops() {
    let (node, _shutdown) = create_test_node("sequential").await;
    let client = node.local_client();

    // PUT multiple keys
    for i in 0..5 {
        let key = format!("key_{i}");
        let value = format!("value_{i}");
        client.put(key.as_bytes(), value.as_bytes()).await.expect("PUT failed");
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // GET and verify all keys
    for i in 0..5 {
        let key = format!("key_{i}");
        let expected_value = format!("value_{i}");
        let result = client.get_eventual(key.as_bytes()).await.expect("GET failed");
        assert_eq!(
            result.unwrap(),
            Bytes::from(expected_value),
            "Value mismatch for key_{i}"
        );
    }

    // DELETE all keys
    for i in 0..5 {
        let key = format!("key_{i}");
        client.delete(key.as_bytes()).await.expect("DELETE failed");
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify all deleted
    for i in 0..5 {
        let key = format!("key_{i}");
        let result = client.get_eventual(key.as_bytes()).await.expect("GET failed");
        assert!(result.is_none(), "key_{i} should be deleted");
    }

    println!("✅ LocalKvClient sequential operations succeeded");
}

/// Test: Concurrent operations from multiple LocalKvClient instances
#[tokio::test]
async fn test_local_client_concurrent_ops() {
    let (node, _shutdown) = create_test_node("concurrent").await;

    // Create multiple client instances
    let client1 = node.local_client();
    let client2 = node.local_client();
    let client3 = node.local_client();

    // Spawn concurrent PUT operations
    let handle1 = tokio::spawn(async move {
        for i in 0..10 {
            let key = format!("concurrent_key_{i}");
            let value = format!("value_from_client1_{i}");
            client1.put(key.as_bytes(), value.as_bytes()).await.expect("Client1 PUT failed");
        }
    });

    let handle2 = tokio::spawn(async move {
        for i in 10..20 {
            let key = format!("concurrent_key_{i}");
            let value = format!("value_from_client2_{i}");
            client2.put(key.as_bytes(), value.as_bytes()).await.expect("Client2 PUT failed");
        }
    });

    let handle3 = tokio::spawn(async move {
        for i in 20..30 {
            let key = format!("concurrent_key_{i}");
            let value = format!("value_from_client3_{i}");
            client3.put(key.as_bytes(), value.as_bytes()).await.expect("Client3 PUT failed");
        }
    });

    // Wait for all to complete
    let (r1, r2, r3) = tokio::join!(handle1, handle2, handle3);
    assert!(
        r1.is_ok() && r2.is_ok() && r3.is_ok(),
        "All concurrent operations should succeed"
    );

    println!("✅ LocalKvClient concurrent operations succeeded");
}

/// Test: Large value handling
#[tokio::test]
async fn test_local_client_large_value() {
    let (node, _shutdown) = create_test_node("large_value").await;
    let client = node.local_client();

    let key = b"large_value_key";
    let large_value = vec![b'X'; 512 * 1024]; // 512KB value

    client.put(key, &large_value).await.expect("Large value PUT failed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = client.get_eventual(key).await.expect("Large value GET failed");

    assert!(result.is_some(), "Large value should exist");
    assert_eq!(
        result.unwrap().len(),
        large_value.len(),
        "Large value size mismatch"
    );

    println!("✅ LocalKvClient large value handling succeeded");
}

/// Test: Empty key and value handling
#[tokio::test]
async fn test_local_client_empty_key_value() {
    let (node, _shutdown) = create_test_node("empty").await;
    let client = node.local_client();

    // Empty key with value
    let result = client.put(b"", b"some_value").await;
    assert!(result.is_ok(), "Empty key PUT should succeed");

    // Regular key with empty value
    let result = client.put(b"key_with_empty_value", b"").await;
    assert!(result.is_ok(), "Empty value PUT should succeed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let get_result = client.get_eventual(b"key_with_empty_value").await.expect("GET failed");
    assert_eq!(
        get_result.unwrap(),
        Bytes::new(),
        "Empty value should be retrievable"
    );

    println!("✅ LocalKvClient empty key/value handling succeeded");
}

/// Test: Update existing key
#[tokio::test]
async fn test_local_client_update() {
    let (node, _shutdown) = create_test_node("update").await;
    let client = node.local_client();

    let key = b"update_key";
    let value1 = b"original_value";
    let value2 = b"updated_value";

    // Initial PUT
    client.put(key, value1).await.expect("Initial PUT failed");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = client.get_eventual(key).await.expect("First GET failed");
    assert_eq!(result.unwrap(), Bytes::from_static(value1));

    // Update PUT
    client.put(key, value2).await.expect("Update PUT failed");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = client.get_eventual(key).await.expect("Second GET failed");
    assert_eq!(
        result.unwrap(),
        Bytes::from_static(value2),
        "Value should be updated"
    );

    println!("✅ LocalKvClient update operation succeeded");
}

/// Test: Client ID and timeout getters
#[tokio::test]
async fn test_local_client_getters() {
    let (node, _shutdown) = create_test_node("getters").await;
    let client = node.local_client();

    let client_id = client.client_id();
    assert!(client_id > 0, "Client ID should be positive");

    let timeout = client.timeout();
    assert!(timeout.as_millis() > 0, "Timeout should be positive");

    println!(
        "✅ LocalKvClient getters work correctly (client_id={}, timeout={}ms)",
        client_id,
        timeout.as_millis()
    );
}

/// Test: Clone functionality
#[tokio::test]
async fn test_local_client_clone() {
    let (node, _shutdown) = create_test_node("clone").await;
    let client1 = node.local_client();
    let client2 = client1.clone();

    // Both clients should work independently
    client1.put(b"key1", b"value1").await.expect("Client1 PUT failed");
    client2.put(b"key2", b"value2").await.expect("Client2 PUT failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let result1 = client1.get_eventual(b"key2").await.expect("Client1 GET failed");
    let result2 = client2.get_eventual(b"key1").await.expect("Client2 GET failed");

    assert!(
        result1.is_some() && result2.is_some(),
        "Both clients should see all data"
    );

    println!("✅ LocalKvClient clone works correctly");
}
