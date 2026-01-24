//! EmbeddedClient Integration Tests
//!
//! Tests for EmbeddedClient in embedded mode:
//! - Basic CRUD operations with real Node
//! - Error handling
//! - Concurrent operations

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::RaftConfig;
use d_engine_server::FileStateMachine;
use d_engine_server::FileStorageEngine;
use d_engine_server::NodeBuilder;
use d_engine_server::node::RaftTypeConfig;
use tokio::sync::watch;

/// Type alias for our test node
type TestNode = Arc<d_engine_server::Node<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>;

/// Helper to create a test node with EmbeddedClient
async fn create_test_node(test_name: &str) -> (TestNode, tokio::sync::watch::Sender<()>) {
    use d_engine_core::ClusterConfig;
    use d_engine_proto::common::NodeRole;
    use d_engine_proto::common::NodeStatus;
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

    // Increase timeout for test reliability (CI environments can be slow)
    let mut raft_config = RaftConfig::default();
    raft_config.read_consistency.state_machine_sync_timeout_ms = 1000; // 1000ms for tests

    let node = NodeBuilder::from_cluster_config(cluster_config, graceful_rx)
        .storage_engine(storage_engine)
        .state_machine(state_machine)
        .raft_config(raft_config)
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

/// Test: Basic PUT operation via EmbeddedClient
#[tokio::test]
async fn test_local_client_put() {
    let (node, _shutdown) = create_test_node("put").await;
    let client = node.local_client();

    let key = b"test_key";
    let value = b"test_value";

    let result = client.put(key, value).await;
    assert!(result.is_ok(), "PUT should succeed: {result:?}");

    println!("✅ EmbeddedClient PUT operation succeeded");
}

/// Test: Basic GET operation via EmbeddedClient
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

    println!("✅ EmbeddedClient GET operation succeeded");
}

/// Test: GET non-existent key returns None
#[tokio::test]
async fn test_local_client_get_not_found() {
    let (node, _shutdown) = create_test_node("not_found").await;
    let client = node.local_client();

    let key = b"non_existent_key";

    let result = client.get_eventual(key).await.expect("GET should not error");
    assert!(result.is_none(), "Non-existent key should return None");

    println!("✅ EmbeddedClient GET not found handled correctly");
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

    println!("✅ EmbeddedClient DELETE operation succeeded");
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

    println!("✅ EmbeddedClient sequential operations succeeded");
}

/// Test: Concurrent operations from multiple EmbeddedClient instances
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

    println!("✅ EmbeddedClient concurrent operations succeeded");
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

    println!("✅ EmbeddedClient large value handling succeeded");
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

    println!("✅ EmbeddedClient empty key/value handling succeeded");
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

    println!("✅ EmbeddedClient update operation succeeded");
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
        "✅ EmbeddedClient getters work correctly (client_id={}, timeout={}ms)",
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

    println!("✅ EmbeddedClient clone works correctly");
}

/// Test: Linearizable read immediately after write (no sleep)
///
/// This test verifies the core guarantee of linearizable reads in Raft:
/// "Once a write completes, all subsequent reads MUST reflect that write"
///
/// Test scenario:
/// 1. PUT key-value pair (write completes when quorum commits)
/// 2. Immediately call get_linearizable() with NO sleep
/// 3. Expect to read the value we just wrote
///
/// Why this test is critical:
/// - Raft linearizability guarantee requires reads to see all committed writes
/// - Single-node mode should behave identically to multi-node mode
/// - This was a P0 bug: get_linearizable() returned None on first startup
///
/// Expected behavior:
/// - PUT returns Ok → log entry committed
/// - get_linearizable() waits for state machine to apply the entry
/// - Returns the value (NOT None)
///
/// Performance note:
/// - get_linearizable() may add ~1ms latency (waiting for state machine)
/// - This is correct behavior per Raft protocol
#[tokio::test]
async fn test_linearizable_read_after_write_no_sleep() {
    let (node, _shutdown) = create_test_node("linearizable_read").await;
    let client = node.local_client();

    let key = b"linearizable_key";
    let value = b"linearizable_value";

    // Step 1: Write the value
    client.put(key, value).await.expect("PUT should succeed - log committed");

    // Step 2: Immediately read with linearizable consistency (NO SLEEP!)
    // This MUST return the value we just wrote, per Raft linearizability guarantee
    let result = client.get_linearizable(key).await.expect("get_linearizable should not error");

    // Step 3: Verify we got the value
    assert!(
        result.is_some(),
        "Linearizable read MUST return value immediately after PUT completes (no sleep needed)"
    );
    assert_eq!(
        result.unwrap(),
        Bytes::from_static(value),
        "Value must match what we wrote"
    );

    println!("✅ Linearizable read correctly waits for state machine to catch up");
}

/// Test: Multiple sequential writes with linearizable reads
///
/// This test verifies that linearizable reads always see the latest committed value,
/// even with rapid sequential writes.
///
/// Test scenario:
/// 1. PUT key=v1
/// 2. get_linearizable() → expect v1
/// 3. PUT key=v2 (overwrite)
/// 4. get_linearizable() → expect v2 (NOT v1!)
/// 5. PUT key=v3
/// 6. get_linearizable() → expect v3
///
/// Why this test is critical:
/// - Ensures state machine apply happens in correct order
/// - Verifies no stale reads even under rapid updates
/// - Tests the wait_applied() mechanism under sequential load
///
/// Expected behavior:
/// - Each get_linearizable() sees the value from the most recent PUT
/// - No stale reads, no None returns
#[tokio::test]
async fn test_linearizable_read_sees_latest_value() {
    let (node, _shutdown) = create_test_node("sequential_writes").await;
    let client = node.local_client();

    let key = b"seq_key";

    // Write v1
    client.put(key, b"v1").await.expect("PUT v1 failed");
    let result = client.get_linearizable(key).await.expect("GET after v1 failed");
    assert_eq!(result.unwrap(), Bytes::from_static(b"v1"), "Should read v1");

    // Overwrite with v2
    client.put(key, b"v2").await.expect("PUT v2 failed");
    let result = client.get_linearizable(key).await.expect("GET after v2 failed");
    assert_eq!(
        result.unwrap(),
        Bytes::from_static(b"v2"),
        "Should read v2 (NOT v1 - no stale reads)"
    );

    // Overwrite with v3
    client.put(key, b"v3").await.expect("PUT v3 failed");
    let result = client.get_linearizable(key).await.expect("GET after v3 failed");
    assert_eq!(
        result.unwrap(),
        Bytes::from_static(b"v3"),
        "Should read v3 (latest value)"
    );

    println!("✅ Linearizable reads always see latest committed value");
}

/// Test: High concurrent read/write - verify no stale reads under load
///
/// This test validates wait_applied behavior under concurrent load:
/// - 100 concurrent writes to different keys
/// - Each write immediately followed by linearizable read (no sleep)
/// - Every linearizable read must see its own write
///
/// This stresses the wait_applied fast path and concurrent waiter handling.
#[tokio::test]
async fn test_concurrent_write_and_linearizable_read() {
    let (node, _shutdown) = create_test_node("concurrent_rw").await;
    let client = node.local_client();

    let mut tasks = vec![];

    // Spawn 100 concurrent write-then-read operations
    for i in 0..100 {
        let c = client.clone();
        tasks.push(tokio::spawn(async move {
            let key = format!("key_{i}").into_bytes();
            let value = format!("value_{i}").into_bytes();

            // Write
            c.put(&key, &value).await.unwrap_or_else(|_| panic!("PUT key_{i} failed"));

            // Immediately linearizable read (no sleep)
            let result =
                c.get_linearizable(&key).await.unwrap_or_else(|_| panic!("GET key_{i} failed"));

            // Must see own write
            assert_eq!(
                result.unwrap(),
                Bytes::from(value),
                "Linearizable read must see its own write for key_{i}"
            );
        }));
    }

    // Wait for all tasks
    for task in tasks {
        task.await.unwrap();
    }

    println!("✅ All 100 concurrent linearizable reads saw their own writes");
}
