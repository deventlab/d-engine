//! EmbeddedClient Integration Tests
//!
//! Tests for EmbeddedClient in embedded mode:
//! - Basic CRUD operations with EmbeddedEngine
//! - Error handling
//! - Concurrent operations

use std::time::Duration;

use bytes::Bytes;
use d_engine_core::ClientApi;
use d_engine_server::EmbeddedEngine;
use tempfile::TempDir;

use crate::common::get_available_ports;

/// Helper to create a test EmbeddedEngine
async fn create_test_engine(test_name: &str) -> (EmbeddedEngine, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join(test_name);

    let config_path = temp_dir.path().join("d-engine.toml");
    let mut port_guard = get_available_ports(1).await;
    port_guard.release_listeners();
    let port = port_guard.as_slice()[0];
    let config_content = format!(
        r#"
[cluster]
listen_address = "127.0.0.1:{}"
db_root_dir = "{}"
single_node = true

[raft.state_machine.lease]
enabled = true
"#,
        port,
        db_path.display()
    );
    std::fs::write(&config_path, config_content).expect("Failed to write config");

    let engine = EmbeddedEngine::start_with(config_path.to_str().unwrap())
        .await
        .expect("Failed to start engine");

    engine.wait_ready(Duration::from_secs(5)).await.expect("Engine not ready");

    (engine, temp_dir)
}

/// Test: Basic PUT operation via EmbeddedClient
#[tokio::test]
async fn test_local_client_put() {
    let (engine, _temp_dir) = create_test_engine("put").await;
    let client = engine.client();

    let key = b"test_key";
    let value = b"test_value";

    let result = client.put(key, value).await;
    assert!(result.is_ok(), "PUT should succeed: {result:?}");

    println!("✅ EmbeddedClient PUT operation succeeded");
}

/// Test: Basic GET operation via EmbeddedClient
#[tokio::test]
async fn test_local_client_get() {
    let (engine, _temp_dir) = create_test_engine("get").await;
    let client = engine.client();

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
    let (engine, _temp_dir) = create_test_engine("not_found").await;
    let client = engine.client();

    let key = b"non_existent_key";

    let result = client.get_eventual(key).await.expect("GET should not error");
    assert!(result.is_none(), "Non-existent key should return None");

    println!("✅ EmbeddedClient GET not found handled correctly");
}

/// Test: DELETE operation
#[tokio::test]
async fn test_local_client_delete() {
    let (engine, _temp_dir) = create_test_engine("delete").await;
    let client = engine.client();

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
    let (engine, _temp_dir) = create_test_engine("sequential").await;
    let client = engine.client();

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
    let (engine, _temp_dir) = create_test_engine("concurrent").await;

    // Clone clients before moving into spawn
    let client1 = engine.client();
    let client2 = engine.client();
    let client3 = engine.client();

    // Spawn concurrent PUT operations
    let handle1 = {
        let client = client1.clone();
        tokio::spawn(async move {
            for i in 0..10 {
                let key = format!("concurrent_key_{i}");
                let value = format!("value_from_client1_{i}");
                client.put(key.as_bytes(), value.as_bytes()).await.expect("Client1 PUT failed");
            }
        })
    };

    let handle2 = {
        let client = client2.clone();
        tokio::spawn(async move {
            for i in 10..20 {
                let key = format!("concurrent_key_{i}");
                let value = format!("value_from_client2_{i}");
                client.put(key.as_bytes(), value.as_bytes()).await.expect("Client2 PUT failed");
            }
        })
    };

    let handle3 = {
        let client = client3.clone();
        tokio::spawn(async move {
            for i in 20..30 {
                let key = format!("concurrent_key_{i}");
                let value = format!("value_from_client3_{i}");
                client.put(key.as_bytes(), value.as_bytes()).await.expect("Client3 PUT failed");
            }
        })
    };

    // Wait for all to complete
    let (r1, r2, r3) = tokio::join!(handle1, handle2, handle3);
    assert!(
        r1.is_ok() && r2.is_ok() && r3.is_ok(),
        "All concurrent operations should succeed"
    );

    // Keep engine alive until here
    drop(engine);
    drop(_temp_dir);

    println!("✅ EmbeddedClient concurrent operations succeeded");
}

/// Test: Large value handling
#[tokio::test]
async fn test_local_client_large_value() {
    let (engine, _temp_dir) = create_test_engine("delete_nonexist").await;
    let client = engine.client();

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
    let (engine, _temp_dir) = create_test_engine("empty").await;
    let client = engine.client();

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
    let (engine, _temp_dir) = create_test_engine("overwrite").await;
    let client = engine.client();

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
    let (engine, _temp_dir) = create_test_engine("large").await;
    let client = engine.client();

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
    let (engine, _temp_dir) = create_test_engine("clone").await;
    let client1 = engine.client();
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
    let (engine, _temp_dir) = create_test_engine("special_chars").await;
    let client = engine.client();

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
    let (engine, _temp_dir) = create_test_engine("multiple_keys").await;
    let client = engine.client();

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
    let (engine, _temp_dir) = create_test_engine("get_multi").await;
    let client = engine.client();

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

// =============================================================================
// put_with_ttl
// =============================================================================

/// Verifies that put_with_ttl stores a value that is immediately readable.
///
/// TTL expiry is not tested here (requires real time passage); this test
/// focuses on the write path: the entry must be committed and readable
/// via linearizable read immediately after the call returns.
#[tokio::test]
async fn test_put_with_ttl_readable_immediately() {
    let (engine, _temp_dir) = create_test_engine("put_with_ttl").await;
    let client = engine.client();

    client
        .put_with_ttl(b"ttl-key", b"ttl-value", 60)
        .await
        .expect("put_with_ttl should succeed");

    let value = client
        .get_linearizable(b"ttl-key")
        .await
        .expect("get_linearizable should succeed");

    assert_eq!(
        value,
        Some(Bytes::from("ttl-value")),
        "value written with TTL must be readable immediately after write completes"
    );
}

// =============================================================================
// get_with_consistency (LeaseRead)
// =============================================================================

/// Verifies that get_with_consistency with LeaseRead returns the correct value.
///
/// On a single-node cluster the leader always holds a valid lease, so
/// LeaseRead should behave identically to LinearizableRead.
#[tokio::test]
async fn test_get_with_consistency_lease_read() {
    use d_engine_proto::client::ReadConsistencyPolicy;

    let (engine, _temp_dir) = create_test_engine("lease_read").await;
    let client = engine.client();

    client.put(b"lease-key", b"lease-val").await.expect("put should succeed");

    let value = client
        .get_with_consistency(b"lease-key", ReadConsistencyPolicy::LeaseRead)
        .await
        .expect("get_with_consistency LeaseRead should succeed");

    assert_eq!(value, Some(Bytes::from("lease-val")));
}

// =============================================================================
// get_multi_linearizable / get_multi_eventual — alignment
// =============================================================================

/// Verifies get_multi_linearizable returns an aligned Vec<Option<Bytes>> where
/// positions corresponding to missing keys contain None.
///
/// This exercises the HashMap-based reconstruction that ensures the result
/// vector length always equals the input key count regardless of which keys
/// actually exist in the store.
#[tokio::test]
async fn test_get_multi_linearizable_aligned_with_missing_keys() {
    let (engine, _temp_dir) = create_test_engine("get_multi_linearizable").await;
    let client = engine.client();

    client.put(b"ml1", b"v1").await.expect("put ml1 should succeed");
    client.put(b"ml3", b"v3").await.expect("put ml3 should succeed");
    // ml2 intentionally not written

    let keys = vec![Bytes::from("ml1"), Bytes::from("ml2"), Bytes::from("ml3")];
    let results = client
        .get_multi_linearizable(&keys)
        .await
        .expect("get_multi_linearizable should succeed");

    assert_eq!(results.len(), 3, "result length must equal key count");
    assert_eq!(results[0], Some(Bytes::from("v1")), "ml1 must be present");
    assert_eq!(results[1], None, "ml2 was not written — must be None");
    assert_eq!(results[2], Some(Bytes::from("v3")), "ml3 must be present");
}

/// Verifies get_multi_eventual returns an aligned Vec<Option<Bytes>>.
///
/// Same alignment guarantee as linearizable, but using the eventual
/// consistency path (local state machine read, no leader round-trip).
#[tokio::test]
async fn test_get_multi_eventual_aligned_with_missing_keys() {
    let (engine, _temp_dir) = create_test_engine("get_multi_eventual").await;
    let client = engine.client();

    client.put(b"me1", b"a").await.expect("put me1 should succeed");
    client.put(b"me2", b"b").await.expect("put me2 should succeed");
    // me3 intentionally not written

    // Allow replication to apply on single-node before eventual read
    tokio::time::sleep(Duration::from_millis(100)).await;

    let keys = vec![Bytes::from("me1"), Bytes::from("me2"), Bytes::from("me3")];
    let results = client
        .get_multi_eventual(&keys)
        .await
        .expect("get_multi_eventual should succeed");

    assert_eq!(results.len(), 3, "result length must equal key count");
    assert_eq!(results[0], Some(Bytes::from("a")));
    assert_eq!(results[1], Some(Bytes::from("b")));
    assert_eq!(results[2], None, "me3 was not written — must be None");
}

/// Verifies that get_multi_linearizable with an empty key slice returns an
/// empty Vec without error.
#[tokio::test]
async fn test_get_multi_linearizable_empty_keys() {
    let (engine, _temp_dir) = create_test_engine("get_multi_empty").await;
    let client = engine.client();

    let results = client
        .get_multi_linearizable(&[])
        .await
        .expect("empty key slice should not error");

    assert!(results.is_empty(), "empty input must produce empty output");
}

// =============================================================================
// Watch operations (requires `watch` feature)
// =============================================================================

/// Helper: create an EmbeddedEngine with watch feature enabled.
///
/// Separate from create_test_engine because watch requires the
/// `[raft.watch]` config section to activate the WatchRegistry.
#[cfg(feature = "watch")]
async fn create_watch_engine(
    test_name: &str
) -> (d_engine_server::EmbeddedEngine, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join(test_name);

    let config_path = temp_dir.path().join("d-engine.toml");
    // Use dynamic port allocation to avoid collisions
    let mut port_guard = get_available_ports(1).await;
    port_guard.release_listeners();
    let port = port_guard.as_slice()[0];
    let config_content = format!(
        r#"
[cluster]
listen_address = "127.0.0.1:{}"
db_root_dir = "{}"
single_node = true

[raft]
general_raft_timeout_duration_in_ms = 100

[raft.commit_handler]
batch_size_threshold = 1
process_idle_flush_interval_ms = 1

[replication]
max_batch_size = 1

[raft.watch]
enabled = true
event_queue_size = 1000
watcher_buffer_size = 10
"#,
        port,
        db_path.display()
    );
    std::fs::write(&config_path, config_content).expect("Failed to write watch config");

    let engine = d_engine_server::EmbeddedEngine::start_with(config_path.to_str().unwrap())
        .await
        .expect("Failed to start watch engine");

    engine.wait_ready(Duration::from_secs(5)).await.expect("Watch engine not ready");

    (engine, temp_dir)
}

/// Verifies that watch() returns a WatcherHandle without error when the
/// watch feature is enabled and WatchRegistry is initialized.
#[cfg(feature = "watch")]
#[tokio::test]
async fn test_watch_registration_succeeds() {
    let (engine, _temp_dir) = create_watch_engine("watch_reg").await;
    let client = engine.client();

    let handle = client.watch(b"watched-key");
    assert!(handle.is_ok(), "watch registration should succeed");
}

/// Verifies that a put on a watched key delivers a Put event to the watcher.
///
/// This tests the full path: EmbeddedClient::put → Raft commit →
/// state machine apply → WatchDispatcher → WatcherHandle receiver.
#[cfg(feature = "watch")]
#[tokio::test]
async fn test_watch_receives_put_event() {
    use d_engine_core::watch::WatchEventType;

    let (engine, _temp_dir) = create_watch_engine("watch_put").await;
    let client = engine.client();

    let mut handle = client.watch(b"w-key").expect("watch should succeed");

    client.put(b"w-key", b"w-val").await.expect("put should succeed");

    let event = tokio::time::timeout(Duration::from_secs(1), handle.receiver_mut().recv())
        .await
        .expect("timed out waiting for Put watch event")
        .expect("watch channel should not be closed");

    assert_eq!(
        event.event_type,
        WatchEventType::Put as i32,
        "expected Put event"
    );
    assert_eq!(event.key, Bytes::from("w-key"));
    assert_eq!(event.value, Bytes::from("w-val"));
}

/// Verifies that a delete on a watched key delivers a Delete event to the watcher.
#[cfg(feature = "watch")]
#[tokio::test]
async fn test_watch_receives_delete_event() {
    use d_engine_core::watch::WatchEventType;

    let (engine, _temp_dir) = create_watch_engine("watch_delete").await;
    let client = engine.client();

    client.put(b"d-key", b"d-val").await.expect("put should succeed");

    let mut handle = client.watch(b"d-key").expect("watch should succeed");

    client.delete(b"d-key").await.expect("delete should succeed");

    let event = tokio::time::timeout(Duration::from_secs(1), handle.receiver_mut().recv())
        .await
        .expect("timed out waiting for Delete watch event")
        .expect("watch channel should not be closed");

    assert_eq!(
        event.event_type,
        WatchEventType::Delete as i32,
        "expected Delete event"
    );
    assert_eq!(event.key, Bytes::from("d-key"));
}

/// Verifies that a watcher does NOT receive events for keys it is not watching.
///
/// Key isolation is a correctness requirement: each watcher must only see
/// events for its own registered key.
#[cfg(feature = "watch")]
#[tokio::test]
async fn test_watch_key_isolation() {
    let (engine, _temp_dir) = create_watch_engine("watch_isolation").await;
    let client = engine.client();

    let mut handle = client.watch(b"my-key").expect("watch should succeed");

    // Write to a different key — watcher must NOT receive this event
    client.put(b"other-key", b"other-val").await.expect("put should succeed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    match handle.receiver_mut().try_recv() {
        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
            // correct — no event for unrelated key
        }
        Ok(event) => panic!("unexpected event for unrelated key: {event:?}"),
        Err(e) => panic!("unexpected channel error: {e:?}"),
    }
}
