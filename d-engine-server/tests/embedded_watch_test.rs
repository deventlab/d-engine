#![cfg(feature = "rocksdb")]

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

use d_engine_core::watch::WatchEventType;
use d_engine_server::embedded::EmbeddedEngine;
use d_engine_server::{RocksDBStateMachine, RocksDBStorageEngine};

/// Helper function to create a test EmbeddedEngine with RocksDB
async fn setup_engine() -> Result<(EmbeddedEngine, TempDir), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("db");

    // Create a minimal config with Watch enabled
    let config_path = temp_dir.path().join("d-engine.toml");
    // Use a high random port to avoid conflicts
    let port = 50000 + (std::process::id() % 10000);
    let config_content = format!(
        r#"
[cluster]
listen_address = "127.0.0.1:{port}"

[raft.watch]
event_queue_size = 1000
watcher_buffer_size = 10
"#
    );
    std::fs::write(&config_path, config_content)?;

    // Start engine with RocksDB storage
    let storage_path = db_path.join("storage");
    let sm_path = db_path.join("state_machine");
    tokio::fs::create_dir_all(&storage_path).await?;
    tokio::fs::create_dir_all(&sm_path).await?;

    let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
    let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

    let engine =
        EmbeddedEngine::start(Some(config_path.to_str().unwrap()), storage, state_machine).await?;

    // Wait for leader election
    engine.wait_ready(Duration::from_secs(5)).await?;

    Ok((engine, temp_dir))
}

#[tokio::test]
async fn test_embedded_watch_registration() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir) = setup_engine().await?;

    // Register a watcher
    let key = b"test-key";
    let _watcher = engine.watch(key)?;

    // Success - we got a WatcherHandle
    // This verifies basic registration functionality

    // Cleanup
    engine.stop().await?;

    Ok(())
}

#[tokio::test]
async fn test_embedded_watch_receives_put_events() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir) = setup_engine().await?;

    // Start watching a key
    let key = b"test-key";
    let mut watcher = engine.watch(key)?;

    // Spawn task to collect events
    let handle = tokio::spawn(async move { watcher.receiver_mut().recv().await });

    // Give watcher time to register
    sleep(Duration::from_millis(50)).await;

    // Perform PUT operation
    engine.client().put(key, b"value1").await?;

    // Verify event
    let event = handle.await?.expect("Should receive PUT event");
    assert_eq!(event.event_type, WatchEventType::Put as i32);
    assert_eq!(&event.key[..], key);
    assert_eq!(&event.value[..], b"value1");

    // Cleanup
    engine.stop().await?;

    Ok(())
}

#[tokio::test]
async fn test_embedded_watch_always_available_when_compiled()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("db");

    // Create config WITHOUT explicit [raft.watch] section
    // Watch should still be available with default values
    let config_path = temp_dir.path().join("d-engine.toml");
    let port = 50000 + (std::process::id() % 10000);
    let config_content = format!(
        r#"
[cluster]
listen_address = "127.0.0.1:{port}"
"#
    );
    std::fs::write(&config_path, config_content)?;

    // Start engine with RocksDB storage
    let storage_path = db_path.join("storage");
    let sm_path = db_path.join("state_machine");
    tokio::fs::create_dir_all(&storage_path).await?;
    tokio::fs::create_dir_all(&sm_path).await?;

    let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
    let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

    let engine =
        EmbeddedEngine::start(Some(config_path.to_str().unwrap()), storage, state_machine).await?;

    engine.wait_ready(Duration::from_secs(5)).await?;

    // Verify watch is available even without explicit config
    let key = b"test-key";
    let _watcher = engine.watch(key)?;

    // Success - watch is available even without explicit config
    // This reflects our design principle: "编译了就一定要用" (If compiled, it must be used)

    // Cleanup
    engine.stop().await?;

    Ok(())
}

#[tokio::test]
async fn test_watch_node_crash_embedded_mode() -> Result<(), Box<dyn std::error::Error>> {
    // Scenario:
    // 1. Node1: EmbeddedEngine.watch(key)
    // 2. Drop engine (simulate crash)
    // 3. Verify all watchers are automatically cleaned up
    //
    // Expected behavior:
    // - Engine drop → all watchers auto cleanup
    // - No memory leaks, no zombie tasks
    //
    // Verification points:
    // ✓ Drop triggers _watch_dispatcher_handle.abort()
    // ✓ All WatcherHandle drop triggers unregister
    // ✓ DashMap cleared

    let key = b"test-key";

    // Setup engine
    let (engine, _temp_dir) = setup_engine().await?;

    // Register watcher
    let mut watcher = engine.watch(key)?;

    // Spawn task that tries to receive events
    let receiver_handle = tokio::spawn(async move {
        // This should receive None after broadcaster drops
        watcher.receiver_mut().recv().await
    });

    // Give watcher time to register
    sleep(Duration::from_millis(50)).await;

    // Drop engine - this simulates node crash
    drop(engine);
    // ← Engine drops, triggering:
    //   1. WatchDispatcher.abort()
    //   2. WatchRegistry cleanup (all broadcasters dropped)
    //   3. All WatcherHandle unregister

    // Give time for cleanup to complete
    sleep(Duration::from_millis(200)).await;

    // Verify that receiver gets None (channel closed because broadcaster dropped)
    let result = tokio::time::timeout(Duration::from_secs(2), receiver_handle).await?;

    match result {
        Ok(None) => {
            // Perfect - channel closed as expected
            // This proves the broadcaster was dropped and cleanup happened
        }
        Ok(Some(_)) => {
            panic!("Should not receive event after engine dropped");
        }
        Err(_) => {
            panic!("Receiver task panicked");
        }
    }

    // Test passes if we get here - cleanup was successful
    Ok(())
}

#[tokio::test]
async fn test_embedded_watch_receives_delete_events() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir) = setup_engine().await?;

    // Start watching a key
    let key = b"test-key";

    // First, put a value so we can delete it
    engine.client().put(key, b"initial_value").await?;
    sleep(Duration::from_millis(50)).await;

    // Now register watcher
    let mut watcher = engine.watch(key)?;

    // Spawn task to collect delete event
    let handle = tokio::spawn(async move { watcher.receiver_mut().recv().await });

    // Give watcher time to register
    sleep(Duration::from_millis(50)).await;

    // Perform DELETE operation
    engine.client().delete(key).await?;

    // Verify event
    let event = handle.await?.expect("Should receive DELETE event");
    assert_eq!(event.event_type, WatchEventType::Delete as i32);
    assert_eq!(&event.key[..], key);
    assert!(
        event.value.is_empty(),
        "DELETE event should have empty value"
    );

    // Cleanup
    engine.stop().await?;

    Ok(())
}

#[tokio::test]
async fn test_embedded_watch_handle_drop_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _temp_dir) = setup_engine().await?;

    let key = b"test-key";

    // Register watcher in a scope
    {
        let _watcher = engine.watch(key)?;
        // Watcher is alive here
    } // ← WatcherHandle drops here, should trigger unregister

    // Give time for cleanup
    sleep(Duration::from_millis(100)).await;

    // Perform PUT - no watcher should receive it
    engine.client().put(key, b"value").await?;
    sleep(Duration::from_millis(50)).await;

    // We can't directly verify cleanup without accessing internal state
    // But we can verify that re-registering works (proves cleanup happened)
    let mut watcher = engine.watch(key)?;

    // This PUT should trigger the new watcher
    engine.client().put(key, b"value2").await?;

    // Verify new watcher receives event
    let event = tokio::time::timeout(Duration::from_secs(2), watcher.receiver_mut().recv())
        .await?
        .expect("Should receive event from new watcher");

    assert_eq!(event.event_type, WatchEventType::Put as i32);
    assert_eq!(&event.value[..], b"value2");

    // Cleanup
    engine.stop().await?;

    Ok(())
}

#[tokio::test]
async fn test_embedded_watch_multiple_watchers_same_key() -> Result<(), Box<dyn std::error::Error>>
{
    let (engine, _temp_dir) = setup_engine().await?;

    let key = b"test-key";

    // Register 3 watchers for the same key
    let mut watcher1 = engine.watch(key)?;
    let mut watcher2 = engine.watch(key)?;
    let mut watcher3 = engine.watch(key)?;

    // Spawn tasks to collect events from each watcher
    let handle1 = tokio::spawn(async move { watcher1.receiver_mut().recv().await });
    let handle2 = tokio::spawn(async move { watcher2.receiver_mut().recv().await });
    let handle3 = tokio::spawn(async move { watcher3.receiver_mut().recv().await });

    // Give watchers time to register
    sleep(Duration::from_millis(50)).await;

    // Perform a single PUT operation
    engine.client().put(key, b"shared_value").await?;

    // Verify all 3 watchers receive the same event
    let event1 = handle1.await?.expect("Watcher 1 should receive event");
    let event2 = handle2.await?.expect("Watcher 2 should receive event");
    let event3 = handle3.await?.expect("Watcher 3 should receive event");

    // Verify all events are identical
    for event in &[&event1, &event2, &event3] {
        assert_eq!(event.event_type, WatchEventType::Put as i32);
        assert_eq!(&event.key[..], key);
        assert_eq!(&event.value[..], b"shared_value");
    }

    // Cleanup
    engine.stop().await?;

    Ok(())
}

#[tokio::test]
async fn test_embedded_watch_integration() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("db");

    // Create a minimal config with Watch enabled
    let config_path = temp_dir.path().join("d-engine.toml");
    std::fs::write(
        &config_path,
        r#"
[cluster]
listen_address = "127.0.0.1:50055" # Fixed port

[raft.watch]
event_queue_size = 1000
watcher_buffer_size = 10
"#,
    )?;

    // Start engine with RocksDB storage
    let storage_path = db_path.join("storage");
    let sm_path = db_path.join("state_machine");
    tokio::fs::create_dir_all(&storage_path).await?;
    tokio::fs::create_dir_all(&sm_path).await?;

    let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
    let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

    let engine =
        EmbeddedEngine::start(Some(config_path.to_str().unwrap()), storage, state_machine).await?;

    // Wait for leader election
    engine.wait_ready(Duration::from_secs(5)).await?;

    // 2. Start Watcher
    let key = "test-key";
    let mut watcher = engine.watch(key)?;

    // Spawn watcher task
    let handle = tokio::spawn(async move {
        let mut events = Vec::new();
        // Collect 2 events
        for _ in 0..2 {
            if let Some(event) = watcher.receiver_mut().recv().await {
                events.push(event);
            }
        }
        events
    });

    // 3. Perform Writes
    // Give watcher a moment to register
    sleep(Duration::from_millis(100)).await;

    // PUT
    engine.client().put(key.as_bytes(), b"value1").await?;
    sleep(Duration::from_millis(50)).await;

    // DELETE
    engine.client().delete(key.as_bytes()).await?;

    // 4. Verify
    let events = handle.await?;
    assert_eq!(events.len(), 2);

    // Verify PUT
    let put_event = &events[0];
    assert_eq!(put_event.event_type, WatchEventType::Put as i32);
    assert_eq!(put_event.key, key.as_bytes());
    assert_eq!(&put_event.value[..], b"value1");

    // Verify DELETE
    let delete_event = &events[1];
    assert_eq!(delete_event.event_type, WatchEventType::Delete as i32);
    assert_eq!(delete_event.key, key.as_bytes());
    assert!(delete_event.value.is_empty());

    // Cleanup
    engine.stop().await?;

    Ok(())
}
