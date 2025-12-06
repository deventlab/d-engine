use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

use d_engine_core::watch::WatchEventType;
use d_engine_server::embedded::EmbeddedEngine;

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
enabled = true
"#,
    )?;

    // Start engine
    // Note: We need to ensure rocksdb feature is enabled for this to work.
    // d-engine-server tests should have access to it if enabled in dev-dependencies or features.
    let engine = EmbeddedEngine::with_rocksdb(
        db_path.to_str().unwrap(),
        Some(config_path.to_str().unwrap()),
    )
    .await?;

    // Wait for leader election
    engine.wait_leader(Duration::from_secs(5)).await?;

    // 2. Start Watcher
    let key = "test-key";
    let mut watcher = engine.watch(key).await?;

    // Spawn watcher task
    let handle = tokio::spawn(async move {
        let mut events = Vec::new();
        // Collect 2 events
        for _ in 0..2 {
            if let Some(event) = watcher.receiver_mut().unwrap().recv().await {
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
    assert_eq!(put_event.event_type, WatchEventType::Put);
    assert_eq!(put_event.key, key.as_bytes());
    assert_eq!(&put_event.value[..], b"value1");

    // Verify DELETE
    let delete_event = &events[1];
    assert_eq!(delete_event.event_type, WatchEventType::Delete);
    assert_eq!(delete_event.key, key.as_bytes());
    assert!(delete_event.value.is_empty());

    // Cleanup
    engine.stop().await?;

    Ok(())
}
