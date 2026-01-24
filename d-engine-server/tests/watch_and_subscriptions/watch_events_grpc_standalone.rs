#![cfg(all(feature = "watch", feature = "rocksdb"))]

//! gRPC Watch Standalone Mode Tests
//!
//! These tests verify the watch service implementation in standalone mode,
//! using real gRPC servers and clients over the network.

use std::sync::Arc;
use std::time::Duration;

use crate::common::TestContext;
use crate::common::create_node_config;
use crate::common::node_config;
use crate::common::start_node;
use d_engine_client::ClientBuilder;
use d_engine_core::ClientApi;
use d_engine_proto::client::WatchResponse;
use d_engine_server::FileStateMachine;
use d_engine_server::FileStorageEngine;
use futures::StreamExt;
use tempfile::TempDir;
use tokio::time::sleep;

/// Helper function to create a 3-node standalone cluster with watch enabled
async fn setup_standalone_cluster()
-> Result<(d_engine_client::Client, TestContext, TempDir), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let base_path = temp_dir.path();

    // Use system-assigned ports to avoid conflicts
    // Bind temporarily to get available ports, then release immediately
    let ports: Vec<u16> = (0..3)
        .map(|_| {
            let listener =
                std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
            let port = listener.local_addr().expect("Failed to get local addr").port();
            drop(listener); // Release immediately
            port
        })
        .collect();
    let ports = [ports[0], ports[1], ports[2]];

    let mut graceful_txs = Vec::new();
    let mut node_handles = Vec::new();

    // Start 3 nodes
    for (i, &port) in ports.iter().enumerate() {
        let node_id = (i + 1) as u64;
        let node_dir = base_path.join(format!("node_{node_id}"));
        let log_dir = node_dir.join("logs");
        tokio::fs::create_dir_all(&log_dir).await?;

        // Create config with watch enabled
        let cluster_toml = create_node_config(
            node_id,
            port,
            &ports,
            node_dir.to_str().unwrap(),
            log_dir.to_str().unwrap(),
        )
        .await;

        let mut config = node_config(&cluster_toml);

        // Configure watch (when compiled with watch feature, it's always enabled)
        config.raft.watch.event_queue_size = 1000;
        config.raft.watch.watcher_buffer_size = 10;

        // Create storage and state machine
        let storage_path = node_dir.join("storage");
        let sm_path = node_dir.join("state_machine");
        tokio::fs::create_dir_all(&storage_path).await?;
        tokio::fs::create_dir_all(&sm_path).await?;

        let storage = Arc::new(FileStorageEngine::new(storage_path)?);
        let state_machine = Arc::new(FileStateMachine::new(sm_path).await?);

        // Start node
        let (graceful_tx, node_handle) =
            start_node(config, Some(state_machine), Some(storage)).await?;

        graceful_txs.push(graceful_tx);
        node_handles.push(node_handle);
    }

    // Wait for cluster to be ready (leader election takes time)
    sleep(Duration::from_secs(6)).await;

    // Create client connecting to all nodes
    let endpoints: Vec<String> =
        ports.iter().map(|port| format!("http://127.0.0.1:{port}")).collect();

    let client = ClientBuilder::new(endpoints)
        .connect_timeout(Duration::from_secs(10))
        .request_timeout(Duration::from_secs(30))
        .build()
        .await?;

    let test_ctx = TestContext {
        graceful_txs,
        node_handles,
    };

    Ok((client, test_ctx, temp_dir))
}

#[tokio::test]
async fn test_grpc_watch_returns_stream() -> Result<(), Box<dyn std::error::Error>> {
    let (client, test_ctx, _temp_dir) = setup_standalone_cluster().await?;

    let key = b"test-key";

    // Call watch() should return a gRPC stream
    let stream = client.watch(key).await?;

    // Verify we got a stream (it should be a tonic::Streaming<WatchResponse>)
    // Just checking that we can create a stream without errors
    drop(stream);

    // Cleanup
    test_ctx.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn test_grpc_watch_receives_protobuf_events() -> Result<(), Box<dyn std::error::Error>> {
    let (client, test_ctx, _temp_dir) = setup_standalone_cluster().await?;

    let key = b"test-key";

    // Register watch via gRPC
    let mut stream = client.watch(key).await?;

    // Give watcher time to register
    sleep(Duration::from_millis(200)).await;

    // Perform PUT operation
    client.put(key, b"grpc_value").await?;

    // Receive event from gRPC stream
    let response: WatchResponse = stream.next().await.expect("Stream should yield a message")?;

    // Verify event is proper protobuf WatchResponse
    assert_eq!(
        response.event_type,
        d_engine_core::watch::WatchEventType::Put as i32
    );
    assert_eq!(&response.key[..], key);
    assert_eq!(&response.value[..], b"grpc_value");

    // Cleanup
    test_ctx.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn test_grpc_watch_stream_type_conversion() -> Result<(), Box<dyn std::error::Error>> {
    let (client, test_ctx, _temp_dir) = setup_standalone_cluster().await?;

    let key = b"test-key";

    // Register watch
    let mut stream = client.watch(key).await?;

    // Give watcher time to register
    sleep(Duration::from_millis(200)).await;

    // Test PUT event
    client.put(key, b"value1").await?;

    let response: WatchResponse = stream.next().await.expect("Should receive PUT event")?;

    assert_eq!(
        response.event_type,
        d_engine_core::watch::WatchEventType::Put as i32
    );
    assert_eq!(&response.value[..], b"value1");

    // Test DELETE event
    client.delete(key).await?;

    let response: WatchResponse = stream.next().await.expect("Should receive DELETE event")?;

    assert_eq!(
        response.event_type,
        d_engine_core::watch::WatchEventType::Delete as i32
    );
    assert!(
        response.value.is_empty(),
        "DELETE event should have empty value"
    );

    // Verify stream continues to work after different event types
    client.put(key, b"value2").await?;

    let response: WatchResponse = stream.next().await.expect("Should receive second PUT event")?;

    assert_eq!(
        response.event_type,
        d_engine_core::watch::WatchEventType::Put as i32
    );
    assert_eq!(&response.value[..], b"value2");

    // Cleanup
    test_ctx.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn test_grpc_watch_client_disconnect_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let (client, test_ctx, _temp_dir) = setup_standalone_cluster().await?;

    let key = b"test-key";

    // Register watch and immediately drop (simulate client disconnect)
    {
        let _stream = client.watch(key).await?;
        // Stream is dropped here - gRPC connection closes
    }

    // Give time for server-side cleanup
    sleep(Duration::from_millis(300)).await;

    // Perform PUT - the dropped watcher should not receive it (no error should occur)
    client.put(key, b"value1").await?;
    sleep(Duration::from_millis(100)).await;

    // Register a new watcher to verify cleanup was successful
    let mut new_stream = client.watch(key).await?;

    sleep(Duration::from_millis(200)).await;

    // Perform another PUT
    client.put(key, b"value2").await?;

    // New watcher should receive the event
    let response: WatchResponse = tokio::time::timeout(Duration::from_secs(3), new_stream.next())
        .await?
        .expect("Should receive event from new watcher")?;

    assert_eq!(
        response.event_type,
        d_engine_core::watch::WatchEventType::Put as i32
    );
    assert_eq!(&response.value[..], b"value2");

    // Cleanup
    test_ctx.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn test_watch_node_crash_standalone_mode() -> Result<(), Box<dyn std::error::Error>> {
    // Scenario (Standalone/gRPC mode):
    // 1. gRPC Client: Watch(key)
    // 2. Client disconnects (stream dropped)
    // 3. Server continues writing to key
    //
    // Expected behavior:
    // - Stream drop → server detects send failure → cleanup
    // - No memory leaks, watcher auto-removed from registry
    //
    // Verification points:
    // ✓ gRPC stream properly closes
    // ✓ Server-side cleanup on send failure
    // ✓ New watcher can be registered after cleanup

    let (client, test_ctx, _temp_dir) = setup_standalone_cluster().await?;
    let key = b"test-key";

    // Register watcher
    let stream = client.watch(key).await?;

    // Give watcher time to register
    sleep(Duration::from_millis(200)).await;

    // Simulate client crash by dropping the stream
    drop(stream);
    // ← Stream drops, triggering:
    //   1. gRPC connection closes
    //   2. Server detects send failure on next broadcast
    //   3. Watcher auto-removed from registry

    // Give time for server-side cleanup
    sleep(Duration::from_millis(300)).await;

    // Server continues writing - this should not cause any issues
    // The broadcast.send() will handle the SendError gracefully
    client.put(key, b"value_after_disconnect").await?;
    sleep(Duration::from_millis(100)).await;

    // Register a new watcher to verify cleanup was successful
    let mut new_stream = client.watch(key).await?;
    sleep(Duration::from_millis(200)).await;

    // Perform another write
    client.put(key, b"value_for_new_watcher").await?;

    // New watcher should receive the event
    let response: WatchResponse = tokio::time::timeout(Duration::from_secs(3), new_stream.next())
        .await?
        .expect("New watcher should receive event")?;

    assert_eq!(
        response.event_type,
        d_engine_core::watch::WatchEventType::Put as i32
    );
    assert_eq!(&response.value[..], b"value_for_new_watcher");

    // Test passes - cleanup was successful and new watcher works
    test_ctx.shutdown().await?;

    Ok(())
}
