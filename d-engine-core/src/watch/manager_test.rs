//! Unit tests for Watch system (WatchRegistry + WatchDispatcher)
//!
//! Tests the core functionality of the watch system including:
//! - Event notification via broadcast channel
//! - Multiple watchers on same key
//! - Automatic cleanup on drop
//! - Key isolation
//! - Dispatcher event distribution

use super::*;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Duration, timeout};

/// Helper to create test watch system components
fn setup_watch_system(
    buffer_size: usize
) -> (
    broadcast::Sender<WatchEvent>,
    Arc<WatchRegistry>,
    tokio::task::JoinHandle<()>,
) {
    let (broadcast_tx, broadcast_rx) = broadcast::channel(1000);
    let (unregister_tx, unregister_rx) = mpsc::unbounded_channel();

    let registry = Arc::new(WatchRegistry::new(buffer_size, unregister_tx));
    let dispatcher = WatchDispatcher::new(Arc::clone(&registry), broadcast_rx, unregister_rx);

    let handle = tokio::spawn(async move {
        dispatcher.run().await;
    });

    (broadcast_tx, registry, handle)
}

#[tokio::test]
async fn test_register_single_watcher() {
    let (_, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("test_key");
    let _handle = registry.register(key.clone());

    assert_eq!(registry.watcher_count(&key), 1);
    assert_eq!(registry.watched_key_count(), 1);
}

#[tokio::test]
async fn test_register_multiple_watchers_same_key() {
    let (_, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("shared_key");

    let _handle1 = registry.register(key.clone());
    let _handle2 = registry.register(key.clone());
    let _handle3 = registry.register(key.clone());

    assert_eq!(registry.watcher_count(&key), 3);
    assert_eq!(registry.watched_key_count(), 1); // Only 1 unique key
}

#[tokio::test]
async fn test_watcher_auto_cleanup_on_drop() {
    let (_, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("cleanup_key");

    {
        let _handle = registry.register(key.clone());
        assert_eq!(registry.watcher_count(&key), 1);
        // Handle dropped here
    }

    // Give cleanup time to process (unregister is async via channel)
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(registry.watcher_count(&key), 0);
    assert_eq!(registry.watched_key_count(), 0);
}

#[tokio::test]
async fn test_dispatcher_dispatch_to_matching_watcher() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("test_key");
    let value = Bytes::from("test_value");

    let mut handle = registry.register(key.clone());

    // Small delay to ensure dispatcher is ready
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send event via broadcast
    let event = WatchEvent {
        key: key.clone(),
        value: value.clone(),
        event_type: WatchEventType::Put as i32,
        error: 0,
    };
    broadcast_tx.send(event).unwrap();

    // Watcher should receive event
    let received = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("Timeout waiting for event")
        .expect("Channel closed");

    assert_eq!(received.key, key);
    assert_eq!(received.value, value);
    assert_eq!(received.event_type, WatchEventType::Put as i32);
}

#[tokio::test]
async fn test_dispatcher_ignores_non_matching_key() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(10);

    let watched_key = Bytes::from("key1");
    let other_key = Bytes::from("key2");

    let mut handle = registry.register(watched_key.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send event for different key
    let event = WatchEvent {
        key: other_key,
        value: Bytes::from("value"),
        event_type: WatchEventType::Put as i32,
        error: 0,
    };
    broadcast_tx.send(event).unwrap();

    // Should timeout (no event received)
    let result = timeout(Duration::from_millis(100), handle.receiver_mut().recv()).await;
    assert!(
        result.is_err(),
        "Should not receive event for different key"
    );
}

#[tokio::test]
async fn test_multiple_watchers_all_receive_event() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("shared_key");
    let value = Bytes::from("shared_value");

    let mut handle1 = registry.register(key.clone());
    let mut handle2 = registry.register(key.clone());
    let mut handle3 = registry.register(key.clone());

    assert_eq!(registry.watcher_count(&key), 3);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Broadcast once
    let event = WatchEvent {
        key: key.clone(),
        value: value.clone(),
        event_type: WatchEventType::Put as i32,
        error: 0,
    };
    broadcast_tx.send(event).unwrap();

    // All 3 should receive
    for handle in [&mut handle1, &mut handle2, &mut handle3].iter_mut() {
        let received = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
            .await
            .expect("Timeout")
            .expect("Channel closed");

        assert_eq!(received.key, key);
        assert_eq!(received.value, value);
    }
}

#[tokio::test]
async fn test_watcher_delete_event() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("test_key");
    let mut handle = registry.register(key.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send DELETE event
    let event = WatchEvent {
        key: key.clone(),
        value: Bytes::new(),
        event_type: WatchEventType::Delete as i32,
        error: 0,
    };
    broadcast_tx.send(event).unwrap();

    let received = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("Timeout")
        .expect("Channel closed");

    assert_eq!(received.event_type, WatchEventType::Delete as i32);
    assert_eq!(received.value, Bytes::new());
}

#[tokio::test]
async fn test_multiple_events_sequential() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("test_key");
    let mut handle = registry.register(key.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send 3 events
    broadcast_tx
        .send(WatchEvent {
            key: key.clone(),
            value: Bytes::from("value1"),
            event_type: WatchEventType::Put as i32,
            error: 0,
        })
        .unwrap();

    broadcast_tx
        .send(WatchEvent {
            key: key.clone(),
            value: Bytes::from("value2"),
            event_type: WatchEventType::Put as i32,
            error: 0,
        })
        .unwrap();

    broadcast_tx
        .send(WatchEvent {
            key: key.clone(),
            value: Bytes::new(),
            event_type: WatchEventType::Delete as i32,
            error: 0,
        })
        .unwrap();

    // Receive all in order
    let event1 = handle.receiver_mut().recv().await.unwrap();
    assert_eq!(event1.value, Bytes::from("value1"));

    let event2 = handle.receiver_mut().recv().await.unwrap();
    assert_eq!(event2.value, Bytes::from("value2"));

    let event3 = handle.receiver_mut().recv().await.unwrap();
    assert_eq!(event3.event_type, WatchEventType::Delete as i32);
}

#[tokio::test]
async fn test_watcher_count_after_partial_cleanup() {
    let (_, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("count_key");

    let handle1 = registry.register(key.clone());
    let _handle2 = registry.register(key.clone());
    let _handle3 = registry.register(key.clone());

    assert_eq!(registry.watcher_count(&key), 3);

    drop(handle1);
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(registry.watcher_count(&key), 2);
}

#[tokio::test]
async fn test_different_keys_isolated() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(10);

    let key1 = Bytes::from("key1");
    let key2 = Bytes::from("key2");

    let mut handle1 = registry.register(key1.clone());
    let mut handle2 = registry.register(key2.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send event only for key1
    broadcast_tx
        .send(WatchEvent {
            key: key1.clone(),
            value: Bytes::from("value1"),
            event_type: WatchEventType::Put as i32,
            error: 0,
        })
        .unwrap();

    // handle1 should receive
    let event = timeout(Duration::from_millis(100), handle1.receiver_mut().recv())
        .await
        .expect("Timeout")
        .expect("Channel closed");
    assert_eq!(event.key, key1);

    // handle2 should NOT receive (timeout)
    let result = timeout(Duration::from_millis(50), handle2.receiver_mut().recv()).await;
    assert!(result.is_err(), "handle2 should not receive event");
}

#[tokio::test]
async fn test_watcher_buffer_overflow() {
    // Small buffer to force overflow
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(2);

    let key = Bytes::from("overflow_key");
    let mut handle = registry.register(key.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send more events than buffer size WITHOUT consuming
    for i in 0..10 {
        broadcast_tx
            .send(WatchEvent {
                key: key.clone(),
                value: Bytes::from(format!("value{i}")),
                event_type: WatchEventType::Put as i32,
                error: 0,
            })
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Try to receive - should get only buffered events (older ones dropped)
    let mut received_count = 0;
    while let Ok(Some(_)) = timeout(Duration::from_millis(50), handle.receiver_mut().recv()).await {
        received_count += 1;
    }

    // Should have lost some events due to buffer overflow
    assert!(
        received_count <= 2,
        "Should receive at most buffer_size events, got {received_count}"
    );
}

#[tokio::test]
async fn test_into_receiver_disables_cleanup() {
    let (_, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("test_key");
    let handle = registry.register(key.clone());

    assert_eq!(registry.watcher_count(&key), 1);

    // into_receiver() disables auto-cleanup
    let (_id, _key, _receiver) = handle.into_receiver();

    // Even after dropping variables, watcher should remain
    // (cleanup disabled because unregister_tx was set to None)
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Watcher still registered (cleanup was disabled)
    // Note: In real usage, receiver drop will eventually trigger cleanup via send failure
    assert_eq!(registry.watcher_count(&key), 1);
}

#[tokio::test]
async fn test_concurrent_register_unregister() {
    let (_broadcast_tx, registry, dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("concurrent_key");

    let mut handles = vec![];
    for _ in 0..10 {
        let reg = Arc::clone(&registry);
        let k = key.clone();
        let handle = tokio::spawn(async move {
            let watcher = reg.register(k);
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(watcher);
            // Yield to ensure drop completes
            tokio::task::yield_now().await;
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Give more time for all unregister messages to be processed
    tokio::time::sleep(Duration::from_millis(200)).await;

    // All should be cleaned up
    assert_eq!(registry.watcher_count(&key), 0);

    // Keep dispatcher alive
    drop(_broadcast_tx);
    let _ = timeout(Duration::from_secs(1), dispatcher_handle).await;
}

#[tokio::test]
async fn test_dispatcher_shutdown_on_broadcast_close() {
    let (broadcast_tx, registry, dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("test_key");
    let _handle = registry.register(key.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Close broadcast channel
    drop(broadcast_tx);

    // Dispatcher should exit gracefully
    let result = timeout(Duration::from_secs(2), dispatcher_handle).await;
    assert!(
        result.is_ok(),
        "Dispatcher should exit within 2s on channel close"
    );
}
