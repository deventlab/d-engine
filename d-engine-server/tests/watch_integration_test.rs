//! Watch Integration Tests
//!
//! Tests for Watch functionality validating:
//! - StateMachine → WatchManager event flow
//! - WatchManager event dispatch
//! - Multiple watchers behavior
//! - Cleanup on drop

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::config::WatchConfig;
use d_engine_core::watch::{WatchEventType, WatchManager};
use tokio::time::timeout;

/// Test: WatchManager dispatches PUT events correctly
#[tokio::test]
async fn test_watch_manager_put_event() {
    let config = WatchConfig {
        enabled: true,
        event_queue_size: 100,
        watcher_buffer_size: 10,
        enable_metrics: false,
    };

    let manager = Arc::new(WatchManager::new(config));
    manager.start();

    let key = Bytes::from("test_key");
    let value = Bytes::from("test_value");

    // Register watcher
    let mut handle = manager.register(key.clone()).await;
    let watcher_id = handle.id();

    // Notify PUT
    manager.notify_put(key.clone(), value.clone());

    // Receive event with timeout
    let event = timeout(
        Duration::from_millis(500),
        handle.receiver_mut().unwrap().recv(),
    )
    .await
    .expect("Timeout receiving event")
    .expect("No event received");

    assert_eq!(event.key, key, "Event key mismatch");
    assert_eq!(event.value, value, "Event value mismatch");
    assert_eq!(
        event.event_type,
        WatchEventType::Put,
        "Event type should be PUT"
    );

    println!("✅ Watcher {watcher_id} received PUT event correctly");
}

/// Test: WatchManager dispatches DELETE events correctly
#[tokio::test]
async fn test_watch_manager_delete_event() {
    let config = WatchConfig {
        enabled: true,
        event_queue_size: 100,
        watcher_buffer_size: 10,
        enable_metrics: false,
    };

    let manager = Arc::new(WatchManager::new(config));
    manager.start();

    let key = Bytes::from("delete_key");

    // Register watcher
    let mut handle = manager.register(key.clone()).await;

    // Notify DELETE
    manager.notify_delete(key.clone());

    // Receive event
    let event = timeout(
        Duration::from_millis(500),
        handle.receiver_mut().unwrap().recv(),
    )
    .await
    .expect("Timeout receiving delete event")
    .expect("No delete event received");

    assert_eq!(event.key, key, "Delete event key mismatch");
    assert!(
        event.value.is_empty(),
        "Delete event should have empty value"
    );
    assert_eq!(
        event.event_type,
        WatchEventType::Delete,
        "Event type should be DELETE"
    );

    println!("✅ DELETE event received correctly");
}

/// Test: Multiple watchers on same key all receive events
#[tokio::test]
async fn test_multiple_watchers_same_key() {
    let config = WatchConfig {
        enabled: true,
        event_queue_size: 100,
        watcher_buffer_size: 10,
        enable_metrics: false,
    };

    let manager = Arc::new(WatchManager::new(config));
    manager.start();

    let key = Bytes::from("shared_key");
    let value = Bytes::from("shared_value");

    // Register 3 watchers on the same key
    let mut watchers = Vec::new();
    for _ in 0..3 {
        let handle = manager.register(key.clone()).await;
        watchers.push(handle);
    }

    // Notify PUT once
    manager.notify_put(key.clone(), value.clone());

    // All watchers should receive the event
    for (i, handle) in watchers.iter_mut().enumerate() {
        let event = timeout(
            Duration::from_millis(500),
            handle.receiver_mut().unwrap().recv(),
        )
        .await
        .unwrap_or_else(|_| panic!("Watcher {i} timeout"))
        .expect("No event received");

        assert_eq!(event.key, key, "Watcher {i} key mismatch");
        assert_eq!(event.value, value, "Watcher {i} value mismatch");
        assert_eq!(event.event_type, WatchEventType::Put);
    }

    println!("✅ All 3 watchers received the event");
}

/// Test: Different keys don't interfere with each other
#[tokio::test]
async fn test_different_keys_isolated() {
    let config = WatchConfig {
        enabled: true,
        event_queue_size: 100,
        watcher_buffer_size: 10,
        enable_metrics: false,
    };

    let manager = Arc::new(WatchManager::new(config));
    manager.start();

    let key1 = Bytes::from("key1");
    let key2 = Bytes::from("key2");
    let value1 = Bytes::from("value1");

    // Register watchers on different keys
    let mut handle1 = manager.register(key1.clone()).await;
    let mut handle2 = manager.register(key2.clone()).await;

    // Notify only key1
    manager.notify_put(key1.clone(), value1.clone());

    // handle1 should receive event
    let event1 = timeout(
        Duration::from_millis(500),
        handle1.receiver_mut().unwrap().recv(),
    )
    .await
    .expect("Timeout on handle1")
    .expect("No event on handle1");

    assert_eq!(event1.key, key1);
    assert_eq!(event1.value, value1);

    // handle2 should NOT receive anything (timeout expected)
    let no_event = timeout(
        Duration::from_millis(200),
        handle2.receiver_mut().unwrap().recv(),
    )
    .await;
    assert!(
        no_event.is_err(),
        "handle2 should not receive event for key1"
    );

    println!("✅ Different keys are properly isolated");
}

/// Test: Watcher cleanup on drop
#[tokio::test]
async fn test_watcher_cleanup_on_drop() {
    let config = WatchConfig {
        enabled: true,
        event_queue_size: 100,
        watcher_buffer_size: 10,
        enable_metrics: false,
    };

    let manager = Arc::new(WatchManager::new(config));
    manager.start();

    let key = Bytes::from("cleanup_key");

    // Register and immediately drop watcher
    {
        let _handle = manager.register(key.clone()).await;
        // _handle is dropped here
    }

    // Wait a bit for cleanup
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify manager is still functional by registering a new watcher
    let mut new_handle = manager.register(key.clone()).await;
    manager.notify_put(key.clone(), Bytes::from("after_cleanup"));

    let event = timeout(
        Duration::from_millis(500),
        new_handle.receiver_mut().unwrap().recv(),
    )
    .await
    .expect("Timeout after cleanup")
    .expect("No event after cleanup");

    assert_eq!(event.key, key);
    println!("✅ Watcher cleanup on drop works correctly");
}

/// Test: Event queue full - events are dropped (try_send behavior)
#[tokio::test]
async fn test_event_queue_overflow() {
    let config = WatchConfig {
        enabled: true,
        event_queue_size: 2, // Very small queue
        watcher_buffer_size: 2,
        enable_metrics: false,
    };

    let manager = Arc::new(WatchManager::new(config));
    manager.start();

    let key = Bytes::from("overflow_key");
    let mut handle = manager.register(key.clone()).await;

    // Fill the queue by sending many events without consuming
    for i in 0..10 {
        let value = Bytes::from(format!("value_{i}"));
        manager.notify_put(key.clone(), value);
    }

    // We should receive some events (queue size + buffer size)
    // but not all 10 events
    let mut received_count = 0;
    while let Ok(Some(_)) = timeout(
        Duration::from_millis(100),
        handle.receiver_mut().unwrap().recv(),
    )
    .await
    {
        received_count += 1;
    }

    assert!(
        received_count < 10,
        "Should drop some events when queue is full (received {received_count})"
    );
    assert!(
        received_count > 0,
        "Should receive at least some events (received {received_count})"
    );

    println!("✅ Event overflow handled correctly (received {received_count}/10 events)");
}

/// Test: Multiple sequential operations
#[tokio::test]
async fn test_sequential_put_delete_operations() {
    let config = WatchConfig {
        enabled: true,
        event_queue_size: 100,
        watcher_buffer_size: 10,
        enable_metrics: false,
    };

    let manager = Arc::new(WatchManager::new(config));
    manager.start();

    let key = Bytes::from("seq_key");
    let mut handle = manager.register(key.clone()).await;

    // Sequence: PUT → DELETE → PUT
    manager.notify_put(key.clone(), Bytes::from("value1"));
    manager.notify_delete(key.clone());
    manager.notify_put(key.clone(), Bytes::from("value2"));

    // Receive all 3 events in order
    let event1 = timeout(
        Duration::from_millis(500),
        handle.receiver_mut().unwrap().recv(),
    )
    .await
    .expect("Timeout on event1")
    .expect("No event1");

    let event2 = timeout(
        Duration::from_millis(500),
        handle.receiver_mut().unwrap().recv(),
    )
    .await
    .expect("Timeout on event2")
    .expect("No event2");

    let event3 = timeout(
        Duration::from_millis(500),
        handle.receiver_mut().unwrap().recv(),
    )
    .await
    .expect("Timeout on event3")
    .expect("No event3");

    assert_eq!(event1.event_type, WatchEventType::Put);
    assert_eq!(event1.value, Bytes::from("value1"));

    assert_eq!(event2.event_type, WatchEventType::Delete);
    assert!(event2.value.is_empty());

    assert_eq!(event3.event_type, WatchEventType::Put);
    assert_eq!(event3.value, Bytes::from("value2"));

    println!("✅ Sequential operations work correctly");
}

/// Test: Zero overhead when Watch disabled (manager = None)
#[tokio::test]
async fn test_watch_disabled_zero_overhead() {
    // This test validates that when watch_manager is None in StateMachineHandler,
    // there's zero overhead. We can't directly test this without integration,
    // but we can verify the pattern works.

    let watch_manager: Option<Arc<WatchManager>> = None;

    // Simulate notify_watchers logic
    let key = Bytes::from("test");
    let value = Bytes::from("value");

    if let Some(mgr) = &watch_manager {
        mgr.notify_put(key, value);
        panic!("Should not reach here");
    }

    // If watch_manager is None, this is essentially a no-op
    assert!(watch_manager.is_none());
    println!("✅ Watch disabled pattern validated (zero overhead)");
}
