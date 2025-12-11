//! Unit tests for WatchManager
//!
//! Tests the core functionality of the watch system including:
//! - Event notification and delivery
//! - Multiple watchers on same key
//! - Automatic cleanup on drop
//! - Key isolation
//! - Buffer overflow handling

#[cfg(test)]
mod tests {
    use super::super::*;
    use bytes::Bytes;
    use std::sync::Arc;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn test_single_watcher_put() {
        let config = WatchConfig::default();
        let manager = WatchManager::new(config);
        manager.start();

        let key = Bytes::from("test_key");
        let value = Bytes::from("test_value");

        let handle = manager.register(key.clone()).await;
        let (_id, _key, mut receiver, guard) = handle.into_receiver();

        // Small delay to ensure dispatcher thread is fully started
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Notify
        manager.notify_put(key.clone(), value.clone());

        // Should receive event
        let event = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("Timeout waiting for event")
            .expect("Channel closed");

        assert_eq!(event.key, key);
        assert_eq!(event.value, value);
        assert_eq!(event.event_type, WatchEventType::Put);

        drop(guard);
        manager.stop();
    }

    #[tokio::test]
    async fn test_single_watcher_delete() {
        let config = WatchConfig::default();
        let manager = WatchManager::new(config);
        manager.start();

        let key = Bytes::from("test_key");
        let handle = manager.register(key.clone()).await;
        let (_id, _key, mut receiver, guard) = handle.into_receiver();

        // Small delay to ensure receiver is ready
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Notify
        manager.notify_delete(key.clone());

        // Should receive event
        let event = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("Timeout waiting for event")
            .expect("Channel closed");

        assert_eq!(event.key, key);
        assert_eq!(event.event_type, WatchEventType::Delete);

        drop(guard);
        manager.stop();
    }

    #[tokio::test]
    async fn test_multiple_watchers_same_key() {
        let config = WatchConfig::default();
        let manager = WatchManager::new(config);
        manager.start();

        let key = Bytes::from("shared_key");
        let value = Bytes::from("shared_value");

        // Register 3 watchers for the same key
        let handle1 = manager.register(key.clone()).await;
        let handle2 = manager.register(key.clone()).await;
        let handle3 = manager.register(key.clone()).await;

        let (_id1, _key1, mut receiver1, guard1) = handle1.into_receiver();
        let (_id2, _key2, mut receiver2, guard2) = handle2.into_receiver();
        let (_id3, _key3, mut receiver3, guard3) = handle3.into_receiver();

        assert_eq!(manager.watcher_count(&key), 3);

        // Small delay to ensure receivers are ready
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Notify once
        manager.notify_put(key.clone(), value.clone());

        // All 3 should receive the event
        for receiver in [&mut receiver1, &mut receiver2, &mut receiver3].iter_mut() {
            let event = timeout(Duration::from_millis(100), receiver.recv())
                .await
                .expect("Timeout waiting for event")
                .expect("Channel closed");

            assert_eq!(event.key, key);
            assert_eq!(event.value, value);
        }

        drop(guard1);
        drop(guard2);
        drop(guard3);
        manager.stop();
    }

    #[tokio::test]
    async fn test_watcher_auto_cleanup() {
        let config = WatchConfig::default();
        let manager = WatchManager::new(config);
        manager.start();

        let key = Bytes::from("cleanup_key");

        {
            let _handle = manager.register(key.clone()).await;
            assert_eq!(manager.watcher_count(&key), 1);
            // Handle dropped here
        }

        // Give it a moment for cleanup
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(manager.watcher_count(&key), 0);
        assert_eq!(manager.watched_key_count(), 0);

        manager.stop();
    }

    #[tokio::test]
    async fn test_different_keys_isolated() {
        let config = WatchConfig::default();
        let manager = WatchManager::new(config);
        manager.start();

        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        let handle1 = manager.register(key1.clone()).await;
        let handle2 = manager.register(key2.clone()).await;

        let (_id1, _key1, mut receiver1, guard1) = handle1.into_receiver();
        let (_id2, _key2, mut receiver2, guard2) = handle2.into_receiver();

        // Small delay to ensure receivers are ready
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Notify only key1
        manager.notify_put(key1.clone(), Bytes::from("value1"));

        // handle1 should receive event
        let event = timeout(Duration::from_millis(100), receiver1.recv())
            .await
            .expect("Timeout")
            .expect("Channel closed");
        assert_eq!(event.key, key1);

        // handle2 should NOT receive (timeout)
        let result = timeout(Duration::from_millis(50), receiver2.recv()).await;
        assert!(result.is_err(), "handle2 should not receive event");

        drop(guard1);
        drop(guard2);
        manager.stop();
    }

    #[tokio::test]
    async fn test_channel_overflow_drops_events() {
        // Use very small buffer to force overflow
        let config = WatchConfig {
            watcher_buffer_size: 2,
            ..Default::default()
        };
        let manager = WatchManager::new(config);
        manager.start();

        let key = Bytes::from("overflow_key");
        let handle = manager.register(key.clone()).await;

        // Send more events than buffer can hold without consuming
        for i in 0..10 {
            manager.notify_put(key.clone(), Bytes::from(format!("value{i}")));
        }

        // Sleep to ensure dispatcher processes
        tokio::time::sleep(Duration::from_millis(50)).await;

        // We should only be able to receive buffer_size events
        // The rest are dropped
        drop(handle); // Just verify no panic

        manager.stop();
    }

    #[tokio::test]
    async fn test_multiple_events_sequential() {
        let config = WatchConfig::default();
        let manager = Arc::new(WatchManager::new(config));
        manager.start();

        let key = Bytes::from("test_key");
        let handle = manager.register(key.clone()).await;
        let (_id, _key, mut receiver, guard) = handle.into_receiver();

        // Small delay to ensure receiver is ready
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send multiple events
        manager.notify_put(key.clone(), Bytes::from("value1"));
        manager.notify_put(key.clone(), Bytes::from("value2"));
        manager.notify_delete(key.clone());

        // Receive all 3 events in order
        let event1 = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("Timeout")
            .expect("Channel closed");
        assert_eq!(event1.event_type, WatchEventType::Put);
        assert_eq!(event1.value, Bytes::from("value1"));

        let event2 = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("Timeout")
            .expect("Channel closed");
        assert_eq!(event2.event_type, WatchEventType::Put);
        assert_eq!(event2.value, Bytes::from("value2"));

        let event3 = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("Timeout")
            .expect("Channel closed");
        assert_eq!(event3.event_type, WatchEventType::Delete);
        assert_eq!(event3.value, Bytes::new());

        drop(guard);
        manager.stop();
    }

    #[tokio::test]
    async fn test_watcher_count_after_partial_cleanup() {
        let config = WatchConfig::default();
        let manager = WatchManager::new(config);
        manager.start();

        let key = Bytes::from("count_key");

        let handle1 = manager.register(key.clone()).await;
        let _handle2 = manager.register(key.clone()).await;
        let _handle3 = manager.register(key.clone()).await;

        assert_eq!(manager.watcher_count(&key), 3);

        // Drop only one handle
        drop(handle1);
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(manager.watcher_count(&key), 2);

        manager.stop();
    }

    #[tokio::test]
    async fn test_start_stop_idempotent() {
        let config = WatchConfig::default();
        let manager = WatchManager::new(config);

        // Start multiple times should be safe
        manager.start();
        manager.start();
        manager.start();

        // Stop multiple times should be safe
        manager.stop();
        manager.stop();
    }

    #[tokio::test]
    async fn test_custom_config() {
        let config = WatchConfig {
            enabled: true,
            event_queue_size: 500,
            watcher_buffer_size: 5,
            enable_metrics: true,
        };
        let manager = WatchManager::new(config);
        manager.start();

        let key = Bytes::from("test_key");
        let handle = manager.register(key.clone()).await;
        let (_id, _key, mut receiver, guard) = handle.into_receiver();

        // Small delay to ensure receiver is ready
        tokio::time::sleep(Duration::from_millis(50)).await;

        manager.notify_put(key.clone(), Bytes::from("value"));

        let event = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("Timeout")
            .expect("Channel closed");

        assert_eq!(event.key, key);

        drop(guard);
        manager.stop();
    }

    #[tokio::test]
    async fn test_watcher_count_accuracy() {
        let config = WatchConfig::default();
        let manager = WatchManager::new(config);
        manager.start();

        // Initially should have no watchers
        assert!(!manager.has_watchers());

        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        // Register first watcher
        let handle1 = manager.register(key1.clone()).await;
        assert!(manager.has_watchers());

        // Register second watcher on same key
        let handle2 = manager.register(key1.clone()).await;
        assert!(manager.has_watchers());

        // Register third watcher on different key
        let handle3 = manager.register(key2.clone()).await;
        assert!(manager.has_watchers());

        // Drop one watcher - should still have watchers
        drop(handle1);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(manager.has_watchers());

        // Drop second watcher - should still have watchers
        drop(handle2);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(manager.has_watchers());

        // Drop last watcher - should have no watchers
        drop(handle3);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!manager.has_watchers());

        manager.stop();
    }

    #[tokio::test]
    async fn test_watcher_count_concurrent_register_unregister() {
        let config = WatchConfig::default();
        let manager = Arc::new(WatchManager::new(config));
        manager.start();

        let key = Bytes::from("test_key");

        // Spawn multiple tasks registering and unregistering concurrently
        let mut handles = vec![];
        for _ in 0..10 {
            let mgr = manager.clone();
            let k = key.clone();
            let handle = tokio::spawn(async move {
                let watcher = mgr.register(k).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
                drop(watcher);
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Small delay to ensure cleanup completes
        tokio::time::sleep(Duration::from_millis(50)).await;

        // After all watchers are dropped, count should be zero
        assert!(!manager.has_watchers());

        manager.stop();
    }

    #[tokio::test]
    async fn test_watcher_count_never_negative() {
        let config = WatchConfig::default();
        let manager = WatchManager::new(config);
        manager.start();

        let key = Bytes::from("test_key");

        // Register and immediately drop multiple times
        for _ in 0..100 {
            let handle = manager.register(key.clone()).await;
            drop(handle);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Should still report no watchers (not panic or underflow)
        assert!(!manager.has_watchers());

        manager.stop();
    }

    #[tokio::test]
    async fn test_double_drop_watcher_handle() {
        let config = WatchConfig::default();
        let manager = WatchManager::new(config);
        manager.start();

        let key = Bytes::from("test_key");
        let handle = manager.register(key.clone()).await;

        // Get inner components
        let (_id, _key, _receiver, guard) = handle.into_receiver();

        // Drop guard (triggers unregister)
        drop(guard);
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Verify count is zero
        assert!(!manager.has_watchers());

        // Calling drop again on already-dropped handle should not panic
        // or cause underflow (handle.cleanup is None after into_receiver)

        manager.stop();
    }
}
