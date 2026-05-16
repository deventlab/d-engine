//! Unit tests for Watch system (WatchRegistry + WatchDispatcher)
//!
//! Tests the core functionality of the watch system including:
//! - Event notification via broadcast channel
//! - Multiple watchers on same key
//! - Automatic cleanup on drop
//! - Key isolation
//! - Dispatcher event distribution

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio::time::timeout;

use super::*;

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

// ---------------------------------------------------------------------------
// #294: overflow protection — buffer overflow → CANCELED notification
//
// Requires proto additions before these compile:
//   WatchEventType::Canceled = 2        (client_api.proto)
//   ErrorCode::WatchBufferOverflow = 5001  (error.proto)
//
// With buffer_size=N, channel capacity=N+1 (1 slot reserved for cancel).
// Dispatch checks capacity() before each normal event:
//   capacity > 1 → send normally
//   capacity == 1 → send CANCELED to the reserved slot, mark dead
// ---------------------------------------------------------------------------

use d_engine_proto::error::ErrorCode;

fn put_event(
    key: &Bytes,
    value: &str,
) -> WatchEvent {
    WatchEvent {
        key: key.clone(),
        value: Bytes::from(value.to_owned()),
        event_type: WatchEventType::Put as i32,
        error: 0,
    }
}

// buffer_size=2 → capacity=3.
// Events 1,2 fill slots (capacity 3→2→1).
// Event 3: capacity==1 → CANCELED sent to the reserved slot.
// Consumer sees: event1, event2, CANCELED (exactly buffer_size+1 items).
#[tokio::test]
async fn test_watcher_buffer_overflow_sends_cancel_event() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(2);
    let key = Bytes::from("overflow_key");
    let mut handle = registry.register(key.clone());

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Fill buffer: 3 events, no consumer
    for i in 0..3 {
        broadcast_tx.send(put_event(&key, &format!("v{i}"))).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain: expect exactly buffer_size normal events then CANCELED
    let e1 = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("timeout e1")
        .expect("channel closed e1");
    assert_eq!(e1.event_type, WatchEventType::Put as i32);

    let e2 = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("timeout e2")
        .expect("channel closed e2");
    assert_eq!(e2.event_type, WatchEventType::Put as i32);

    let cancel = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("timeout cancel")
        .expect("channel closed cancel");
    assert_eq!(cancel.event_type, WatchEventType::Canceled as i32);

    // Channel drained, no further events. After unregistration the sender is
    // dropped, so recv() returns None (Ok(None)) rather than timing out.
    let nothing = timeout(Duration::from_millis(50), handle.receiver_mut().recv()).await;
    assert!(
        matches!(nothing, Err(_) | Ok(None)),
        "expected no more events after CANCELED, got: {:?}",
        nothing
    );
}

// CANCELED must carry the correct key, empty value, and WATCH_BUFFER_OVERFLOW error.
#[tokio::test]
async fn test_cancel_event_has_correct_fields() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(1);
    let key = Bytes::from("field_check_key");
    let mut handle = registry.register(key.clone());

    tokio::time::sleep(Duration::from_millis(20)).await;

    // buffer_size=1 → capacity=2; 2 events trigger cancel on the second
    broadcast_tx.send(put_event(&key, "v1")).unwrap();
    broadcast_tx.send(put_event(&key, "v2")).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let _normal = handle.receiver_mut().recv().await.unwrap(); // consume v1

    let cancel = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("timeout")
        .expect("channel closed");

    assert_eq!(cancel.event_type, WatchEventType::Canceled as i32);
    assert_eq!(cancel.error, ErrorCode::WatchBufferOverflow as i32);
    assert_eq!(cancel.key, key);
    assert_eq!(cancel.value, Bytes::new());
}

// After overflow, the watcher must be removed from the registry.
#[tokio::test]
async fn test_overflow_removes_watcher_from_registry() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(1);
    let key = Bytes::from("registry_key");
    let _handle = registry.register(key.clone());

    assert_eq!(registry.watcher_count(&key), 1);

    tokio::time::sleep(Duration::from_millis(20)).await;

    // 2 events → overflow (buffer_size=1, capacity=2)
    broadcast_tx.send(put_event(&key, "v1")).unwrap();
    broadcast_tx.send(put_event(&key, "v2")).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(registry.watcher_count(&key), 0);
}

// CANCELED must be the last event. No normal events follow it.
#[tokio::test]
async fn test_normal_events_precede_cancel_in_channel() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(2);
    let key = Bytes::from("order_key");
    let mut handle = registry.register(key.clone());

    tokio::time::sleep(Duration::from_millis(20)).await;

    broadcast_tx.send(put_event(&key, "v1")).unwrap();
    broadcast_tx.send(put_event(&key, "v2")).unwrap();
    broadcast_tx.send(put_event(&key, "v3")).unwrap(); // triggers cancel
    broadcast_tx.send(put_event(&key, "v4")).unwrap(); // watcher already dead
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut events: Vec<WatchEvent> = Vec::new();
    while let Ok(Some(ev)) = timeout(Duration::from_millis(50), handle.receiver_mut().recv()).await
    {
        events.push(ev);
    }

    // Last event must be CANCELED, everything before must be PUT
    assert!(!events.is_empty());
    let last = events.last().unwrap();
    assert_eq!(last.event_type, WatchEventType::Canceled as i32);
    for ev in &events[..events.len() - 1] {
        assert_eq!(ev.event_type, WatchEventType::Put as i32);
    }
    // v4 must NOT appear (watcher was dead when v4 was dispatched)
    assert!(
        !events.iter().any(|e| e.value == "v4"),
        "v4 delivered to dead watcher"
    );
}

// Receiver drop (TrySendError::Closed) → silent cleanup, no CANCELED sent.
// Verification: registry count drops to 0 after next dispatch.
#[tokio::test]
async fn test_closed_receiver_cleaned_up_silently() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(10);
    let key = Bytes::from("closed_key");
    let handle = registry.register(key.clone());

    assert_eq!(registry.watcher_count(&key), 1);

    // Drop the receiver, keeping the channel open only on the sender side
    let (_id, _key_bytes, receiver) = handle.into_receiver();
    drop(receiver); // sender now gets Closed on next try_send

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Dispatch triggers Closed path → silent cleanup, no CANCELED
    broadcast_tx.send(put_event(&key, "v1")).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(registry.watcher_count(&key), 0);
}

// Overflow of one slow watcher must not cancel a healthy watcher on the same key.
#[tokio::test]
async fn test_slow_watcher_overflow_does_not_affect_healthy_watcher() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(2);
    let key = Bytes::from("isolation_key");

    let mut slow = registry.register(key.clone()); // never consumed
    let mut fast = registry.register(key.clone()); // consumed between dispatches

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Interleave: send one event, drain fast, repeat
    // After 2 rounds: slow capacity=1 (reserved), fast capacity=3 (always drained)
    for i in 0..2 {
        broadcast_tx.send(put_event(&key, &format!("v{i}"))).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        // drain fast so it never overflows
        let _ = timeout(Duration::from_millis(50), fast.receiver_mut().recv()).await;
    }

    // This event: slow capacity==1 → CANCELED; fast receives normally
    broadcast_tx.send(put_event(&key, "overflow_trigger")).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // slow: drained earlier events + final CANCELED
    loop {
        let ev = timeout(Duration::from_millis(50), slow.receiver_mut().recv())
            .await
            .ok()
            .flatten();
        match ev {
            Some(e) if e.event_type == WatchEventType::Canceled as i32 => break,
            Some(_) => continue,
            None => panic!("slow watcher closed without CANCELED"),
        }
    }

    // fast: still alive, registry count == 1
    assert_eq!(
        registry.watcher_count(&key),
        1,
        "fast watcher should remain"
    );

    // Drain the "overflow_trigger" event that fast also received
    let _ = timeout(Duration::from_millis(100), fast.receiver_mut().recv()).await;

    // fast continues to receive new events
    broadcast_tx.send(put_event(&key, "after_overflow")).unwrap();
    let ev = timeout(Duration::from_millis(100), fast.receiver_mut().recv())
        .await
        .expect("fast watcher timed out after peer overflow")
        .expect("fast watcher channel closed");
    assert_eq!(ev.value, Bytes::from("after_overflow"));
}

// Healthy watcher: consume every event before the reserved slot is reached.
// Must never trigger CANCELED regardless of how many events are sent.
#[tokio::test]
async fn test_healthy_watcher_no_false_cancel() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(2);
    let key = Bytes::from("healthy_key");
    let mut handle = registry.register(key.clone());

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Send and consume 20 events; consumer always drains before next send
    for i in 0..20 {
        broadcast_tx.send(put_event(&key, &format!("v{i}"))).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let ev = timeout(Duration::from_millis(50), handle.receiver_mut().recv())
            .await
            .expect("timeout")
            .expect("channel closed");

        assert_ne!(
            ev.event_type,
            WatchEventType::Canceled as i32,
            "false cancel on event {i}"
        );
    }

    // Watcher still registered
    assert_eq!(registry.watcher_count(&key), 1);
}

// buffer_size=1 → capacity=2 (1 normal + 1 cancel reserved).
// First event fills the normal slot (capacity 2→1).
// Second event: capacity==1 → send CANCELED immediately.
#[tokio::test]
async fn test_buffer_size_1_overflows_on_second_event() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(1);
    let key = Bytes::from("tiny_buffer_key");
    let mut handle = registry.register(key.clone());

    tokio::time::sleep(Duration::from_millis(20)).await;

    broadcast_tx.send(put_event(&key, "v1")).unwrap();
    broadcast_tx.send(put_event(&key, "v2")).unwrap(); // triggers CANCELED
    tokio::time::sleep(Duration::from_millis(50)).await;

    let e1 = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("timeout e1")
        .expect("closed e1");
    assert_eq!(e1.event_type, WatchEventType::Put as i32);
    assert_eq!(e1.value, Bytes::from("v1"));

    let cancel = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("timeout cancel")
        .expect("closed cancel");
    assert_eq!(cancel.event_type, WatchEventType::Canceled as i32);
    assert_eq!(cancel.error, ErrorCode::WatchBufferOverflow as i32);

    // Exactly 2 items (buffer_size + 1), no more. The sender is dropped
    // when the watcher is unregistered, so recv() returns None immediately.
    let nothing = timeout(Duration::from_millis(50), handle.receiver_mut().recv()).await;
    assert!(
        matches!(nothing, Err(_) | Ok(None)),
        "expected no more events after CANCELED, got: {:?}",
        nothing
    );
}

// After overflow and unregistration, subsequent broadcasts must not
// attempt delivery to the dead watcher (no panic, registry stable).
#[tokio::test]
async fn test_events_not_delivered_after_overflow() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(1);
    let key = Bytes::from("post_overflow_key");
    let _handle = registry.register(key.clone());

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Trigger overflow
    broadcast_tx.send(put_event(&key, "v1")).unwrap();
    broadcast_tx.send(put_event(&key, "v2")).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(registry.watcher_count(&key), 0);

    // Subsequent events for the same key → no watcher in registry → silently ignored
    for i in 0..5 {
        broadcast_tx.send(put_event(&key, &format!("post{i}"))).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Registry remains stable
    assert_eq!(registry.watcher_count(&key), 0);
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
