//! Unit tests for Watch system (WatchRegistry + WatchDispatcher)
//!
//! Tests the core functionality of the watch system including:
//! - Event notification via broadcast channel
//! - Multiple watchers on same key
//! - Automatic cleanup on drop
//! - Key isolation
//! - Dispatcher event distribution

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use d_engine_proto::client::WatchResponse;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time::Duration;

use crate::watch::manager::prefix_segments;
use tokio::time::timeout;

use super::*;

/// Helper to create test watch system components.
/// `heartbeat_interval_ms` = 0 disables heartbeat (avoids Progress noise in most tests).
fn setup_watch_system(
    buffer_size: usize
) -> (
    broadcast::Sender<WatchResponse>,
    Arc<WatchRegistry>,
    tokio::task::JoinHandle<()>,
) {
    setup_watch_system_with_heartbeat(buffer_size, 0)
}

fn setup_watch_system_with_heartbeat(
    buffer_size: usize,
    heartbeat_interval_ms: u64,
) -> (
    broadcast::Sender<WatchResponse>,
    Arc<WatchRegistry>,
    tokio::task::JoinHandle<()>,
) {
    let (broadcast_tx, broadcast_rx) = broadcast::channel(1000);
    let (unregister_tx, unregister_rx) = mpsc::unbounded_channel();

    let registry = Arc::new(WatchRegistry::new(buffer_size, unregister_tx));
    let last_applied = Arc::new(AtomicU64::new(0));
    let dispatcher = WatchDispatcher::new(
        Arc::clone(&registry),
        broadcast_rx,
        unregister_rx,
        last_applied,
        heartbeat_interval_ms,
    );

    let handle = tokio::spawn(async move {
        dispatcher.run().await;
    });

    (broadcast_tx, registry, handle)
}

// Convenience: build a WatchResponse with the proto event type int.
fn proto_put_response(
    key: &Bytes,
    value: &str,
    revision: u64,
) -> WatchResponse {
    WatchResponse {
        key: key.clone(),
        value: Bytes::from(value.to_owned()),
        prev_value: Bytes::new(),
        event_type: d_engine_proto::client::WatchEventType::Put as i32,
        error: 0,
        revision,
    }
}

fn proto_delete_response(
    key: &Bytes,
    revision: u64,
) -> WatchResponse {
    WatchResponse {
        key: key.clone(),
        value: Bytes::new(),
        prev_value: Bytes::new(),
        event_type: d_engine_proto::client::WatchEventType::Delete as i32,
        error: 0,
        revision,
    }
}

#[tokio::test]
async fn test_register_single_watcher() {
    let (_, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("test_key");
    let _handle = registry.register(key.clone(), false).unwrap();

    assert_eq!(registry.watcher_count(&key), 1);
    assert_eq!(registry.watched_key_count(), 1);
}

#[tokio::test]
async fn test_register_multiple_watchers_same_key() {
    let (_, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("shared_key");

    let _handle1 = registry.register(key.clone(), false).unwrap();
    let _handle2 = registry.register(key.clone(), false).unwrap();
    let _handle3 = registry.register(key.clone(), false).unwrap();

    assert_eq!(registry.watcher_count(&key), 3);
    assert_eq!(registry.watched_key_count(), 1); // Only 1 unique key
}

#[tokio::test]
async fn test_watcher_auto_cleanup_on_drop() {
    let (_, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("cleanup_key");

    {
        let _handle = registry.register(key.clone(), false).unwrap();
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

    let mut handle = registry.register(key.clone(), false).unwrap();

    // Small delay to ensure dispatcher is ready
    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx.send(proto_put_response(&key, "test_value", 0)).unwrap();

    // Watcher should receive event
    let received = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("Timeout waiting for event")
        .expect("Channel closed");

    assert_eq!(received.key, key);
    assert_eq!(received.value, value);
    assert_eq!(received.event_type, WatchEventType::Put);
}

#[tokio::test]
async fn test_dispatcher_ignores_non_matching_key() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(10);

    let watched_key = Bytes::from("key1");
    let other_key = Bytes::from("key2");

    let mut handle = registry.register(watched_key.clone(), false).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx.send(proto_put_response(&other_key, "value", 0)).unwrap();

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

    let mut handle1 = registry.register(key.clone(), false).unwrap();
    let mut handle2 = registry.register(key.clone(), false).unwrap();
    let mut handle3 = registry.register(key.clone(), false).unwrap();

    assert_eq!(registry.watcher_count(&key), 3);

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx.send(proto_put_response(&key, "shared_value", 0)).unwrap();

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
    let mut handle = registry.register(key.clone(), false).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx.send(proto_delete_response(&key, 0)).unwrap();

    let received = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("Timeout")
        .expect("Channel closed");

    assert_eq!(received.event_type, WatchEventType::Delete);
    assert_eq!(received.value, Bytes::new());
}

#[tokio::test]
async fn test_multiple_events_sequential() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("test_key");
    let mut handle = registry.register(key.clone(), false).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx.send(proto_put_response(&key, "value1", 0)).unwrap();
    broadcast_tx.send(proto_put_response(&key, "value2", 0)).unwrap();
    broadcast_tx.send(proto_delete_response(&key, 0)).unwrap();

    // Receive all in order
    let event1 = handle.receiver_mut().recv().await.unwrap();
    assert_eq!(event1.value, Bytes::from("value1"));

    let event2 = handle.receiver_mut().recv().await.unwrap();
    assert_eq!(event2.value, Bytes::from("value2"));

    let event3 = handle.receiver_mut().recv().await.unwrap();
    assert_eq!(event3.event_type, WatchEventType::Delete);
}

#[tokio::test]
async fn test_watcher_count_after_partial_cleanup() {
    let (_, registry, _dispatcher_handle) = setup_watch_system(10);

    let key = Bytes::from("count_key");

    let handle1 = registry.register(key.clone(), false).unwrap();
    let _handle2 = registry.register(key.clone(), false).unwrap();
    let _handle3 = registry.register(key.clone(), false).unwrap();

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

    let mut handle1 = registry.register(key1.clone(), false).unwrap();
    let mut handle2 = registry.register(key2.clone(), false).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx.send(proto_put_response(&key1, "value1", 0)).unwrap();

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
// With buffer_size=N, channel capacity=N+1 (1 slot reserved for cancel).
// Dispatch checks capacity() before each normal event:
//   capacity > 1 → send normally
//   capacity == 1 → send CANCELED to the reserved slot, mark dead
// ---------------------------------------------------------------------------

fn put_event(
    key: &Bytes,
    value: &str,
) -> WatchResponse {
    WatchResponse {
        key: key.clone(),
        value: Bytes::from(value.to_owned()),
        prev_value: Bytes::new(),
        event_type: d_engine_proto::client::WatchEventType::Put as i32,
        error: 0,
        revision: 0,
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
    let mut handle = registry.register(key.clone(), false).unwrap();

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
    assert_eq!(e1.event_type, WatchEventType::Put);

    let e2 = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("timeout e2")
        .expect("channel closed e2");
    assert_eq!(e2.event_type, WatchEventType::Put);

    let cancel = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("timeout cancel")
        .expect("channel closed cancel");
    assert_eq!(cancel.event_type, WatchEventType::Canceled);

    // Channel drained, no further events. After unregistration the sender is
    // dropped, so recv() returns None rather than timing out.
    let nothing = timeout(Duration::from_millis(50), handle.receiver_mut().recv()).await;
    assert!(
        matches!(nothing, Err(_) | Ok(None)),
        "expected no more events after CANCELED, got: {:?}",
        nothing
    );
}

// CANCELED must carry the correct key and empty value.
#[tokio::test]
async fn test_cancel_event_has_correct_fields() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(1);
    let key = Bytes::from("field_check_key");
    let mut handle = registry.register(key.clone(), false).unwrap();

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

    assert_eq!(cancel.event_type, WatchEventType::Canceled);
    assert_eq!(cancel.key, key);
    assert_eq!(cancel.value, Bytes::new());
}

// After overflow, the watcher must be removed from the registry.
#[tokio::test]
async fn test_overflow_removes_watcher_from_registry() {
    let (broadcast_tx, registry, _dispatcher_handle) = setup_watch_system(1);
    let key = Bytes::from("registry_key");
    let _handle = registry.register(key.clone(), false).unwrap();

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
    let mut handle = registry.register(key.clone(), false).unwrap();

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
    assert_eq!(last.event_type, WatchEventType::Canceled);
    for ev in &events[..events.len() - 1] {
        assert_eq!(ev.event_type, WatchEventType::Put);
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
    let handle = registry.register(key.clone(), false).unwrap();

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

    let mut slow = registry.register(key.clone(), false).unwrap(); // never consumed
    let mut fast = registry.register(key.clone(), false).unwrap(); // consumed between dispatches

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
            Some(e) if e.event_type == WatchEventType::Canceled => break,
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
    let mut handle = registry.register(key.clone(), false).unwrap();

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
            WatchEventType::Canceled,
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
    let mut handle = registry.register(key.clone(), false).unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    broadcast_tx.send(put_event(&key, "v1")).unwrap();
    broadcast_tx.send(put_event(&key, "v2")).unwrap(); // triggers CANCELED
    tokio::time::sleep(Duration::from_millis(50)).await;

    let e1 = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("timeout e1")
        .expect("closed e1");
    assert_eq!(e1.event_type, WatchEventType::Put);
    assert_eq!(e1.value, Bytes::from("v1"));

    let cancel = timeout(Duration::from_millis(100), handle.receiver_mut().recv())
        .await
        .expect("timeout cancel")
        .expect("closed cancel");
    assert_eq!(cancel.event_type, WatchEventType::Canceled);

    // Exactly 2 items (buffer_size + 1), no more.
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
    let _handle = registry.register(key.clone(), false).unwrap();

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
    let handle = registry.register(key.clone(), false).unwrap();

    assert_eq!(registry.watcher_count(&key), 1);

    // into_receiver() disables auto-cleanup
    let (_id, _key, _receiver) = handle.into_receiver();

    // Even after dropping variables, watcher should remain
    // (cleanup disabled because unregister_tx was set to None)
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Watcher still registered (cleanup was disabled)
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
            let watcher = reg.register(k, false);
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
    let _handle = registry.register(key.clone(), false).unwrap();

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

// =============================================================================
// #300: Prefix Watch + Revision
// =============================================================================

fn setup_watch_system_with_max(
    buffer_size: usize,
    max_watcher_count: usize,
) -> (
    broadcast::Sender<WatchResponse>,
    Arc<WatchRegistry>,
    tokio::task::JoinHandle<()>,
) {
    let (broadcast_tx, broadcast_rx) = broadcast::channel(1000);
    let (unregister_tx, unregister_rx) = mpsc::unbounded_channel();
    let registry = Arc::new(WatchRegistry::new_with_limits(
        buffer_size,
        max_watcher_count,
        unregister_tx,
    ));
    let last_applied = Arc::new(AtomicU64::new(0));
    let dispatcher = WatchDispatcher::new(
        Arc::clone(&registry),
        broadcast_rx,
        unregister_rx,
        last_applied,
        0,
    );
    let handle = tokio::spawn(async move { dispatcher.run().await });
    (broadcast_tx, registry, handle)
}

// --- prefix_segments() unit tests (pure function) ---

/// #300: prefix_segments is the core of O(depth) dispatch.
#[test]
fn test_prefix_segments_decomposes_path_correctly() {
    let key = Bytes::from("/config/db/host");
    let segments = prefix_segments(&key);
    assert_eq!(
        segments,
        vec![
            Bytes::from("/"),
            Bytes::from("/config/"),
            Bytes::from("/config/db/"),
        ]
    );
}

#[test]
fn test_prefix_segments_root_key() {
    let key = Bytes::from("/");
    let segments = prefix_segments(&key);
    assert_eq!(segments, vec![Bytes::from("/")]);
}

#[test]
fn test_prefix_segments_single_level() {
    let key = Bytes::from("/config");
    let segments = prefix_segments(&key);
    assert_eq!(segments, vec![Bytes::from("/")]);
}

#[test]
fn test_prefix_segments_trailing_slash_included() {
    let key = Bytes::from("/config/");
    let segments = prefix_segments(&key);
    assert_eq!(segments, vec![Bytes::from("/"), Bytes::from("/config/")]);
}

// --- prefix watch: basic dispatch ---

/// #300: Core use case — a client watching an entire namespace receives events.
#[tokio::test]
async fn test_prefix_watch_matches_child_key() {
    let (broadcast_tx, registry, _handle) = setup_watch_system(10);
    let mut watcher = registry.register_prefix(Bytes::from("/config/"), false).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx
        .send(WatchResponse {
            key: Bytes::from("/config/db/host"),
            value: Bytes::from("10.0.0.1"),
            prev_value: Bytes::new(),
            event_type: d_engine_proto::client::WatchEventType::Put as i32,
            error: 0,
            revision: 1,
        })
        .unwrap();

    let received = timeout(Duration::from_millis(100), watcher.receiver_mut().recv())
        .await
        .expect("timeout")
        .expect("closed");
    assert_eq!(received.key, Bytes::from("/config/db/host"));
}

/// #300: Prefix isolation — a watcher on "/config/" must not receive events for "/dns/".
#[tokio::test]
async fn test_prefix_watch_does_not_match_unrelated_key() {
    let (broadcast_tx, registry, _handle) = setup_watch_system(10);
    let mut watcher = registry.register_prefix(Bytes::from("/config/"), false).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx
        .send(WatchResponse {
            key: Bytes::from("/dns/xyz"),
            value: Bytes::from("1.1.1.1"),
            prev_value: Bytes::new(),
            event_type: d_engine_proto::client::WatchEventType::Put as i32,
            error: 0,
            revision: 1,
        })
        .unwrap();

    let result = timeout(Duration::from_millis(100), watcher.receiver_mut().recv()).await;
    assert!(result.is_err(), "/config/ prefix must not match /dns/xyz");
}

/// #300: Slash boundary — "/config/" must not fire for the parent key "/config".
#[tokio::test]
async fn test_prefix_watch_slash_boundary_not_matched() {
    let (broadcast_tx, registry, _handle) = setup_watch_system(10);
    let mut watcher = registry.register_prefix(Bytes::from("/config/"), false).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx
        .send(WatchResponse {
            key: Bytes::from("/config"),
            value: Bytes::from("val"),
            prev_value: Bytes::new(),
            event_type: d_engine_proto::client::WatchEventType::Put as i32,
            error: 0,
            revision: 1,
        })
        .unwrap();

    let result = timeout(Duration::from_millis(100), watcher.receiver_mut().recv()).await;
    assert!(
        result.is_err(),
        "/config/ prefix must not match key /config"
    );
}

/// #300: The root prefix "/" acts as a global watch.
#[tokio::test]
async fn test_root_prefix_matches_child_keys() {
    let (broadcast_tx, registry, _handle) = setup_watch_system(10);
    let mut watcher = registry.register_prefix(Bytes::from("/"), false).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx
        .send(WatchResponse {
            key: Bytes::from("/services/api"),
            value: Bytes::from("up"),
            prev_value: Bytes::new(),
            event_type: d_engine_proto::client::WatchEventType::Put as i32,
            error: 0,
            revision: 1,
        })
        .unwrap();

    let received = timeout(Duration::from_millis(100), watcher.receiver_mut().recv())
        .await
        .expect("timeout")
        .expect("closed");
    assert_eq!(received.key, Bytes::from("/services/api"));
}

/// #300: Edge case fix — when the event key IS "/" (root key itself).
#[tokio::test]
async fn test_root_prefix_matches_root_key_itself() {
    let (broadcast_tx, registry, _handle) = setup_watch_system(10);
    let mut watcher = registry.register_prefix(Bytes::from("/"), false).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx
        .send(WatchResponse {
            key: Bytes::from("/"),
            value: Bytes::from("root"),
            prev_value: Bytes::new(),
            event_type: d_engine_proto::client::WatchEventType::Put as i32,
            error: 0,
            revision: 1,
        })
        .unwrap();

    let received = timeout(Duration::from_millis(100), watcher.receiver_mut().recv())
        .await
        .expect("root prefix must match root key")
        .expect("closed");
    assert_eq!(received.key, Bytes::from("/"));
}

/// #300: Exact and prefix watchers are independent and must both fire.
#[tokio::test]
async fn test_exact_and_prefix_both_notified_on_same_event() {
    let (broadcast_tx, registry, _handle) = setup_watch_system(10);
    let key = Bytes::from("/config/db/host");

    let mut exact_watcher = registry.register(key.clone(), false).unwrap();
    let mut prefix_watcher = registry.register_prefix(Bytes::from("/config/"), false).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx
        .send(WatchResponse {
            key: key.clone(),
            value: Bytes::from("10.0.0.1"),
            prev_value: Bytes::new(),
            event_type: d_engine_proto::client::WatchEventType::Put as i32,
            error: 0,
            revision: 5,
        })
        .unwrap();

    let exact_ev = timeout(
        Duration::from_millis(100),
        exact_watcher.receiver_mut().recv(),
    )
    .await
    .expect("exact timeout")
    .expect("exact closed");
    let prefix_ev = timeout(
        Duration::from_millis(100),
        prefix_watcher.receiver_mut().recv(),
    )
    .await
    .expect("prefix timeout")
    .expect("prefix closed");

    assert_eq!(exact_ev.key, key);
    assert_eq!(prefix_ev.key, key);
}

// --- register_prefix: validation ---

#[test]
fn test_register_prefix_rejects_missing_trailing_slash() {
    let (unregister_tx, _) = mpsc::unbounded_channel();
    let registry = WatchRegistry::new(10, unregister_tx);

    assert!(
        registry.register_prefix(Bytes::from("/config"), false).is_err(),
        "/config must be rejected — no trailing slash"
    );
    assert!(
        registry.register_prefix(Bytes::from("config/"), false).is_err(),
        "config/ must be rejected — no leading slash"
    );
}

// --- prefix watcher: overflow sends CANCELED ---

/// #300: Prefix watchers are subject to the same overflow protection as exact watchers.
#[tokio::test]
async fn test_prefix_watcher_overflow_sends_cancel() {
    let (broadcast_tx, registry, _handle) = setup_watch_system(1); // buffer_size=1
    let mut watcher = registry.register_prefix(Bytes::from("/config/"), false).unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    // 2 events fill buffer + trigger overflow (capacity = buffer_size+1 = 2)
    broadcast_tx
        .send(WatchResponse {
            key: Bytes::from("/config/key1"),
            value: Bytes::from("v1"),
            prev_value: Bytes::new(),
            event_type: d_engine_proto::client::WatchEventType::Put as i32,
            error: 0,
            revision: 1,
        })
        .unwrap();
    broadcast_tx
        .send(WatchResponse {
            key: Bytes::from("/config/key2"),
            value: Bytes::from("v2"),
            prev_value: Bytes::new(),
            event_type: d_engine_proto::client::WatchEventType::Put as i32,
            error: 0,
            revision: 2,
        })
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let _normal = watcher.receiver_mut().recv().await.unwrap();

    let cancel = timeout(Duration::from_millis(100), watcher.receiver_mut().recv())
        .await
        .expect("timeout")
        .expect("closed");
    assert_eq!(cancel.event_type, WatchEventType::Canceled);
}

// --- prefix watcher: auto-cleanup on drop ---

/// #300: Prefix watcher handles follow the same RAII cleanup contract as exact watchers.
#[tokio::test]
async fn test_prefix_watcher_cleanup_on_drop() {
    let (_, registry, _handle) = setup_watch_system(10);
    let prefix = Bytes::from("/config/");

    {
        let _watcher = registry.register_prefix(prefix.clone(), false).unwrap();
        assert_eq!(registry.prefix_watcher_count(&prefix), 1);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(registry.prefix_watcher_count(&prefix), 0);
}

// --- revision field ---

/// #300: The revision field (= Raft applied index) is the client's consistency anchor.
#[tokio::test]
async fn test_revision_field_passed_through_to_watcher() {
    let (broadcast_tx, registry, _handle) = setup_watch_system(10);
    let key = Bytes::from("/config/host");
    let mut watcher = registry.register(key.clone(), false).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx
        .send(WatchResponse {
            key: key.clone(),
            value: Bytes::from("val"),
            prev_value: Bytes::new(),
            event_type: d_engine_proto::client::WatchEventType::Put as i32,
            error: 0,
            revision: 42,
        })
        .unwrap();

    let received = timeout(Duration::from_millis(100), watcher.receiver_mut().recv())
        .await
        .expect("timeout")
        .expect("closed");
    assert_eq!(
        received.revision, 42,
        "revision must be passed through unchanged"
    );
}

// --- max_watcher_count limit ---

/// #300: The dispatcher runs in a single task and does O(N) work per event.
/// Without a hard cap, an unbounded number of watchers degrades write latency.
#[tokio::test]
async fn test_max_watcher_count_rejects_registration_when_exceeded() {
    let (_, registry, _handle) = setup_watch_system_with_max(10, 2);
    let key = Bytes::from("/config/host");

    let _h1 = registry.register(key.clone(), false).unwrap();
    let _h2 = registry.register(key.clone(), false).unwrap();
    let h3 = registry.register(key.clone(), false);

    assert!(
        h3.is_err(),
        "register must fail when max_watcher_count=2 is reached"
    );
}

/// #300: prefix watchers consume the same shared cap as exact watchers.
#[tokio::test]
async fn test_prefix_watcher_cap_enforced() {
    let (_, registry, _handle) = setup_watch_system_with_max(10, 2);

    let _h1 = registry.register_prefix(Bytes::from("/a/"), false).unwrap();
    let _h2 = registry.register_prefix(Bytes::from("/b/"), false).unwrap();
    let h3 = registry.register_prefix(Bytes::from("/c/"), false);

    assert!(
        h3.is_err(),
        "register_prefix must fail when max_watcher_count=2 is reached"
    );
}

/// #300: exact and prefix watchers share a single cap counter.
#[tokio::test]
async fn test_mixed_exact_and_prefix_watchers_share_cap() {
    let (_, registry, _handle) = setup_watch_system_with_max(10, 2);

    let _h1 = registry.register(Bytes::from("/exact/key"), false).unwrap();
    let _h2 = registry.register_prefix(Bytes::from("/prefix/"), false).unwrap();

    let h3_exact = registry.register(Bytes::from("/exact/other"), false);
    assert!(
        h3_exact.is_err(),
        "exact register must fail after mixed cap is reached"
    );

    let h3_prefix = registry.register_prefix(Bytes::from("/prefix2/"), false);
    assert!(
        h3_prefix.is_err(),
        "prefix register must fail after mixed cap is reached"
    );
}

/// #300 / fix: the TOCTOU fix must prevent concurrent registrations from exceeding cap.
#[tokio::test]
async fn test_concurrent_registrations_never_exceed_cap() {
    let max = 5usize;
    let (_, registry, _handle) = setup_watch_system_with_max(10, max);
    let registry = Arc::clone(&registry);

    // Launch 4× more tasks than the cap; only exactly `max` must succeed.
    let handles: Vec<_> = (0..20)
        .map(|i| {
            let registry = Arc::clone(&registry);
            tokio::spawn(async move {
                let key = Bytes::from(format!("/key/{i}"));
                registry.register(key, false).is_ok()
            })
        })
        .collect();

    let mut success_count = 0usize;
    for handle in handles {
        if handle.await.unwrap() {
            success_count += 1;
        }
    }

    assert_eq!(
        success_count, max,
        "exactly max_watcher_count registrations must succeed under concurrent load, got {success_count}"
    );
}

// =============================================================================
// #379: prev_kv + Progress Heartbeat
// =============================================================================

/// #379: A watcher registered with prev_kv=false receives empty prev_value,
/// even when the broadcast WatchResponse carries a non-empty prev_value.
/// This ensures watchers that didn't opt in never see prev_kv data.
#[tokio::test]
async fn test_prev_kv_false_watcher_receives_empty_prev_value() {
    let (broadcast_tx, registry, _handle) = setup_watch_system(10);
    let key = Bytes::from("/k");
    let mut watcher = registry.register(key.clone(), false).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx
        .send(WatchResponse {
            key: key.clone(),
            value: Bytes::from("new_val"),
            prev_value: Bytes::from("old_val"), // handler populated this
            event_type: d_engine_proto::client::WatchEventType::Put as i32,
            error: 0,
            revision: 1,
        })
        .unwrap();

    let event = timeout(Duration::from_millis(100), watcher.receiver_mut().recv())
        .await
        .expect("timeout")
        .expect("closed");

    assert_eq!(event.event_type, WatchEventType::Put);
    assert_eq!(event.value, Bytes::from("new_val"));
    // prev_kv=false → dispatcher zeroes out prev_value before delivery
    assert_eq!(
        event.prev_value,
        Bytes::new(),
        "prev_kv=false watcher must receive empty prev_value"
    );
}

/// #379: A watcher registered with prev_kv=true receives the prev_value
/// that was populated in the broadcast WatchResponse by the handler.
#[tokio::test]
async fn test_prev_kv_true_watcher_receives_prev_value() {
    let (broadcast_tx, registry, _handle) = setup_watch_system(10);
    let key = Bytes::from("/k");
    let mut watcher = registry.register(key.clone(), true).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx
        .send(WatchResponse {
            key: key.clone(),
            value: Bytes::from("new_val"),
            prev_value: Bytes::from("old_val"),
            event_type: d_engine_proto::client::WatchEventType::Put as i32,
            error: 0,
            revision: 1,
        })
        .unwrap();

    let event = timeout(Duration::from_millis(100), watcher.receiver_mut().recv())
        .await
        .expect("timeout")
        .expect("closed");

    assert_eq!(event.event_type, WatchEventType::Put);
    assert_eq!(
        event.prev_value,
        Bytes::from("old_val"),
        "prev_kv=true watcher must receive prev_value"
    );
}

/// #379: Two watchers on the same key with different prev_kv flags each get
/// their respective behavior — per-watcher isolation, not global toggle.
#[tokio::test]
async fn test_prev_kv_per_watcher_isolation() {
    let (broadcast_tx, registry, _handle) = setup_watch_system(10);
    let key = Bytes::from("/shared");

    let mut watcher_no_prev = registry.register(key.clone(), false).unwrap();
    let mut watcher_with_prev = registry.register(key.clone(), true).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    broadcast_tx
        .send(WatchResponse {
            key: key.clone(),
            value: Bytes::from("new"),
            prev_value: Bytes::from("old"),
            event_type: d_engine_proto::client::WatchEventType::Put as i32,
            error: 0,
            revision: 1,
        })
        .unwrap();

    let ev_no = timeout(
        Duration::from_millis(100),
        watcher_no_prev.receiver_mut().recv(),
    )
    .await
    .expect("timeout no_prev")
    .expect("closed");
    let ev_yes = timeout(
        Duration::from_millis(100),
        watcher_with_prev.receiver_mut().recv(),
    )
    .await
    .expect("timeout with_prev")
    .expect("closed");

    assert_eq!(
        ev_no.prev_value,
        Bytes::new(),
        "prev_kv=false must receive empty prev_value"
    );
    assert_eq!(
        ev_yes.prev_value,
        Bytes::from("old"),
        "prev_kv=true must receive prev_value"
    );
}

/// #379: prev_kv_watcher_count tracks registrations and unregistrations correctly.
/// This counter is shared with the state machine handler to gate RocksDB reads.
#[tokio::test]
async fn test_prev_kv_watcher_count_tracks_registration() {
    // Keep _broadcast_tx alive: dropping it closes the channel, dispatcher exits,
    // and subsequent unregister messages would never be processed.
    let (_broadcast_tx, registry, _handle) = setup_watch_system(10);

    assert_eq!(registry.prev_kv_watcher_count(), 0, "starts at 0");

    let h1 = registry.register(Bytes::from("/k1"), true).unwrap();
    assert_eq!(
        registry.prev_kv_watcher_count(),
        1,
        "after first prev_kv=true"
    );

    let _h2 = registry.register(Bytes::from("/k2"), false).unwrap();
    assert_eq!(
        registry.prev_kv_watcher_count(),
        1,
        "prev_kv=false does not increment"
    );

    let h3 = registry.register(Bytes::from("/k3"), true).unwrap();
    assert_eq!(
        registry.prev_kv_watcher_count(),
        2,
        "after second prev_kv=true"
    );

    drop(h1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(registry.prev_kv_watcher_count(), 1, "after dropping h1");

    drop(h3);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        registry.prev_kv_watcher_count(),
        0,
        "after all prev_kv watchers unregistered"
    );
}

/// #379: Progress heartbeat — dispatcher sends periodic Progress events to all watchers.
/// Watchers can use these to detect stream liveness without waiting for key mutations.
#[tokio::test]
async fn test_progress_heartbeat_delivered_to_watcher() {
    // 50ms interval so the test completes quickly.
    // Keep _broadcast_tx alive so the dispatcher does not exit early.
    let (_broadcast_tx, registry, _handle) = setup_watch_system_with_heartbeat(10, 50);
    let key = Bytes::from("/watch/key");
    let mut watcher = registry.register(key.clone(), false).unwrap();

    // Wait long enough for at least one heartbeat tick
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain events: at least one must be Progress
    let mut got_progress = false;
    while let Ok(Some(ev)) = timeout(Duration::from_millis(50), watcher.receiver_mut().recv()).await
    {
        if ev.event_type == WatchEventType::Progress {
            got_progress = true;
            break;
        }
    }

    assert!(
        got_progress,
        "no Progress event received within 200ms with 50ms heartbeat interval"
    );
}

/// #379: When heartbeat_interval_ms = 0, no Progress events are ever sent.
/// Clients that don't need liveness pings should not receive unsolicited events.
#[tokio::test]
async fn test_no_progress_events_when_heartbeat_disabled() {
    // heartbeat disabled (interval = 0)
    let (_, registry, _handle) = setup_watch_system_with_heartbeat(10, 0);
    let key = Bytes::from("/watch/key");
    let mut watcher = registry.register(key.clone(), false).unwrap();

    // Wait — if heartbeat were enabled with any short interval we'd see events
    tokio::time::sleep(Duration::from_millis(200)).await;

    // No events at all should arrive (nothing was broadcast)
    let result = timeout(Duration::from_millis(30), watcher.receiver_mut().recv()).await;
    assert!(
        result.is_err(),
        "received unexpected event with heartbeat disabled: {:?}",
        result
    );
}
