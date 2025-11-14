//! Watch Manager for monitoring key changes
//!
//! This module provides a lock-free, high-performance watch mechanism for tracking
//! changes to specific keys in the state machine. It is designed to replace etcd's
//! Watch functionality with minimal overhead on the write path.
//!
//! # Architecture
//!
//! ```text
//! Write Path (hot path):
//!   apply_chunk() -> notify_change() -> try_send(event_queue) [~10ns, non-blocking]
//!                                              ↓
//! Background Dispatcher Thread:
//!   event_queue.recv() -> lookup in DashMap -> try_send(per-watcher channel)
//!                                                       ↓
//! gRPC Stream:
//!   ReceiverStream -> send to client
//! ```
//!
//! # Performance Characteristics
//!
//! - Write path overhead: < 0.01% with 100+ watchers (target: <10ns per notify)
//! - Event notification latency: typically < 100μs end-to-end
//! - Memory footprint: ~2.4KB per watcher (with default buffer=10)
//!
//! # Error Handling
//!
//! - When event buffers are full, events are dropped via `try_send`
//! - Clients should use Read API to re-sync if they detect gaps
//! - This trade-off prioritizes write performance over guaranteed delivery

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::{debug, trace};

use crate::config::WatchConfig;

/// Event type for watch notifications
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEventType {
    /// Key was inserted or updated
    Put,
    /// Key was explicitly deleted (not including TTL expiration)
    Delete,
}

/// Watch event containing key change information
#[derive(Debug, Clone)]
pub struct WatchEvent {
    /// The key that changed
    pub key: Bytes,
    /// The new value (empty for DELETE events)
    pub value: Bytes,
    /// Type of change
    pub event_type: WatchEventType,
}

/// Handle for a registered watcher
///
/// When dropped, the watcher is automatically unregistered from the WatchManager.
pub struct WatcherHandle {
    /// Unique identifier for this watcher
    id: u64,
    /// The key being watched
    key: Bytes,
    /// Channel receiver for watch events (Option to allow move out)
    receiver: Option<mpsc::Receiver<WatchEvent>>,
    /// Reference to manager for cleanup on drop
    manager: Arc<WatchManagerInner>,
}

impl WatcherHandle {
    /// Get the unique identifier for this watcher
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the key being watched
    pub fn key(&self) -> &Bytes {
        &self.key
    }

    /// Consume the handle and return the event receiver
    ///
    /// This is useful when you need to pass the receiver to another component
    /// (e.g., WatchStreamHandler) while keeping the cleanup logic via Drop.
    ///
    /// Note: After calling this, the WatcherHandle cannot receive events anymore,
    /// but cleanup will still happen when the guard is dropped.
    ///
    /// # Panics
    ///
    /// Panics if the receiver has already been taken.
    pub fn into_receiver(mut self) -> (u64, Bytes, mpsc::Receiver<WatchEvent>, WatcherHandleGuard) {
        let id = self.id;
        let key = self.key.clone();
        let receiver = self.receiver.take().expect("receiver already taken");

        let guard = WatcherHandleGuard {
            id,
            key: key.clone(),
            manager: self.manager.clone(),
        };

        // Prevent Drop from being called on self, since guard will handle cleanup
        std::mem::forget(self);

        (id, key, receiver, guard)
    }

    /// Get a mutable reference to the receiver
    ///
    /// Returns None if the receiver has been taken via `into_receiver()`.
    pub fn receiver_mut(&mut self) -> Option<&mut mpsc::Receiver<WatchEvent>> {
        self.receiver.as_mut()
    }
}

/// Guard that handles cleanup when dropped
///
/// This is returned from `WatcherHandle::into_receiver()` to ensure
/// cleanup happens even after the receiver has been moved out.
pub struct WatcherHandleGuard {
    id: u64,
    key: Bytes,
    manager: Arc<WatchManagerInner>,
}

impl Drop for WatcherHandleGuard {
    fn drop(&mut self) {
        // Guard cleanup: remove watcher from registry
        if let Some(mut watchers) = self.manager.watchers.get_mut(&self.key) {
            watchers.retain(|w| w.id != self.id);

            if watchers.is_empty() {
                drop(watchers);
                self.manager.watchers.remove(&self.key);
            }
        }

        trace!(
            watcher_id = self.id,
            key = ?self.key,
            "Watcher unregistered via guard"
        );
    }
}

impl Drop for WatcherHandle {
    fn drop(&mut self) {
        // Automatic cleanup when client disconnects
        if let Some(mut watchers) = self.manager.watchers.get_mut(&self.key) {
            watchers.retain(|w| w.id != self.id);

            if watchers.is_empty() {
                drop(watchers);
                self.manager.watchers.remove(&self.key);
            }
        }

        trace!(
            watcher_id = self.id,
            key = ?self.key,
            "Watcher unregistered"
        );
    }
}

/// Internal watcher state
#[derive(Debug)]
struct Watcher {
    /// Unique identifier
    id: u64,
    /// Channel sender for events
    sender: mpsc::Sender<WatchEvent>,
}

/// Internal state of WatchManager
#[derive(Debug)]
struct WatchManagerInner {
    /// Watchers grouped by key (lock-free concurrent HashMap)
    watchers: DashMap<Bytes, Vec<Watcher>>,

    /// Next watcher ID (monotonically increasing)
    next_id: AtomicU64,

    /// Whether the dispatcher thread is running
    running: AtomicBool,

    /// Configuration
    config: WatchConfig,
}

/// High-performance watch manager with lock-free event dispatch
///
/// # Thread Safety
///
/// All methods are thread-safe and can be called concurrently from multiple threads.
/// The internal event queue and watcher registry use lock-free data structures.
///
/// # Example
///
/// ```ignore
/// use d_engine_core::watch::{WatchManager, WatchConfig, WatchEventType};
/// use bytes::Bytes;
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let config = WatchConfig::default();
/// let manager = WatchManager::new(config);
/// manager.start();
///
/// // Register a watcher
/// let key = Bytes::from("mykey");
/// let mut handle = manager.register(key.clone()).await;
///
/// // Notify of changes (from state machine)
/// manager.notify_put(key.clone(), Bytes::from("value1"));
///
/// // Receive events
/// if let Some(event) = handle.receiver_mut().unwrap().recv().await {
///     assert_eq!(event.event_type, WatchEventType::Put);
/// }
///
/// manager.stop();
/// # });
/// ```
#[derive(Debug)]
pub struct WatchManager {
    /// Shared inner state
    inner: Arc<WatchManagerInner>,

    /// Sender for global event queue (lock-free)
    event_sender: Sender<WatchEvent>,

    /// Receiver for global event queue (used by dispatcher)
    /// Wrapped in Arc to allow cloning WatchManager, but receiver is only consumed once
    event_receiver: Arc<Receiver<WatchEvent>>,
}

impl WatchManager {
    /// Create a new WatchManager with the given configuration
    pub fn new(config: WatchConfig) -> Self {
        let (event_sender, event_receiver) = if config.event_queue_size > 0 {
            bounded(config.event_queue_size)
        } else {
            unbounded()
        };

        let inner = Arc::new(WatchManagerInner {
            watchers: DashMap::new(),
            next_id: AtomicU64::new(1),
            running: AtomicBool::new(false),
            config,
        });

        Self {
            inner,
            event_sender,
            event_receiver: Arc::new(event_receiver),
        }
    }

    /// Start the background event dispatcher thread
    ///
    /// This spawns a dedicated thread that reads from the event queue and
    /// dispatches events to registered watchers.
    pub fn start(&self) {
        if self.inner.running.swap(true, Ordering::SeqCst) {
            // Already running
            return;
        }

        let inner = self.inner.clone();
        let receiver = Arc::clone(&self.event_receiver);

        std::thread::spawn(move || {
            debug!("Watch dispatcher thread started");

            while inner.running.load(Ordering::Acquire) {
                match receiver.recv() {
                    Ok(event) => {
                        Self::dispatch_event(&inner, event);
                    }
                    Err(_) => {
                        // Channel closed, exit thread
                        break;
                    }
                }
            }

            debug!("Watch dispatcher thread stopped");
        });
    }

    /// Stop the background dispatcher thread
    pub fn stop(&self) {
        self.inner.running.store(false, Ordering::Release);
    }

    /// Register a new watcher for a specific key
    ///
    /// Returns a handle that receives watch events via an mpsc channel.
    /// The watcher is automatically unregistered when the handle is dropped.
    ///
    /// # Arguments
    ///
    /// * `key` - The exact key to watch for changes
    ///
    /// # Returns
    ///
    /// A `WatcherHandle` containing the receiver channel for events
    pub async fn register(
        &self,
        key: Bytes,
    ) -> WatcherHandle {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let buffer_size = self.inner.config.watcher_buffer_size;

        let (sender, receiver) = mpsc::channel(buffer_size);

        let watcher = Watcher { id, sender };

        // Insert into DashMap (lock-free)
        self.inner.watchers.entry(key.clone()).or_default().push(watcher);

        trace!(
            watcher_id = id,
            key = ?key,
            buffer_size = buffer_size,
            "Watcher registered"
        );

        WatcherHandle {
            id,
            key,
            receiver: Some(receiver),
            manager: self.inner.clone(),
        }
    }

    /// Notify watchers of a PUT event (insert or update)
    ///
    /// This is called from the hot write path and MUST be non-blocking.
    /// Uses `try_send` to avoid blocking if the event queue is full.
    ///
    /// # Arguments
    ///
    /// * `key` - The key that was modified
    /// * `value` - The new value
    pub fn notify_put(
        &self,
        key: Bytes,
        value: Bytes,
    ) {
        let event = WatchEvent {
            key,
            value,
            event_type: WatchEventType::Put,
        };

        // Non-blocking send (drop if full)
        let _ = self.event_sender.try_send(event);
    }

    /// Notify watchers of a DELETE event
    ///
    /// This is called from the hot write path and MUST be non-blocking.
    ///
    /// # Arguments
    ///
    /// * `key` - The key that was deleted
    pub fn notify_delete(
        &self,
        key: Bytes,
    ) {
        let event = WatchEvent {
            key,
            value: Bytes::new(),
            event_type: WatchEventType::Delete,
        };

        // Non-blocking send (drop if full)
        let _ = self.event_sender.try_send(event);
    }

    /// Dispatch an event to all watchers of a specific key
    ///
    /// This runs on the background dispatcher thread and performs the actual
    /// lookup and distribution of events.
    fn dispatch_event(
        inner: &Arc<WatchManagerInner>,
        event: WatchEvent,
    ) {
        if let Some(watchers) = inner.watchers.get(&event.key) {
            for watcher in watchers.iter() {
                // Non-blocking send to per-watcher channel
                let _ = watcher.sender.try_send(event.clone());
            }

            trace!(
                key = ?event.key,
                event_type = ?event.event_type,
                watchers = watchers.len(),
                "Event dispatched"
            );
        }
    }

    /// Get the number of active watchers for a specific key
    ///
    /// This is primarily for testing and monitoring purposes.
    pub fn watcher_count(
        &self,
        key: &Bytes,
    ) -> usize {
        self.inner.watchers.get(key).map(|w| w.len()).unwrap_or(0)
    }

    /// Get the total number of watched keys
    ///
    /// This is primarily for testing and monitoring purposes.
    pub fn watched_key_count(&self) -> usize {
        self.inner.watchers.len()
    }
}
