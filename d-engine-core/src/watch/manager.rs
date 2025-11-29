//! Watch Manager for monitoring key changes
//!
//! This module provides a lock-free, high-performance watch mechanism for tracking
//! changes to specific keys in the state machine with minimal overhead on the write path.
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
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

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

/// Cleanup state for a watcher
///
/// This struct holds the metadata required to unregister a watcher
/// from the manager when it's no longer needed.
struct WatcherCleanup {
    id: u64,
    key: Bytes,
    manager: Arc<WatchManagerInner>,
}

/// Handle for a registered watcher
///
/// When dropped, the watcher is automatically unregistered from the WatchManager.
pub struct WatcherHandle {
    /// Cleanup state (None if moved to guard via into_receiver)
    cleanup: Option<WatcherCleanup>,
    /// Channel receiver for watch events (Option to allow move out)
    receiver: Option<mpsc::Receiver<WatchEvent>>,
}

impl WatcherHandle {
    /// Get the unique identifier for this watcher
    pub fn id(&self) -> u64 {
        self.cleanup.as_ref().expect("cleanup state moved").id
    }

    /// Get the key being watched
    pub fn key(&self) -> &Bytes {
        &self.cleanup.as_ref().expect("cleanup state moved").key
    }

    /// Consume the handle and return the event receiver
    ///
    /// This transfers ownership of the cleanup responsibility to the returned guard.
    /// The guard will unregister the watcher when dropped.
    ///
    /// # Panics
    ///
    /// Panics if the receiver or cleanup state has already been taken.
    pub fn into_receiver(mut self) -> (u64, Bytes, mpsc::Receiver<WatchEvent>, WatcherHandleGuard) {
        let cleanup = self.cleanup.take().expect("cleanup already taken");
        let receiver = self.receiver.take().expect("receiver already taken");

        let id = cleanup.id;
        let key = cleanup.key.clone();

        let guard = WatcherHandleGuard { cleanup };

        // No need for mem::forget - Drop will see cleanup is None and do nothing
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
    cleanup: WatcherCleanup,
}

impl Drop for WatcherHandleGuard {
    fn drop(&mut self) {
        unregister_watcher(&self.cleanup);
        trace!(
            watcher_id = self.cleanup.id,
            key = ?self.cleanup.key,
            "Watcher unregistered via guard"
        );
    }
}

impl Drop for WatcherHandle {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            unregister_watcher(&cleanup);
            trace!(
                watcher_id = cleanup.id,
                key = ?cleanup.key,
                "Watcher unregistered"
            );
        }
    }
}

/// Unregister a watcher from the manager using atomic operation
///
/// Uses DashMap's `remove_if_mut` to atomically check and remove empty watcher lists,
/// preventing race conditions where a new watcher could be added between the check
/// and the remove operation.
fn unregister_watcher(cleanup: &WatcherCleanup) {
    cleanup.manager.watchers.remove_if_mut(&cleanup.key, |_key, watchers| {
        watchers.retain(|w| w.id != cleanup.id);
        watchers.is_empty()
    });
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
struct WatchManagerInner {
    /// Watchers grouped by key (lock-free concurrent HashMap)
    watchers: DashMap<Bytes, Vec<Watcher>>,

    /// Next watcher ID (monotonically increasing)
    next_id: AtomicU64,

    /// Dispatcher thread handle (None when not running)
    thread_handle: Mutex<Option<JoinHandle<()>>>,

    /// Shutdown signal sender (None when not running)
    shutdown_tx: Mutex<Option<Sender<()>>>,

    /// Configuration
    config: WatchConfig,
}

impl std::fmt::Debug for WatchManagerInner {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("WatchManagerInner")
            .field("watchers", &self.watchers)
            .field("next_id", &self.next_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
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
            thread_handle: Mutex::new(None),
            shutdown_tx: Mutex::new(None),
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
    /// Spawns a dedicated thread that reads from the event queue and dispatches
    /// events to registered watchers. The thread can be cleanly stopped via `stop()`.
    ///
    /// Calling `start()` when a dispatcher is already running is a no-op.
    ///
    /// # Thread Safety
    ///
    /// This method is idempotent and safe to call concurrently from multiple threads.
    pub fn start(&self) {
        let mut handle_guard = self.inner.thread_handle.lock().unwrap();

        // Already running
        if handle_guard.is_some() {
            return;
        }

        let (shutdown_tx, shutdown_rx) = bounded(1);
        let inner = self.inner.clone();
        let receiver = Arc::clone(&self.event_receiver);

        let handle = std::thread::spawn(move || {
            debug!("Watch dispatcher thread started");

            loop {
                crossbeam_channel::select! {
                    recv(receiver) -> result => {
                        match result {
                            Ok(event) => {
                                Self::dispatch_event(&inner, event);
                            }
                            Err(_) => {
                                // Event channel closed, exit thread
                                warn!("Watch event channel closed unexpectedly");
                                break;
                            }
                        }
                    }
                    recv(shutdown_rx) -> _ => {
                        // Shutdown signal received
                        debug!("Watch dispatcher received shutdown signal");
                        break;
                    }
                }
            }

            debug!("Watch dispatcher thread stopped");
        });

        *handle_guard = Some(handle);
        *self.inner.shutdown_tx.lock().unwrap() = Some(shutdown_tx);
    }

    /// Stop the background dispatcher thread
    ///
    /// Sends a shutdown signal to the dispatcher thread and waits for it to exit.
    /// This ensures the thread is cleanly terminated before returning.
    ///
    /// Calling `stop()` when no dispatcher is running is a no-op.
    ///
    /// # Thread Safety
    ///
    /// This method is safe to call concurrently from multiple threads.
    pub fn stop(&self) {
        // Take the shutdown sender to signal thread exit
        if let Some(tx) = self.inner.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }

        // Take and join the thread handle
        if let Some(handle) = self.inner.thread_handle.lock().unwrap().take() {
            let _ = handle.join();
        }
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
            cleanup: Some(WatcherCleanup {
                id,
                key,
                manager: self.inner.clone(),
            }),
            receiver: Some(receiver),
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
