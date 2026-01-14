//! Watch mechanism for monitoring key changes
//!
//! Architecture: Shared State + Background Dispatcher
//!
//! ```text
//! StateMachine:
//!   apply_chunk() -> broadcast_watch_events() -> broadcast::send(WatchEvent) [fire-and-forget]
//!                                                        ↓
//! WatchDispatcher (spawned in Builder):
//!   broadcast::subscribe() -> match key in DashMap -> mpsc::send(per-watcher)
//!                                                           ↓
//! Watchers:
//!   Embedded: mpsc::Receiver<WatchEvent>
//!   Standalone: mpsc::Receiver -> gRPC stream (protobuf conversion)
//! ```
//!
//! # Design Principles
//!
//! - **No hidden resource allocation**: All tokio::spawn calls are explicit in Builder
//! - **Minimal abstraction**: Only essential data structures, no unnecessary wrappers
//! - **Composable**: Registry and Dispatcher are independent, composed in Builder

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use bytes::Bytes;
// Re-export protobuf types for watch events
pub use d_engine_proto::client::{WatchEventType, WatchResponse as WatchEvent};
use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::trace;
use tracing::warn;

/// Handle for a registered watcher
///
/// When dropped, the watcher is automatically unregistered (if unregister_tx is Some).
pub struct WatcherHandle {
    /// Unique identifier
    id: u64,
    /// Key being watched
    key: Bytes,
    /// Channel receiver for watch events
    receiver: mpsc::Receiver<WatchEvent>,
    /// Unregister channel (None if cleanup disabled via into_receiver)
    unregister_tx: Option<mpsc::UnboundedSender<(u64, Bytes)>>,
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

    /// Get a mutable reference to the receiver
    pub fn receiver_mut(&mut self) -> &mut mpsc::Receiver<WatchEvent> {
        &mut self.receiver
    }

    /// Consume the handle and return the event receiver
    ///
    /// Disables automatic unregistration. The watcher will remain active until
    /// the receiver is dropped (causing send failures that trigger cleanup).
    ///
    /// Use this for long-lived streams (e.g., gRPC) where the receiver lifetime
    /// extends beyond the handle's scope.
    pub fn into_receiver(mut self) -> (u64, Bytes, mpsc::Receiver<WatchEvent>) {
        let id = self.id;
        let key = self.key.clone();

        // Clear unregister_tx to disable Drop cleanup
        self.unregister_tx = None;

        // Create dummy receiver to satisfy Rust's move checker
        let (dummy_tx, dummy_rx) = mpsc::channel(1);
        drop(dummy_tx); // Close immediately
        let receiver = std::mem::replace(&mut self.receiver, dummy_rx);

        (id, key, receiver)
    }
}

impl Drop for WatcherHandle {
    fn drop(&mut self) {
        if let Some(ref tx) = self.unregister_tx {
            // Send unregister request (ignore errors if dispatcher stopped)
            let _ = tx.send((self.id, self.key.clone()));
            trace!(watcher_id = self.id, key = ?self.key, "Watcher unregistered");
        }
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

/// Watch registry - manages watcher registration (Arc-shareable)
///
/// This is the shared state that both Builder (for registration) and
/// WatchDispatcher (for event dispatch) can access concurrently.
pub struct WatchRegistry {
    /// Watchers grouped by key (lock-free concurrent HashMap)
    watchers: DashMap<Bytes, Vec<Watcher>>,

    /// Next watcher ID (monotonically increasing)
    next_id: AtomicU64,

    /// Per-watcher channel buffer size
    watcher_buffer_size: usize,

    /// Unregister channel sender (cloned for each WatcherHandle)
    unregister_tx: mpsc::UnboundedSender<(u64, Bytes)>,
}

impl WatchRegistry {
    /// Create a new watch registry
    ///
    /// # Arguments
    /// * `watcher_buffer_size` - Buffer size for per-watcher mpsc channels
    /// * `unregister_tx` - Channel for receiving unregister requests
    pub fn new(
        watcher_buffer_size: usize,
        unregister_tx: mpsc::UnboundedSender<(u64, Bytes)>,
    ) -> Self {
        Self {
            watchers: DashMap::new(),
            next_id: AtomicU64::new(1),
            watcher_buffer_size,
            unregister_tx,
        }
    }

    /// Register a new watcher for a specific key
    ///
    /// Returns a handle that receives watch events via an mpsc channel.
    /// The watcher is automatically unregistered when the handle is dropped.
    pub fn register(
        &self,
        key: Bytes,
    ) -> WatcherHandle {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(self.watcher_buffer_size);

        let watcher = Watcher { id, sender };

        // Insert into DashMap (lock-free)
        self.watchers.entry(key.clone()).or_default().push(watcher);

        trace!(watcher_id = id, key = ?key, "Watcher registered");

        WatcherHandle {
            id,
            key,
            receiver,
            unregister_tx: Some(self.unregister_tx.clone()),
        }
    }

    /// Unregister a watcher
    fn unregister(
        &self,
        id: u64,
        key: &Bytes,
    ) {
        self.watchers.remove_if_mut(key, |_key, watchers| {
            watchers.retain(|w| w.id != id);
            watchers.is_empty()
        });
    }

    /// Get the number of active watchers for a specific key (for testing)
    #[cfg(test)]
    pub(crate) fn watcher_count(
        &self,
        key: &Bytes,
    ) -> usize {
        self.watchers.get(key).map(|w| w.len()).unwrap_or(0)
    }

    /// Get the total number of watched keys (for testing)
    #[cfg(test)]
    pub(crate) fn watched_key_count(&self) -> usize {
        self.watchers.len()
    }
}

/// Watch dispatcher - distributes events to watchers (background task)
///
/// This is spawned explicitly in NodeBuilder::build() to make resource
/// allocation visible. It continuously:
/// 1. Receives events from broadcast channel
/// 2. Matches keys in the registry
/// 3. Dispatches to matching watchers
pub struct WatchDispatcher {
    /// Shared registry (same instance held by Node)
    registry: Arc<WatchRegistry>,

    /// Broadcast receiver for global events
    broadcast_rx: broadcast::Receiver<WatchEvent>,

    /// Unregister channel receiver
    unregister_rx: mpsc::UnboundedReceiver<(u64, Bytes)>,
}

impl WatchDispatcher {
    /// Create a new watch dispatcher
    ///
    /// # Arguments
    /// * `registry` - Shared registry for looking up watchers
    /// * `broadcast_rx` - Receiver for watch events from StateMachine
    /// * `unregister_rx` - Receiver for unregister requests from WatcherHandles
    pub fn new(
        registry: Arc<WatchRegistry>,
        broadcast_rx: broadcast::Receiver<WatchEvent>,
        unregister_rx: mpsc::UnboundedReceiver<(u64, Bytes)>,
    ) -> Self {
        Self {
            registry,
            broadcast_rx,
            unregister_rx,
        }
    }

    /// Run the dispatcher event loop
    ///
    /// This should be spawned as a tokio task in NodeBuilder::build().
    /// It will run until the broadcast channel is closed.
    pub async fn run(mut self) {
        debug!("WatchDispatcher started");

        loop {
            tokio::select! {
                biased;

                // Handle unregister requests first (cleanup priority)
                Some((id, key)) = self.unregister_rx.recv() => {
                    self.registry.unregister(id, &key);
                }

                // Receive broadcast event
                result = self.broadcast_rx.recv() => {
                    match result {
                        Ok(event) => {
                            self.dispatch_event(event).await;
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("WatchDispatcher lagged {} events (slow watchers)", n);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            debug!("Broadcast channel closed, WatchDispatcher stopping");
                            break;
                        }
                    }
                }
            }
        }

        debug!("WatchDispatcher stopped");
    }

    /// Dispatch an event to all watchers of a specific key
    async fn dispatch_event(
        &self,
        event: WatchEvent,
    ) {
        if let Some(watchers) = self.registry.watchers.get(&event.key) {
            let mut dead_watchers = Vec::new();

            for watcher in watchers.iter() {
                // Non-blocking send
                if watcher.sender.try_send(event.clone()).is_err() {
                    // Receiver dropped or full, mark for cleanup
                    dead_watchers.push(watcher.id);
                }
            }

            // Cleanup dead watchers
            drop(watchers);
            if !dead_watchers.is_empty() {
                for id in dead_watchers {
                    self.registry.unregister(id, &event.key);
                }
            }

            trace!(key = ?event.key, event_type = ?event.event_type, "Event dispatched");
        }
    }
}
