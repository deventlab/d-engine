//! Watch mechanism for monitoring key changes
//!
//! Architecture: Shared State + Background Dispatcher
//!
//! ```text
//! StateMachine:
//!   apply_chunk() -> broadcast_watch_events() -> broadcast::send(WatchEvent) [fire-and-forget]
//!                                                        ↓
//! WatchDispatcher (spawned in Builder):
//!   broadcast::subscribe() ->
//!     exact:  DashMap<Bytes, Vec<Watcher>>  O(1) lookup per event
//!     prefix: DashMap<Bytes, Vec<Watcher>>  O(depth) decomposition + O(1) lookup per segment
//!                                                           ↓
//! Watchers:
//!   Embedded:   mpsc::Receiver<WatchEvent>
//!   Standalone: mpsc::Receiver -> gRPC stream (protobuf conversion)
//! ```
//!
//! # Design Principles
//!
//! - **No hidden resource allocation**: All tokio::spawn calls are explicit in Builder
//! - **Minimal abstraction**: Only essential data structures, no unnecessary wrappers
//! - **Composable**: Registry and Dispatcher are independent, composed in Builder
//! - **O(depth) prefix dispatch**: Decompose event key into slash-terminated path segments,
//!   O(1) DashMap lookup per segment — dispatch cost is independent of prefix watcher count

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
pub use d_engine_proto::client::{WatchEventType, WatchResponse as WatchEvent};
use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::trace;
use tracing::warn;

/// Errors returned by WatchRegistry registration methods.
#[derive(Debug)]
pub enum WatchError {
    /// Active watcher count has reached the configured max_watcher_count cap.
    LimitExceeded(usize),
    /// Prefix must start with '/' and end with '/'.
    InvalidPrefix,
}

impl std::fmt::Display for WatchError {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            WatchError::LimitExceeded(n) => write!(f, "watcher limit ({n}) exceeded"),
            WatchError::InvalidPrefix => {
                write!(f, "prefix must start with '/' and end with '/'")
            }
        }
    }
}

impl std::error::Error for WatchError {}

/// Decompose a key into all its slash-terminated path prefix candidates.
///
/// The dispatcher calls this on every event key to perform O(depth) prefix
/// lookup via O(1) DashMap gets — one per path segment — instead of scanning
/// all registered prefixes linearly.
///
/// Examples:
///   "/config/db/host" → ["/", "/config/", "/config/db/"]
///   "/"               → ["/"]
///   "/config"         → ["/"]
///   "/config/"        → ["/", "/config/"]
pub(crate) fn prefix_segments(key: &Bytes) -> Vec<Bytes> {
    key.iter()
        .enumerate()
        .filter(|&(_, &b)| b == b'/')
        .map(|(i, _)| key.slice(0..i + 1))
        .collect()
}

/// Handle for a registered watcher.
///
/// When dropped, the watcher is automatically unregistered (if unregister_tx is Some).
pub struct WatcherHandle {
    /// Unique identifier
    id: u64,
    /// Key being watched (exact key) or prefix being watched (prefix watcher)
    key: Bytes,
    /// True when registered via register_prefix()
    is_prefix: bool,
    /// Channel receiver for watch events
    receiver: mpsc::Receiver<WatchEvent>,
    /// Unregister channel (None if cleanup disabled via into_receiver)
    unregister_tx: Option<mpsc::UnboundedSender<(u64, Bytes)>>,
}

impl WatcherHandle {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn key(&self) -> &Bytes {
        &self.key
    }

    /// True if this handle was registered via register_prefix().
    pub fn is_prefix(&self) -> bool {
        self.is_prefix
    }

    pub fn receiver_mut(&mut self) -> &mut mpsc::Receiver<WatchEvent> {
        &mut self.receiver
    }

    /// Consume the handle and return the event receiver.
    ///
    /// Disables automatic unregistration. The watcher remains active until
    /// the receiver is dropped (causing send failures that trigger cleanup).
    ///
    /// Use this for long-lived streams (e.g., gRPC) where the receiver lifetime
    /// extends beyond the handle's scope.
    pub fn into_receiver(mut self) -> (u64, Bytes, mpsc::Receiver<WatchEvent>) {
        let id = self.id;
        let key = self.key.clone();
        self.unregister_tx = None;
        let (dummy_tx, dummy_rx) = mpsc::channel(1);
        drop(dummy_tx);
        let receiver = std::mem::replace(&mut self.receiver, dummy_rx);
        (id, key, receiver)
    }
}

impl Drop for WatcherHandle {
    fn drop(&mut self) {
        if let Some(ref tx) = self.unregister_tx {
            let _ = tx.send((self.id, self.key.clone()));
            trace!(
                watcher_id = self.id,
                key = ?self.key,
                is_prefix = self.is_prefix,
                "Watcher unregistered"
            );
        }
    }
}

/// Internal watcher state
#[derive(Debug)]
struct Watcher {
    id: u64,
    sender: mpsc::Sender<WatchEvent>,
}

/// Watch registry — manages watcher registration (Arc-shareable).
///
/// Two independent DashMaps keep exact and prefix watchers separate so
/// dispatch can use different lookup strategies for each without coupling.
pub struct WatchRegistry {
    /// Exact-match watchers: event_key → watchers
    exact: DashMap<Bytes, Vec<Watcher>>,
    /// Prefix watchers: prefix → watchers (prefix must start and end with '/')
    prefix: DashMap<Bytes, Vec<Watcher>>,
    /// Next watcher ID (monotonically increasing, globally unique)
    next_id: AtomicU64,
    /// Total active watchers across exact + prefix (for limit enforcement)
    total_count: AtomicUsize,
    /// Per-watcher channel buffer size
    watcher_buffer_size: usize,
    /// Hard cap on total active watchers; register() returns LimitExceeded when reached
    max_watcher_count: usize,
    /// Unregister channel sender (cloned for each WatcherHandle)
    unregister_tx: mpsc::UnboundedSender<(u64, Bytes)>,
}

impl WatchRegistry {
    /// Create a new registry with no watcher count limit.
    pub fn new(
        watcher_buffer_size: usize,
        unregister_tx: mpsc::UnboundedSender<(u64, Bytes)>,
    ) -> Self {
        Self::new_with_limits(watcher_buffer_size, usize::MAX, unregister_tx)
    }

    /// Create a new registry with a hard watcher count cap.
    ///
    /// `register()` and `register_prefix()` return `WatchError::LimitExceeded`
    /// once `max_watcher_count` active watchers are registered.
    pub fn new_with_limits(
        watcher_buffer_size: usize,
        max_watcher_count: usize,
        unregister_tx: mpsc::UnboundedSender<(u64, Bytes)>,
    ) -> Self {
        Self {
            exact: DashMap::new(),
            prefix: DashMap::new(),
            next_id: AtomicU64::new(1),
            total_count: AtomicUsize::new(0),
            watcher_buffer_size,
            max_watcher_count,
            unregister_tx,
        }
    }

    /// Register an exact-key watcher.
    ///
    /// Returns `WatchError::LimitExceeded` if max_watcher_count is reached.
    pub fn register(
        &self,
        key: Bytes,
    ) -> Result<WatcherHandle, WatchError> {
        self.do_register(key, false)
    }

    /// Register a prefix watcher.
    ///
    /// `prefix` must start with '/' and end with '/'.
    /// E.g. "/config/" watches all keys under /config/.
    ///
    /// Returns `WatchError::InvalidPrefix` if format is wrong.
    /// Returns `WatchError::LimitExceeded` if max_watcher_count is reached.
    pub fn register_prefix(
        &self,
        prefix: Bytes,
    ) -> Result<WatcherHandle, WatchError> {
        if !prefix.starts_with(b"/") || !prefix.ends_with(b"/") {
            return Err(WatchError::InvalidPrefix);
        }
        self.do_register(prefix, true)
    }

    fn do_register(
        &self,
        key: Bytes,
        is_prefix: bool,
    ) -> Result<WatcherHandle, WatchError> {
        if self.total_count.load(Ordering::Relaxed) >= self.max_watcher_count {
            return Err(WatchError::LimitExceeded(self.max_watcher_count));
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // +1 reserves one slot for the CANCELED sentinel (always deliverable even when full)
        let (sender, receiver) = mpsc::channel(self.watcher_buffer_size + 1);
        let watcher = Watcher { id, sender };

        if is_prefix {
            self.prefix.entry(key.clone()).or_default().push(watcher);
        } else {
            self.exact.entry(key.clone()).or_default().push(watcher);
        }

        self.total_count.fetch_add(1, Ordering::Relaxed);
        trace!(watcher_id = id, key = ?key, is_prefix, "Watcher registered");

        Ok(WatcherHandle {
            id,
            key,
            is_prefix,
            receiver,
            unregister_tx: Some(self.unregister_tx.clone()),
        })
    }

    fn unregister(
        &self,
        id: u64,
        key: &Bytes,
    ) {
        // Watcher IDs are globally unique — try exact first, then prefix.
        // Cold path: called only on drop or after overflow, not on every event.
        let mut found = false;
        self.exact.remove_if_mut(key, |_, watchers| {
            let before = watchers.len();
            watchers.retain(|w| w.id != id);
            found = watchers.len() < before;
            watchers.is_empty()
        });
        if !found {
            self.prefix.remove_if_mut(key, |_, watchers| {
                let before = watchers.len();
                watchers.retain(|w| w.id != id);
                found = watchers.len() < before;
                watchers.is_empty()
            });
        }
        if found {
            self.total_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    #[cfg(test)]
    pub(crate) fn watcher_count(
        &self,
        key: &Bytes,
    ) -> usize {
        self.exact.get(key).map(|w| w.len()).unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn watched_key_count(&self) -> usize {
        self.exact.len()
    }

    #[cfg(test)]
    pub(crate) fn prefix_watcher_count(
        &self,
        prefix: &Bytes,
    ) -> usize {
        self.prefix.get(prefix).map(|w| w.len()).unwrap_or(0)
    }
}

/// Watch dispatcher — distributes events to watchers (background task).
///
/// Spawned explicitly in NodeBuilder::build() to make resource allocation visible.
pub struct WatchDispatcher {
    registry: Arc<WatchRegistry>,
    broadcast_rx: broadcast::Receiver<WatchEvent>,
    unregister_rx: mpsc::UnboundedReceiver<(u64, Bytes)>,
}

impl WatchDispatcher {
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

    pub async fn run(mut self) {
        debug!("WatchDispatcher started");
        loop {
            tokio::select! {
                biased;
                // Cleanup first so dead watchers don't receive the next event
                Some((id, key)) = self.unregister_rx.recv() => {
                    self.registry.unregister(id, &key);
                }
                result = self.broadcast_rx.recv() => {
                    match result {
                        Ok(event) => self.dispatch_event(event).await,
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

    async fn dispatch_event(
        &self,
        event: WatchEvent,
    ) {
        // Step 1: exact match — O(1) DashMap lookup
        self.dispatch_to_map(&self.registry.exact, &event.key, &event).await;

        // Step 2: prefix match — O(depth) where depth = number of '/' in key
        for prefix in prefix_segments(&event.key) {
            self.dispatch_to_map(&self.registry.prefix, &prefix, &event).await;
        }
    }

    /// Dispatch an event to all watchers under `lookup_key` in `map`.
    ///
    /// Handles overflow detection and dead watcher cleanup identically for
    /// both the exact and prefix maps, keeping the two dispatch paths DRY.
    async fn dispatch_to_map(
        &self,
        map: &DashMap<Bytes, Vec<Watcher>>,
        lookup_key: &Bytes,
        event: &WatchEvent,
    ) {
        if let Some(watchers) = map.get(lookup_key) {
            let mut dead_watchers = Vec::new();

            for watcher in watchers.iter() {
                let available = watcher.sender.capacity();

                if available <= 1 {
                    // capacity == 1: only the reserved cancel slot remains → overflow
                    // capacity == 0: defensive, shouldn't happen in normal flow
                    if available == 1 {
                        warn!(
                            watcher_id = watcher.id,
                            key = ?event.key,
                            buffer_capacity = watcher.sender.max_capacity(),
                            buffer_len = watcher.sender.max_capacity() - available,
                            "watcher buffer overflow, sending cancel"
                        );
                        let _ = watcher
                            .sender
                            .try_send(crate::watch::make_cancel_event(event.key.clone()));
                    }
                    dead_watchers.push(watcher.id);
                    continue;
                }

                // Normal send: reserved slot untouched
                if let Err(mpsc::error::TrySendError::Closed(_)) =
                    watcher.sender.try_send(event.clone())
                {
                    // Receiver dropped: silent cleanup, no cancel needed
                    dead_watchers.push(watcher.id);
                }
            }

            drop(watchers);
            for id in dead_watchers {
                self.registry.unregister(id, lookup_key);
            }

            trace!(
                event_key = ?event.key,
                lookup_key = ?lookup_key,
                event_type = ?event.event_type,
                "Event dispatched"
            );
        }
    }
}
