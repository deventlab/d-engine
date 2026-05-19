//! Watch mechanism for monitoring key changes
//!
//! Architecture: Shared State + Background Dispatcher
//!
//! ```text
//! StateMachine:
//!   apply_chunk() -> broadcast_watch_events() -> broadcast::send(WatchResponse) [fire-and-forget]
//!                                                        ↓
//! WatchDispatcher (spawned in Builder):
//!   broadcast::subscribe() ->
//!     exact:  DashMap<Bytes, Vec<Watcher>>  O(1) lookup per event
//!     prefix: DashMap<Bytes, Vec<Watcher>>  O(depth) decomposition + O(1) lookup per segment
//!                                                           ↓
//! Watchers:
//!   Embedded:   mpsc::Receiver<WatchEvent>   (opaque Rust type, no proto dependency)
//!   Standalone: mpsc::Receiver -> gRPC stream (caller converts WatchEvent → WatchResponse)
//! ```
//!
//! # Design Principles
//!
//! - **No hidden resource allocation**: All tokio::spawn calls are explicit in Builder
//! - **Minimal abstraction**: Only essential data structures, no unnecessary wrappers
//! - **Composable**: Registry and Dispatcher are independent, composed in Builder
//! - **O(depth) prefix dispatch**: Decompose event key into slash-terminated path segments,
//!   O(1) DashMap lookup per segment — dispatch cost is independent of prefix watcher count
//! - **Proto boundary**: WatchResponse (proto) lives only in the broadcast channel and handler;
//!   WatchEvent (opaque) is what callers see — no proto import required.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use d_engine_proto::client::WatchResponse;
use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::trace;
use tracing::warn;

// ── Public opaque types ───────────────────────────────────────────────────────

/// High-level watch event type.  No proto import required by callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEventType {
    Put,
    Delete,
    Canceled,
    Progress,
}

/// Opaque watch event delivered to callers.
///
/// All fields use standard Rust types — callers never need to import proto types.
///
/// `prev_value` semantics:
/// - `None` — watcher was registered with `prev_kv = false`, or the event is
///   `Progress` / `Canceled` (not a data mutation).
/// - `Some(Bytes::new())` — watcher has `prev_kv = true` and the key did not exist
///   before this write (fresh insert).
/// - `Some(v)` — watcher has `prev_kv = true` and `v` is the previous value.
#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub event_type: WatchEventType,
    pub key: Bytes,
    pub value: Bytes,
    pub prev_value: Option<Bytes>,
    pub revision: u64,
}

// ── Proto ↔ opaque conversions ────────────────────────────────────────────────

/// Convert a proto WatchResponse into an opaque WatchEvent.
///
/// Called by the dispatcher when delivering to a per-watcher mpsc channel.
/// `prev_kv`: when false, prev_value is zeroed so callers that didn't opt in
/// never receive stale memory from a watcher that did.
fn proto_to_event(
    proto: &WatchResponse,
    prev_kv: bool,
) -> WatchEvent {
    use d_engine_proto::client::WatchEventType as ProtoType;

    let event_type = match ProtoType::try_from(proto.event_type) {
        Ok(ProtoType::Put) => WatchEventType::Put,
        Ok(ProtoType::Delete) => WatchEventType::Delete,
        Ok(ProtoType::Canceled) => WatchEventType::Canceled,
        Ok(ProtoType::Progress) => WatchEventType::Progress,
        Err(_) => WatchEventType::Canceled,
    };

    WatchEvent {
        event_type,
        key: proto.key.clone(),
        value: proto.value.clone(),
        // None when prev_kv = false so callers can distinguish "not requested" from
        // "key didn't exist" (Some(empty)).
        prev_value: if prev_kv {
            Some(proto.prev_value.clone())
        } else {
            None
        },
        revision: proto.revision,
    }
}

/// Convert an opaque WatchEvent back to a proto WatchResponse (for gRPC).
impl From<&WatchEvent> for WatchResponse {
    fn from(e: &WatchEvent) -> Self {
        use d_engine_proto::client::WatchEventType as ProtoType;
        WatchResponse {
            key: e.key.clone(),
            value: e.value.clone(),
            // Proto transport uses bytes (no optional); None collapses to empty.
            prev_value: e.prev_value.clone().unwrap_or_default(),
            event_type: match e.event_type {
                WatchEventType::Put => ProtoType::Put as i32,
                WatchEventType::Delete => ProtoType::Delete as i32,
                WatchEventType::Canceled => ProtoType::Canceled as i32,
                WatchEventType::Progress => ProtoType::Progress as i32,
            },
            error: 0,
            revision: e.revision,
        }
    }
}

// ── WatchError ────────────────────────────────────────────────────────────────

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

// ── Prefix helpers ────────────────────────────────────────────────────────────

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

// ── WatcherHandle ─────────────────────────────────────────────────────────────

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

// ── Internal watcher state ────────────────────────────────────────────────────

/// Internal watcher state
#[derive(Debug)]
struct Watcher {
    id: u64,
    sender: mpsc::Sender<WatchEvent>,
    /// When true, prev_value is populated before delivery.
    prev_kv: bool,
}

// ── WatchRegistry ─────────────────────────────────────────────────────────────

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
    /// Count of active watchers that requested prev_kv = true.
    /// Shared Arc lets the state machine handler poll this without holding a registry ref.
    prev_kv_watcher_count: Arc<AtomicUsize>,
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
            prev_kv_watcher_count: Arc::new(AtomicUsize::new(0)),
            watcher_buffer_size,
            max_watcher_count,
            unregister_tx,
        }
    }

    /// Register an exact-key watcher.
    ///
    /// `prev_kv`: when true the server reads the old value before each write
    /// and populates `WatchEvent::prev_value`.  Only pays the read cost when
    /// at least one watcher has prev_kv = true.
    ///
    /// Returns `WatchError::LimitExceeded` if max_watcher_count is reached.
    pub fn register(
        &self,
        key: Bytes,
        prev_kv: bool,
    ) -> Result<WatcherHandle, WatchError> {
        self.do_register(key, false, prev_kv)
    }

    /// Register a prefix watcher.
    ///
    /// `prefix` must start with '/' and end with '/'.
    /// E.g. "/config/" watches all keys under /config/.
    ///
    /// `prev_kv`: same semantics as `register()`.
    ///
    /// Returns `WatchError::InvalidPrefix` if format is wrong.
    /// Returns `WatchError::LimitExceeded` if max_watcher_count is reached.
    pub fn register_prefix(
        &self,
        prefix: Bytes,
        prev_kv: bool,
    ) -> Result<WatcherHandle, WatchError> {
        if !prefix.starts_with(b"/") || !prefix.ends_with(b"/") {
            return Err(WatchError::InvalidPrefix);
        }
        self.do_register(prefix, true, prev_kv)
    }

    fn do_register(
        &self,
        key: Bytes,
        is_prefix: bool,
        prev_kv: bool,
    ) -> Result<WatcherHandle, WatchError> {
        // Reserve the slot first, then check. This eliminates the TOCTOU window
        // between a load-check and a separate fetch_add under concurrent registration.
        let prev = self.total_count.fetch_add(1, Ordering::Relaxed);
        if prev >= self.max_watcher_count {
            self.total_count.fetch_sub(1, Ordering::Relaxed);
            return Err(WatchError::LimitExceeded(self.max_watcher_count));
        }

        if prev_kv {
            self.prev_kv_watcher_count.fetch_add(1, Ordering::Relaxed);
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // +1 reserves one slot for the CANCELED sentinel (always deliverable even when full)
        let (sender, receiver) = mpsc::channel(self.watcher_buffer_size + 1);
        let watcher = Watcher {
            id,
            sender,
            prev_kv,
        };

        if is_prefix {
            self.prefix.entry(key.clone()).or_default().push(watcher);
        } else {
            self.exact.entry(key.clone()).or_default().push(watcher);
        }
        trace!(watcher_id = id, key = ?key, is_prefix, prev_kv, "Watcher registered");

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
        let mut found = false;
        let mut had_prev_kv = false;

        // Try exact map first
        self.exact.remove_if_mut(key, |_, watchers| {
            if let Some(pos) = watchers.iter().position(|w| w.id == id) {
                had_prev_kv = watchers[pos].prev_kv;
                watchers.remove(pos);
                found = true;
            }
            watchers.is_empty()
        });

        // Fall back to prefix map
        if !found {
            self.prefix.remove_if_mut(key, |_, watchers| {
                if let Some(pos) = watchers.iter().position(|w| w.id == id) {
                    had_prev_kv = watchers[pos].prev_kv;
                    watchers.remove(pos);
                    found = true;
                }
                watchers.is_empty()
            });
        }

        if found {
            self.total_count.fetch_sub(1, Ordering::Relaxed);
            if had_prev_kv {
                self.prev_kv_watcher_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Number of active watchers that opted in to prev_kv.
    pub fn prev_kv_watcher_count(&self) -> usize {
        self.prev_kv_watcher_count.load(Ordering::Relaxed)
    }

    /// Clone of the shared prev_kv counter Arc.
    ///
    /// Pass this to `DefaultStateMachineHandler` so it can poll the live value
    /// without holding a reference to the registry.
    pub fn prev_kv_watcher_count_arc(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.prev_kv_watcher_count)
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

// ── WatchDispatcher ───────────────────────────────────────────────────────────

/// Watch dispatcher — distributes events to watchers (background task).
///
/// Spawned explicitly in NodeBuilder::build() to make resource allocation visible.
/// Receives proto WatchResponse from the broadcast channel, converts to opaque
/// WatchEvent per-watcher, and delivers via per-watcher mpsc channels.
pub struct WatchDispatcher {
    registry: Arc<WatchRegistry>,
    broadcast_rx: broadcast::Receiver<WatchResponse>,
    unregister_rx: mpsc::UnboundedReceiver<(u64, Bytes)>,
    /// Shared applied index for Progress event revision field.
    last_applied: Arc<std::sync::atomic::AtomicU64>,
    /// Heartbeat interval.  0 = disabled.
    heartbeat_interval_ms: u64,
}

impl WatchDispatcher {
    pub fn new(
        registry: Arc<WatchRegistry>,
        broadcast_rx: broadcast::Receiver<WatchResponse>,
        unregister_rx: mpsc::UnboundedReceiver<(u64, Bytes)>,
        last_applied: Arc<std::sync::atomic::AtomicU64>,
        heartbeat_interval_ms: u64,
    ) -> Self {
        Self {
            registry,
            broadcast_rx,
            unregister_rx,
            last_applied,
            heartbeat_interval_ms,
        }
    }

    pub async fn run(mut self) {
        debug!("WatchDispatcher started");

        // Build optional heartbeat future.  When interval_ms == 0 the future
        // is a pending sleep that never fires, adding zero overhead.
        // Build the heartbeat interval only when enabled.
        // Constructing `interval_at(now + Duration::from_millis(u64::MAX), ...)` panics on
        // overflow, so we must skip interval creation entirely when heartbeat is off.
        let mut heartbeat: Option<tokio::time::Interval> = if self.heartbeat_interval_ms > 0 {
            let base_ms = self.heartbeat_interval_ms;
            let jitter = (base_ms / 10).max(1);
            // Mix thread ID and wall-clock nanoseconds into a single hash so that nodes
            // started simultaneously (e.g. k8s rolling restart within the same millisecond)
            // still get different offsets.  No external crate needed.
            let mut h = DefaultHasher::new();
            std::thread::current().id().hash(&mut h);
            std::time::SystemTime::now().hash(&mut h);
            let seed = h.finish();
            let offset = seed % (jitter * 2);
            let first_tick_ms = base_ms.saturating_sub(jitter) + offset;
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_millis(first_tick_ms),
                Duration::from_millis(base_ms),
            );
            // Skip missed ticks: heartbeat is a liveness signal, not a counter.
            // Bursting N progress events after a slow-watcher stall would mislead clients
            // into thinking the stream was alive during the stall period.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            Some(interval)
        } else {
            None
        };

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
                Some(t) = async { if let Some(ref mut hb) = heartbeat { Some(hb.tick().await) } else { std::future::pending().await } } => {
                    let _ = t;
                    self.broadcast_progress().await;
                }
            }
        }
        debug!("WatchDispatcher stopped");
    }

    /// Broadcast a synthetic Progress event to ALL active watchers regardless of key.
    ///
    /// Unlike data events (routed by key), Progress is a liveness signal — every watcher
    /// must receive it so clients can detect silent stream death.
    /// Keys are collected first to avoid holding DashMap shard locks across awaits.
    async fn broadcast_progress(&self) {
        let revision = self.last_applied.load(Ordering::Relaxed);
        let progress = WatchResponse {
            key: Bytes::new(),
            value: Bytes::new(),
            prev_value: Bytes::new(),
            event_type: d_engine_proto::client::WatchEventType::Progress as i32,
            error: 0,
            revision,
        };

        let exact_keys: Vec<Bytes> = self.registry.exact.iter().map(|e| e.key().clone()).collect();
        let prefix_keys: Vec<Bytes> =
            self.registry.prefix.iter().map(|e| e.key().clone()).collect();

        for key in exact_keys {
            self.dispatch_to_map(&self.registry.exact, &key, &progress).await;
        }
        for key in prefix_keys {
            self.dispatch_to_map(&self.registry.prefix, &key, &progress).await;
        }
    }

    async fn dispatch_event(
        &self,
        event: WatchResponse,
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
    /// Converts proto WatchResponse → opaque WatchEvent per watcher, respecting
    /// each watcher's prev_kv preference.  Handles overflow detection and dead
    /// watcher cleanup identically for both the exact and prefix maps.
    async fn dispatch_to_map(
        &self,
        map: &DashMap<Bytes, Vec<Watcher>>,
        lookup_key: &Bytes,
        event: &WatchResponse,
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

                // Convert proto → opaque, respecting per-watcher prev_kv flag
                let watch_event = proto_to_event(event, watcher.prev_kv);

                // Normal send: reserved slot untouched
                if let Err(mpsc::error::TrySendError::Closed(_)) =
                    watcher.sender.try_send(watch_event)
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
