//! Zero-overhead KV client for embedded d-engine.
//!
//! [`EmbeddedClient`] provides direct access to Raft state machine
//! without gRPC serialization or network traversal.
//!
//! # Performance
//! - **10-20x faster** than gRPC (localhost)
//! - **<0.1ms latency** per operation
//! - Zero serialization overhead
//!
//! # Usage
//! ```rust,ignore
//! let engine = EmbeddedEngine::start("./data").await?;
//! let client = engine.client();
//! client.put(b"key", b"value").await?;
//! ```

#[cfg(feature = "watch")]
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::InboundEvent;
use d_engine_core::MaybeCloneOneshot;
use d_engine_core::RaftOneshot;
use d_engine_core::ScanResult;
use d_engine_core::TypeConfig;
use d_engine_core::client::{
    ClientApi, ClientApiResult, ClientResponsePayload, ClientWriteRequest, ErrorCode,
    WriteOperation,
};
use d_engine_core::config::ReadConsistencyPolicy;
use tokio::sync::mpsc;

use super::embedded_read_handle::EmbeddedReadHandle;
pub(crate) use super::standalone_read_handle::{
    channel_closed_error, map_error_response, server_error, timeout_error,
};

#[cfg(feature = "watch")]
use d_engine_core::client::ClientApiError;
#[cfg(feature = "watch")]
use d_engine_core::watch::WatchRegistry;

/// Zero-overhead KV client for embedded mode. Obtained via `EmbeddedEngine::client()`.
///
/// `T` is the [`TypeConfig`] that carries the concrete `StateMachine` type, enabling
/// direct monomorphized calls to `T::SM::get()` on the fast path — no vtable dispatch.
///
/// For standalone/gRPC mode use `GrpcClient` instead. Both implement `ClientApi`.
pub struct EmbeddedClient<T: TypeConfig> {
    event_tx: mpsc::Sender<InboundEvent>,
    read_handle: EmbeddedReadHandle<T>,
    client_id: u32,
    timeout: Duration,
    #[cfg(feature = "watch")]
    watch_registry: Option<Arc<WatchRegistry>>,
}

impl<T: TypeConfig> Clone for EmbeddedClient<T> {
    fn clone(&self) -> Self {
        Self {
            event_tx: self.event_tx.clone(),
            read_handle: self.read_handle.clone(),
            client_id: self.client_id,
            timeout: self.timeout,
            #[cfg(feature = "watch")]
            watch_registry: self.watch_registry.clone(),
        }
    }
}

impl<T: TypeConfig> EmbeddedClient<T> {
    /// Internal constructor (used by EmbeddedEngine).
    pub(crate) fn new_internal(
        event_tx: mpsc::Sender<InboundEvent>,
        read_handle: EmbeddedReadHandle<T>,
        client_id: u32,
        timeout: Duration,
    ) -> Self {
        Self {
            event_tx,
            read_handle,
            client_id,
            timeout,
            #[cfg(feature = "watch")]
            watch_registry: None,
        }
    }

    /// Set watch registry for watch operations
    #[cfg(feature = "watch")]
    pub(crate) fn with_watch_registry(
        mut self,
        registry: Arc<WatchRegistry>,
    ) -> Self {
        self.watch_registry = Some(registry);
        self
    }

    /// Store a key-value pair with strong consistency.
    ///
    /// # Errors
    /// Returns an error if the node is not the leader, the channel is closed,
    /// the operation times out, or the state machine returns a server error.
    pub async fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> ClientApiResult<()> {
        let request = ClientWriteRequest {
            client_id: self.client_id,
            command: Some(WriteOperation::Insert {
                key: Bytes::copy_from_slice(key.as_ref()),
                value: Bytes::copy_from_slice(value.as_ref()),
                ttl_secs: None,
            }),
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.read_handle
            .cmd_tx
            .send(d_engine_core::ClientCmd::Propose(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        let response =
            result.map_err(|status| server_error(format!("RPC error: {}", status.message())))?;

        if response.error != ErrorCode::Success {
            return Err(map_error_response(
                response.error,
                response.leader_hint,
                response.retry_after_ms,
            ));
        }

        Ok(())
    }

    /// Strongly consistent read (linearizable).
    ///
    /// Guarantees reading the latest committed value by querying the Leader.
    /// Use for critical reads where staleness is unacceptable.
    ///
    /// # Performance
    /// - Latency: ~1-5ms (network RTT to Leader)
    /// - Throughput: Limited by Leader capacity
    ///
    /// # Raft Protocol
    /// Implements linearizable read per Raft §8.
    ///
    /// # Example
    /// ```ignore
    /// let client = engine.client();
    /// let value = client.get_linearizable(b"critical-config").await?;
    /// ```
    pub async fn get_linearizable(
        &self,
        key: impl AsRef<[u8]>,
    ) -> ClientApiResult<Option<Bytes>> {
        self.get_with_consistency(key, ReadConsistencyPolicy::LinearizableRead).await
    }

    /// Eventually consistent read (stale OK).
    ///
    /// Reads from local state machine without Leader coordination.
    /// Fast but may return stale data if replication is lagging.
    ///
    /// # Performance
    /// - Latency: ~0.1ms (local memory access)
    /// - Throughput: High (no Leader bottleneck)
    ///
    /// # Use Cases
    /// - Read-heavy workloads
    /// - Analytics/reporting (staleness acceptable)
    /// - Caching scenarios
    ///
    /// # Example
    /// ```ignore
    /// let client = engine.client();
    /// let cached_value = client.get_eventual(b"user-preference").await?;
    /// ```
    pub async fn get_eventual(
        &self,
        key: impl AsRef<[u8]>,
    ) -> ClientApiResult<Option<Bytes>> {
        self.get_with_consistency(key, ReadConsistencyPolicy::EventualConsistency).await
    }

    /// Advanced: Read with explicit consistency policy.
    ///
    /// For fine-grained control over read consistency vs performance trade-off.
    ///
    /// # Consistency Policies
    /// - `LinearizableRead`: Read from Leader (strong consistency, may be slower)
    /// - `EventualConsistency`: Read from local node (fast, may be stale)
    /// - `LeaseRead`: Optimized Leader read using lease mechanism
    ///
    /// # Example
    /// ```ignore
    /// use d_engine_proto::client::ReadConsistencyPolicy;
    ///
    /// let value = client.get_with_consistency(
    ///     b"key",
    ///     ReadConsistencyPolicy::LeaseRead,
    /// ).await?;
    /// ```
    pub async fn get_with_consistency(
        &self,
        key: impl AsRef<[u8]>,
        consistency: ReadConsistencyPolicy,
    ) -> ClientApiResult<Option<Bytes>> {
        self.read_handle
            .get(key.as_ref(), consistency, self.client_id, self.timeout)
            .await
    }

    /// Get multiple keys with linearizable consistency.
    ///
    /// Reads multiple keys from the Leader with strong consistency guarantee.
    ///
    /// # Example
    /// ```ignore
    /// let keys = vec![Bytes::from("key1"), Bytes::from("key2")];
    /// let values = client.get_multi_linearizable(&keys).await?;
    /// ```
    pub async fn get_multi_linearizable(
        &self,
        keys: &[Bytes],
    ) -> ClientApiResult<Vec<Option<Bytes>>> {
        self.get_multi_with_consistency(keys, ReadConsistencyPolicy::LinearizableRead)
            .await
    }

    /// Get multiple keys with eventual consistency.
    ///
    /// Reads multiple keys from local state machine (fast, may be stale).
    ///
    /// # Example
    /// ```ignore
    /// let keys = vec![Bytes::from("key1"), Bytes::from("key2")];
    /// let values = client.get_multi_eventual(&keys).await?;
    /// ```
    pub async fn get_multi_eventual(
        &self,
        keys: &[Bytes],
    ) -> ClientApiResult<Vec<Option<Bytes>>> {
        self.get_multi_with_consistency(keys, ReadConsistencyPolicy::EventualConsistency)
            .await
    }

    /// Advanced: Get multiple keys with explicit consistency policy.
    ///
    /// Eventual and LeaseRead policies use the ReadActor fast path (no Raft round-trip).
    /// LinearizableRead always goes through the Raft readIndex protocol via cmd_tx.
    pub async fn get_multi_with_consistency(
        &self,
        keys: &[Bytes],
        consistency: ReadConsistencyPolicy,
    ) -> ClientApiResult<Vec<Option<Bytes>>> {
        self.read_handle
            .get_batch(keys, consistency, self.client_id, self.timeout)
            .await
    }

    /// Delete a key-value pair with strong consistency.
    ///
    /// # Errors
    /// Returns an error if the node is not the leader, the channel is closed,
    /// the operation times out, or the state machine returns a server error.
    pub async fn delete(
        &self,
        key: impl AsRef<[u8]>,
    ) -> ClientApiResult<()> {
        let request = ClientWriteRequest {
            client_id: self.client_id,
            command: Some(WriteOperation::Delete {
                key: Bytes::copy_from_slice(key.as_ref()),
            }),
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.read_handle
            .cmd_tx
            .send(d_engine_core::ClientCmd::Propose(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        let response =
            result.map_err(|status| server_error(format!("RPC error: {}", status.message())))?;

        if response.error != ErrorCode::Success {
            return Err(map_error_response(
                response.error,
                response.leader_hint,
                response.retry_after_ms,
            ));
        }

        Ok(())
    }

    /// Returns the client ID assigned to this local client
    pub fn client_id(&self) -> u32 {
        self.client_id
    }

    /// Returns the configured timeout duration for operations
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Watch for changes to a specific key
    ///
    /// Returns a `WatcherHandle` that yields watch events when the key's value changes.
    /// The stream will continue until explicitly dropped or a connection error occurs.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to watch
    ///
    /// # Returns
    ///
    /// A `WatcherHandle` that can be used to receive watch events
    ///
    /// # Errors
    ///
    /// Returns error if watch feature is not enabled or WatchRegistry not initialized
    ///
    /// # Example
    ///
    /// ```ignore
    /// let client = engine.client();
    /// let mut watcher = client.watch(b"config/timeout").await?;
    /// while let Some(event) = watcher.next().await {
    ///     println!("Value changed: {:?}", event);
    /// }
    /// ```
    #[cfg(feature = "watch")]
    pub fn watch(
        &self,
        key: impl AsRef<[u8]>,
    ) -> ClientApiResult<d_engine_core::watch::WatcherHandle> {
        self.watch_with_options(key, false)
    }

    /// Watch a key and opt in to receiving the previous value on each mutation.
    ///
    /// This is the lower-level form of [`watch`](Self::watch). Use it when you need
    /// `event.prev_value` — for example to detect what a key held before a write, or
    /// to implement an audit log.
    ///
    /// # Arguments
    ///
    /// * `key`     - The exact key to watch.
    /// * `prev_kv` - When `true`, every `Put` and `Delete` event carries the value the
    ///   key held **before** the mutation in `event.prev_value`. When
    ///   `false` (the default via [`watch`](Self::watch)), `prev_value` is
    ///   always empty.
    ///
    /// # Performance note
    ///
    /// When at least one watcher has `prev_kv = true`, the state machine reads the old
    /// value from storage before each `apply_chunk`.  The read is amortised across the
    /// whole write batch — cost scales with **write rate**, not watcher count.  Disable
    /// when you don't need the previous value.
    ///
    /// # `prev_value` is empty in these cases
    ///
    /// - The watcher was registered with `prev_kv = false`.
    /// - The event type is `Progress` or `Canceled` (not a data mutation).
    /// - The key did not exist before a `Put` (i.e. it was a fresh insert).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Distributed-lock audit: know who held the lock before it changed hands.
    /// let watcher = client.watch_with_options(b"lock/resource_a", true)?;
    /// let (_, _, mut rx) = watcher.into_receiver();
    ///
    /// tokio::spawn(async move {
    ///     while let Some(event) = rx.recv().await {
    ///         match event.event_type {
    ///             WatchEventType::Put => println!(
    ///                 "lock acquired by {:?}, was held by {:?}",
    ///                 event.value, event.prev_value
    ///             ),
    ///             WatchEventType::Delete => println!(
    ///                 "lock released, was held by {:?}",
    ///                 event.prev_value
    ///             ),
    ///             WatchEventType::Canceled => { /* re-register */ break; }
    ///             WatchEventType::Progress => {}
    ///         }
    ///     }
    /// });
    /// ```
    #[cfg(feature = "watch")]
    pub fn watch_with_options(
        &self,
        key: impl AsRef<[u8]>,
        prev_kv: bool,
    ) -> ClientApiResult<d_engine_core::watch::WatcherHandle> {
        let registry = self.watch_registry.as_ref().ok_or_else(|| ClientApiError::Business {
            code: ErrorCode::Uncategorized,
            message: "Watch feature disabled (WatchRegistry not initialized)".to_string(),
            required_action: None,
        })?;

        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        registry.register(key_bytes, prev_kv).map_err(|e| ClientApiError::Business {
            code: ErrorCode::Uncategorized,
            message: e.to_string(),
            required_action: None,
        })
    }

    /// Register a prefix watcher — notified on every key under the given path prefix.
    ///
    /// `prefix` must start with '/' and end with '/'.  E.g. `b"/services/"` watches
    /// all keys whose path begins with `/services/`.
    #[cfg(feature = "watch")]
    pub fn watch_prefix(
        &self,
        prefix: impl AsRef<[u8]>,
    ) -> ClientApiResult<d_engine_core::watch::WatcherHandle> {
        self.watch_prefix_with_options(prefix, false)
    }

    /// Register a prefix watcher and opt in to receiving the previous value on each mutation.
    ///
    /// Prefix form of [`watch_with_options`](Self::watch_with_options).  Every key whose
    /// path starts with `prefix` triggers an event; `event.key` is the full child key.
    ///
    /// See [`watch_with_options`](Self::watch_with_options) for the `prev_kv` semantics,
    /// performance note, and the cases where `prev_value` is empty.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Track every endpoint change under /services/ together with the old address.
    /// let watcher = client.watch_prefix_with_options(b"/services/", true)?;
    /// let (_, _, mut rx) = watcher.into_receiver();
    ///
    /// tokio::spawn(async move {
    ///     while let Some(event) = rx.recv().await {
    ///         match event.event_type {
    ///             WatchEventType::Put => println!(
    ///                 "{:?} moved from {:?} → {:?}",
    ///                 event.key, event.prev_value, event.value
    ///             ),
    ///             WatchEventType::Delete => println!(
    ///                 "{:?} removed (was {:?})",
    ///                 event.key, event.prev_value
    ///             ),
    ///             WatchEventType::Canceled => { /* buffer overflow — re-register */ break; }
    ///             WatchEventType::Progress => {}
    ///         }
    ///     }
    /// });
    /// ```
    #[cfg(feature = "watch")]
    pub fn watch_prefix_with_options(
        &self,
        prefix: impl AsRef<[u8]>,
        prev_kv: bool,
    ) -> ClientApiResult<d_engine_core::watch::WatcherHandle> {
        let registry = self.watch_registry.as_ref().ok_or_else(|| ClientApiError::Business {
            code: ErrorCode::Uncategorized,
            message: "Watch feature disabled (WatchRegistry not initialized)".to_string(),
            required_action: None,
        })?;

        let prefix_bytes = Bytes::copy_from_slice(prefix.as_ref());
        registry
            .register_prefix(prefix_bytes, prev_kv)
            .map_err(|e| ClientApiError::Business {
                code: ErrorCode::Uncategorized,
                message: e.to_string(),
                required_action: None,
            })
    }

    /// Scan all keys under `prefix` with linearizable consistency.
    ///
    /// Returns a `ScanResult` containing all matching `(key, value)` pairs and the
    /// `revision` (applied index) at scan time. Use `revision` to filter watch events
    /// during reconnection: skip events where `event.revision <= revision`.
    pub async fn scan_prefix(
        &self,
        prefix: impl AsRef<[u8]>,
    ) -> ClientApiResult<ScanResult> {
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.read_handle
            .cmd_tx
            .send(d_engine_core::ClientCmd::Scan(
                Bytes::copy_from_slice(prefix.as_ref()),
                resp_tx,
            ))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        result.map_err(|status| server_error(format!("RPC error: {}", status.message())))
    }

    /// Internal helper: Get cluster membership via ClusterConf event
    async fn get_cluster_membership(
        &self
    ) -> ClientApiResult<d_engine_proto::server::cluster::ClusterMembership> {
        let request = d_engine_proto::server::cluster::MetadataRequest {};

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(InboundEvent::ClusterConf(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        result.map_err(|status| server_error(format!("ClusterConf error: {}", status.message())))
    }
}

impl<T: TypeConfig> std::fmt::Debug for EmbeddedClient<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("EmbeddedClient")
            .field("client_id", &self.client_id)
            .field("timeout", &self.timeout)
            .finish()
    }
}

// Implement ClientApi trait
#[async_trait::async_trait]
impl<T: TypeConfig> ClientApi for EmbeddedClient<T> {
    async fn put(
        &self,
        key: impl AsRef<[u8]> + Send,
        value: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<()> {
        self.put(key, value).await
    }

    async fn put_with_ttl(
        &self,
        key: impl AsRef<[u8]> + Send,
        value: impl AsRef<[u8]> + Send,
        ttl_secs: u64,
    ) -> ClientApiResult<()> {
        let request = ClientWriteRequest {
            client_id: self.client_id,
            command: Some(WriteOperation::Insert {
                key: Bytes::copy_from_slice(key.as_ref()),
                value: Bytes::copy_from_slice(value.as_ref()),
                ttl_secs: Some(ttl_secs),
            }),
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.read_handle
            .cmd_tx
            .send(d_engine_core::ClientCmd::Propose(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        let response =
            result.map_err(|status| server_error(format!("RPC error: {}", status.message())))?;

        if response.error != ErrorCode::Success {
            return Err(map_error_response(
                response.error,
                response.leader_hint,
                response.retry_after_ms,
            ));
        }

        Ok(())
    }

    async fn get(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<Option<Bytes>> {
        self.get_linearizable(key).await
    }

    async fn get_multi(
        &self,
        keys: &[Bytes],
    ) -> ClientApiResult<Vec<Option<Bytes>>> {
        self.get_multi_linearizable(keys).await
    }

    async fn delete(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<()> {
        self.delete(key).await
    }

    async fn compare_and_swap(
        &self,
        key: impl AsRef<[u8]> + Send,
        expected_value: Option<impl AsRef<[u8]> + Send>,
        new_value: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<bool> {
        let request = ClientWriteRequest {
            client_id: self.client_id,
            command: Some(WriteOperation::CompareAndSwap {
                key: Bytes::copy_from_slice(key.as_ref()),
                expected: expected_value.map(|v| Bytes::copy_from_slice(v.as_ref())),
                new_value: Bytes::copy_from_slice(new_value.as_ref()),
            }),
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.read_handle
            .cmd_tx
            .send(d_engine_core::ClientCmd::Propose(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        let response =
            result.map_err(|status| server_error(format!("RPC error: {}", status.message())))?;

        if response.error != ErrorCode::Success {
            return Err(map_error_response(
                response.error,
                response.leader_hint,
                response.retry_after_ms,
            ));
        }

        match response.result {
            Some(ClientResponsePayload::Write(result)) => Ok(result.succeeded),
            _ => Err(server_error("Invalid CAS response".to_string())),
        }
    }

    async fn list_members(
        &self
    ) -> ClientApiResult<Vec<d_engine_proto::server::cluster::NodeMeta>> {
        let cluster_membership = self.get_cluster_membership().await?;
        Ok(cluster_membership.nodes)
    }

    async fn get_leader_id(&self) -> ClientApiResult<Option<u32>> {
        let cluster_membership = self.get_cluster_membership().await?;
        Ok(cluster_membership.current_leader_id)
    }

    async fn get_multi_with_policy(
        &self,
        keys: &[Bytes],
        consistency_policy: Option<d_engine_core::config::ReadConsistencyPolicy>,
    ) -> ClientApiResult<Vec<Option<Bytes>>> {
        self.get_multi_with_consistency(
            keys,
            consistency_policy
                .unwrap_or(d_engine_core::config::ReadConsistencyPolicy::LinearizableRead),
        )
        .await
    }

    async fn get_linearizable(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<Option<Bytes>> {
        self.get_linearizable(key).await
    }

    async fn get_lease(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<Option<Bytes>> {
        self.get_with_consistency(key, ReadConsistencyPolicy::LeaseRead).await
    }

    async fn get_eventual(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<Option<Bytes>> {
        self.get_eventual(key).await
    }

    async fn scan_prefix(
        &self,
        prefix: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<ScanResult> {
        self.scan_prefix(prefix).await
    }
}

#[cfg(test)]
#[path = "embedded_client_test/embedded_client_test.rs"]
mod tests;
