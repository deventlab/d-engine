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
//! let engine = EmbeddedEngine::start().await?;
//! let client = engine.client();
//! client.put(b"key", b"value").await?;
//! ```

use std::time::Duration;

#[cfg(feature = "watch")]
use std::sync::Arc;

use bytes::Bytes;
use d_engine_core::MaybeCloneOneshot;
use d_engine_core::RaftEvent;
use d_engine_core::RaftOneshot;
use d_engine_core::client::{ClientApi, ClientApiError, ClientApiResult};
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::ReadConsistencyPolicy;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::common::LeaderHint;
use d_engine_proto::error::ErrorCode;
use tokio::sync::mpsc;

#[cfg(feature = "watch")]
use d_engine_core::watch::WatchRegistry;

// ============================================================================
// Error helpers - simplify ClientApiError construction for embedded client
// ============================================================================

fn channel_closed_error() -> ClientApiError {
    ClientApiError::Network {
        code: ErrorCode::ConnectionTimeout,
        message: "Channel closed, node may be shutting down".to_string(),
        retry_after_ms: None,
        leader_hint: None,
    }
}

fn timeout_error(duration: Duration) -> ClientApiError {
    ClientApiError::Network {
        code: ErrorCode::ConnectionTimeout,
        message: format!("Operation timed out after {duration:?}"),
        retry_after_ms: Some(1000),
        leader_hint: None,
    }
}

fn not_leader_error(
    leader_id: Option<String>,
    leader_address: Option<String>,
) -> ClientApiError {
    let message = match (&leader_address, &leader_id) {
        (Some(addr), _) => format!("Not leader, try leader at: {addr}"),
        (None, Some(id)) => format!("Not leader, leader_id: {id}"),
        (None, None) => "Not leader".to_string(),
    };

    let leader_hint = match (&leader_id, &leader_address) {
        (Some(id_str), Some(addr)) => id_str.parse::<u32>().ok().map(|id| LeaderHint {
            leader_id: id,
            address: addr.clone(),
        }),
        _ => None,
    };

    ClientApiError::Network {
        code: ErrorCode::NotLeader,
        message,
        retry_after_ms: Some(100),
        leader_hint,
    }
}

fn server_error(msg: String) -> ClientApiError {
    ClientApiError::Business {
        code: ErrorCode::Uncategorized,
        message: msg,
        required_action: None,
    }
}

/// Zero-overhead KV client for embedded mode.
///
/// Directly calls Raft core without gRPC overhead.
#[derive(Clone)]
pub struct EmbeddedClient {
    event_tx: mpsc::Sender<RaftEvent>,
    client_id: u32,
    timeout: Duration,
    #[cfg(feature = "watch")]
    watch_registry: Option<Arc<WatchRegistry>>,
}

impl EmbeddedClient {
    /// Internal constructor (used by EmbeddedEngine)
    pub(crate) fn new_internal(
        event_tx: mpsc::Sender<RaftEvent>,
        client_id: u32,
        timeout: Duration,
    ) -> Self {
        Self {
            event_tx,
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

    /// Map ErrorCode and ErrorMetadata to ClientApiError
    fn map_error_response(
        error_code: i32,
        metadata: Option<d_engine_proto::error::ErrorMetadata>,
    ) -> ClientApiError {
        match ErrorCode::try_from(error_code) {
            Ok(ErrorCode::NotLeader) => {
                let (leader_id, leader_address) = if let Some(meta) = metadata {
                    (meta.leader_id, meta.leader_address)
                } else {
                    (None, None)
                };
                not_leader_error(leader_id, leader_address)
            }
            _ => server_error(format!("Error code: {error_code}")),
        }
    }

    /// Store a key-value pair with strong consistency.
    pub async fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> ClientApiResult<()> {
        let command = WriteCommand::insert(
            Bytes::copy_from_slice(key.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
        );

        let request = ClientWriteRequest {
            client_id: self.client_id,
            commands: vec![command],
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(RaftEvent::ClientPropose(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        let response =
            result.map_err(|status| server_error(format!("RPC error: {}", status.message())))?;

        if response.error != ErrorCode::Success as i32 {
            return Err(Self::map_error_response(response.error, response.metadata));
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
        let request = ClientReadRequest {
            client_id: self.client_id,
            keys: vec![Bytes::copy_from_slice(key.as_ref())],
            consistency_policy: Some(consistency as i32),
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(RaftEvent::ClientReadRequest(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        let response =
            result.map_err(|status| server_error(format!("RPC error: {}", status.message())))?;

        if response.error != ErrorCode::Success as i32 {
            return Err(Self::map_error_response(response.error, response.metadata));
        }

        match response.success_result {
            Some(d_engine_proto::client::client_response::SuccessResult::ReadData(
                read_results,
            )) => {
                // If results list is empty, key doesn't exist
                // Otherwise, return the value (even if empty bytes)
                Ok(read_results.results.first().map(|r| r.value.clone()))
            }
            _ => Ok(None),
        }
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
    pub async fn get_multi_with_consistency(
        &self,
        keys: &[Bytes],
        consistency: ReadConsistencyPolicy,
    ) -> ClientApiResult<Vec<Option<Bytes>>> {
        let request = ClientReadRequest {
            client_id: self.client_id,
            keys: keys.to_vec(),
            consistency_policy: Some(consistency as i32),
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(RaftEvent::ClientReadRequest(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        let response =
            result.map_err(|status| server_error(format!("RPC error: {}", status.message())))?;

        if response.error != ErrorCode::Success as i32 {
            return Err(Self::map_error_response(response.error, response.metadata));
        }

        match response.success_result {
            Some(d_engine_proto::client::client_response::SuccessResult::ReadData(
                read_results,
            )) => {
                // Reconstruct result vector in requested key order.
                // Server only returns results for keys that exist, so we must
                // map by key to preserve positional correspondence with input.
                let results_by_key: std::collections::HashMap<_, _> =
                    read_results.results.into_iter().map(|r| (r.key, r.value)).collect();

                Ok(keys.iter().map(|k| results_by_key.get(k).cloned()).collect())
            }
            _ => Ok(vec![None; keys.len()]),
        }
    }

    /// Delete a key-value pair.
    pub async fn delete(
        &self,
        key: impl AsRef<[u8]>,
    ) -> ClientApiResult<()> {
        let command = WriteCommand::delete(Bytes::copy_from_slice(key.as_ref()));

        let request = ClientWriteRequest {
            client_id: self.client_id,
            commands: vec![command],
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(RaftEvent::ClientPropose(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        let response =
            result.map_err(|status| server_error(format!("RPC error: {}", status.message())))?;

        if response.error != ErrorCode::Success as i32 {
            return Err(Self::map_error_response(response.error, response.metadata));
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

    /// Returns the node ID for testing purposes
    pub fn node_id(&self) -> u32 {
        self.client_id
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
        let registry = self.watch_registry.as_ref().ok_or_else(|| ClientApiError::Business {
            code: ErrorCode::Uncategorized,
            message: "Watch feature disabled (WatchRegistry not initialized)".to_string(),
            required_action: None,
        })?;

        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        Ok(registry.register(key_bytes))
    }

    /// Internal helper: Get cluster membership via ClusterConf event
    async fn get_cluster_membership(
        &self
    ) -> ClientApiResult<d_engine_proto::server::cluster::ClusterMembership> {
        let request = d_engine_proto::server::cluster::MetadataRequest {};

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(RaftEvent::ClusterConf(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        result.map_err(|status| server_error(format!("ClusterConf error: {}", status.message())))
    }
}

impl std::fmt::Debug for EmbeddedClient {
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
impl ClientApi for EmbeddedClient {
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
        let command = WriteCommand::insert_with_ttl(
            Bytes::copy_from_slice(key.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
            ttl_secs,
        );

        let request = ClientWriteRequest {
            client_id: self.client_id,
            commands: vec![command],
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(RaftEvent::ClientPropose(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        let response =
            result.map_err(|status| server_error(format!("RPC error: {}", status.message())))?;

        if response.error != ErrorCode::Success as i32 {
            return Err(Self::map_error_response(response.error, response.metadata));
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
        let command = WriteCommand::compare_and_swap(
            Bytes::copy_from_slice(key.as_ref()),
            expected_value.map(|v| Bytes::copy_from_slice(v.as_ref())),
            Bytes::copy_from_slice(new_value.as_ref()),
        );

        let request = ClientWriteRequest {
            client_id: self.client_id,
            commands: vec![command],
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(RaftEvent::ClientPropose(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(self.timeout))?
            .map_err(|_| channel_closed_error())?;

        let response =
            result.map_err(|status| server_error(format!("RPC error: {}", status.message())))?;

        if response.error != ErrorCode::Success as i32 {
            return Err(Self::map_error_response(response.error, response.metadata));
        }

        match response.success_result {
            Some(d_engine_proto::client::client_response::SuccessResult::WriteResult(result)) => {
                Ok(result.succeeded)
            }
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
}
