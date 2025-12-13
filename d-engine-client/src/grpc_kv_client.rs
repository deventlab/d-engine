use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tracing::debug;
use tracing::error;
use tracing::warn;

use super::ClientInner;
use crate::ClientApiError;
use crate::ClientResponseExt;
use crate::scoped_timer::ScopedTimer;
use crate::{KvClient as CoreKvClient, KvResult};
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientResult;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::ReadConsistencyPolicy;
use d_engine_proto::client::WatchRequest;
use d_engine_proto::client::WatchResponse;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::raft_client_service_client::RaftClientServiceClient;
use d_engine_proto::error::ErrorCode;

/// gRPC-based key-value store client
///
/// Implements remote CRUD operations via gRPC protocol.
/// All write operations use strong consistency.
#[derive(Clone)]
pub struct GrpcKvClient {
    pub(super) client_inner: Arc<ArcSwap<ClientInner>>,
}

impl GrpcKvClient {
    pub(crate) fn new(client_inner: Arc<ArcSwap<ClientInner>>) -> Self {
        Self { client_inner }
    }

    /// Stores a value with strong consistency
    ///
    /// # Errors
    /// - [`crate::ClientApiError::Network`] on network failures
    /// - [`crate::ClientApiError::Protocol`] for protocol errors
    /// - [`crate::ClientApiError::Storage`] for server-side storage errors
    pub async fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> std::result::Result<(), ClientApiError> {
        let _timer = ScopedTimer::new("client::put");

        let client_inner = self.client_inner.load();

        // Build request
        let mut commands = Vec::new();
        let client_command_insert = WriteCommand::insert(
            Bytes::copy_from_slice(key.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
        );
        commands.push(client_command_insert);

        let request = ClientWriteRequest {
            client_id: client_inner.client_id,
            commands,
        };

        let mut client = self.make_leader_client().await?;
        // Send write request
        match client.handle_client_write(request).await {
            Ok(response) => {
                debug!("[:KvClient:write] response: {:?}", response);
                let client_response = response.get_ref();
                client_response.validate_error()
            }
            Err(status) => {
                error!("[:KvClient:write] status: {:?}", status);
                Err(status.into())
            }
        }
    }

    /// Stores a value with TTL (time-to-live) and strong consistency
    ///
    /// Key will automatically expire and be deleted after ttl_secs seconds.
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to store
    /// * `ttl_secs` - Time-to-live in seconds
    ///
    /// # Errors
    /// - [`crate::ClientApiError::Network`] on network failures
    /// - [`crate::ClientApiError::Protocol`] for protocol errors
    /// - [`crate::ClientApiError::Storage`] for server-side storage errors
    pub async fn put_with_ttl(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl_secs: u64,
    ) -> std::result::Result<(), ClientApiError> {
        let _timer = ScopedTimer::new("client::put_with_ttl");

        let client_inner = self.client_inner.load();

        // Build request with TTL
        let mut commands = Vec::new();
        let client_command_insert = WriteCommand::insert_with_ttl(
            Bytes::copy_from_slice(key.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
            ttl_secs,
        );
        commands.push(client_command_insert);

        let request = ClientWriteRequest {
            client_id: client_inner.client_id,
            commands,
        };

        let mut client = self.make_leader_client().await?;
        // Send write request
        match client.handle_client_write(request).await {
            Ok(response) => {
                debug!("[:KvClient:put_with_ttl] response: {:?}", response);
                let client_response = response.get_ref();
                client_response.validate_error()
            }
            Err(status) => {
                error!("[:KvClient:put_with_ttl] status: {:?}", status);
                Err(status.into())
            }
        }
    }

    /// Deletes a key with strong consistency guarantees
    ///
    /// Permanently removes the specified key and its associated value from the store.
    ///
    /// # Parameters
    /// - `key`: The byte-serialized key to delete. Supports any type implementing `AsRef<[u8]>`
    ///   (e.g. `String`, `&str`, `Vec<u8>`)
    ///
    /// # Errors
    /// - [`crate::ClientApiError::Network`] if unable to reach the leader node
    /// - [`crate::ClientApiError::Protocol`] for protocol errors
    /// - [`crate::ClientApiError::Storage`] for server-side storage errors
    pub async fn delete(
        &self,
        key: impl AsRef<[u8]>,
    ) -> std::result::Result<(), ClientApiError> {
        let client_inner = self.client_inner.load();
        // Build request
        let mut commands = Vec::new();
        let client_command_delete = WriteCommand::delete(Bytes::copy_from_slice(key.as_ref()));
        commands.push(client_command_delete);

        let request = ClientWriteRequest {
            client_id: client_inner.client_id,
            commands,
        };

        let mut client = self.make_leader_client().await?;

        // Send delete request
        match client.handle_client_write(request).await {
            Ok(response) => {
                debug!("[:KvClient:delete] response: {:?}", response);
                let client_response = response.get_ref();
                client_response.validate_error()
            }
            Err(status) => {
                error!("[:KvClient:delete] status: {:?}", status);
                Err(status.into())
            }
        }
    }

    // Convenience methods for explicit consistency levels
    pub async fn get_linearizable(
        &self,
        key: impl AsRef<[u8]>,
    ) -> std::result::Result<Option<ClientResult>, ClientApiError> {
        self.get_with_policy(key, Some(ReadConsistencyPolicy::LinearizableRead)).await
    }

    pub async fn get_lease(
        &self,
        key: impl AsRef<[u8]>,
    ) -> std::result::Result<Option<ClientResult>, ClientApiError> {
        self.get_with_policy(key, Some(ReadConsistencyPolicy::LeaseRead)).await
    }

    pub async fn get_eventual(
        &self,
        key: impl AsRef<[u8]>,
    ) -> std::result::Result<Option<ClientResult>, ClientApiError> {
        self.get_with_policy(key, Some(ReadConsistencyPolicy::EventualConsistency))
            .await
    }

    /// Retrieves a single key's value using server's default consistency policy
    ///
    /// Uses the cluster's configured default consistency policy as defined in
    /// the server's ReadConsistencyConfig.default_policy setting.
    ///
    /// # Parameters
    /// * `key` - The key to retrieve, accepts any type implementing `AsRef<[u8]>`
    ///
    /// # Returns
    /// * `Ok(Some(ClientResult))` - Key exists, returns key-value pair
    /// * `Ok(None)` - Key does not exist in the store
    /// * `Err(ClientApiError)` - Read failed due to network or consistency issues
    pub async fn get(
        &self,
        key: impl AsRef<[u8]>,
    ) -> std::result::Result<Option<ClientResult>, ClientApiError> {
        self.get_with_policy(key, None).await
    }

    /// Retrieves a single key's value with explicit consistency policy
    ///
    /// Allows client to override server's default consistency policy for this specific request.
    /// If server's allow_client_override is false, the override will be ignored.
    ///
    /// # Parameters
    /// * `key` - The key to retrieve, accepts any type implementing `AsRef<[u8]>`
    /// * `policy` - Explicit consistency policy for this request
    pub async fn get_with_policy(
        &self,
        key: impl AsRef<[u8]>,
        consistency_policy: Option<ReadConsistencyPolicy>,
    ) -> std::result::Result<Option<ClientResult>, ClientApiError> {
        // Delegate to multi-get implementation
        let mut results =
            self.get_multi_with_policy(std::iter::once(key), consistency_policy).await?;

        // Extract single result (safe due to single-key input)
        Ok(results.pop().unwrap_or(None))
    }

    /// Fetches multiple keys using server's default consistency policy
    ///
    /// Uses the cluster's configured default consistency policy as defined in
    /// the server's ReadConsistencyConfig.default_policy setting.
    pub async fn get_multi(
        &self,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> std::result::Result<Vec<Option<ClientResult>>, ClientApiError> {
        self.get_multi_with_policy(keys, None).await
    }

    /// Fetches multiple keys with explicit consistency policy override
    ///
    /// Allows client to override server's default consistency policy for this batch request.
    /// If server's allow_client_override is false, the override will be ignored.
    pub async fn get_multi_with_policy(
        &self,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
        consistency_policy: Option<ReadConsistencyPolicy>,
    ) -> std::result::Result<Vec<Option<ClientResult>>, ClientApiError> {
        let _timer = ScopedTimer::new("client::get_multi");

        let client_inner = self.client_inner.load();
        // Convert keys to commands
        let keys: Vec<Bytes> =
            keys.into_iter().map(|k| Bytes::copy_from_slice(k.as_ref())).collect();

        // Validate at least one key
        if keys.is_empty() {
            warn!("Attempted multi-get with empty key collection");
            return Err(ErrorCode::InvalidRequest.into());
        }

        // Build request
        let request = ClientReadRequest {
            client_id: client_inner.client_id,
            keys,
            consistency_policy: consistency_policy.map(|p| p as i32),
        };

        // Select client based on policy (if specified)
        let mut client = match consistency_policy {
            Some(ReadConsistencyPolicy::LinearizableRead)
            | Some(ReadConsistencyPolicy::LeaseRead) => {
                debug!("Using leader client for explicit consistency policy");
                self.make_leader_client().await?
            }
            Some(ReadConsistencyPolicy::EventualConsistency) | None => {
                debug!("Using load-balanced client for cluster default policy");
                self.make_client().await?
            }
        };

        // Execute request
        match client.handle_client_read(request).await {
            Ok(response) => {
                debug!("Read response: {:?}", response);
                response.into_inner().into_read_results()
            }
            Err(status) => {
                error!("Read request failed: {:?}", status);
                Err(status.into())
            }
        }
    }

    /// Watch for changes on a specific key
    ///
    /// Returns a stream of watch events whenever the specified key is modified (PUT or DELETE).
    /// The stream will continue until the client drops the receiver or disconnects.
    ///
    /// # Arguments
    /// * `key` - The exact key to watch (prefix/range watch not supported in v1)
    ///
    /// # Returns
    /// * `Ok(Streaming<WatchResponse>)` - Stream of watch events
    /// * `Err(ClientApiError)` - If watch feature is disabled or connection fails
    ///
    /// # Example
    /// ```rust,ignore
    /// use futures::StreamExt;
    ///
    /// let mut stream = client.kv().watch("my_key").await?;
    /// while let Some(event) = stream.next().await {
    ///     match event {
    ///         Ok(response) => println!("Key changed: {:?}", response),
    ///         Err(e) => eprintln!("Watch error: {:?}", e),
    ///     }
    /// }
    /// ```
    pub async fn watch(
        &self,
        key: impl AsRef<[u8]>,
    ) -> std::result::Result<tonic::Streaming<WatchResponse>, ClientApiError> {
        let client_inner = self.client_inner.load();

        let request = WatchRequest {
            client_id: client_inner.client_id,
            key: Bytes::copy_from_slice(key.as_ref()),
        };

        // Watch can connect to any node (leader or follower)
        let mut client = self.make_client().await?;

        match client.watch(request).await {
            Ok(response) => {
                debug!("Watch stream established");
                Ok(response.into_inner())
            }
            Err(status) => {
                error!("Watch request failed: {:?}", status);
                Err(status.into())
            }
        }
    }

    async fn make_leader_client(
        &self
    ) -> std::result::Result<RaftClientServiceClient<Channel>, ClientApiError> {
        let client_inner = self.client_inner.load();

        let channel = client_inner.pool.get_leader();
        let mut client = RaftClientServiceClient::new(channel);
        if client_inner.pool.config.enable_compression {
            client = client
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
        }

        Ok(client)
    }

    pub(super) async fn make_client(
        &self
    ) -> std::result::Result<RaftClientServiceClient<Channel>, ClientApiError> {
        let client_inner = self.client_inner.load();

        // Balance from read clients
        let mut rng = StdRng::from_entropy();
        let channels = client_inner.pool.get_all_channels();
        let i = rng.gen_range(0..channels.len());

        let mut client = RaftClientServiceClient::new(channels[i].clone());

        if client_inner.pool.config.enable_compression {
            client = client
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
        }

        Ok(client)
    }
}

// ==================== Core KvClient Trait Implementation ====================

// Implement d_engine_core::KvClient trait for GrpcKvClient
#[async_trait::async_trait]
impl CoreKvClient for GrpcKvClient {
    async fn put(
        &self,
        key: impl AsRef<[u8]> + Send,
        value: impl AsRef<[u8]> + Send,
    ) -> KvResult<()> {
        GrpcKvClient::put(self, key, value).await.map_err(Into::into)
    }

    async fn put_with_ttl(
        &self,
        key: impl AsRef<[u8]> + Send,
        value: impl AsRef<[u8]> + Send,
        ttl_secs: u64,
    ) -> KvResult<()> {
        GrpcKvClient::put_with_ttl(self, key, value, ttl_secs).await.map_err(Into::into)
    }

    async fn get(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> KvResult<Option<Bytes>> {
        match GrpcKvClient::get(self, key).await {
            Ok(Some(result)) => Ok(Some(result.value)),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn get_multi(
        &self,
        keys: &[Bytes],
    ) -> KvResult<Vec<Option<Bytes>>> {
        match GrpcKvClient::get_multi(self, keys.iter().cloned()).await {
            Ok(results) => Ok(results.into_iter().map(|opt| opt.map(|r| r.value)).collect()),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> KvResult<()> {
        GrpcKvClient::delete(self, key).await.map_err(Into::into)
    }
}
