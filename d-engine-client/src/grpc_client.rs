use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientResult;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::ReadConsistencyPolicy;
use d_engine_proto::client::WatchRequest;
use d_engine_proto::client::WatchResponse;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::raft_client_service_client::RaftClientServiceClient;
use d_engine_proto::error::ErrorCode;
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
use d_engine_core::client::{ClientApi, ClientApiResult};

/// gRPC-based key-value store client
///
/// Implements remote CRUD operations via gRPC protocol.
/// All write operations use strong consistency.
#[derive(Clone)]
pub struct GrpcClient {
    pub(super) client_inner: Arc<ArcSwap<ClientInner>>,
}

impl GrpcClient {
    pub(crate) fn new(client_inner: Arc<ArcSwap<ClientInner>>) -> Self {
        Self { client_inner }
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

    /// Watch for changes to a specific key
    ///
    /// Returns a stream of watch events when the key's value changes.
    /// The stream will continue until explicitly closed or a connection error occurs.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to watch
    ///
    /// # Returns
    ///
    /// A streaming response that yields `WatchResponse` events
    ///
    /// # Errors
    ///
    /// Returns error if unable to establish watch connection
    pub async fn watch(
        &self,
        key: impl AsRef<[u8]>,
    ) -> ClientApiResult<tonic::Streaming<WatchResponse>> {
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
}

// ==================== Core ClientApi Trait Implementation ====================

// Implement ClientApi trait for GrpcClient
#[async_trait::async_trait]
impl ClientApi for GrpcClient {
    async fn put(
        &self,
        key: impl AsRef<[u8]> + Send,
        value: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<()> {
        // Performance tracking for put operation
        let _timer = ScopedTimer::new("client::put");

        let client_inner = self.client_inner.load();

        // Build write request with insert command
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

        // Send write request to leader node (strong consistency required)
        let mut client = self.make_leader_client().await.map_err(Into::<ClientApiError>::into)?;
        match client.handle_client_write(request).await {
            Ok(response) => {
                debug!("[:GrpcClient:write] response: {:?}", response);
                let client_response = response.get_ref();
                client_response.validate_error().map_err(Into::<ClientApiError>::into)
            }
            Err(status) => {
                error!("[:GrpcClient:write] status: {:?}", status);
                Err(Into::<ClientApiError>::into(ClientApiError::from(status)))
            }
        }
    }

    async fn put_with_ttl(
        &self,
        key: impl AsRef<[u8]> + Send,
        value: impl AsRef<[u8]> + Send,
        ttl_secs: u64,
    ) -> ClientApiResult<()> {
        // Performance tracking for put_with_ttl operation
        let _timer = ScopedTimer::new("client::put_with_ttl");

        let client_inner = self.client_inner.load();

        // Build write request with TTL-enabled insert command
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

        // Send write request to leader node (strong consistency required)
        let mut client = self.make_leader_client().await.map_err(Into::<ClientApiError>::into)?;
        match client.handle_client_write(request).await {
            Ok(response) => {
                debug!("[:GrpcClient:put_with_ttl] response: {:?}", response);
                let client_response = response.get_ref();
                client_response.validate_error().map_err(Into::<ClientApiError>::into)
            }
            Err(status) => {
                error!("[:GrpcClient:put_with_ttl] status: {:?}", status);
                Err(Into::<ClientApiError>::into(ClientApiError::from(status)))
            }
        }
    }

    async fn get(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<Option<Bytes>> {
        // Delegate to get_with_policy with server's default consistency policy
        let result = self.get_with_policy(key, None).await;

        match result {
            Ok(Some(client_result)) => Ok(Some(client_result.value)),
            Ok(None) => Ok(None),
            Err(e) => Err(Into::<ClientApiError>::into(e)),
        }
    }

    async fn get_multi(
        &self,
        keys: &[Bytes],
    ) -> ClientApiResult<Vec<Option<Bytes>>> {
        // Delegate to get_multi_with_policy with server's default consistency policy
        let result = self.get_multi_with_policy(keys.iter().cloned(), None).await;

        match result {
            Ok(results) => {
                // Extract values from ClientResult, preserving None for missing keys
                Ok(results.into_iter().map(|opt| opt.map(|r| r.value)).collect())
            }
            Err(e) => Err(Into::<ClientApiError>::into(e)),
        }
    }

    async fn delete(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<()> {
        let client_inner = self.client_inner.load();

        // Build delete request
        let mut commands = Vec::new();
        let client_command_delete = WriteCommand::delete(Bytes::copy_from_slice(key.as_ref()));
        commands.push(client_command_delete);

        let request = ClientWriteRequest {
            client_id: client_inner.client_id,
            commands,
        };

        // Send delete request to leader node (strong consistency required)
        let mut client = self.make_leader_client().await.map_err(Into::<ClientApiError>::into)?;
        match client.handle_client_write(request).await {
            Ok(response) => {
                debug!("[:GrpcClient:delete] response: {:?}", response);
                let client_response = response.get_ref();
                client_response.validate_error().map_err(Into::<ClientApiError>::into)
            }
            Err(status) => {
                error!("[:GrpcClient:delete] status: {:?}", status);
                Err(Into::<ClientApiError>::into(ClientApiError::from(status)))
            }
        }
    }

    async fn compare_and_swap(
        &self,
        key: impl AsRef<[u8]> + Send,
        expected_value: Option<impl AsRef<[u8]> + Send>,
        new_value: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<bool> {
        let client_inner = self.client_inner.load();

        // Build CAS request
        let mut commands = Vec::new();
        let expected = expected_value.map(|v| Bytes::copy_from_slice(v.as_ref()));
        let client_command_cas = WriteCommand::compare_and_swap(
            Bytes::copy_from_slice(key.as_ref()),
            expected,
            Bytes::copy_from_slice(new_value.as_ref()),
        );
        commands.push(client_command_cas);

        let request = ClientWriteRequest {
            client_id: client_inner.client_id,
            commands,
        };

        // Send CAS request to leader node
        let mut client = self.make_leader_client().await.map_err(Into::<ClientApiError>::into)?;
        match client.handle_client_write(request).await {
            Ok(response) => {
                debug!("[:GrpcClient:compare_and_swap] response: {:?}", response);
                let client_response = response.get_ref();

                // Validate no error occurred
                client_response.validate_error().map_err(Into::<ClientApiError>::into)?;

                // Extract CAS result (true = succeeded, false = failed comparison)
                Ok(client_response.succeeded())
            }
            Err(status) => {
                error!("[:GrpcClient:compare_and_swap] status: {:?}", status);
                Err(Into::<ClientApiError>::into(ClientApiError::from(status)))
            }
        }
    }

    async fn list_members(
        &self
    ) -> ClientApiResult<Vec<d_engine_proto::server::cluster::NodeMeta>> {
        let client_inner = self.client_inner.load();
        Ok(client_inner.pool.get_all_members())
    }

    async fn get_leader_id(&self) -> ClientApiResult<Option<u32>> {
        let client_inner = self.client_inner.load();
        Ok(client_inner.pool.get_leader_id())
    }
}
