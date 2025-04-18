use std::sync::Arc;

use arc_swap::ArcSwap;
use log::debug;
use log::error;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

use super::ClientInner;
use crate::proto::rpc_service_client::RpcServiceClient;
use crate::proto::ClientCommand;
use crate::proto::ClientProposeRequest;
use crate::proto::ClientReadRequest;
use crate::proto::ClientResult;
use crate::proto::ErrorCode;
use crate::ClientApiError;

/// Key-value store client interface
///
/// Implements CRUD operations with configurable consistency levels.
/// All write operations use strong consistency.
#[derive(Clone)]
pub struct KvClient {
    pub(super) client_inner: Arc<ArcSwap<ClientInner>>,
}

impl KvClient {
    pub(crate) fn new(client_inner: Arc<ArcSwap<ClientInner>>) -> Self {
        Self { client_inner }
    }

    // Stores a value with strong consistency
    ///
    /// # Errors
    /// - [`Error::FailedToSendWriteRequestError`] on network failures
    /// - [`Error::InvalidResponse`] for malformed server responses
    pub async fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> std::result::Result<(), ClientApiError> {
        let client_inner = self.client_inner.load();

        // Build request
        let mut commands = Vec::new();
        let client_command_insert = ClientCommand::insert(key, value);
        commands.push(client_command_insert);

        let request = ClientProposeRequest {
            client_id: client_inner.client_id,
            commands,
        };

        let mut client = self.make_leader_client().await?;
        // Send write request
        match client.handle_client_propose(request).await {
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

    /// Deletes a key with strong consistency guarantees
    ///
    /// Permanently removes the specified key and its associated value from the store.
    ///
    /// # Parameters
    /// - `key`: The byte-serialized key to delete. Supports any type implementing `AsRef<[u8]>`
    ///   (e.g. `String`, `&str`, `Vec<u8>`)
    ///
    /// # Errors
    /// - [`Error::FailedToSendWriteRequestError`] if unable to reach the leader node
    /// - [`Error::InvalidResponse`] for malformed server responses
    pub async fn delete(
        &self,
        key: impl AsRef<[u8]>,
    ) -> std::result::Result<(), ClientApiError> {
        let client_inner = self.client_inner.load();
        // Build request
        let mut commands = Vec::new();
        let client_command_insert = ClientCommand::delete(key);
        commands.push(client_command_insert);

        let request = ClientProposeRequest {
            client_id: client_inner.client_id,
            commands,
        };

        let mut client = self.make_leader_client().await?;

        // Send delete request
        match client.handle_client_propose(request).await {
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

    /// Retrieves a single key's value from the cluster
    ///
    /// # Parameters
    /// - `key`: The key to retrieve, accepts any byte slice compatible type
    /// - `linear`: Whether to use linearizable read consistency
    ///
    /// # Returns
    /// - `Ok(Some(ClientResult))` if key exists
    /// - `Ok(None)` if key not found
    /// - `Err` on network failures or invalid responses
    pub async fn get(
        &self,
        key: impl AsRef<[u8]>,
        linear: bool,
    ) -> std::result::Result<Option<ClientResult>, ClientApiError> {
        // Delegate to multi-get implementation
        let mut results = self.get_multi(std::iter::once(key), linear).await?;

        // Extract single result (safe due to single-key input)
        Ok(results.pop().unwrap_or(None))
    }
    /// Fetches values for multiple keys from the cluster
    ///
    /// # Parameters
    /// - `keys`: Iterable collection of keys to fetch
    /// - `linear`: Whether to use linearizable read consistency
    ///
    /// # Returns
    /// Ordered list of results matching input keys. Missing keys return `None`.
    ///
    /// # Errors
    /// - `Error::EmptyKeys` if no keys provided
    /// - `Error::FailedToSendReadRequestError` on network failures
    pub async fn get_multi(
        &self,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
        linear: bool,
    ) -> std::result::Result<Vec<Option<ClientResult>>, ClientApiError> {
        let client_inner = self.client_inner.load();
        // Convert keys to commands
        let commands: Vec<ClientCommand> = keys.into_iter().map(|k| ClientCommand::get(k.as_ref())).collect();

        // Validate at least one key
        if commands.is_empty() {
            return Err(ErrorCode::InvalidRequest.into());
        }

        // Select client based on consistency level
        let mut client = if linear {
            self.make_leader_client().await?
        } else {
            self.make_client().await?
        };

        // Build request
        let request = ClientReadRequest {
            client_id: client_inner.client_id,
            linear,
            commands,
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

    async fn make_leader_client(&self) -> std::result::Result<RpcServiceClient<Channel>, ClientApiError> {
        let client_inner = self.client_inner.load();

        let channel = client_inner.pool.get_leader();
        let mut client = RpcServiceClient::new(channel);
        if client_inner.pool.config.enable_compression {
            client = client
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
        }

        Ok(client)
    }

    async fn make_client(&self) -> std::result::Result<RpcServiceClient<Channel>, ClientApiError> {
        let client_inner = self.client_inner.load();

        // Balance from read clients
        let mut rng = StdRng::from_entropy();
        let channels = client_inner.pool.get_all_channels();
        let i = rng.gen_range(0..channels.len());

        let mut client = RpcServiceClient::new(channels[i].clone());

        if client_inner.pool.config.enable_compression {
            client = client
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
        }

        Ok(client)
    }
}
