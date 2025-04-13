use arc_swap::ArcSwap;
use log::debug;
use log::error;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use std::sync::Arc;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::Code;

use super::ClientInner;
use crate::proto::rpc_service_client::RpcServiceClient;
use crate::proto::ClientCommand;
use crate::proto::ClientProposeRequest;
use crate::proto::ClientReadRequest;
use crate::proto::ClientRequestError;
use crate::proto::ClientResponse;
use crate::proto::ClientResult;
use crate::Error;
use crate::Result;

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
    ) -> Result<()> {
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
                match response.get_ref() {
                    ClientResponse { error_code, result: _ } => {
                        if matches!(
                            ClientRequestError::try_from(*error_code).unwrap_or(ClientRequestError::NoError),
                            ClientRequestError::NoError
                        ) {
                            return Ok(());
                        } else {
                            error!("handle_client_propose error_code:{:?}", error_code);
                        }
                    }
                }
            }
            Err(status) => {
                error!("[:KvClient:write] status: {:?}", status);
            }
        }
        Err(Error::FailedToSendWriteRequestError)
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
    ) -> Result<()> {
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

                return Ok(());
            }
            Err(status) => {
                error!("[:KvClient:delete] status: {:?}", status);
            }
        }
        Err(Error::FailedToSendWriteRequestError)
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
    ) -> Result<Option<ClientResult>> {
        // Delegate to multi-get implementation
        let mut results = self.get_multi(std::iter::once(key), linear).await?;

        // Extract single result (safe due to single-key input)
        results.pop().ok_or_else(|| {
            error!("Internal error: empty results from single-key read");
            Error::InvalidResponse
        })
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
    ) -> Result<Vec<Option<ClientResult>>> {
        let client_inner = self.client_inner.load();
        // Convert keys to commands
        let commands: Vec<ClientCommand> = keys.into_iter().map(|k| ClientCommand::get(k.as_ref())).collect();

        // Validate at least one key
        if commands.is_empty() {
            return Err(Error::EmptyKeys);
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
                match status.code() {
                    Code::PermissionDenied => Err(Error::NodeIsNotLeaderError),
                    Code::Cancelled => Err(Error::ClientRequestCanceledError),
                    _ => Err(Error::FailedToSendReadRequestError),
                }
            }
        }
    }

    async fn make_leader_client(&self) -> Result<RpcServiceClient<Channel>> {
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

    async fn make_client(&self) -> Result<RpcServiceClient<Channel>> {
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
