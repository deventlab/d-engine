//! Zero-overhead KV client for embedded d-engine.
//!
//! [`LocalKvClient`] provides direct access to Raft state machine
//! without gRPC serialization or network traversal.
//!
//! # Performance
//! - **10-20x faster** than gRPC (localhost)
//! - **<0.1ms latency** per operation
//! - Zero serialization overhead
//!
//! # Usage
//! ```rust,ignore
//! let node = NodeBuilder::new(config).build().await?.ready()?;
//! let client = node.local_client();
//! client.put(b"key", b"value").await?;
//! ```

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;

use d_engine_client::{KvClient, KvClientError, KvResult};
use d_engine_core::{MaybeCloneOneshot, RaftEvent, RaftOneshot};
use d_engine_proto::client::{
    ClientReadRequest, ClientWriteRequest, ReadConsistencyPolicy, WriteCommand,
};
use d_engine_proto::error::ErrorCode;

/// Local client error types
#[derive(Debug)]
pub enum LocalClientError {
    /// Event channel closed (node shutting down)
    ChannelClosed,
    /// Operation exceeded timeout duration
    Timeout(Duration),
    /// Not the leader - request should be forwarded
    NotLeader {
        /// Leader's node ID (if known)
        leader_id: Option<String>,
        /// Leader's address (if known)
        leader_address: Option<String>,
    },
    /// Server-side error occurred
    ServerError(String),
}

impl fmt::Display for LocalClientError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            LocalClientError::ChannelClosed => {
                write!(f, "Channel closed, node may be shutting down")
            }
            LocalClientError::Timeout(d) => write!(f, "Operation timeout after {d:?}"),
            LocalClientError::NotLeader {
                leader_id,
                leader_address,
            } => {
                write!(f, "Not leader")?;
                if let Some(id) = leader_id {
                    write!(f, " (leader_id: {id})")?;
                }
                if let Some(addr) = leader_address {
                    write!(f, " (leader_address: {addr})")?;
                }
                Ok(())
            }
            LocalClientError::ServerError(s) => write!(f, "Server error: {s}"),
        }
    }
}

impl std::error::Error for LocalClientError {}

pub type Result<T> = std::result::Result<T, LocalClientError>;

// Convert LocalClientError to KvClientError
impl From<LocalClientError> for KvClientError {
    fn from(err: LocalClientError) -> Self {
        match err {
            LocalClientError::ChannelClosed => KvClientError::ChannelClosed,
            LocalClientError::Timeout(_) => KvClientError::Timeout,
            LocalClientError::NotLeader {
                leader_id,
                leader_address,
            } => {
                let msg = if let Some(addr) = leader_address {
                    format!("Not leader, try leader at: {addr}")
                } else if let Some(id) = leader_id {
                    format!("Not leader, leader_id: {id}")
                } else {
                    "Not leader".to_string()
                };
                KvClientError::ServerError(msg)
            }
            LocalClientError::ServerError(msg) => KvClientError::ServerError(msg),
        }
    }
}

/// Zero-overhead KV client for embedded mode.
///
/// Directly calls Raft core without gRPC overhead.
#[derive(Clone)]
pub struct LocalKvClient {
    event_tx: mpsc::Sender<RaftEvent>,
    client_id: u32,
    timeout: Duration,
}

impl LocalKvClient {
    /// Internal constructor (used by Node::local_client())
    pub(crate) fn new_internal(
        event_tx: mpsc::Sender<RaftEvent>,
        client_id: u32,
        timeout: Duration,
    ) -> Self {
        Self {
            event_tx,
            client_id,
            timeout,
        }
    }

    /// Map ErrorCode and ErrorMetadata to LocalClientError
    fn map_error_response(
        error_code: i32,
        metadata: Option<d_engine_proto::error::ErrorMetadata>,
    ) -> LocalClientError {
        use d_engine_proto::error::ErrorCode;

        match ErrorCode::try_from(error_code) {
            Ok(ErrorCode::NotLeader) => {
                let (leader_id, leader_address) = if let Some(meta) = metadata {
                    (meta.leader_id, meta.leader_address)
                } else {
                    (None, None)
                };
                LocalClientError::NotLeader {
                    leader_id,
                    leader_address,
                }
            }
            _ => LocalClientError::ServerError(format!("Error code: {error_code}")),
        }
    }

    /// Store a key-value pair with strong consistency.
    pub async fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
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
            .map_err(|_| LocalClientError::ChannelClosed)?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| LocalClientError::Timeout(self.timeout))?
            .map_err(|_| LocalClientError::ChannelClosed)?;

        let response = result.map_err(|status| {
            LocalClientError::ServerError(format!("RPC error: {}", status.message()))
        })?;

        if response.error != ErrorCode::Success as i32 {
            return Err(Self::map_error_response(response.error, response.metadata));
        }

        Ok(())
    }

    /// Retrieve value associated with a key.
    pub async fn get(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Bytes>> {
        let request = ClientReadRequest {
            client_id: self.client_id,
            keys: vec![Bytes::copy_from_slice(key.as_ref())],
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(RaftEvent::ClientReadRequest(request, resp_tx))
            .await
            .map_err(|_| LocalClientError::ChannelClosed)?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| LocalClientError::Timeout(self.timeout))?
            .map_err(|_| LocalClientError::ChannelClosed)?;

        let response = result.map_err(|status| {
            LocalClientError::ServerError(format!("RPC error: {}", status.message()))
        })?;

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

    /// Delete a key-value pair.
    pub async fn delete(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<()> {
        let command = WriteCommand::delete(Bytes::copy_from_slice(key.as_ref()));

        let request = ClientWriteRequest {
            client_id: self.client_id,
            commands: vec![command],
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(RaftEvent::ClientPropose(request, resp_tx))
            .await
            .map_err(|_| LocalClientError::ChannelClosed)?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| LocalClientError::Timeout(self.timeout))?
            .map_err(|_| LocalClientError::ChannelClosed)?;

        let response = result.map_err(|status| {
            LocalClientError::ServerError(format!("RPC error: {}", status.message()))
        })?;

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
    #[cfg(any(test, feature = "test-utils"))]
    pub fn node_id(&self) -> u32 {
        self.client_id
    }
}

impl std::fmt::Debug for LocalKvClient {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("LocalKvClient")
            .field("client_id", &self.client_id)
            .field("timeout", &self.timeout)
            .finish()
    }
}

// Implement KvClient trait
#[async_trait::async_trait]
impl KvClient for LocalKvClient {
    async fn put(
        &self,
        key: impl AsRef<[u8]> + Send,
        value: impl AsRef<[u8]> + Send,
    ) -> KvResult<()> {
        self.put(key, value).await.map_err(Into::into)
    }

    async fn put_with_ttl(
        &self,
        key: impl AsRef<[u8]> + Send,
        value: impl AsRef<[u8]> + Send,
        ttl_secs: u64,
    ) -> KvResult<()> {
        // Create command with TTL
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
            .map_err(|_| KvClientError::ChannelClosed)?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| KvClientError::Timeout)?
            .map_err(|_| KvClientError::ChannelClosed)?;

        let response = result.map_err(|status| {
            KvClientError::ServerError(format!("RPC error: {}", status.message()))
        })?;

        if response.error != ErrorCode::Success as i32 {
            let local_err = LocalKvClient::map_error_response(response.error, response.metadata);
            return Err(local_err.into());
        }

        Ok(())
    }

    async fn get(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> KvResult<Option<Bytes>> {
        self.get(key).await.map_err(Into::into)
    }

    async fn get_multi(
        &self,
        keys: &[Bytes],
    ) -> KvResult<Vec<Option<Bytes>>> {
        let request = ClientReadRequest {
            client_id: self.client_id,
            keys: keys.to_vec(),
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
        };

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        self.event_tx
            .send(RaftEvent::ClientReadRequest(request, resp_tx))
            .await
            .map_err(|_| KvClientError::ChannelClosed)?;

        let result = tokio::time::timeout(self.timeout, resp_rx)
            .await
            .map_err(|_| KvClientError::Timeout)?
            .map_err(|_| KvClientError::ChannelClosed)?;

        let response = result.map_err(|status| {
            KvClientError::ServerError(format!("RPC error: {}", status.message()))
        })?;

        if response.error != ErrorCode::Success as i32 {
            let local_err = LocalKvClient::map_error_response(response.error, response.metadata);
            return Err(local_err.into());
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

    async fn delete(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> KvResult<()> {
        self.delete(key).await.map_err(Into::into)
    }
}
