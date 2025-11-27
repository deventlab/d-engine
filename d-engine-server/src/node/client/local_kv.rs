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

use d_engine_core::{KvClient, KvClientError, KvResult, MaybeCloneOneshot, RaftEvent, RaftOneshot};
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
            return Err(LocalClientError::ServerError(format!(
                "Error code: {}",
                response.error
            )));
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
            return Err(LocalClientError::ServerError(format!(
                "Error code: {}",
                response.error
            )));
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
            return Err(LocalClientError::ServerError(format!(
                "Error code: {}",
                response.error
            )));
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
            return Err(KvClientError::ServerError(format!(
                "Error code: {}",
                response.error
            )));
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
            return Err(KvClientError::ServerError(format!(
                "Error code: {}",
                response.error
            )));
        }

        match response.success_result {
            Some(d_engine_proto::client::client_response::SuccessResult::ReadData(
                read_results,
            )) => {
                // Map results by position: if result exists, return value (even if empty)
                Ok(read_results.results.into_iter().map(|r| Some(r.value)).collect())
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
