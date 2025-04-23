//! Client module for distributed consensus system
//!
//! Provides core components for interacting with the d_engine cluster:
//! - [`Client`] - Main entry point with cluster access
//! - [`ClientBuilder`] - Configurable client construction
//! - [`KvClient`] - Key-value store operations
//! - [`ClusterClient`] - Cluster management operations
//! - [`ConnectionPool`] - Underlying connection management
//!
//! # Basic Usage
//! ```no_run
//! use d_engine::client::{Client, ClientBuilder};
//! use std::time::Duration;
//! use core::error::Error;
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main(){
//!     // Initialize client with automatic cluster discovery
//!     let client = Client::builder(vec![
//!         "http://node1:9081".into(),
//!         "http://node2:9082".into()
//!     ])
//!     .connect_timeout(Duration::from_secs(3))
//!     .request_timeout(Duration::from_secs(1))
//!     .enable_compression(true)
//!     .build()
//!     .await
//!     .unwrap();
//!
//!     // Execute key-value operations
//!     client.kv().put("user:1001", "Alice").await.unwrap();
//!
//!     let value = client.kv().get("user:1001", false).await.unwrap();
//!
//!     println!("User data: {:?}", value);
//!
//!     // Perform cluster management
//!     let members = client.cluster().list_members().await.unwrap();
//!     println!("Cluster members: {:?}", members);
//!
//! }
//! ```

mod builder;
mod cluster;
mod config;
mod error;
mod kv;
mod pool;

pub use builder::*;
pub use cluster::*;
pub use config::*;
pub use error::*;
pub use kv::*;
pub use pool::*;

#[cfg(test)]
mod pool_test;

use std::sync::Arc;

use arc_swap::ArcSwap;
use tracing::error;

use crate::proto::client_command;
use crate::proto::client_response::SuccessResult;
use crate::proto::ClientCommand;
use crate::proto::ClientResponse;
use crate::proto::ClientResult;
use crate::proto::ErrorCode;
use crate::proto::ReadResults;

/// Main entry point for interacting with the d_engine cluster
///
/// Manages connections and provides access to specialized clients:
/// - Use [`kv()`](Client::kv) for data operations
/// - Use [`cluster()`](Client::cluster) for cluster administration
///
/// Created through the [`builder()`](Client::builder) method
#[derive(Clone)]
pub struct Client {
    /// Key-value store client interface
    pub(super) kv: KvClient,

    /// Cluster management client interface
    pub(super) cluster: ClusterClient,

    pub(super) inner: Arc<ArcSwap<ClientInner>>,
}

#[derive(Clone)]
pub struct ClientInner {
    pub(super) pool: ConnectionPool,
    pub(super) client_id: u32,
    pub(super) config: ClientConfig,
    pub(super) endpoints: Vec<String>,
}

impl Client {
    /// Access the key-value operations client
    ///
    /// # Examples
    /// ```rust,ignore
    /// client.kv().put("key", "value").await?;
    /// ```
    pub fn kv(&self) -> &KvClient {
        &self.kv
    }

    /// Access the cluster management client
    ///
    /// # Examples
    /// ```rust,ignore
    /// client.cluster().add_node("node3:9083").await?;
    /// ```
    pub fn cluster(&self) -> &ClusterClient {
        &self.cluster
    }

    /// Create a configured client builder
    ///
    /// Starts client construction process with specified bootstrap endpoints.
    /// Chain configuration methods before calling
    /// [`build()`](ClientBuilder::build).
    ///
    /// # Arguments
    /// * `endpoints` - Initial cluster nodes for discovery
    ///
    /// # Panics
    /// Will panic if no valid endpoints provided
    pub fn builder(endpoints: Vec<String>) -> ClientBuilder {
        assert!(!endpoints.is_empty(), "At least one endpoint required");
        ClientBuilder::new(endpoints)
    }

    pub async fn refresh(
        &mut self,
        new_endpoints: Option<Vec<String>>,
    ) -> std::result::Result<(), ClientApiError> {
        // Get a writable lock
        let old_inner = self.inner.load();
        let config = old_inner.config.clone();
        let endpoints = new_endpoints.unwrap_or(old_inner.endpoints.clone());

        let new_pool = ConnectionPool::create(endpoints.clone(), config.clone()).await?;

        let new_inner = Arc::new(ClientInner {
            pool: new_pool,
            client_id: old_inner.client_id,
            config,
            endpoints,
        });

        self.inner.store(new_inner);
        Ok(())
    }
}

impl ClientCommand {
    /// Create read command for specified key
    ///
    /// # Parameters
    /// - `key`: Byte array representing data key
    pub fn get(key: impl AsRef<[u8]>) -> Self {
        Self {
            command: Some(client_command::Command::Get(key.as_ref().to_vec())),
        }
    }

    /// Create write command for key-value pair
    ///
    /// # Parameters
    /// - `key`: Byte array for storage key
    /// - `value`: Byte array to be stored
    pub fn insert(
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Self {
        let insert_cmd = client_command::Insert {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
        };
        Self {
            command: Some(client_command::Command::Insert(insert_cmd)),
        }
    }

    /// Create deletion command for specified key
    ///
    /// # Parameters
    /// - `key`: Byte array of key to delete
    pub fn delete(key: impl AsRef<[u8]>) -> Self {
        Self {
            command: Some(client_command::Command::Delete(key.as_ref().to_vec())),
        }
    }

    /// Create empty operation command for heartbeat detection
    ///
    /// # Usage
    /// Maintains connection activity without data operation
    pub(crate) fn no_op() -> Self {
        Self {
            command: Some(client_command::Command::NoOp(true)),
        }
    }
}
impl ClientResponse {
    /// Build success response for write operations
    ///
    /// # Returns
    /// Response with NoError code and write confirmation
    pub fn write_success() -> Self {
        Self {
            error: ErrorCode::Success as i32,
            success_result: Some(SuccessResult::WriteAck(true)),
            metadata: None,
        }
    }

    /// Build success response for read operations
    ///
    /// # Parameters
    /// - `results`: Vector of retrieved key-value pairs
    pub fn read_results(results: Vec<ClientResult>) -> Self {
        Self {
            error: ErrorCode::Success as i32,
            success_result: Some(SuccessResult::ReadData(ReadResults { results })),
            metadata: None,
        }
    }

    /// Build generic error response for any operation type
    ///
    /// # Parameters
    /// - `error_code`: Predefined client request error code
    pub fn client_error(error_code: ErrorCode) -> Self {
        Self {
            error: error_code as i32,
            success_result: None,
            metadata: None,
        }
    }

    /// Convert response to boolean write result
    ///
    /// # Returns
    /// - `Ok(true)` on successful write
    /// - `Err` with converted error code on failure
    pub fn into_write_result(&self) -> std::result::Result<bool, ClientApiError> {
        self.validate_error()?;
        Ok(match self.success_result {
            Some(SuccessResult::WriteAck(success)) => success,
            _ => false,
        })
    }

    /// Convert response to read results
    ///
    /// # Returns
    /// Vector of optional key-value pairs wrapped in Result
    pub fn into_read_results(&self) -> std::result::Result<Vec<Option<ClientResult>>, ClientApiError> {
        self.validate_error()?;
        match &self.success_result {
            Some(SuccessResult::ReadData(data)) => data
                .results
                .clone()
                .into_iter()
                .map(|item| {
                    Ok(Some(ClientResult {
                        key: item.key.to_vec(),
                        value: item.value.to_vec(),
                    }))
                })
                .collect(),
            _ => {
                error!("Invalid response type for read operation");
                unreachable!()
            }
        }
    }

    /// Validate error code in response header
    ///
    /// # Internal Logic
    /// Converts numeric error code to enum variant
    pub(crate) fn validate_error(&self) -> std::result::Result<(), ClientApiError> {
        match ErrorCode::try_from(self.error).unwrap_or(ErrorCode::Uncategorized) {
            ErrorCode::Success => Ok(()),
            e => Err(e.into()),
        }
    }
}
