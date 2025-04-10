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
mod client;
mod cluster;
mod config;
mod kv;
mod pool;

pub use builder::*;
pub use client::*;
pub use cluster::*;
pub use config::*;
pub use kv::*;
use log::error;
pub use pool::*;

#[cfg(test)]
mod pool_test;

//---

use crate::proto::client_command;
use crate::proto::client_response;
use crate::proto::ClientCommand;
use crate::proto::ClientRequestError;
use crate::proto::ClientResponse;
use crate::proto::ClientResult;
use crate::proto::ReadResults;
use crate::Error;
use crate::Result;

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
            error_code: ClientRequestError::NoError as i32,
            result: Some(client_response::Result::WriteResult(true)),
        }
    }
    /// Build error response for write operations
    ///
    /// # Parameters
    /// - `error`: Error type implementing conversion to ClientRequestError
    pub fn write_error(error: Error) -> Self {
        Self {
            error_code: <ClientRequestError as Into<i32>>::into(ClientRequestError::from(error)),
            result: None,
        }
    }

    /// Build success response for read operations
    ///
    /// # Parameters
    /// - `results`: Vector of retrieved key-value pairs
    pub fn read_results(results: Vec<ClientResult>) -> Self {
        Self {
            error_code: ClientRequestError::NoError as i32,
            result: Some(client_response::Result::ReadResults(ReadResults { results })),
        }
    }

    /// Build generic error response for any operation type
    ///
    /// # Parameters
    /// - `error`: Predefined client request error code
    pub fn error(error: ClientRequestError) -> Self {
        Self {
            error_code: error as i32,
            result: None,
        }
    }

    /// Convert response to boolean write result
    ///
    /// # Returns
    /// - `Ok(true)` on successful write
    /// - `Err` with converted error code on failure
    pub fn into_write_result(&self) -> Result<bool> {
        self.validate_error()?;
        Ok(match self.result {
            Some(client_response::Result::WriteResult(success)) => success,
            _ => false,
        })
    }

    /// Convert response to read results
    ///
    /// # Returns
    /// Vector of optional key-value pairs wrapped in Result
    pub fn into_read_results(&self) -> Result<Vec<Option<ClientResult>>> {
        self.validate_error()?;
        match &self.result {
            Some(client_response::Result::ReadResults(read_results)) => read_results
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
                Err(Error::InvalidResponseType)
            }
        }
    }

    /// Validate error code in response header
    ///
    /// # Internal Logic
    /// Converts numeric error code to enum variant
    fn validate_error(&self) -> Result<()> {
        match ClientRequestError::try_from(self.error_code).unwrap_or(ClientRequestError::NoError) {
            ClientRequestError::NoError => Ok(()),
            e => Err(e.into()),
        }
    }
}
