//! # d-engine-client
//!
//! Client library for interacting with d-engine Raft clusters via gRPC
//!
//! ## ⚠️ You Probably Don't Need This Crate
//!
//! **Use [`d-engine`](https://crates.io/crates/d-engine) instead:**
//!
//! ```toml
//! [dependencies]
//! d-engine = { version = "0.2", features = ["client"] }
//! ```
//!
//! This provides the same API with simpler dependency management. The `d-engine-client` crate
//! is automatically included when you enable the `client` feature.
//!
//! ## For Contributors
//!
//! This crate exists for architectural reasons:
//! - Clean boundaries between client and server
//! - Faster builds during development
//! - Isolated client testing
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use d_engine_client::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::connect(vec!["http://localhost:50051"]).await?;
//!
//!     // Write data
//!     client.put(b"key".to_vec(), b"value".to_vec()).await?;
//!
//!     // Read data
//!     if let Some(value) = client.get(b"key".to_vec()).await? {
//!         println!("Value: {:?}", value);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Read Consistency
//!
//! Choose consistency level based on your needs:
//!
//! - `get_linearizable()` - Strong consistency (read from Leader)
//! - `get_eventual()` - Fast local reads (stale OK)
//! - `get_lease()` - Optimized with leader lease
//!
//! ## Features
//!
//! This crate provides:
//! - [`Client`] - Main entry point with cluster access
//! - [`ClientBuilder`] - Configurable client construction
//! - [`ClientApi`] - Client operations trait
//! - [`ClusterClient`] - Cluster management operations
//!
//! ## Documentation
//!
//! For comprehensive guides:
//! - [Read Consistency](https://docs.rs/d-engine/latest/d_engine/docs/client_guide/read_consistency/index.html)
//! - [Error Handling](https://docs.rs/d-engine/latest/d_engine/docs/client_guide/error_handling/index.html)

mod builder;
mod config;
mod grpc_client;
mod pool;
mod proto;
mod scoped_timer;
mod utils;

pub use builder::*;
pub use config::*;
pub use d_engine_core::client::{ClientApi, ClientApiError, ClientApiResult};
pub use grpc_client::*;
pub use pool::*;
pub use utils::*;

// ==================== Protocol Types (Essential for Public API) ====================

/// Protocol types needed for client operations
///
/// These types are used in the public API and must be imported for client usage:
/// - `ClientResult`: Response type from read operations
/// - `ReadConsistencyPolicy`: Consistency guarantees for reads
/// - `WriteCommand`: Write operation specifications
pub mod protocol {
    pub use d_engine_proto::client::ClientResult;
    pub use d_engine_proto::client::ReadConsistencyPolicy;
    pub use d_engine_proto::client::WatchEventType;
    pub use d_engine_proto::client::WatchRequest;
    pub use d_engine_proto::client::WatchResponse;
    pub use d_engine_proto::client::WriteCommand;
}

/// Cluster management protocol types
///
/// Types required for cluster administration operations:
/// - `NodeMeta`: Cluster node metadata
/// - `NodeStatus`: Node status enumeration
pub mod cluster_types {
    pub use d_engine_proto::common::NodeStatus;
    pub use d_engine_proto::server::cluster::NodeMeta;
}

// ==================== Hide Implementation Details ====================
pub(crate) use proto::*;

#[cfg(test)]
mod error_test;
#[cfg(test)]
mod grpc_client_test;
#[cfg(test)]
mod mock_rpc;
#[cfg(test)]
mod mock_rpc_service;
#[cfg(test)]
mod pool_test;
#[cfg(test)]
mod utils_test;

/// Main entry point for interacting with the d_engine cluster
///
/// Manages connections and provides access to specialized clients:
/// - Use [`kv()`](Client::kv) for data operations
/// - Use [`cluster()`](Client::cluster) for cluster administration
///
/// Created through the [`builder()`](Client::builder) method
#[derive(Clone)]
pub struct Client {
    inner: std::sync::Arc<GrpcClient>,
}

#[derive(Clone)]
pub struct ClientInner {
    pool: ConnectionPool,
    client_id: u32,
    config: ClientConfig,
    endpoints: Vec<String>,
}

impl std::ops::Deref for Client {
    type Target = GrpcClient;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Client {
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
        let old_inner = self.inner.client_inner.load();
        let config = old_inner.config.clone();
        let endpoints = new_endpoints.unwrap_or(old_inner.endpoints.clone());

        let new_pool = ConnectionPool::create(endpoints.clone(), config.clone()).await?;

        let new_inner = std::sync::Arc::new(ClientInner {
            pool: new_pool,
            client_id: old_inner.client_id,
            config,
            endpoints,
        });

        self.inner.client_inner.store(new_inner);
        Ok(())
    }
}
