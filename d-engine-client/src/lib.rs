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
//! use d_engine_client::Client;
//! use d_engine_client::ClientBuilder;
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
//!     let value = client.kv().get("user:1001").await.unwrap();
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
mod grpc_kv_client;
mod pool;
mod proto;
mod scoped_timer;
mod utils;

pub use builder::*;
pub use cluster::*;
pub use config::*;
pub use error::*;
pub use grpc_kv_client::*;
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
    pub use d_engine_proto::client::{ClientResult, ReadConsistencyPolicy, WriteCommand};
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
mod cluster_test;
#[cfg(test)]
mod kv_test;
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
    /// Key-value store client interface
    kv: GrpcKvClient,

    /// Cluster management client interface
    cluster: ClusterClient,

    inner: std::sync::Arc<arc_swap::ArcSwap<ClientInner>>,
}

#[derive(Clone)]
pub struct ClientInner {
    pool: ConnectionPool,
    client_id: u32,
    config: ClientConfig,
    endpoints: Vec<String>,
}

impl Client {
    /// Access the key-value operations client
    ///
    /// # Examples
    /// ```rust,ignore
    /// client.kv().put("key", "value").await?;
    /// ```
    pub fn kv(&self) -> &GrpcKvClient {
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

        let new_inner = std::sync::Arc::new(ClientInner {
            pool: new_pool,
            client_id: old_inner.client_id,
            config,
            endpoints,
        });

        self.inner.store(new_inner);
        Ok(())
    }
}
