//! Distributed consensus client implementation
//!
//! Contains the primary interface [`Client`] that combines:
//! - Key-value store operations through [`KvClient`]
//! - Cluster management via [`ClusterClient`]
//!
//! Manages connection pooling and request routing to cluster nodes.

use std::sync::Arc;

use arc_swap::ArcSwap;

use super::ClientApiError;
use super::ClientBuilder;
use super::ClientConfig;
use super::ClusterClient;
use super::ConnectionPool;
use super::KvClient;

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
