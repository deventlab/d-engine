//! Distributed consensus client implementation
//!
//! Contains the primary interface [`Client`] that combines:
//! - Key-value store operations through [`KvClient`]
//! - Cluster management via [`ClusterClient`]
//!
//! Manages connection pooling and request routing to cluster nodes.

use super::ClientBuilder;
use super::ClusterClient;
use super::KvClient;

/// Main entry point for interacting with the dengine cluster
///
/// Manages connections and provides access to specialized clients:
/// - Use [`kv()`](Client::kv) for data operations
/// - Use [`cluster()`](Client::cluster) for cluster administration
///
/// Created through the [`builder()`](Client::builder) method
pub struct Client {
    /// Key-value store client interface
    pub kv: KvClient,

    /// Cluster management client interface
    pub cluster: ClusterClient,
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
}
