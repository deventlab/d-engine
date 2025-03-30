//! Distributed consensus client module
//!
//! Provides high-level abstractions for interacting with the dengine cluster.
//! Contains two main components:
//! - [`KvClient`] for key-value operations
//! - [`ClusterClient`] for cluster management
//!
//! # Examples
//! ```rust,no_run
//! use dengine::{Client, Error};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     // Initialize client with automatic cluster discovery
//!     let client = Client::builder(vec![
//!         "http://node1:9081".into(),
//!         "http://node2:9082".into()
//!     ])
//!     .connect_timeout(Duration::from_secs(3))
//!     .request_timeout(Duration::from_secs(1))
//!     .enable_compression(true)
//!     .build()
//!     .await?;
//!
//!     // Execute key-value operations
//!     client.kv().put("user:1001", "Alice").await?;
//!     let value = client.kv().get("user:1001").await?;
//!     println!("User data: {:?}", value);
//!
//!     // Perform cluster management
//!     let members = client.cluster().list_members().await?;
//!     println!("Cluster members: {:?}", members);
//!
//!     Ok(())
//! }
//! ```

use super::{ClientBuilder, ClusterClient, KvClient};

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
    /// Chain configuration methods before calling [`build()`](ClientBuilder::build).
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
