use std::net::SocketAddr;
use std::path::PathBuf;

use config::ConfigError;
use serde::Deserialize;
use serde::Serialize;

use super::validate_directory;
use crate::Error;
use crate::Result;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::NodeMeta;

/// Cluster node configuration parameters
///
/// Encapsulates all essential settings for cluster node initialization and operation,
/// including network settings, storage paths, and cluster topology.
///
/// # Defaults
/// Configuration can be loaded from file with default values generated via `serde`'s
/// default implementations. Field-level defaults use helper functions prefixed with `default_`.

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterConfig {
    /// Unique node identifier in cluster
    ///
    /// Default: `default_node_id()` (typically 0 for single-node setup)
    #[serde(default = "default_node_id")]
    pub node_id: u32,

    /// Network listening address (IP:PORT)
    ///
    /// Default: `default_listen_addr()` (127.0.0.1:8000)
    #[serde(default = "default_listen_addr")]
    pub listen_address: SocketAddr,

    /// Seed nodes for cluster initialization
    ///
    /// Default: `default_initial_cluster()` (empty vector)
    ///
    /// # Note
    /// Should contain at least 3 nodes for production deployment
    #[serde(default = "default_initial_cluster")]
    pub initial_cluster: Vec<NodeMeta>,

    /// Database storage root directory
    ///
    /// Default: `default_db_dir()` (/tmp/db)
    #[serde(default = "default_db_dir")]
    pub db_root_dir: PathBuf,

    /// Log files output directory
    ///
    /// Default: `default_log_dir()` (./logs)
    #[serde(default = "default_log_dir")]
    pub log_dir: PathBuf,
}
impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: default_node_id(),
            listen_address: default_listen_addr(),
            initial_cluster: vec![],
            db_root_dir: default_db_dir(),
            log_dir: default_log_dir(),
        }
    }
}

impl ClusterConfig {
    /// Validates cluster configuration consistency
    /// # Errors
    /// Returns `Error::InvalidConfig` if any configuration rules are violated
    pub fn validate(&self) -> Result<()> {
        // Validate node identity
        if self.node_id == 0 {
            return Err(Error::Config(ConfigError::Message(
                "node_id cannot be 0 (reserved for invalid nodes)".into(),
            )));
        }

        // Validate cluster membership
        if self.initial_cluster.is_empty() {
            return Err(Error::Config(ConfigError::Message(
                "initial_cluster must contain at least one node".into(),
            )));
        }

        // Check node existence in cluster
        let self_in_cluster = self.initial_cluster.iter().any(|n| n.id == self.node_id);
        if !self_in_cluster {
            return Err(Error::Config(ConfigError::Message(format!(
                "Current node {} not found in initial_cluster",
                self.node_id
            ))));
        }

        // Check unique node IDs
        let mut ids = std::collections::HashSet::new();
        for node in &self.initial_cluster {
            if !ids.insert(node.id) {
                return Err(Error::Config(ConfigError::Message(format!(
                    "Duplicate node_id {} in initial_cluster",
                    node.id
                ))));
            }
        }

        // Validate network configuration
        if self.listen_address.port() == 0 {
            return Err(Error::Config(ConfigError::Message(
                "listen_address must specify a non-zero port".into(),
            )));
        }

        // Validate storage paths
        validate_directory(&self.db_root_dir, "db_root_dir")?;
        validate_directory(&self.log_dir, "log_dir")?;

        Ok(())
    }
}

fn default_node_id() -> u32 {
    1
}
fn default_initial_cluster() -> Vec<NodeMeta> {
    vec![NodeMeta {
        id: 1,
        address: "127.0.0.1:8080".to_string(),
        role: Follower.into(),
        status: NodeStatus::Active.into(),
    }]
}
fn default_listen_addr() -> SocketAddr {
    "127.0.0.1:9081".parse().unwrap()
}
fn default_db_dir() -> PathBuf {
    PathBuf::from("/tmp/db")
}
fn default_log_dir() -> PathBuf {
    PathBuf::from("/tmp/logs")
}
