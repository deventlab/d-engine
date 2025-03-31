use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use crate::grpc::rpc_service::NodeMeta;
use crate::Error;
use crate::Result;
use crate::FOLLOWER;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterConfig {
    #[serde(default = "default_node_id")]
    pub node_id: u32,

    #[serde(default = "default_listen_addr")]
    pub listen_address: SocketAddr,

    #[serde(default = "default_initial_cluster")]
    pub initial_cluster: Vec<NodeMeta>,

    #[serde(default = "default_db_dir")]
    pub db_root_dir: PathBuf,

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
            return Err(Error::InvalidConfig(
                "node_id cannot be 0 (reserved for invalid nodes)".into(),
            ));
        }

        // Validate cluster membership
        if self.initial_cluster.is_empty() {
            return Err(Error::InvalidConfig(
                "initial_cluster must contain at least one node".into(),
            ));
        }

        // Check node existence in cluster
        let self_in_cluster = self.initial_cluster.iter().any(|n| n.id == self.node_id);
        if !self_in_cluster {
            return Err(Error::InvalidConfig(format!(
                "Current node {} not found in initial_cluster",
                self.node_id
            )));
        }

        // Check unique node IDs
        let mut ids = std::collections::HashSet::new();
        for node in &self.initial_cluster {
            if !ids.insert(node.id) {
                return Err(Error::InvalidConfig(format!(
                    "Duplicate node_id {} in initial_cluster",
                    node.id
                )));
            }
        }

        // Validate network configuration
        if self.listen_address.port() == 0 {
            return Err(Error::InvalidConfig(
                "listen_address must specify a non-zero port".into(),
            ));
        }

        // Validate storage paths
        self.validate_directory(&self.db_root_dir, "db_root_dir")?;
        self.validate_directory(&self.log_dir, "log_dir")?;

        Ok(())
    }

    /// Ensures directory path is valid and writable
    fn validate_directory(
        &self,
        path: &PathBuf,
        name: &str,
    ) -> Result<()> {
        if path.as_os_str().is_empty() {
            return Err(Error::InvalidConfig(format!("{} path cannot be empty", name)));
        }

        #[cfg(not(test))]
        {
            use std::fs;
            // Check directory existence or create ability
            if !path.exists() {
                fs::create_dir_all(path).map_err(|e| {
                    Error::InvalidConfig(format!(
                        "Failed to create {} directory at {}: {}",
                        name,
                        path.display(),
                        e
                    ))
                })?;
            }

            // Check write permissions
            let test_file = path.join(".permission_test");
            fs::write(&test_file, b"test").map_err(|e| {
                Error::InvalidConfig(format!(
                    "No write permission in {} directory {}: {}",
                    name,
                    path.display(),
                    e
                ))
            })?;
            fs::remove_file(&test_file).ok();
        }

        Ok(())
    }
}

fn default_node_id() -> u32 {
    1
}
fn default_initial_cluster() -> Vec<NodeMeta> {
    vec![NodeMeta {
        id: 1,
        ip: "127.0.0.1".to_string(),
        port: 8080,
        role: FOLLOWER,
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
