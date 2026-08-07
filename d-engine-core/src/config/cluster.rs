use std::net::SocketAddr;

use config::ConfigError;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::NodeMeta;
use serde::Deserialize;
use serde::Serialize;

use crate::Error;
use crate::Result;

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
    /// Default: `default_initial_cluster()`
    ///
    /// Accepts either the full form (`{id, address, role, status}`) or the
    /// common shorthand (`{id, address}` — role/status default to
    /// Follower/Active). Both forms can be mixed in the same list.
    ///
    /// # Note
    /// Should contain at least 3 nodes for production deployment
    #[serde(
        default = "default_initial_cluster",
        deserialize_with = "deserialize_initial_cluster"
    )]
    pub initial_cluster: Vec<NodeMeta>,
}
impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: default_node_id(),
            listen_address: default_listen_addr(),
            initial_cluster: default_initial_cluster(),
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

        // Check unique listen addresses. Left unchecked, a copy-paste mistake across a
        // multi-host deployment fails silently: each node binds its own address successfully,
        // and the cluster just never elects a leader instead of erroring at startup.
        let mut addresses = std::collections::HashSet::new();
        for node in &self.initial_cluster {
            if !addresses.insert(&node.address) {
                return Err(Error::Config(ConfigError::Message(format!(
                    "Duplicate listen_address {:?} in initial_cluster",
                    node.address
                ))));
            }
        }

        // Validate network configuration
        if self.listen_address.port() == 0 {
            return Err(Error::Config(ConfigError::Message(
                "listen_address must specify a non-zero port".into(),
            )));
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
        address: "127.0.0.1:8080".to_string(),
        role: Follower as i32,
        status: NodeStatus::Active.into(),
    }]
}
fn default_listen_addr() -> SocketAddr {
    "127.0.0.1:9081".parse().unwrap()
}

/// Accepts either the full `NodeMeta` shape (`{id, address, role, status}`)
/// or the common shorthand (`{id, address}`) — `role`/`status` default to
/// Follower/Active when omitted. Only this one field's deserialization is
/// affected; `NodeMeta` itself is untouched (it's a proto wire type reused
/// elsewhere — e.g. membership changes — where role/status are genuinely
/// required, not defaultable).
fn deserialize_initial_cluster<'de, D>(
    deserializer: D
) -> std::result::Result<Vec<NodeMeta>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct NodeMetaInput {
        id: u32,
        address: String,
        #[serde(default = "default_role_i32")]
        role: i32,
        #[serde(default = "default_status_i32")]
        status: i32,
    }

    let inputs = Vec::<NodeMetaInput>::deserialize(deserializer)?;
    Ok(inputs
        .into_iter()
        .map(|n| NodeMeta {
            id: n.id,
            address: n.address,
            role: n.role,
            status: n.status,
        })
        .collect())
}

fn default_role_i32() -> i32 {
    Follower as i32
}
fn default_status_i32() -> i32 {
    NodeStatus::Active.into()
}
