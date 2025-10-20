#[cfg(test)]
use mockall::automock;
use tonic::async_trait;
use tonic::transport::Channel;

use crate::Result;
use crate::TypeConfig;
use d_engine_proto::common::MembershipChange;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::NodeMeta;

// Add connection type management in RpcPeerChannels
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ConnectionType {
    Control, // Used for key operations such as elections/heartbeats
    Data,    // Used for log replication
    Bulk,    // Used for high-traffic operations such as snapshot transmission
}
impl ConnectionType {
    pub(crate) fn all() -> Vec<ConnectionType> {
        vec![
            ConnectionType::Control,
            ConnectionType::Data,
            ConnectionType::Bulk,
        ]
    }
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait Membership<T>: Sync + Send + 'static
where
    T: TypeConfig,
{
    /// All nodes (including itself)
    #[allow(unused)]
    async fn members(&self) -> Vec<NodeMeta>;

    /// All non-self nodes (including Syncing and Active)
    /// Note:
    /// Joining node has not start its Raft event processing engine yet.
    async fn replication_peers(&self) -> Vec<NodeMeta>;

    /// All non-self nodes in Active state
    async fn voters(&self) -> Vec<NodeMeta>;

    /// All pending active nodes in Active state
    #[allow(unused)]
    async fn nodes_with_status(
        &self,
        status: NodeStatus,
    ) -> Vec<NodeMeta>;

    async fn get_node_status(
        &self,
        node_id: u32,
    ) -> Option<NodeStatus>;

    async fn check_cluster_is_ready(&self) -> Result<()>;

    async fn get_peers_id_with_condition<F>(
        &self,
        condition: F,
    ) -> Vec<u32>
    where
        F: Fn(i32) -> bool + Send + Sync + 'static;

    async fn mark_leader_id(
        &self,
        leader_id: u32,
    ) -> Result<()>;

    async fn current_leader_id(&self) -> Option<u32>;

    /// Reset old leader to follower
    async fn reset_leader(&self) -> Result<()>;

    /// If node role not found return Error
    async fn update_node_role(
        &self,
        node_id: u32,
        new_role: i32,
    ) -> Result<()>;

    /// retrieve latest cluster membership
    async fn retrieve_cluster_membership_config(&self) -> ClusterMembership;

    /// invoked when receive requests from Leader
    async fn update_cluster_conf_from_leader(
        &self,
        my_id: u32,
        my_current_term: u64,
        current_conf_version: u64,
        current_leader_id: Option<u32>,
        cluster_conf_change_req: &ClusterConfChangeRequest,
    ) -> Result<ClusterConfUpdateResponse>;

    async fn get_cluster_conf_version(&self) -> u64;

    async fn update_conf_version(
        &self,
        version: u64,
    );

    #[allow(unused)]
    async fn incr_conf_version(&self);

    /// Add a new node as a learner
    async fn add_learner(
        &self,
        node_id: u32,
        address: String,
    ) -> Result<()>;

    /// Activate node
    #[allow(unused)]
    async fn activate_node(
        &mut self,
        new_node_id: u32,
    ) -> Result<()>;

    /// Update status of a node
    async fn update_node_status(
        &self,
        node_id: u32,
        status: NodeStatus,
    ) -> Result<()>;

    /// Check if the node already exists
    async fn contains_node(
        &self,
        node_id: u32,
    ) -> bool;

    async fn retrieve_node_meta(
        &self,
        node_id: u32,
    ) -> Option<NodeMeta>;

    /// Elegantly remove nodes
    async fn remove_node(
        &self,
        node_id: u32,
    ) -> Result<()>;

    /// Forcefully remove faulty nodes
    #[allow(unused)]
    async fn force_remove_node(
        &self,
        node_id: u32,
    ) -> Result<()>;

    /// Get all node status
    #[allow(unused)]
    async fn get_all_nodes(&self) -> Vec<NodeMeta>;

    /// Pre-warms connection cache for all replication peers
    async fn pre_warm_connections(&self) -> Result<()>;

    async fn get_peer_channel(
        &self,
        node_id: u32,
        conn_type: ConnectionType,
    ) -> Option<Channel>;

    async fn get_address(
        &self,
        node_id: u32,
    ) -> Option<String>;

    /// Apply committed configuration change
    async fn apply_config_change(
        &self,
        change: MembershipChange,
    ) -> Result<()>;

    async fn notify_config_applied(
        &self,
        index: u64,
    );

    async fn get_zombie_candidates(&self) -> Vec<u32>;

    /// If new node could rejoin the cluster again
    async fn can_rejoin(
        &self,
        node_id: u32,
        role: i32,
    ) -> Result<()>;
}
