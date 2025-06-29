mod health_checker;
mod raft_membership;
pub use raft_membership::*;

#[cfg(test)]
mod health_checker_test;
#[cfg(test)]
mod raft_membership_test;

use crate::proto::cluster::cluster_conf_update_response::ErrorCode;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::ClusterConfUpdateResponse;
use crate::proto::cluster::ClusterMembership;
use crate::proto::common::NodeStatus;
use crate::Result;
use crate::TypeConfig;
#[cfg(test)]
use mockall::automock;
use tonic::async_trait;
use tonic::transport::Channel;

// Add connection type management in RpcPeerChannels
#[derive(Eq, Hash, PartialEq)]
pub(crate) enum ConnectionType {
    Control, // Used for key operations such as elections/heartbeats
    Data,    // Used for log replication
    Bulk,    // Used for high-traffic operations such as snapshot transmission
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait Membership<T>: Sync + Send + 'static
where
    T: TypeConfig,
{
    /// All nodes (including itself)
    #[allow(unused)]
    fn members(&self) -> Vec<crate::proto::cluster::NodeMeta>;

    /// All non-self nodes (including PendingActive and Active)
    /// Note:
    /// Joining node has not start its Raft event processing engine yet.
    fn replication_peers(&self) -> Vec<crate::proto::cluster::NodeMeta>;

    /// All non-self nodes in Active state
    fn voters(&self) -> Vec<crate::proto::cluster::NodeMeta>;

    /// All pending active nodes in Active state
    fn nodes_with_status(
        &self,
        status: NodeStatus,
    ) -> Vec<crate::proto::cluster::NodeMeta>;

    async fn check_cluster_is_ready(&self) -> Result<()>;

    fn get_peers_id_with_condition<F>(
        &self,
        condition: F,
    ) -> Vec<u32>
    where
        F: Fn(i32) -> bool + 'static;

    fn mark_leader_id(
        &self,
        leader_id: u32,
    ) -> Result<()>;

    fn current_leader_id(&self) -> Option<u32>;

    /// Reset old leader to follower
    fn reset_leader(&self) -> Result<()>;

    /// If node role not found return Error
    fn update_node_role(
        &self,
        node_id: u32,
        new_role: i32,
    ) -> Result<()>;

    /// retrieve latest cluster membership
    fn retrieve_cluster_membership_config(&self) -> ClusterMembership;

    /// invoked when receive requests from Leader
    async fn update_cluster_conf_from_leader(
        &self,
        my_id: u32,
        my_current_term: u64,
        current_conf_version: u64,
        current_leader_id: Option<u32>,
        cluster_conf_change_req: &ClusterConfChangeRequest,
    ) -> Result<ClusterConfUpdateResponse>;

    fn get_cluster_conf_version(&self) -> u64;

    fn update_cluster_conf_from_leader_version(
        &self,
        new_version: u64,
    );

    fn auto_incr_cluster_conf_version(&self);

    /// Add a new node as a learner
    async fn add_learner(
        &self,
        node_id: u32,
        address: String,
        status: NodeStatus,
    ) -> Result<()>;

    /// Activate node
    #[allow(unused)]
    fn activate_node(
        &mut self,
        new_node_id: u32,
    ) -> Result<()>;

    /// Update status of a node
    fn update_node_status(
        &self,
        node_id: u32,
        status: NodeStatus,
    ) -> Result<()>;

    /// Check if the node already exists
    fn contains_node(
        &self,
        node_id: u32,
    ) -> bool;

    fn retrieve_node_meta(
        &self,
        node_id: u32,
    ) -> Option<crate::proto::cluster::NodeMeta>;

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
    fn get_all_nodes(&self) -> Vec<crate::proto::cluster::NodeMeta>;

    async fn get_peer_channel(
        &self,
        node_id: u32,
        conn_type: ConnectionType,
    ) -> Option<Channel>;

    fn get_address(
        &self,
        node_id: u32,
    ) -> Option<String>;
}

impl ClusterConfUpdateResponse {
    /// Generate a successful response (full success)
    pub(crate) fn success(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self {
        Self {
            id: node_id,
            term,
            version,
            success: true,
            error_code: ErrorCode::None.into(),
        }
    }

    /// Generate a failed response (Stale leader term)
    pub(crate) fn higher_term(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self {
        Self {
            id: node_id,
            term,
            version,
            success: false,
            error_code: ErrorCode::TermOutdated.into(),
        }
    }

    /// Generate a failed response (Request sent to non-leader or an out-dated leader)
    pub(crate) fn not_leader(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self {
        Self {
            id: node_id,
            term,
            version,
            success: false,
            error_code: ErrorCode::NotLeader.into(),
        }
    }

    /// Generate a failed response (Stale configuration version)
    pub(crate) fn version_conflict(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self {
        Self {
            id: node_id,
            term,
            version,
            success: false,
            error_code: ErrorCode::VersionConflict.into(),
        }
    }

    /// Generate a failed response (Malformed change request)
    #[allow(unused)]
    pub(crate) fn invalid_change(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self {
        Self {
            id: node_id,
            term,
            version,
            success: false,
            error_code: ErrorCode::InvalidChange.into(),
        }
    }

    /// Generate a failed response (Server-side processing error)
    pub(crate) fn internal_error(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self {
        Self {
            id: node_id,
            term,
            version,
            success: false,
            error_code: ErrorCode::InternalError.into(),
        }
    }

    #[allow(unused)]
    pub(crate) fn is_higher_term(&self) -> bool {
        self.error_code == <ErrorCode as Into<i32>>::into(ErrorCode::TermOutdated)
    }
}
