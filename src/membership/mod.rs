mod health_checker;
mod raft_membership;
mod rpc_peer_channels;
pub use raft_membership::*;
pub use rpc_peer_channels::*;
#[cfg(test)]
mod health_checker_test;
#[cfg(test)]
mod raft_membership_test;
#[cfg(test)]
mod rpc_peer_channels_test;

use dashmap::DashMap;
#[cfg(test)]
use mockall::automock;
use std::sync::Arc;

///-----------------------------------------------
/// Membership behavior definition
use tonic::async_trait;
///-----------------------------------------------
/// Membership behavior definition
use tonic::transport::Channel;

use crate::alias::POF;
use crate::proto::cluster::cluster_conf_update_response::ErrorCode;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::ClusterConfUpdateResponse;
use crate::proto::cluster::ClusterMembership;
use crate::RaftNodeConfig;
use crate::Result;
use crate::TypeConfig;

#[derive(Clone, Debug)]
pub struct ChannelWithAddress {
    pub(crate) address: String,
    pub(crate) channel: Channel,
}
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ChannelWithAddressAndRole {
    pub(crate) id: u32,
    pub(crate) channel_with_address: ChannelWithAddress,
    pub(crate) role: i32,
}

#[cfg_attr(test, automock)]
pub trait PeerChannelsFactory {
    fn create(
        node_id: u32,
        settings: Arc<RaftNodeConfig>,
    ) -> Self;
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait PeerChannels: Sync + Send + 'static {
    async fn connect_with_peers(
        &mut self,
        my_id: u32,
    ) -> Result<()>;
    async fn check_cluster_is_ready(&self) -> Result<()>;

    /// Get all peers channel regardless peer's role
    fn voting_members(&self) -> DashMap<u32, ChannelWithAddress>;
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait Membership<T>: Sync + Send + 'static
where
    T: TypeConfig,
{
    fn voting_members(
        &self,
        peer_channels: Arc<POF<T>>,
    ) -> Vec<ChannelWithAddressAndRole>;

    fn get_followers_candidates_channel_and_role(
        &self,
        channels: &DashMap<u32, ChannelWithAddress>,
    ) -> Vec<ChannelWithAddressAndRole>;

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

    fn current_leader(&self) -> Option<u32>;

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
    ) -> Result<()>;

    /// Check if the node already exists
    fn contains_node(
        &self,
        node_id: u32,
    ) -> bool;

    /// Elegantly remove nodes
    async fn remove_node(
        &self,
        node_id: u32,
    ) -> Result<()>;

    /// Forcefully remove faulty nodes
    async fn force_remove_node(
        &self,
        node_id: u32,
    ) -> Result<()>;

    /// Get all node status
    fn get_all_nodes(&self) -> Vec<crate::proto::cluster::NodeMeta>;
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

    pub(crate) fn is_higher_term(&self) -> bool {
        self.error_code == <ErrorCode as Into<i32>>::into(ErrorCode::TermOutdated)
    }
}
