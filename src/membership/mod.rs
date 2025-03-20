mod health_checker;
mod raft_membership;
mod rpc_peer_channels;

pub use raft_membership::*;
pub use rpc_peer_channels::*;
use tokio::sync::mpsc;

///-----------------------------------------------
/// Membership behavior definition
///
use crate::{
    alias::POF,
    grpc::rpc_service::{ClusteMembershipChangeRequest, ClusterMembership},
    RaftEvent, Result, Settings, TypeConfig,
};
use dashmap::DashMap;
#[cfg(test)]
use mockall::automock;
use std::sync::Arc;
use tonic::{async_trait, transport::Channel};

#[derive(Clone, Debug)]
pub struct ChannelWithAddress {
    pub(crate) address: String,
    pub(crate) channel: Channel,
}
#[derive(Clone, Debug)]
pub struct ChannelWithAddressAndRole {
    pub(crate) id: u32,
    pub(crate) channel_with_address: ChannelWithAddress,
    pub(crate) role: i32,
}

#[cfg_attr(test, automock)]
pub trait PeerChannelsFactory {
    fn create(node_id: u32, event_tx: mpsc::Sender<RaftEvent>, settings: Arc<Settings>) -> Self;
}

// #[allow(unused)]
#[cfg_attr(test, automock)]
#[async_trait]
pub trait PeerChannels: Sync + Send + 'static {
    async fn connect_with_peers(&mut self, my_id: u32, settings: Arc<Settings>) -> Result<()>;
    async fn check_cluster_is_ready(&self) -> Result<()>;

    /// Get all peers channel regardless peer's role
    ///
    fn voting_members(&self) -> DashMap<u32, ChannelWithAddress>;
}

// #[allow(unused)]
#[cfg_attr(test, automock)]
#[async_trait]
pub trait Membership<T>: Sync + Send + 'static
where
    T: TypeConfig,
{
    fn voting_members(&self, peer_channels: Arc<POF<T>>) -> Vec<ChannelWithAddressAndRole>;

    fn get_followers_candidates_channel_and_role(
        &self,
        channels: &DashMap<u32, ChannelWithAddress>,
    ) -> Vec<ChannelWithAddressAndRole>;

    fn mark_leader_id(&self, leader_id: u32);

    fn current_leader(&self) -> Option<u32>;

    /// Reset old leader to follower
    fn reset_leader(&self) -> Result<()>;

    /// If node role not found return Error
    fn update_node_role(&self, node_id: u32, new_role: i32) -> Result<()>;

    /// retrieve latest cluster membership
    fn retrieve_cluster_membership_config(&self) -> ClusterMembership;

    /// invoked when receive requests from Leader
    async fn update_cluster_conf_from_leader(
        &self,
        my_current_term: u64,
        cluster_conf_change_req: &ClusteMembershipChangeRequest,
    ) -> bool;

    fn get_cluster_conf_version(&self) -> u64;

    fn update_cluster_conf_version(&self, new_version: u64);
}
