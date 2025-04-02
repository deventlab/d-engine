//! Manages the Raft cluster membership as the single source of truth for node
//! roles and configuration.
//!
//! This module:
//! - Tracks all cluster members' metadata (ID, role, term, etc.)
//! - Handles membership configuration changes and versioning
//! - Maintains leader election state
//! - Provides authoritative cluster view for consensus algorithm
//! - Decouples network channel management from membership state
//!
//! The membership data is completely separate from network connections (managed
//! by `rpc_peer_channels`) but depends on its correct initialization. All Raft
//! protocol decisions are made based on the state maintained here.
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use autometrics::autometrics;
use dashmap::DashMap;
use log::debug;
use log::error;
use log::trace;
use log::warn;
use tonic::async_trait;

use super::ChannelWithAddress;
use super::Membership;
use super::PeerChannels;
use crate::alias::POF;
use crate::grpc::rpc_service::ClusteMembershipChangeRequest;
use crate::grpc::rpc_service::ClusterMembership;
use crate::grpc::rpc_service::NodeMeta;
use crate::is_candidate;
use crate::is_follower;
use crate::ChannelWithAddressAndRole;
use crate::Error;
use crate::Result;
use crate::TypeConfig;
use crate::API_SLO;
use crate::FOLLOWER;
use crate::LEADER;

pub struct RaftMembership<T>
where T: TypeConfig
{
    node_id: u32,
    membership: DashMap<u32, NodeMeta>, //stores all members meta
    cluster_conf_version: AtomicU64,
    _phantom: PhantomData<T>,
}

#[async_trait]
impl<T> Membership<T> for RaftMembership<T>
where T: TypeConfig
{
    fn get_followers_candidates_channel_and_role(
        &self,
        channels: &DashMap<u32, ChannelWithAddress>,
    ) -> Vec<ChannelWithAddressAndRole> {
        self.get_peers_address_with_role_condition(channels, |peer_role| {
            is_follower(peer_role) || is_candidate(peer_role)
        })
    }

    #[autometrics(objective = API_SLO)]
    fn mark_leader_id(
        &self,
        leader_id: u32,
    ) -> Result<()> {
        trace!("mark {} as Leader", leader_id);

        // Step 1: Reset the role of any old leader (if any)
        if let Err(e) = self.reset_leader() {
            error!("reset_leader failed: {:?}", e);
            return Err(e);
        }

        // Step 2: Update the new leader's role
        if let Err(e) = self.update_node_role(leader_id, LEADER) {
            error!(
                "cluster_membership_controller.update_node_role({}, Leader) failed: {:?}",
                leader_id, e
            );
            return Err(e);
        }

        Ok(())
    }
    // Reset old leader to follower
    #[autometrics(objective = API_SLO)]
    fn reset_leader(&self) -> Result<()> {
        // self.leader_id.store(0, Ordering::SeqCst);

        for mut node_meta in self.membership.iter_mut() {
            if node_meta.role == LEADER {
                node_meta.role = FOLLOWER;
            }
        }
        Ok(())
    }

    /// If node role not found return Error
    #[autometrics(objective = API_SLO)]
    fn update_node_role(
        &self,
        node_id: u32,
        new_role: i32,
    ) -> Result<()> {
        if let Some(mut node_meta) = self.membership.get_mut(&node_id) {
            trace!(
                "update_node_role(in cluster membership meta={:?}): id({})'s role been changed to: {}",
                &node_meta,
                node_id,
                new_role
            );
            node_meta.role = new_role;
            Ok(())
        } else {
            error!(
                "update_node_role(in cluster membership meta): id({}) not found. update to role({}) failed.",
                node_id, new_role
            );
            Err(Error::ClusterMetadataNodeMetaNotFound(node_id))
        }
    }

    // Joined PeerChannel with Membership
    fn voting_members(
        &self,
        peer_channels: Arc<POF<T>>,
    ) -> Vec<ChannelWithAddressAndRole> {
        let peers = peer_channels.voting_members();
        self.get_followers_candidates_channel_and_role(&peers)
    }

    fn current_leader(&self) -> Option<u32> {
        for node_meta in self.membership.iter_mut() {
            if node_meta.role == LEADER {
                return Some(node_meta.id);
            }
        }
        None
    }

    fn retrieve_cluster_membership_config(&self) -> ClusterMembership {
        let nodes: Vec<NodeMeta> = self.membership.iter().map(|entry| entry.clone()).collect();
        ClusterMembership { nodes }
    }

    #[autometrics(objective = API_SLO)]
    async fn update_cluster_conf_from_leader(
        &self,
        my_current_term: u64,
        cluster_conf_change_req: &ClusteMembershipChangeRequest,
    ) -> Result<()> {
        let leader_id = cluster_conf_change_req.id;
        debug!(
            "[update_cluster_conf_from_leader] receive cluster_conf_change_req({:?}) from leader_id({})",
            cluster_conf_change_req, leader_id,
        );

        // Step 1: compare term
        if my_current_term > cluster_conf_change_req.term {
            warn!(
                "[update_cluster_conf_from_leader] my_current_term({}) bigger than cluster request one:{:?}",
                my_current_term, cluster_conf_change_req.term
            );
            return Err(Error::ClusterMembershipUpdateFailed(format!(
                "[update_cluster_conf_from_leader] my_current_term({}) bigger than cluster request one:{:?}",
                my_current_term, cluster_conf_change_req.term
            )));
        }

        // Step 2: compare configure version
        if self.get_cluster_conf_version() > cluster_conf_change_req.version {
            warn!(
                "[update_cluster_conf_from_leader] currenter conf version than cluster request one:{:?}",
                cluster_conf_change_req.version
            );

            return Err(Error::ClusterMembershipUpdateFailed(format!(
                "[update_cluster_conf_from_leader] currenter conf version than cluster request one:{:?}",
                cluster_conf_change_req.version
            )));
        }

        // Step 3: install latest configure and update configure version
        debug!("success! going to update cluster role and myself one");
        if let Some(new_cluster_metadata) = &cluster_conf_change_req.cluster_membership {
            for node in &new_cluster_metadata.nodes {
                let received_node_role = node.role;

                if let Err(e) = self.update_node_role(node.id, received_node_role) {
                    error!(
                        "failed to update_node_role({}, {:?}): {:?}",
                        node.id, received_node_role, e
                    );
                    return Err(e);
                }
            }
            self.update_cluster_conf_version(cluster_conf_change_req.version);
        }

        Ok(())
    }

    #[autometrics(objective = API_SLO)]
    fn get_cluster_conf_version(&self) -> u64 {
        self.cluster_conf_version.load(Ordering::Acquire)
    }

    #[autometrics(objective = API_SLO)]
    fn update_cluster_conf_version(
        &self,
        new_version: u64,
    ) {
        self.cluster_conf_version.store(new_version, Ordering::Release);
    }
}

impl<T> RaftMembership<T>
where T: TypeConfig
{
    /// Creates a new `RaftMembership` instance.
    pub fn new(
        node_id: u32,
        initial_cluster: Vec<NodeMeta>,
    ) -> Self {
        Self {
            node_id,
            membership: into_map(initial_cluster),
            cluster_conf_version: AtomicU64::new(0),
            _phantom: PhantomData,
        }
    }

    pub(super) fn get_peers_address_with_role_condition<F>(
        &self,
        channels: &DashMap<u32, ChannelWithAddress>,
        condition: F,
    ) -> Vec<ChannelWithAddressAndRole>
    where
        F: Fn(i32) -> bool,
    {
        self.membership
            .iter()
            .filter_map(|entry| {
                let node_id = *entry.key();
                // Exclude own node
                if node_id == self.node_id {
                    return None;
                }

                let meta = entry.value();
                // Apply role filter condition
                if !condition(meta.role) {
                    return None;
                }

                // Get channel information (assuming RpcPeerChannels is associated with
                // membership)
                channels.get(&node_id).map(|channel_entry| ChannelWithAddressAndRole {
                    id: node_id,
                    channel_with_address: channel_entry.value().clone(),
                    role: meta.role,
                })
            })
            .collect()
    }
}

fn into_map(initial_cluster: Vec<NodeMeta>) -> DashMap<u32, NodeMeta> {
    let dash_map = DashMap::new();
    for node in initial_cluster {
        dash_map.insert(node.id, node.clone());
    }
    dash_map
}
