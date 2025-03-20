use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

use autometrics::autometrics;
use dashmap::DashMap;
use log::{debug, error, warn};
use tokio::sync::mpsc;
use tonic::async_trait;

use crate::{
    alias::POF,
    grpc::rpc_service::{ClusteMembershipChangeRequest, ClusterMembership, NodeMeta},
    is_candidate, is_follower, ChannelWithAddressAndRole, Error, RaftEvent, Result, Settings,
    TypeConfig, API_SLO, FOLLOWER, LEADER,
};

use super::{ChannelWithAddress, Membership, PeerChannels};

pub struct RaftMembership<T>
where
    T: TypeConfig,
{
    node_id: u32,
    leader_id: Arc<AtomicU32>,
    event_tx: mpsc::Sender<RaftEvent>,
    membership: DashMap<u32, NodeMeta>, //stores all members meta
    settings: Arc<Settings>,
    cluster_conf_version: AtomicU64,
    _phantom: PhantomData<T>,
}

#[async_trait]
impl<T> Membership<T> for RaftMembership<T>
where
    T: TypeConfig,
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
    fn mark_leader_id(&self, leader_id: u32) {
        debug!("mark {} as Leader", leader_id);

        // Step 1: Reset the role of any old leader (if any)
        if let Err(e) = self.reset_leader() {
            error!("reset_leader failed: {:?}", e);
        }

        // Step 2: Update leader id
        self.leader_id.store(leader_id, Ordering::SeqCst);

        // Step 3: Update the new leader's role
        if let Err(e) = self.update_node_role(leader_id, LEADER) {
            error!(
                "cluster_membership_controller.update_node_role({}, Leader) failed: {:?}",
                leader_id, e
            );
        }
    }
    // Reset old leader to follower
    #[autometrics(objective = API_SLO)]
    fn reset_leader(&self) -> Result<()> {
        self.leader_id.store(0, Ordering::SeqCst);

        for mut node_meta in self.membership.iter_mut() {
            if node_meta.role == LEADER {
                node_meta.role = FOLLOWER;
            }
        }
        Ok(())
    }

    /// If node role not found return Error
    ///
    #[autometrics(objective = API_SLO)]
    fn update_node_role(&self, node_id: u32, new_role: i32) -> Result<()> {
        if let Some(mut node_meta) = self.membership.get_mut(&node_id) {
            debug!(
                "update_node_role(in cluster membership meta={:?}): id({})'s role been changed to: {}",
                &node_meta, node_id, new_role
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
    fn voting_members(&self, peer_channels: Arc<POF<T>>) -> Vec<ChannelWithAddressAndRole> {
        let peers = peer_channels.voting_members();
        self.get_followers_candidates_channel_and_role(&peers)
    }

    fn current_leader(&self) -> Option<u32> {
        let leader_id = self.leader_id.load(Ordering::Acquire);

        if leader_id > 0 {
            Some(leader_id)
        } else {
            None
        }
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
    ) -> bool {
        let leader_id = cluster_conf_change_req.id;
        debug!(
            "[update_cluster_conf_from_leader] receive cluster_conf_change_req({:?}) from leader_id({})",
            cluster_conf_change_req, leader_id,
        );

        let mut success = true;

        // Step 1: compare term
        if my_current_term > cluster_conf_change_req.term {
            success = false;
            warn!("[update_cluster_conf_from_leader] my_current_term({}) bigger than cluster request one:{:?}", my_current_term, cluster_conf_change_req.term);
        }

        // Step 2: compare configure version
        if self.get_cluster_conf_version() > cluster_conf_change_req.version {
            success = false;
            warn!("[update_cluster_conf_from_leader] currenter conf version than cluster request one:{:?}", cluster_conf_change_req.version);
        }

        // Step 3: install latest configure and update configure version
        if success {
            debug!("success! going to update cluster role and myself one");
            if let Some(new_cluster_metadata) = &cluster_conf_change_req.cluster_membership {
                for node in &new_cluster_metadata.nodes {
                    let received_node_role = node.role;

                    if let Err(e) = self.update_node_role(node.id, received_node_role) {
                        error!(
                            "failed to update_node_role({}, {:?}): {:?}",
                            node.id, received_node_role, e
                        );
                    }
                }
            }
        }

        success
    }

    #[autometrics(objective = API_SLO)]
    fn get_cluster_conf_version(&self) -> u64 {
        self.cluster_conf_version.load(Ordering::Acquire)
    }

    #[autometrics(objective = API_SLO)]
    fn update_cluster_conf_version(&self, new_version: u64) {
        self.cluster_conf_version
            .store(new_version, Ordering::Release);
    }
}

impl<T> RaftMembership<T>
where
    T: TypeConfig,
{
    /// Creates a new `RaftMembership` instance.
    pub fn new(
        node_id: u32,
        initial_cluster: Vec<NodeMeta>,
        event_tx: mpsc::Sender<RaftEvent>,
        settings: Arc<Settings>,
    ) -> Self {
        Self {
            node_id,
            leader_id: Arc::new(AtomicU32::new(0)),
            membership: into_map(initial_cluster),
            cluster_conf_version: AtomicU64::new(0),
            event_tx,
            settings,
            _phantom: PhantomData,
        }
    }

    pub(self) fn get_peers_address_with_role_condition<F>(
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

                // Get channel information (assuming RpcPeerChannels is associated with membership)
                channels
                    .get(&node_id)
                    .map(|channel_entry| ChannelWithAddressAndRole {
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
