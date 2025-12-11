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

use std::fmt::Debug;
use std::marker::PhantomData;

use d_engine_core::ensure_safe_join;
use futures::FutureExt;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::task;
use tonic::async_trait;
use tonic::transport::Channel;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::MembershipGuard;
use crate::network::ConnectionCache;
use crate::network::HealthChecker;
use crate::network::HealthCheckerApis;
use crate::network::HealthMonitor;
use crate::network::RaftHealthMonitor;
use crate::utils::async_task::task_with_timeout_and_exponential_backoff;
use crate::utils::net::address_str;
use d_engine_core::ConnectionType;
use d_engine_core::Membership;
use d_engine_core::MembershipError;
use d_engine_core::RaftNodeConfig;
use d_engine_core::Result;
use d_engine_core::TypeConfig;
use d_engine_proto::common::MembershipChange;
use d_engine_proto::common::NodeRole::Follower;

use d_engine_proto::common::NodeRole::Learner;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::common::membership_change::Change;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::NodeMeta;

pub struct RaftMembership<T>
where
    T: TypeConfig,
{
    node_id: u32,
    membership: MembershipGuard,
    config: RaftNodeConfig,
    /// Initial cluster size from configuration (immutable after startup)
    initial_cluster_size: usize,
    pub(super) health_monitor: RaftHealthMonitor,
    pub(super) connection_cache: ConnectionCache,
    _phantom: PhantomData<T>,
}

impl<T: TypeConfig> Debug for RaftMembership<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RaftMembership").field("node_id", &self.node_id).finish()
    }
}

#[async_trait]
impl<T> Membership<T> for RaftMembership<T>
where
    T: TypeConfig,
{
    async fn members(&self) -> Vec<NodeMeta> {
        self.membership
            .blocking_read(|guard| guard.nodes.values().cloned().collect())
            .await
    }

    async fn replication_peers(&self) -> Vec<NodeMeta> {
        self.membership
            .blocking_read(|guard| {
                guard
                    .nodes
                    .values()
                    .filter(|node| {
                        node.id != self.node_id
                            && (node.status == NodeStatus::Active as i32
                                || node.status == NodeStatus::Syncing as i32)
                    })
                    .cloned()
                    .collect()
            })
            .await
    }

    async fn voters(&self) -> Vec<NodeMeta> {
        self.membership
            .blocking_read(|guard| {
                guard
                    .nodes
                    .values()
                    .filter(|node| {
                        node.id != self.node_id && node.status == NodeStatus::Active as i32
                    })
                    .cloned()
                    .collect()
            })
            .await
    }

    async fn initial_cluster_size(&self) -> usize {
        self.initial_cluster_size
    }

    async fn nodes_with_status(
        &self,
        status: NodeStatus,
    ) -> Vec<NodeMeta> {
        self.membership
            .blocking_read(|guard| {
                guard
                    .nodes
                    .values()
                    .filter(|node| node.id != self.node_id && node.status == status as i32)
                    .cloned()
                    .collect()
            })
            .await
    }

    async fn get_node_status(
        &self,
        node_id: u32,
    ) -> Option<NodeStatus> {
        self.membership
            .blocking_read(|guard| {
                guard
                    .nodes
                    .get(&node_id)
                    .and_then(|node| NodeStatus::try_from(node.status).ok())
            })
            .await
    }

    async fn activate_node(
        &mut self,
        new_node_id: u32,
    ) -> Result<()> {
        let current_voters = self
            .membership
            .blocking_read(|guard| {
                guard
                    .nodes
                    .values()
                    .filter(|node| node.status == NodeStatus::Active as i32)
                    .count()
            })
            .await;

        ensure_safe_join(self.node_id, current_voters)?;

        self.membership
            .blocking_write(|guard| {
                if let Some(node) = guard.nodes.get_mut(&new_node_id) {
                    node.status = NodeStatus::Active as i32;
                }
                Ok(())
            })
            .await
    }

    async fn check_cluster_is_ready(&self) -> Result<()> {
        info!("check_cluster_is_ready...");
        let mut tasks = FuturesUnordered::new();

        let settings = self.config.network.clone();
        let raft = self.config.raft.clone();
        let retry = self.config.retry.clone();

        let mut peer_ids = Vec::new();
        for peer in self.voters().await {
            let peer_id = peer.id;
            debug!("check_cluster_is_ready for peer: {}", peer_id);
            peer_ids.push(peer_id);
            let addr: String = peer.address.clone();

            let settings = settings.clone();
            let cluster_healthcheck_probe_service_name =
                raft.membership.cluster_healthcheck_probe_service_name.clone();

            let task_handle = task::spawn(async move {
                match task_with_timeout_and_exponential_backoff(
                    move || {
                        HealthChecker::check_peer_is_ready(
                            addr.clone(),
                            settings.clone(),
                            cluster_healthcheck_probe_service_name.clone(),
                        )
                    },
                    retry.membership,
                )
                .await
                {
                    Ok(response) => {
                        debug!("healthcheck: {:?} response: {:?}", peer.address, response);

                        Ok(response)
                    }
                    Err(e) => {
                        warn!("Received RPC error: {}", e);
                        Err(e)
                    }
                }
            });
            tasks.push(task_handle.boxed());
        }

        // Wait for all tasks to complete
        let mut success_count = 0;
        while let Some(result) = tasks.next().await {
            match result {
                Ok(Ok(_)) => success_count += 1,
                Ok(Err(e)) => error!("Task failed with error: {:?}", e),
                Err(e) => error!("Task failed with error: {:?}", e),
            }
        }

        if peer_ids.len() == success_count {
            info!(
                "

                ... CLUSTER IS READY ...

            "
            );
            println!(
                "

                ... CLUSTER IS READY ...

            "
            );

            return Ok(());
        } else {
            error!(
                "

                ... CLUSTER IS NOT READY ...

            "
            );
            return Err(MembershipError::ClusterIsNotReady.into());
        }
    }

    async fn get_peers_id_with_condition<F>(
        &self,
        condition: F,
    ) -> Vec<u32>
    where
        F: Fn(i32) -> bool + Send + Sync + 'static,
    {
        self.membership
            .blocking_read(|guard| {
                guard
                    .nodes
                    .values()
                    .filter(|node| condition(node.role))
                    .map(|node| node.id)
                    .collect()
            })
            .await
    }

    async fn retrieve_cluster_membership_config(
        &self,
        current_leader_id: Option<u32>,
    ) -> ClusterMembership {
        self.membership
            .blocking_read(|guard| ClusterMembership {
                version: guard.cluster_conf_version,
                nodes: guard.nodes.values().cloned().collect(),
                current_leader_id,
            })
            .await
    }

    async fn update_cluster_conf_from_leader(
        &self,
        my_id: u32,
        my_current_term: u64,
        current_conf_version: u64,
        current_leader_id: Option<u32>,
        req: &ClusterConfChangeRequest,
    ) -> Result<ClusterConfUpdateResponse> {
        debug!("[{}] update_cluster_conf_from_leader: {:?}", my_id, &req);

        // Validation logic
        let leader_id = if let Some(leader_id) = current_leader_id {
            leader_id
        } else {
            return Err(MembershipError::NoLeaderFound.into());
        };

        if leader_id != req.id {
            return Ok(ClusterConfUpdateResponse::not_leader(
                my_id,
                my_current_term,
                current_conf_version,
            ));
        }

        if my_current_term > req.term {
            return Ok(ClusterConfUpdateResponse::higher_term(
                my_id,
                my_current_term,
                current_conf_version,
            ));
        }

        if self.get_cluster_conf_version().await > req.version {
            return Ok(ClusterConfUpdateResponse::version_conflict(
                my_id,
                my_current_term,
                current_conf_version,
            ));
        }

        // Handle configuration changes
        if let Some(membership_change) = &req.change {
            match &membership_change.change {
                Some(Change::AddNode(add)) => {
                    self.add_learner(add.node_id, add.address.clone()).await?;
                }
                Some(Change::RemoveNode(remove)) => {
                    self.remove_node(remove.node_id).await?;
                }
                Some(Change::Promote(promote)) => {
                    self.update_single_node(promote.node_id, |node| {
                        if node.role == Learner as i32 {
                            node.role = Follower as i32;
                            Ok(())
                        } else {
                            Err(MembershipError::InvalidPromotion {
                                node_id: promote.node_id,
                                role: node.role,
                            }
                            .into())
                        }
                    })
                    .await?;
                }
                Some(Change::BatchPromote(bp)) => {
                    self.update_multiple_nodes(&bp.node_ids, |node| {
                        if NodeStatus::is_i32_promotable(node.status) {
                            node.status = bp.new_status;
                            node.role = Follower as i32;
                        }
                        Ok(())
                    })
                    .await?;
                    self.update_conf_version(req.version).await;
                    return Ok(ClusterConfUpdateResponse::success(
                        my_id,
                        my_current_term,
                        req.version,
                    ));
                }

                Some(Change::BatchRemove(br)) => {
                    if br.node_ids.contains(&leader_id) {
                        return Err(MembershipError::RemoveNodeIsLeader(leader_id).into());
                    }
                    // Atomic batch removal
                    self.membership
                        .blocking_write(|guard| -> Result<()> {
                            for node_id in &br.node_ids {
                                guard.nodes.remove(node_id);
                            }
                            Ok(())
                        })
                        .await?;
                    self.update_conf_version(req.version).await;
                    return Ok(ClusterConfUpdateResponse::success(
                        my_id,
                        my_current_term,
                        req.version,
                    ));
                }

                None => return Err(MembershipError::InvalidChangeRequest.into()),
            }
        } else {
            return Err(MembershipError::InvalidChangeRequest.into());
        }

        self.update_conf_version(req.version).await;
        Ok(ClusterConfUpdateResponse::success(
            my_id,
            my_current_term,
            self.get_cluster_conf_version().await,
        ))
    }

    async fn get_cluster_conf_version(&self) -> u64 {
        self.membership.blocking_read(|guard| guard.cluster_conf_version).await
    }

    async fn update_conf_version(
        &self,
        version: u64,
    ) {
        self.membership
            .blocking_write(|guard| guard.cluster_conf_version = version)
            .await;
    }

    async fn incr_conf_version(&self) {
        self.membership.blocking_write(|guard| guard.cluster_conf_version += 1).await;
    }

    /// Node can only be added as a learner.
    /// If the node already exists, update should fail
    async fn add_learner(
        &self,
        node_id: u32,
        address: String,
    ) -> Result<()> {
        info!("Adding learner node: {}", node_id);
        self.membership
            .blocking_write(|guard| {
                if guard.nodes.contains_key(&node_id) {
                    error!(
                        "[node-{}] Adding a learner node failed: node already exists. {}",
                        self.node_id, node_id
                    );
                    return Err(MembershipError::NodeAlreadyExists(node_id).into());
                    // return  Ok(());
                }
                guard.nodes.insert(
                    node_id,
                    NodeMeta {
                        id: node_id,
                        address,
                        role: Learner as i32,
                        status: NodeStatus::Syncing as i32,
                    },
                );
                info!(
                    "[node-{}] Adding a learner node successed: {}",
                    self.node_id, node_id
                );

                Ok(())
            })
            .await
    }

    async fn update_node_status(
        &self,
        node_id: u32,
        status: NodeStatus,
    ) -> Result<()> {
        self.update_single_node(node_id, |node| {
            node.status = status as i32;
            Ok(())
        })
        .await
    }

    async fn remove_node(
        &self,
        node_id: u32,
    ) -> Result<()> {
        // Allow leader self-removal per Raft protocol
        // Leader will step down after applying this config change

        // Purge cached connections
        self.connection_cache.remove_node(node_id);

        self.membership
            .blocking_write(|guard| {
                guard.nodes.remove(&node_id);
                guard.cluster_conf_version += 1;
                Ok(())
            })
            .await
    }

    async fn force_remove_node(
        &self,
        node_id: u32,
    ) -> Result<()> {
        // Purge cached connections
        self.connection_cache.remove_node(node_id);

        self.membership
            .blocking_write(|guard| {
                guard.nodes.remove(&node_id);
                guard.cluster_conf_version += 1;
                Ok(())
            })
            .await
    }

    async fn contains_node(
        &self,
        node_id: u32,
    ) -> bool {
        self.membership.blocking_read(|guard| guard.nodes.contains_key(&node_id)).await
    }

    async fn retrieve_node_meta(
        &self,
        node_id: u32,
    ) -> Option<NodeMeta> {
        self.membership.blocking_read(|guard| guard.nodes.get(&node_id).cloned()).await
    }

    async fn get_all_nodes(&self) -> Vec<NodeMeta> {
        self.membership
            .blocking_read(|guard| guard.nodes.values().cloned().collect())
            .await
    }

    async fn pre_warm_connections(&self) -> Result<()> {
        let peers = self.replication_peers().await;

        if peers.is_empty() {
            info!("No replication peers found");
            return Ok(());
        }

        let conn_types = ConnectionType::all();
        // Materialize task futures to a Vec
        let mut tasks = Vec::new();
        for peer in &peers {
            for conn_type in &conn_types {
                let peer_id = peer.id;
                let conn_type = conn_type.clone();
                let fut = async move {
                    if let Some(channel) = self.get_peer_channel(peer_id, conn_type.clone()).await {
                        info!("Pre-warmed {:?} connection to node {}", conn_type, peer_id);
                        Ok::<_, ()>(channel)
                    } else {
                        warn!(
                            "Failed to pre-warm {:?} connection to node {}",
                            conn_type, peer_id
                        );
                        Err(())
                    }
                };
                tasks.push(fut);
            }
        }

        // Rate-limit concurrent futures
        futures::stream::iter(tasks)
            .buffer_unordered(10)
            .for_each(|result| async move {
                if result.is_err() {
                    warn!("Connection pre-warming failed for one or more peers");
                }
            })
            .await;

        info!("Connection pre-warming completed");
        Ok(())
    }

    async fn get_peer_channel(
        &self,
        node_id: u32,
        conn_type: ConnectionType,
    ) -> Option<Channel> {
        let addr = match self.get_address(node_id).await {
            Some(addr) => addr,
            None => {
                // Record failure if node address is missing
                self.health_monitor.record_failure(node_id).await;
                return None;
            }
        };
        // Use cached connection if available
        match self.connection_cache.get_channel(node_id, conn_type, addr).await {
            Ok(channel) => {
                self.health_monitor.record_success(node_id).await;
                Some(channel)
            }
            Err(e) => {
                // Remove failed connection from cache
                self.connection_cache.remove_node(node_id);
                self.health_monitor.record_failure(node_id).await;
                warn!("Connection to node {} failed: {}", node_id, e);
                None
            }
        }
    }

    async fn get_address(
        &self,
        node_id: u32,
    ) -> Option<String> {
        self.membership
            .blocking_read(|guard| guard.nodes.get(&node_id).map(|n| address_str(&n.address)))
            .await
    }

    async fn apply_config_change(
        &self,
        membership_change: MembershipChange,
    ) -> Result<()> {
        info!("Applying membership change: {:?}", membership_change);
        match membership_change.change {
            Some(Change::AddNode(add)) => self.add_learner(add.node_id, add.address).await,
            Some(Change::RemoveNode(remove)) => self.remove_node(remove.node_id).await,
            Some(Change::Promote(promote)) => {
                self.membership
                    .blocking_write(|guard| {
                        guard
                            .nodes
                            .get_mut(&promote.node_id)
                            .map(|node| {
                                node.role = Follower as i32;
                                node.status = NodeStatus::Active as i32;
                                Ok(())
                            })
                            .unwrap_or_else(|| {
                                Err(MembershipError::NoMetadataFoundForNode {
                                    node_id: promote.node_id,
                                }
                                .into())
                            })
                    })
                    .await
                // self.update_node_role(promote.node_id, Follower as i32).await?;
                // self.update_node_status(promote.node_id, NodeStatus::Active).await
            }
            Some(Change::BatchPromote(bp)) => {
                self.membership
                    .blocking_write(|guard| -> Result<()> {
                        for node_id in &bp.node_ids {
                            let node = guard.nodes.get_mut(node_id).ok_or(
                                MembershipError::NoMetadataFoundForNode { node_id: *node_id },
                            )?;

                            node.role = Follower as i32;
                            node.status = NodeStatus::try_from(bp.new_status)
                                .unwrap_or(NodeStatus::Active)
                                as i32;
                        }
                        Ok(())
                    })
                    .await?;
                Ok(())
            }

            Some(Change::BatchRemove(br)) => {
                // Atomic batch removal
                self.membership
                    .blocking_write(|guard| {
                        for node_id in &br.node_ids {
                            guard.nodes.remove(node_id);
                        }
                        guard.cluster_conf_version += 1;
                        Ok(())
                    })
                    .await
            }
            None => Ok(()),
        }
    }

    async fn notify_config_applied(
        &self,
        index: u64,
    ) {
        // TODO:
        // // Update replication layer
        // self.config.replication_layer.update_peers(
        //     self.replication_peers().await
        // ).await;

        // // Update leader routing
        // if let Some(leader) = self.get_cluster_conf_version().await {
        //     self.config.router.update_leader(leader);
        // }
        info!("Config change applied at index {}", index);
    }

    async fn get_zombie_candidates(&self) -> Vec<u32> {
        self.health_monitor.get_zombie_candidates().await
    }

    async fn can_rejoin(
        &self,
        node_id: u32,
        role: i32,
    ) -> Result<()> {
        // New nodes must be learners
        if role != Learner as i32 {
            return Err(MembershipError::NotLearner.into());
        }

        if !self.contains_node(node_id).await {
            return Ok(());
        }

        match self.get_node_status(node_id).await {
            Some(NodeStatus::Zombie) => Ok(()),
            _ => Err(MembershipError::NodeAlreadyExists(node_id).into()),
        }
    }
}

impl<T> RaftMembership<T>
where
    T: TypeConfig,
{
    pub(crate) fn new(
        node_id: u32,
        initial_nodes: Vec<NodeMeta>,
        config: RaftNodeConfig,
    ) -> Self {
        let zombie_threshold = config.raft.membership.zombie.threshold;
        let connection_cache = ConnectionCache::new(config.network.clone());
        let initial_cluster_size = initial_nodes.len();
        Self {
            node_id,
            membership: MembershipGuard::new(initial_nodes, 0),
            config,
            initial_cluster_size,
            _phantom: PhantomData,
            health_monitor: RaftHealthMonitor::new(zombie_threshold),
            connection_cache,
        }
    }

    /// Updates a single node atomically
    pub(super) async fn update_single_node(
        &self,
        node_id: u32,
        f: impl FnOnce(&mut NodeMeta) -> Result<()>,
    ) -> Result<()> {
        self.membership
            .blocking_write(|guard| {
                guard.nodes.get_mut(&node_id).map(f).unwrap_or_else(|| {
                    Err(MembershipError::NoMetadataFoundForNode { node_id }.into())
                })
            })
            .await
    }

    /// Updates multiple nodes in a batch
    pub(super) async fn update_multiple_nodes(
        &self,
        node_ids: &[u32],
        f: impl Fn(&mut NodeMeta) -> Result<()>,
    ) -> Result<()> {
        self.membership
            .blocking_write(|guard| {
                for node_id in node_ids {
                    if let Some(node) = guard.nodes.get_mut(node_id) {
                        f(node)?;
                    }
                }
                Ok(())
            })
            .await
    }

    #[cfg(test)]
    pub(crate) async fn get_role_by_node_id(
        &self,
        node_id: u32,
    ) -> Option<i32> {
        self.membership
            .blocking_read(|guard| guard.nodes.get(&node_id).map(|n| n.role))
            .await
    }

    #[cfg(test)]
    pub(crate) async fn update_node_address(
        &self,
        node_id: u32,
        address: String,
    ) -> Result<()> {
        self.update_single_node(node_id, |node| {
            node.address = address;
            Ok(())
        })
        .await
    }
}
