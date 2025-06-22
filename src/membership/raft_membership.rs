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

use super::ChannelWithAddress;
use crate::async_task::task_with_timeout_and_exponential_backoff;
use crate::membership::health_checker::HealthChecker;
use crate::membership::health_checker::HealthCheckerApis;
use crate::net::address_str;
use crate::proto::cluster::cluster_conf_change_request::Change;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::ClusterConfUpdateResponse;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::NodeMeta;
use crate::proto::common::NodeStatus;
use crate::ConnectionParams;
use crate::ConnectionType;
use crate::Membership;
use crate::MembershipError;
use crate::NetworkError;
use crate::RaftNodeConfig;
use crate::Result;
use crate::TypeConfig;
use crate::API_SLO;
use crate::FOLLOWER;
use crate::LEADER;
use crate::LEARNER;
use autometrics::autometrics;
use dashmap::DashMap;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::task;
use tonic::async_trait;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::trace;
use tracing::warn;

pub struct RaftMembership<T>
where
    T: TypeConfig,
{
    node_id: u32,
    membership: DashMap<u32, NodeMeta>, //stores all members meta
    cluster_conf_version: AtomicU64,
    config: RaftNodeConfig,
    _phantom: PhantomData<T>,
}

impl<T: TypeConfig> Debug for RaftMembership<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RaftMembership")
            .field("node_id", &self.node_id)
            .finish()
    }
}

#[async_trait]
impl<T> Membership<T> for RaftMembership<T>
where
    T: TypeConfig,
{
    fn members(&self) -> Vec<NodeMeta> {
        self.membership.iter().map(|entry| entry.value().clone()).collect()
    }

    fn replication_peers(&self) -> Vec<NodeMeta> {
        let active_status = NodeStatus::Active as i32;
        let pending_active_status = NodeStatus::PendingActive as i32;
        self.membership
            .iter()
            .filter(|node_meta| {
                node_meta.id != self.node_id
                    && (node_meta.status == active_status || node_meta.status == pending_active_status)
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    fn voters(&self) -> Vec<NodeMeta> {
        let active_status = NodeStatus::Active as i32;
        self.membership
            .iter()
            .filter(|node_meta| node_meta.id != self.node_id && node_meta.status == active_status)
            .map(|entry| entry.value().clone())
            .collect()
    }

    fn nodes_with_status(
        &self,
        status: NodeStatus,
    ) -> Vec<NodeMeta> {
        self.membership
            .iter()
            .filter(|node_meta| node_meta.id != self.node_id && node_meta.status == (status as i32))
            .map(|entry| entry.value().clone())
            .collect()
    }

    fn activate_node(
        &mut self,
        new_node_id: u32,
    ) -> Result<()> {
        // 1. Get current ACTIVE voters count (including self)
        let current_voters = self
            .membership
            .iter()
            .filter(|n| n.status == NodeStatus::Active as i32)
            .count();

        // 2. Verify safety BEFORE allowing activation
        ensure_safe_join(current_voters)?;

        // 3. Only update status if safe
        if let Some(mut node) = self.membership.get_mut(&new_node_id) {
            node.status = NodeStatus::Active as i32;
        }
        Ok(())
    }

    async fn check_cluster_is_ready(&self) -> Result<()> {
        info!("check_cluster_is_ready...");
        let mut tasks = FuturesUnordered::new();

        let node_config = self.config.network.clone();
        let raft = self.config.raft.clone();
        let retry = self.config.retry.clone();

        let mut peer_ids = Vec::new();
        for peer in self.voters() {
            let peer_id = peer.id;
            debug!("check_cluster_is_ready for peer: {}", peer_id);
            peer_ids.push(peer_id);
            let addr: String = peer.address.clone();

            let node_config = node_config.clone();
            let cluster_healthcheck_probe_service_name = raft.membership.cluster_healthcheck_probe_service_name.clone();

            let task_handle = task::spawn(async move {
                match task_with_timeout_and_exponential_backoff(
                    move || {
                        HealthChecker::check_peer_is_ready(
                            addr.clone(),
                            node_config.clone(),
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

    fn get_peers_id_with_condition<F>(
        &self,
        condition: F,
    ) -> Vec<u32>
    where
        F: Fn(i32) -> bool + 'static,
    {
        self.membership
            .iter()
            .filter(|node_meta| condition(node_meta.role))
            .map(|node_meta| node_meta.id)
            .collect()
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
            Err(MembershipError::NoMetadataFoundForNode { node_id }.into())
        }
    }

    fn current_leader_id(&self) -> Option<u32> {
        for node_meta in self.membership.iter_mut() {
            if node_meta.role == LEADER {
                return Some(node_meta.id);
            }
        }
        None
    }

    #[instrument(skip(self))]
    fn retrieve_cluster_membership_config(&self) -> ClusterMembership {
        let nodes: Vec<NodeMeta> = self.membership.iter().map(|entry| entry.clone()).collect();
        let version = self.cluster_conf_version.load(Ordering::Acquire);
        ClusterMembership { version, nodes }
    }

    #[autometrics(objective = API_SLO)]
    async fn update_cluster_conf_from_leader(
        &self,
        my_id: u32,
        my_current_term: u64,
        current_conf_version: u64,
        current_leader_id: Option<u32>,
        cluster_conf_change_req: &ClusterConfChangeRequest,
    ) -> Result<ClusterConfUpdateResponse> {
        let leader_id = cluster_conf_change_req.id;
        debug!(
            "[update_cluster_conf_from_leader] receive cluster_conf_change_req({:?}) from leader_id({})",
            cluster_conf_change_req, leader_id,
        );

        // Step 1: validate leader ID matches current known leader
        if current_leader_id.is_some() && current_leader_id.unwrap() != cluster_conf_change_req.id {
            warn!("Rejecting config change from non-leader");
            return Ok(ClusterConfUpdateResponse::not_leader(
                my_id,
                my_current_term,
                current_conf_version,
            ));
        }

        // Step 2: compare term
        if my_current_term > cluster_conf_change_req.term {
            warn!(
                "[update_cluster_conf_from_leader] my_current_term({}) bigger than cluster request one:{:?}",
                my_current_term, cluster_conf_change_req.term
            );
            return Ok(ClusterConfUpdateResponse::higher_term(
                my_id,
                my_current_term,
                current_conf_version,
            ));
        }

        // Step 2: compare configure version
        if self.get_cluster_conf_version() > cluster_conf_change_req.version {
            warn!(
                "[update_cluster_conf_from_leader] current conf version ({}) is higher than cluster request one:{}",
                self.get_cluster_conf_version(),
                cluster_conf_change_req.version
            );

            return Ok(ClusterConfUpdateResponse::version_conflict(
                my_id,
                my_current_term,
                current_conf_version,
            ));
        }

        // Step 3: Handle specific change type
        match &cluster_conf_change_req.change {
            Some(Change::AddNode(add_node)) => {
                self.add_learner(add_node.node_id, add_node.address.clone(), NodeStatus::Joining)
                    .await?;
            }
            Some(Change::RemoveNode(remove_node)) => {
                self.remove_node(remove_node.node_id).await?;
            }
            Some(Change::PromoteLearner(promote_learner)) => {
                let node_id = promote_learner.node_id;
                if let Some(mut node_meta) = self.membership.get_mut(&node_id) {
                    if node_meta.role == LEARNER {
                        node_meta.role = FOLLOWER;
                    } else {
                        warn!(
                            "Cannot promote node {}: current role is {} (expected LEARNER)",
                            node_id, node_meta.role
                        );
                        return Err(MembershipError::InvalidPromotion {
                            node_id,
                            role: node_meta.role,
                        }
                        .into());
                    }
                } else {
                    warn!("No metadata found for node({node_id})");
                    return Err(MembershipError::NoMetadataFoundForNode { node_id }.into());
                }
            }
            None => {
                warn!("No change specified in ClusterConfChangeRequest");
                return Err(MembershipError::InvalidChangeRequest.into());
            }
        }

        // Step 4: Update cluster configuration version
        self.update_cluster_conf_from_leader_version(cluster_conf_change_req.version);

        return Ok(ClusterConfUpdateResponse::success(
            my_id,
            my_current_term,
            current_conf_version,
        ));
    }

    #[autometrics(objective = API_SLO)]
    fn get_cluster_conf_version(&self) -> u64 {
        self.cluster_conf_version.load(Ordering::Acquire)
    }

    #[autometrics(objective = API_SLO)]
    fn update_cluster_conf_from_leader_version(
        &self,
        new_version: u64,
    ) {
        self.cluster_conf_version.store(new_version, Ordering::Release);
    }

    fn auto_incr_cluster_conf_version(&self) {
        self.cluster_conf_version.fetch_add(1, Ordering::AcqRel);
    }

    /// Add a new learner node
    #[autometrics(objective = API_SLO)]
    async fn add_learner(
        &self,
        node_id: u32,
        address: String,
        status: NodeStatus,
    ) -> Result<()> {
        if self.contains_node(node_id) {
            return Err(MembershipError::NodeAlreadyExists(node_id).into());
        }

        let new_node = NodeMeta {
            id: node_id,
            address,
            role: LEARNER, // Defined as a learner
            status: status.into(),
        };

        self.membership.insert(node_id, new_node);
        self.auto_incr_cluster_conf_version();

        Ok(())
    }

    /// Update node status
    #[autometrics(objective = API_SLO)]
    #[instrument(skip(self))]
    fn update_node_status(
        &self,
        node_id: u32,
        status: NodeStatus,
    ) -> Result<()> {
        // Validate node exists
        if !self.contains_node(node_id) {
            return Err(MembershipError::NoMetadataFoundForNode { node_id }.into());
        }

        // Update status in membership metadata
        if let Some(mut node_meta) = self.membership.get_mut(&node_id) {
            trace!(
                "Updating node {} status from {:?} to {:?}",
                node_id,
                node_meta.status,
                status
            );

            node_meta.status = status as i32;
            Ok(())
        } else {
            // This should never happen due to the contains_node check, but handle it anyway
            Err(MembershipError::NoMetadataFoundForNode { node_id }.into())
        }
    }

    /// Remove node
    #[autometrics(objective = API_SLO)]
    async fn remove_node(
        &self,
        node_id: u32,
    ) -> Result<()> {
        if !self.contains_node(node_id) {
            return Err(MembershipError::NoMetadataFoundForNode { node_id }.into());
        }

        // If it is the leader, you need to transfer leadership first
        if self.current_leader_id() == Some(node_id) {
            return Err(MembershipError::RemoveNodeIsLeader(node_id).into());
        }

        self.membership.remove(&node_id);
        self.auto_incr_cluster_conf_version();

        Ok(())
    }

    async fn force_remove_node(
        &self,
        node_id: u32,
    ) -> Result<()> {
        if !self.contains_node(node_id) {
            return Err(MembershipError::NoMetadataFoundForNode { node_id }.into());
        }

        self.membership.remove(&node_id);
        self.auto_incr_cluster_conf_version();

        Ok(())
    }

    fn contains_node(
        &self,
        node_id: u32,
    ) -> bool {
        self.membership.contains_key(&node_id)
    }

    fn retrieve_node_meta(
        &self,
        node_id: u32,
    ) -> Option<NodeMeta> {
        self.membership.get(&node_id).map(|v| v.clone())
    }

    fn get_all_nodes(&self) -> Vec<NodeMeta> {
        self.membership.iter().map(|entry| entry.value().clone()).collect()
    }

    async fn get_peer_channel(
        &self,
        node_id: u32,
        conn_type: ConnectionType,
    ) -> Option<ChannelWithAddress> {
        let address = address_str(&self.get_address(node_id)?);

        let channel = match conn_type {
            ConnectionType::Control => {
                Self::connect_with_params(address.clone(), self.config.network.control.clone()).await
            }
            ConnectionType::Data => Self::connect_with_params(address.clone(), self.config.network.data.clone()).await,
            ConnectionType::Bulk => Self::connect_with_params(address.clone(), self.config.network.bulk.clone()).await,
        }
        .ok()?;

        debug!("Successfully connected with ({})", &address);
        Some(ChannelWithAddress { address, channel })
    }

    fn get_address(
        &self,
        node_id: u32,
    ) -> Option<String> {
        self.membership.get(&node_id).map(|entry| entry.value().address.clone())
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
        config: RaftNodeConfig,
    ) -> Self {
        Self {
            node_id,
            membership: into_map(initial_cluster),
            cluster_conf_version: AtomicU64::new(0),
            config,
            _phantom: PhantomData,
        }
    }

    pub(super) async fn connect_with_params(
        address: String,
        params: ConnectionParams,
    ) -> Result<Channel> {
        Endpoint::try_from(address.clone())?
            .connect_timeout(Duration::from_millis(params.connect_timeout_in_ms))
            .timeout(Duration::from_millis(params.request_timeout_in_ms))
            .tcp_keepalive(Some(Duration::from_secs(params.tcp_keepalive_in_secs)))
            .http2_keep_alive_interval(Duration::from_secs(params.http2_keep_alive_interval_in_secs))
            .keep_alive_timeout(Duration::from_secs(params.http2_keep_alive_timeout_in_secs))
            .initial_connection_window_size(params.connection_window_size)
            .initial_stream_window_size(params.stream_window_size)
            .connect()
            .await
            .map_err(|err| {
                error!("connect to {} failed: {}", &address, err);
                eprintln!("{err:?}");
                NetworkError::ConnectError.into()
            })
    }

    #[cfg(test)]
    pub(crate) fn get_role_by_node_id(
        &self,
        node_id: u32,
    ) -> Option<i32> {
        Some(self.membership.get(&node_id)?.role)
    }
}

fn into_map(initial_cluster: Vec<NodeMeta>) -> DashMap<u32, NodeMeta> {
    let dash_map = DashMap::new();
    for node in initial_cluster {
        dash_map.insert(node.id, node.clone());
    }
    dash_map
}

pub fn ensure_safe_join(current_voters: usize) -> Result<()> {
    let new_size = current_voters + 1;
    let new_quorum = new_size / 2 + 1;
    let current_quorum = current_voters / 2 + 1;

    if new_quorum > current_quorum {
        return Err(MembershipError::JoinClusterError("Join operation would break quorum safety".to_string()).into());
    }
    Ok(())
}
