//! Manages persistent RPC connections between cluster peers.
//!
//! This module:
//! - Maintains a connection pool to all cluster nodes
//! - Handles initial connection setup using configured cluster addresses
//! - Provides access to active peer channels for Raft RPC communication
//! - Implements connection health checks and reconnection logic
//!
//! The channel connections are established at cluster startup based on the
//! initial configuration and must be properly initialized before
//! `raft_membership` can operate. This layer abstracts network implementation
//! details from the consensus algorithm.
//!

use super::ChannelWithAddress;
use super::PeerChannels;
use super::PeerChannelsFactory;
use crate::async_task::task_with_timeout_and_exponential_backoff;
use crate::membership::health_checker::HealthChecker;
use crate::membership::health_checker::HealthCheckerApis;
use crate::proto::NodeMeta;
use crate::utils::net::address_str;
use crate::MembershipError;
use crate::NetworkConfig;
use crate::NetworkError;
use crate::RaftNodeConfig;
use crate::Result;
use crate::RetryPolicies;
use dashmap::DashMap;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use log::debug;
use log::error;
use log::info;
use log::warn;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tonic::async_trait;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

#[derive(Clone)]
pub struct RpcPeerChannels {
    pub(super) node_id: u32,

    pub(super) channels: DashMap<u32, ChannelWithAddress>, //store peers connection
    pub(super) settings: Arc<RaftNodeConfig>,
}

impl Debug for RpcPeerChannels {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RpcPeerChannels")
            .field("node_id", &self.node_id)
            .finish()
    }
}
impl PeerChannelsFactory for RpcPeerChannels {
    fn create(
        node_id: u32,
        settings: Arc<RaftNodeConfig>,
    ) -> Self {
        Self {
            node_id,
            channels: DashMap::new(),
            settings,
        }
    }
}

#[async_trait]
impl PeerChannels for RpcPeerChannels {
    /// When peer channel setup during server bootstrap stage,
    ///  cluster membership listener is not ready yet.
    async fn connect_with_peers(
        &mut self,
        my_id: u32,
    ) -> Result<()> {
        let initial_cluster = &self.settings.cluster.initial_cluster;
        info!("Connecting with peers: {:?}", initial_cluster);

        let cluster_size = initial_cluster.len();
        let tasks = self.spawn_connection_tasks(
            my_id,
            initial_cluster,
            self.settings.retry.clone(),
            &self.settings.network,
        );
        let channels = self.collect_connections(tasks, cluster_size - 1).await?;

        self.channels = channels;
        Ok(())
    }

    async fn check_cluster_is_ready(&self) -> Result<()> {
        info!("check_cluster_is_ready...");
        let mut tasks = FuturesUnordered::new();

        let settings = self.settings.network.clone();
        let raft = self.settings.raft.clone();
        let retry = self.settings.retry.clone();

        let mut peer_ids = Vec::new();
        for peer in self.voting_members().iter() {
            let peer_id = *peer.key();
            debug!("check_cluster_is_ready for peer: {}", peer_id);
            peer_ids.push(peer_id);
            let peer_channel_with_addr = peer.value().clone();
            let addr: String = peer_channel_with_addr.address.clone();

            let settings = settings.clone();
            let cluster_healthcheck_probe_service_name = raft.membership.cluster_healthcheck_probe_service_name.clone();

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
                        debug!(
                            "healthcheck: {:?} response: {:?}",
                            peer_channel_with_addr.address, response
                        );

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

    fn voting_members(&self) -> DashMap<u32, ChannelWithAddress> {
        self.channels.clone()
    }
}

impl RpcPeerChannels {
    /// Spawns asynchronous tasks to connect with each peer.
    pub(super) fn spawn_connection_tasks(
        &self,
        my_id: u32,
        peers: &[NodeMeta],
        retry: RetryPolicies,
        rpc_settings: &NetworkConfig,
    ) -> FuturesUnordered<task::JoinHandle<Result<(u32, ChannelWithAddress)>>> {
        let tasks = FuturesUnordered::new();

        for node_meta in peers {
            if node_meta.id == my_id {
                continue; // Skip self
            }

            let task = self.spawn_connection_task(node_meta.clone(), retry.clone(), rpc_settings);
            tasks.push(task);
        }

        tasks
    }

    /// Spawns a single connection task for a peer.
    fn spawn_connection_task(
        &self,
        node_meta: NodeMeta,
        retry: RetryPolicies,
        rpc_settings: &NetworkConfig,
    ) -> task::JoinHandle<Result<(u32, ChannelWithAddress)>> {
        let rpc_settings = rpc_settings.clone();
        task::spawn(async move {
            let channel = Self::connect_with_retry(&node_meta, &retry, &rpc_settings).await?;
            let address = address_str(&node_meta.ip, node_meta.port as u16);

            debug!("Successfully connected with ({}:{})", node_meta.ip, node_meta.port);
            Ok((node_meta.id, ChannelWithAddress { address, channel }))
        })
    }

    /// Collects results from connection tasks and validates success count.
    pub(super) async fn collect_connections(
        &self,
        mut tasks: FuturesUnordered<task::JoinHandle<Result<(u32, ChannelWithAddress)>>>,
        expected_count: usize,
    ) -> Result<DashMap<u32, ChannelWithAddress>> {
        let channels = DashMap::new();
        let mut success_count = 0;

        while let Some(result) = tasks.next().await {
            match result {
                Ok(Ok((key, value))) => {
                    channels.insert(key, value);
                    success_count += 1;
                }
                Ok(Err(e)) => error!("Connection task failed: {:?}", e),
                Err(e) => error!("Task panicked: {:?}", e),
            }
        }

        if success_count != expected_count {
            error!(
                "Failed to connect to all peers: success_count({}) != expected_count({})",
                success_count, expected_count
            );
            Err(NetworkError::ConnectError.into())
        } else {
            Ok(channels)
        }
    }

    /// Attempts to connect to a peer with retries and exponential backoff.
    pub(super) async fn connect_with_retry(
        node_meta: &NodeMeta,
        retry: &RetryPolicies,
        rpc_settings: &NetworkConfig,
    ) -> Result<Channel> {
        task_with_timeout_and_exponential_backoff(
            || Self::connect(node_meta.clone(), rpc_settings.clone()),
            retry.membership,
        )
        .await
    }

    async fn connect(
        node_meta: NodeMeta,
        settings: NetworkConfig,
    ) -> Result<Channel> {
        let addr = address_str(&node_meta.ip, node_meta.port as u16);
        Endpoint::try_from(addr.clone())?
            .connect_timeout(Duration::from_millis(settings.connect_timeout_in_ms))
            .timeout(Duration::from_millis(settings.request_timeout_in_ms))
            .tcp_keepalive(Some(Duration::from_secs(settings.tcp_keepalive_in_secs)))
            .http2_keep_alive_interval(Duration::from_secs(settings.http2_keep_alive_interval_in_secs))
            .keep_alive_timeout(Duration::from_secs(settings.http2_keep_alive_timeout_in_secs))
            .initial_connection_window_size(settings.initial_connection_window_size) // 5MB initial connection window
            .initial_stream_window_size(settings.initial_stream_window_size) // 2MB initial stream window
            .connect()
            .await
            .map_err(|err| {
                error!("connect to {} failed: {}", &addr, err);
                eprintln!("{:?}", err);
                NetworkError::ConnectError.into()
            })
    }
    #[cfg(test)]
    pub(crate) fn set_peer_channel(
        &self,
        node_id: u32,
        address: ChannelWithAddress,
    ) {
        self.channels.insert(node_id, address);
    }
}
