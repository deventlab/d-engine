//! Manages persistent RPC connections between cluster peers.
//!
//! This module:
//! - Maintains a connection pool to all cluster nodes
//! - Handles initial connection setup using configured cluster addresses
//! - Provides access to active peer channels for Raft RPC communication
//! - Implements connection health checks and reconnection logic
//!
//! The channel connections are established at cluster startup based on the initial
//! configuration and must be properly initialized before `raft_membership` can operate.
//! This layer abstracts network implementation details from the consensus algorithm.
//!
use super::{ChannelWithAddress, PeerChannels, PeerChannelsFactory};
use crate::{
    grpc::rpc_service::NodeMeta,
    membership::health_checker::{HealthChecker, HealthCheckerApis},
    utils::util::{self, address_str},
    ClusterSettings, Error, Result, RpcConnectionSettings, Settings,
};
use dashmap::DashMap;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use log::{debug, error, info, warn};
use std::{sync::Arc, time::Duration};
use tokio::task;
use tonic::{
    async_trait,
    transport::{Channel, Endpoint},
};

#[derive(Clone)]
pub struct RpcPeerChannels {
    pub(super) node_id: u32,

    pub(super) channels: DashMap<u32, ChannelWithAddress>, //store peers connection
    pub(super) settings: Arc<Settings>,
}

impl PeerChannelsFactory for RpcPeerChannels {
    fn create(node_id: u32, settings: Arc<Settings>) -> Self {
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
    async fn connect_with_peers(&mut self, my_id: u32, settings: Arc<Settings>) -> Result<()> {
        let initial_cluster = &settings.server_settings.initial_cluster;
        info!("Connecting with peers: {:?}", initial_cluster);

        let cluster_size = initial_cluster.len();
        let cluster_settings = settings.cluster_settings.clone();
        let rpc_settings = &settings.rpc_connection_settings;

        let tasks =
            self.spawn_connection_tasks(my_id, initial_cluster, cluster_settings, rpc_settings);
        let channels = self.collect_connections(tasks, cluster_size - 1).await?;

        self.channels = channels;
        Ok(())
    }

    async fn check_cluster_is_ready(&self) -> Result<()> {
        info!("check_cluster_is_ready...");
        let mut tasks = FuturesUnordered::new();

        let settings = self.settings.rpc_connection_settings.clone();
        let cluster_settings = self.settings.cluster_settings.clone();

        let mut peer_ids = Vec::new();
        for peer in self.voting_members().iter() {
            let peer_id = *peer.key();
            debug!("check_cluster_is_ready for peer: {}", peer_id);
            peer_ids.push(peer_id);
            let peer_channel_with_addr = peer.value().clone();
            let addr: String = peer_channel_with_addr.address.clone();

            let settings = settings.clone();
            let cluster_healthcheck_probe_service_name = cluster_settings
                .cluster_healthcheck_probe_service_name
                .clone();

            let task_handle = task::spawn(async move {
                match util::task_with_timeout_and_exponential_backoff(
                    move || {
                        HealthChecker::check_peer_is_ready(
                            addr.clone(),
                            settings.clone(),
                            cluster_healthcheck_probe_service_name.clone(),
                        )
                    },
                    cluster_settings.cluster_healtcheck_max_retries,
                    Duration::from_millis(
                        cluster_settings.cluster_healtcheck_exponential_backoff_duration_in_ms,
                    ),
                    Duration::from_millis(
                        cluster_settings.cluster_healtcheck_timeout_duration_in_ms,
                    ),
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
            return Err(Error::ServerIsNotReadyError);
        }
    }

    fn voting_members(&self) -> DashMap<u32, ChannelWithAddress> {
        self.channels.clone()
    }
}

impl RpcPeerChannels {
    /// Creates a new `RaftMembership` instance.
    // pub fn new(node_id: u32, event_tx: mpsc::Sender<RaftEvent>, settings: Settings) -> Self {
    //     Self {
    //         channels: DashMap::new(),
    //         settings,
    //         node_id,
    //         event_tx,
    //     }
    // }

    /// Spawns asynchronous tasks to connect with each peer.
    fn spawn_connection_tasks(
        &self,
        my_id: u32,
        peers: &[NodeMeta],
        cluster_settings: ClusterSettings,
        rpc_settings: &RpcConnectionSettings,
    ) -> FuturesUnordered<task::JoinHandle<Result<(u32, ChannelWithAddress)>>> {
        let tasks = FuturesUnordered::new();

        for node_meta in peers {
            if node_meta.id == my_id {
                continue; // Skip self
            }

            let task = self.spawn_connection_task(
                node_meta.clone(),
                cluster_settings.clone(),
                rpc_settings,
            );
            tasks.push(task);
        }

        tasks
    }

    /// Spawns a single connection task for a peer.
    fn spawn_connection_task(
        &self,
        node_meta: NodeMeta,
        cluster_settings: ClusterSettings,
        rpc_settings: &RpcConnectionSettings,
    ) -> task::JoinHandle<Result<(u32, ChannelWithAddress)>> {
        let rpc_settings = rpc_settings.clone();
        task::spawn(async move {
            let channel =
                Self::connect_with_retry(&node_meta, &cluster_settings, &rpc_settings).await?;
            let address = address_str(&node_meta.ip, node_meta.port as u16);

            debug!(
                "Successfully connected with ({}:{})",
                node_meta.ip, node_meta.port
            );
            Ok((node_meta.id, ChannelWithAddress { address, channel }))
        })
    }

    /// Collects results from connection tasks and validates success count.
    async fn collect_connections(
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
            Err(Error::ConnectError)
        } else {
            Ok(channels)
        }
    }

    /// Attempts to connect to a peer with retries and exponential backoff.
    async fn connect_with_retry(
        node_meta: &NodeMeta,
        cluster_settings: &ClusterSettings,
        rpc_settings: &RpcConnectionSettings,
    ) -> Result<Channel> {
        util::task_with_timeout_and_exponential_backoff(
            || Self::connect(node_meta.clone(), rpc_settings.clone()),
            cluster_settings.cluster_membership_sync_max_retries,
            Duration::from_millis(
                cluster_settings.cluster_membership_sync_exponential_backoff_duration_in_ms,
            ),
            Duration::from_millis(cluster_settings.cluster_membership_sync_timeout_duration_in_ms),
        )
        .await
    }

    async fn connect(node_meta: NodeMeta, settings: RpcConnectionSettings) -> Result<Channel> {
        let addr = address_str(&node_meta.ip, node_meta.port as u16);
        Endpoint::try_from(addr.clone())?
            .connect_timeout(Duration::from_millis(settings.connect_timeout_in_ms))
            .timeout(Duration::from_millis(settings.request_timeout_in_ms))
            .tcp_keepalive(Some(Duration::from_secs(settings.tcp_keepalive_in_secs)))
            .http2_keep_alive_interval(Duration::from_secs(
                settings.http2_keep_alive_interval_in_secs,
            ))
            .keep_alive_timeout(Duration::from_secs(
                settings.http2_keep_alive_timeout_in_secs,
            ))
            .initial_connection_window_size(settings.initial_connection_window_size) // 5MB initial connection window
            .initial_stream_window_size(settings.initial_stream_window_size) // 2MB initial stream window
            .connect()
            .await
            .map_err(|err| {
                error!("connect to {} failed: {}", &addr, err);
                eprintln!("{:?}", err);
                Error::ConnectError
            })
    }
    #[cfg(test)]
    pub(crate) fn set_peer_channel(&self, node_id: u32, address: ChannelWithAddress) {
        self.channels.insert(node_id, address);
    }
}
