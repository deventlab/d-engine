use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tracing::debug;
use tracing::error;
use tracing::info;

use super::ClientApiError;
use crate::ClientConfig;
use crate::utils::address_str;
use d_engine_proto::error::ErrorCode;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::cluster::cluster_management_service_client::ClusterManagementServiceClient;

/// Manages connections to cluster nodes
///
/// Implements connection pooling and leader/follower routing.
/// Automatically handles connection health checks and failover.
#[derive(Clone)]
pub struct ConnectionPool {
    // Tonic's Channel is thread-safe and reference-counted.
    pub(super) leader_conn: Channel,
    pub(super) follower_conns: Vec<Channel>,
    pub(super) config: ClientConfig,
    pub(super) members: Vec<NodeMeta>,
    pub(super) endpoints: Vec<String>,
    pub(super) current_leader_id: Option<u32>,
}

impl ConnectionPool {
    /// Creates new connection pool with bootstrap nodes
    ///
    /// # Implementation Details
    /// 1. Discovers cluster metadata
    /// 2. Establishes leader connection
    /// 3. Creates follower connections
    pub(crate) async fn create(
        endpoints: Vec<String>,
        config: ClientConfig,
    ) -> std::result::Result<Self, ClientApiError> {
        let (leader_conn, follower_conns, members, current_leader_id) =
            Self::build_connections(&endpoints, &config).await?;

        Ok(Self {
            leader_conn,
            follower_conns,
            config,
            members,
            endpoints,
            current_leader_id,
        })
    }

    /// Refreshes cluster connections by reloading metadata and rebuilding channels
    ///
    /// # Behavior
    /// 1. Discovers fresh cluster metadata from provided endpoints
    /// 2. Re-establishes leader connection using latest config
    /// 3. Rebuilds follower connections pool
    #[allow(dead_code)]
    pub(crate) async fn refresh(
        &mut self,
        new_endpoints: Option<Vec<String>>,
    ) -> std::result::Result<(), ClientApiError> {
        if let Some(endpoints) = new_endpoints {
            self.endpoints = endpoints;
        }
        let (leader_conn, follower_conns, members, current_leader_id) =
            Self::build_connections(&self.endpoints, &self.config).await?;

        // Atomic update of fields
        self.leader_conn = leader_conn;
        self.follower_conns = follower_conns;
        self.members = members;
        self.current_leader_id = current_leader_id;

        Ok(())
    }

    /// Create the core logic of the connection pool (extract common code)
    async fn build_connections(
        endpoints: &[String],
        config: &ClientConfig,
    ) -> std::result::Result<(Channel, Vec<Channel>, Vec<NodeMeta>, Option<u32>), ClientApiError>
    {
        // 1. Load cluster metadata
        let membership = Self::load_cluster_metadata(endpoints, config).await?;
        info!("Cluster members discovered: {:?}", membership.nodes);

        // 2. Parse leader and follower addresses
        let (leader_addr, follower_addrs) = Self::parse_cluster_metadata(&membership)?;

        // 3. Establish all connections in parallel
        let leader_future = Self::create_channel(leader_addr, config);
        let follower_futures =
            follower_addrs.into_iter().map(|addr| Self::create_channel(addr, config));

        let (leader_conn, follower_conns) =
            tokio::join!(leader_future, futures::future::join_all(follower_futures));

        // 4. Filter valid connections
        let leader_conn = leader_conn?;
        let follower_conns =
            follower_conns.into_iter().filter_map(std::result::Result::ok).collect();

        Ok((
            leader_conn,
            follower_conns,
            membership.nodes,
            membership.current_leader_id,
        ))
    }

    pub(super) async fn create_channel(
        addr: String,
        config: &ClientConfig,
    ) -> std::result::Result<Channel, ClientApiError> {
        debug!("create_channel, addr = {:?}", &addr);
        Endpoint::try_from(addr)?
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .tcp_keepalive(Some(config.tcp_keepalive))
            .http2_keep_alive_interval(config.http2_keepalive_interval)
            .keep_alive_timeout(config.http2_keepalive_timeout)
            .connect()
            .await
            .map_err(Into::into)
    }
    /// Retrieves active leader connection
    ///
    /// Used for all write operations and linear reads
    pub(crate) fn get_leader(&self) -> Channel {
        self.leader_conn.clone()
    }

    pub(crate) fn get_all_channels(&self) -> Vec<Channel> {
        let mut cloned = self.follower_conns.clone();
        cloned.push(self.leader_conn.clone());
        cloned
    }

    pub(crate) fn get_all_members(&self) -> Vec<NodeMeta> {
        self.members.clone()
    }

    /// Get the current leader ID
    pub(crate) fn get_leader_id(&self) -> Option<u32> {
        self.current_leader_id
    }

    /// Discover cluster metadata by probing nodes
    pub(super) async fn load_cluster_metadata(
        endpoints: &[String],
        config: &ClientConfig,
    ) -> std::result::Result<ClusterMembership, ClientApiError> {
        for addr in endpoints {
            match Self::create_channel(addr.clone(), config).await {
                Ok(channel) => {
                    let mut client = ClusterManagementServiceClient::new(channel);
                    if config.enable_compression {
                        client = client
                            .send_compressed(CompressionEncoding::Gzip)
                            .accept_compressed(CompressionEncoding::Gzip);
                    }
                    match client.get_cluster_metadata(tonic::Request::new(MetadataRequest {})).await
                    {
                        Ok(response) => return Ok(response.into_inner()),
                        Err(e) => {
                            error!("get_cluster_metadata: {:?}", e);
                            // Try next node
                            continue;
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "load_cluster_metadata from addr: {:?}, failed: {:?}",
                        &addr, e
                    );
                    continue;
                } // Connection failed, try next
            }
        }
        Err(ErrorCode::ClusterUnavailable.into())
    }

    /// Extract leader address from metadata using current_leader_id
    pub(super) fn parse_cluster_metadata(
        membership: &ClusterMembership
    ) -> std::result::Result<(String, Vec<String>), ClientApiError> {
        let leader_id = membership.current_leader_id.ok_or(ErrorCode::ClusterUnavailable)?;

        let mut leader_addr = None;
        let mut followers = Vec::new();

        for node in &membership.nodes {
            let addr = address_str(&node.address);
            debug!(
                "parse_cluster_metadata, node_id: {}, addr: {:?}",
                node.id, &addr
            );
            if node.id == leader_id {
                leader_addr = Some(addr);
            } else {
                followers.push(addr);
            }
        }

        leader_addr
            .ok_or(ErrorCode::ClusterUnavailable.into())
            .map(|addr| (addr, followers))
    }
}
