use crate::proto::rpc_service_client::RpcServiceClient;
use crate::proto::MetadataRequest;
use crate::proto::NodeMeta;
use crate::ClientConfig;
use crate::Error;
use crate::Result;
use log::debug;
use log::error;
use log::info;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use super::ClientApiError;

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
    ) -> Result<Self> {
        let (leader_conn, follower_conns, members) = Self::build_connections(&endpoints, &config).await?;

        Ok(Self {
            leader_conn,
            follower_conns,
            config,
            members,
            endpoints,
        })
    }

    /// Refreshes cluster connections by reloading metadata and rebuilding channels
    ///
    /// # Behavior
    /// 1. Discovers fresh cluster metadata from provided endpoints
    /// 2. Re-establishes leader connection using latest config
    /// 3. Rebuilds follower connections pool
    ///
    pub(crate) async fn refresh(
        &mut self,
        new_endpoints: Option<Vec<String>>,
    ) -> Result<()> {
        if let Some(endpoints) = new_endpoints {
            self.endpoints = endpoints;
        }
        let (leader_conn, follower_conns, members) = Self::build_connections(&self.endpoints, &self.config).await?;

        // Atomic update of fields
        self.leader_conn = leader_conn;
        self.follower_conns = follower_conns;
        self.members = members;

        Ok(())
    }

    /// Create the core logic of the connection pool (extract common code)
    async fn build_connections(
        endpoints: &[String],
        config: &ClientConfig,
    ) -> Result<(Channel, Vec<Channel>, Vec<NodeMeta>)> {
        // 1. Load cluster metadata
        let members = Self::load_cluster_metadata(endpoints, config).await?;
        info!("Cluster members discovered: {:?}", members);

        // 2. Parse leader and follower addresses
        let (leader_addr, follower_addrs) = Self::parse_cluster_metadata(&members)?;

        // 3. Establish all connections in parallel
        let leader_future = Self::create_channel(leader_addr, config);
        let follower_futures = follower_addrs
            .into_iter()
            .map(|addr| Self::create_channel(addr, config));

        let (leader_conn, follower_conns) = tokio::join!(leader_future, futures::future::join_all(follower_futures));

        // 4. Filter valid connections
        let leader_conn = leader_conn?;
        let follower_conns = follower_conns.into_iter().filter_map(Result::ok).collect();

        Ok((leader_conn, follower_conns, members))
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

    /// Discover cluster metadata by probing nodes
    pub(super) async fn load_cluster_metadata(
        endpoints: &[String],
        config: &ClientConfig,
    ) -> Result<Vec<NodeMeta>> {
        for addr in endpoints {
            match Self::create_channel(addr.clone(), config).await {
                Ok(channel) => {
                    let mut client = RpcServiceClient::new(channel);
                    if config.enable_compression {
                        client = client
                            .send_compressed(CompressionEncoding::Gzip)
                            .accept_compressed(CompressionEncoding::Gzip);
                    }
                    match client
                        .get_cluster_metadata(tonic::Request::new(MetadataRequest {}))
                        .await
                    {
                        Ok(response) => return Ok(response.into_inner().nodes),
                        Err(e) => {
                            error!("get_cluster_metadata: {:?}", e);
                            // Try next node
                            continue;
                        }
                    }
                }
                Err(e) => {
                    error!("load_cluster_metadata from addr: {:?}, failed: {:?}", &addr, e);
                    continue;
                } // Connection failed, try next
            }
        }
        Err(Error::ClusterMembershipNotFound)
    }

    /// Extract leader address from metadata
    pub(super) fn parse_cluster_metadata(nodes: &Vec<NodeMeta>) -> Result<(String, Vec<String>)> {
        let mut leader_addr = None;
        let mut followers = Vec::new();

        for node in nodes {
            let addr = format!("http://{}:{}", node.ip, node.port);
            debug!("parse_cluster_metadata, addr: {:?}", &addr);
            if node.role == crate::LEADER {
                leader_addr = Some(addr);
            } else {
                followers.push(addr);
            }
        }

        leader_addr.map(|addr| (addr, followers)).ok_or(Error::NoLeaderFound)
    }
}
