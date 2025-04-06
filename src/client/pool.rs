use log::debug;
use log::error;
use log::info;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::grpc::rpc_service::rpc_service_client::RpcServiceClient;
use crate::grpc::rpc_service::MetadataRequest;
use crate::grpc::rpc_service::NodeMeta;
use crate::ClientConfig;
use crate::Error;
use crate::Result;

#[derive(Clone)]
pub struct ConnectionPool {
    // Tonic's Channel is thread-safe and reference-counted.
    pub leader_conn: Channel,
    pub follower_conns: Vec<Channel>,
    pub config: ClientConfig,
    pub members: Vec<NodeMeta>,
}

impl ConnectionPool {
    pub async fn new(
        endpoints: Vec<String>,
        config: ClientConfig,
    ) -> Result<Self> {
        let members = Self::load_cluster_metadata(&endpoints, &config).await?;
        info!("Retrieved members: {:?}", &members);
        let (leader_addr, followers) = Self::parse_cluster_metadata(&members)?;

        let leader_conn = Self::create_channel(leader_addr, &config).await?;
        let mut follower_conns = Vec::new();

        // Build follower connections asynchronously
        let follower_futures = followers.into_iter().map(|addr| Self::create_channel(addr, &config));
        let connections = futures::future::join_all(follower_futures).await;

        for conn in connections {
            if let Ok(channel) = conn {
                follower_conns.push(channel);
            }
        }
        Ok(Self {
            leader_conn,
            follower_conns,
            config,
            members,
        })
    }

    pub(super) async fn create_channel(
        addr: String,
        config: &ClientConfig,
    ) -> Result<Channel> {
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

    pub fn get_leader(&self) -> Channel {
        self.leader_conn.clone()
    }

    pub fn get_all_channels(&self) -> Vec<Channel> {
        let mut cloned = self.follower_conns.clone();
        cloned.push(self.leader_conn.clone());
        cloned
    }

    pub fn get_all_members(&self) -> Vec<NodeMeta> {
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
                        Err(_) => continue, // Try next node
                    }
                }
                Err(e) => {
                    error!("load_cluster_metadata from addr: {:?}, failed: {:?}", &addr, e);
                    continue;
                } // Connection failed, try next
            }
        }
        Err(Error::ClusterUnavailable)
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
