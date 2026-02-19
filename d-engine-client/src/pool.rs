use d_engine_proto::error::ErrorCode;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::cluster::cluster_management_service_client::ClusterManagementServiceClient;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tracing::debug;
use tracing::info;

use super::ClientApiError;
use crate::ClientConfig;
use crate::utils::address_str;

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
        // 1. Load cluster metadata + establish verified leader channel atomically.
        //    Returns only when a ready leader is confirmed AND its channel is connectable.
        let (membership, leader_conn) = Self::load_cluster_metadata(endpoints, config).await?;
        info!("Cluster members discovered: {:?}", membership.nodes);

        // 2. Parse follower addresses (leader channel already established above)
        let (_, follower_addrs) = Self::parse_cluster_metadata(&membership)?;

        // 3. Establish follower connections in parallel (best-effort, failures are filtered)
        let follower_conns = futures::future::join_all(
            follower_addrs.into_iter().map(|addr| Self::create_channel(addr, config)),
        )
        .await
        .into_iter()
        .filter_map(std::result::Result::ok)
        .collect();

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

    /// Probe a single endpoint and classify the cluster metadata state.
    ///
    /// Returns:
    /// - `Some(Ok(membership))` — leader is known and present in member list (ready)
    /// - `Some(Err(()))`        — node responded but cluster not yet ready (election in progress
    ///   or stale leader ID); caller should try next node
    /// - `None`                 — node unreachable; caller should try next node
    pub(super) async fn probe_endpoint(
        addr: &str,
        config: &ClientConfig,
    ) -> Option<std::result::Result<ClusterMembership, ()>> {
        let channel = Self::create_channel(addr.to_string(), config).await.ok()?;
        let mut client = ClusterManagementServiceClient::new(channel);
        if config.enable_compression {
            client = client
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
        }
        let membership = client
            .get_cluster_metadata(tonic::Request::new(MetadataRequest {}))
            .await
            .ok()?
            .into_inner();

        // Classify: ready only if leader_id is known AND present in member list
        let ready = membership
            .current_leader_id
            .is_some_and(|leader_id| membership.nodes.iter().any(|n| n.id == leader_id));

        if ready {
            Some(Ok(membership))
        } else {
            debug!(
                "probe_endpoint {}: cluster not ready (leader_id={:?})",
                addr, membership.current_leader_id
            );
            Some(Err(()))
        }
    }

    /// Wait until the cluster has a reachable, noop-committed leader, then return
    /// its metadata and an established leader channel.
    ///
    /// Probe and connect are treated as a single atomic step inside one retry loop
    /// sharing `config.cluster_ready_timeout`. This prevents the TOCTOU race where
    /// probe succeeds but the leader crashes before the channel is established.
    ///
    /// Ready = `current_leader_id` is `Some`, present in member list, AND the leader
    /// channel can be established (TCP connect succeeds).
    pub(super) async fn load_cluster_metadata(
        endpoints: &[String],
        config: &ClientConfig,
    ) -> std::result::Result<(ClusterMembership, Channel), ClientApiError> {
        const RETRY_BACKOFF_MS: u64 = 200;

        let deadline = tokio::time::Instant::now() + config.cluster_ready_timeout;

        loop {
            // Probe every endpoint in this round
            for addr in endpoints {
                if tokio::time::Instant::now() >= deadline {
                    return Err(ErrorCode::ClusterUnavailable.into());
                }
                let membership = match Self::probe_endpoint(addr, config).await {
                    Some(Ok(m)) => m,
                    Some(Err(())) => continue, // election in progress — try next
                    None => continue,          // node unreachable — try next
                };

                // Probe succeeded: try to establish leader channel in the same step.
                // If connect fails (leader crashed between probe and connect), continue
                // to next endpoint — do NOT return error (shared deadline handles timeout).
                let (leader_addr, _) = Self::parse_cluster_metadata(&membership)?;
                match Self::create_channel(leader_addr, config).await {
                    Ok(leader_conn) => return Ok((membership, leader_conn)),
                    Err(e) => {
                        debug!("load_cluster_metadata: leader connect failed ({e:?}), retrying");
                        continue;
                    }
                }
            }

            // Full round completed with no ready+connectable leader — backoff then retry
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(ErrorCode::ClusterUnavailable.into());
            }
            let backoff = std::time::Duration::from_millis(RETRY_BACKOFF_MS).min(remaining);
            debug!(
                "load_cluster_metadata: no ready leader found, retrying in {:?}",
                backoff
            );
            tokio::time::sleep(backoff).await;
        }
    }

    /// Extract leader address from metadata using current_leader_id.
    ///
    /// Precondition: `membership` must be in Ready state (leader_id is Some and
    /// present in member list). Guaranteed by `load_cluster_metadata`.
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
