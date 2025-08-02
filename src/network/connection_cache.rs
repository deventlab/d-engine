use std::time::Duration;
use std::time::Instant;

use dashmap::DashMap;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tracing::debug;
use tracing::trace;

use crate::ConnectionType;
use crate::NetworkConfig;
use crate::NetworkError;
use crate::Result;

/// Cached gRPC channel with metadata
#[derive(Clone)]
pub(crate) struct CachedChannel {
    pub(crate) channel: Channel,
    pub(crate) address: String,
    pub(crate) last_used: Instant,
}

/// Thread-safe connection cache manager
#[derive(Clone)]
pub(crate) struct ConnectionCache {
    // (node_id, connection_type) -> CachedChannel
    pub(crate) cache: DashMap<(u32, ConnectionType), CachedChannel>,
    config: NetworkConfig,
}

impl ConnectionCache {
    pub(crate) fn new(config: NetworkConfig) -> Self {
        Self {
            cache: DashMap::new(),
            config,
        }
    }

    /// Get or create a channel with caching and reconnection logic
    pub(crate) async fn get_channel(
        &self,
        node_id: u32,
        conn_type: ConnectionType,
        current_address: String,
    ) -> Result<Channel> {
        trace!("Current address: {}", current_address);
        let key = (node_id, conn_type.clone());

        // Fast path: check if valid channel exists
        if let Some(mut entry) = self.cache.get_mut(&key) {
            let cached = entry.value_mut();

            // Validate channel state and address
            if cached.address == current_address {
                // Update last used timestamp
                cached.last_used = Instant::now();
                return Ok(cached.channel.clone());
            }
        }

        // Slow path: create new channel and update cache
        debug!(node_id, conn_type=?conn_type, "Establishing new gRPC connection");
        let channel = self.create_channel(current_address.clone(), conn_type).await?;

        trace!(?key, "Cache updated: address: {}", current_address.clone());
        self.cache.insert(
            key,
            CachedChannel {
                channel: channel.clone(),
                address: current_address,
                last_used: Instant::now(),
            },
        );

        Ok(channel)
    }

    /// Create pre-configured endpoint
    async fn create_channel(
        &self,
        address: String,
        conn_type: ConnectionType,
    ) -> Result<Channel> {
        let params = match conn_type {
            ConnectionType::Control => self.config.control.clone(),
            ConnectionType::Data => self.config.data.clone(),
            ConnectionType::Bulk => self.config.bulk.clone(),
        };

        Endpoint::try_from(address)?
            .connect_timeout(Duration::from_millis(params.connect_timeout_in_ms))
            .timeout(Duration::from_millis(params.request_timeout_in_ms))
            .tcp_keepalive(Some(Duration::from_secs(params.tcp_keepalive_in_secs)))
            .http2_keep_alive_interval(Duration::from_secs(
                params.http2_keep_alive_interval_in_secs,
            ))
            .keep_alive_timeout(Duration::from_secs(params.http2_keep_alive_timeout_in_secs))
            .initial_connection_window_size(params.connection_window_size)
            .initial_stream_window_size(params.stream_window_size)
            .connect()
            .await
            .map_err(|e| NetworkError::ConnectError(e.to_string()).into())
    }

    /// Remove all connections for a node
    pub(crate) fn remove_node(
        &self,
        node_id: u32,
    ) {
        self.cache.retain(|(id, _), _| *id != node_id);
    }
}
