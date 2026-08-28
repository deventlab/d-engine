use config::ConfigError;
use serde::Deserialize;
use serde::Serialize;

use crate::Error;
use crate::Result;

/// Hierarchical network configuration for different Raft connection types
///
/// Provides specialized tuning for three distinct communication patterns:
/// - Control plane: Election/heartbeat (low bandwidth, high priority)
/// - Data plane: Log replication (balanced throughput/latency)
/// - Bulk transfer: Snapshotting (high bandwidth, tolerant to latency)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NetworkConfig {
    /// Configuration for control plane connections (leader election/heartbeats)
    #[serde(default = "default_control_params")]
    pub control: ConnectionParams,

    /// Configuration for data plane connections (log replication)
    #[serde(default = "default_data_params")]
    pub data: ConnectionParams,

    /// Configuration for bulk transfer connections (snapshot installation)
    #[serde(default = "default_bulk_params")]
    pub bulk: ConnectionParams,

    #[serde(default = "default_server_transport_params")]
    pub server: ServerTransportParams,

    /// Common TCP setting for all connection types
    #[serde(default = "default_tcp_nodelay")]
    pub tcp_nodelay: bool,

    /// I/O buffer size in bytes for all connections
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
}

/// Low-level network parameters for a specific connection type
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConnectionParams {
    /// TCP connect timeout in milliseconds
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_in_ms: u64,

    /// gRPC request completion timeout in milliseconds
    #[serde(default = "default_request_timeout")]
    pub request_timeout_in_ms: u64,

    /// TCP keepalive in seconds (None to disable)
    #[serde(default = "default_tcp_keepalive")]
    pub tcp_keepalive_in_secs: u64,

    /// HTTP2 keepalive ping interval in seconds
    #[serde(default = "default_h2_keepalive_interval")]
    pub http2_keep_alive_interval_in_secs: u64,

    /// HTTP2 keepalive timeout in seconds
    #[serde(default = "default_h2_keepalive_timeout")]
    pub http2_keep_alive_timeout_in_secs: u64,

    /// Initial connection-level flow control window in bytes
    #[serde(default = "default_conn_window_size")]
    pub connection_window_size: u32,

    /// Initial stream-level flow control window in bytes
    #[serde(default = "default_stream_window_size")]
    pub stream_window_size: u32,
}

impl Default for ConnectionParams {
    /// Must stay in lockstep with the `#[serde(default = "...")]` helper on each field
    /// above — a `#[derive(Default)]` here would silently diverge from them (every
    /// field would fall back to its Rust zero value, e.g. `connection_window_size: 0`,
    /// instead of `default_conn_window_size()`'s `20_971_520`), so any caller reaching
    /// for `ConnectionParams::default()` — not just serde deserializing a partial
    /// config — gets the same values a missing field would.
    fn default() -> Self {
        Self {
            connect_timeout_in_ms: default_connect_timeout(),
            request_timeout_in_ms: default_request_timeout(),
            tcp_keepalive_in_secs: default_tcp_keepalive(),
            http2_keep_alive_interval_in_secs: default_h2_keepalive_interval(),
            http2_keep_alive_timeout_in_secs: default_h2_keepalive_timeout(),
            connection_window_size: default_conn_window_size(),
            stream_window_size: default_stream_window_size(),
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            control: default_control_params(),
            data: default_data_params(),
            bulk: default_bulk_params(),
            tcp_nodelay: default_tcp_nodelay(),
            buffer_size: default_buffer_size(),
            server: default_server_transport_params(),
        }
    }
}

/// Transport-level parameters for the gRPC *server* (receiving side). Unlike
/// `ConnectionParams` (which the client uses per-plane — control/data/bulk each
/// dialing out with different tuning), the server has exactly one listener
/// serving every RPC type uniformly — there's no "plane" to pick per incoming
/// connection. This type is deliberately separate from `ConnectionParams` so
/// the server never accidentally "borrows" one plane's client-tuned values
/// (see PR #442 review: the server used to borrow `control`'s 100ms timeout
/// for every RPC, including multi-minute snapshot transfers).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerTransportParams {
    /// Max concurrent requests per connection
    #[serde(default = "default_server_concurrency_limit")]
    pub concurrency_limit_per_connection: usize,

    /// HTTP2 SETTINGS_MAX_CONCURRENT_STREAMS, server-wide
    #[serde(default = "default_server_max_concurrent_streams")]
    pub max_concurrent_streams: u32,

    /// Max pending-accept streams remotely reset before h2 closes the connection
    /// (mitigates HTTP/2 Rapid Reset false-positives)
    #[serde(default = "default_max_pending_accept_reset_streams")]
    pub max_pending_accept_reset_streams: usize,

    /// HTTP2 keepalive ping interval, server-wide
    #[serde(default = "default_server_h2_keepalive_interval")]
    pub http2_keepalive_interval_in_secs: u64,

    /// HTTP2 keepalive timeout, server-wide — must be generous enough that a
    /// server busy processing a legitimate large chunk doesn't miss the PING
    /// ack window and get its connection killed mid-transfer.
    #[serde(default = "default_server_h2_keepalive_timeout")]
    pub http2_keepalive_timeout_in_secs: u64,

    /// Initial HTTP2 connection-level flow control window
    #[serde(default = "default_conn_window_size")]
    pub initial_connection_window_size: u32,

    /// Initial HTTP2 stream-level flow control window
    #[serde(default = "default_stream_window_size")]
    pub initial_stream_window_size: u32,

    /// Inbound message size ceiling (bytes), applied per-service — the one
    /// tonic mechanism that genuinely supports per-RPC-type differentiation
    /// (unlike the other fields here, which are connection/transport-level
    /// and necessarily apply uniformly to every service on this listener).
    #[serde(default = "default_max_decoding_message_size")]
    pub max_decoding_message_size: usize,
}

impl ServerTransportParams {
    /// Validates server transport parameter sanity
    pub(crate) fn validate(&self) -> Result<()> {
        if self.concurrency_limit_per_connection == 0 {
            return Err(Error::Config(ConfigError::Message(
                "server concurrency_limit_per_connection must be > 0".to_string(),
            )));
        }

        if self.max_concurrent_streams == 0 {
            return Err(Error::Config(ConfigError::Message(
                "server max_concurrent_streams must be > 0".to_string(),
            )));
        }

        if self.http2_keepalive_timeout_in_secs >= self.http2_keepalive_interval_in_secs {
            return Err(Error::Config(ConfigError::Message(format!(
                "server keepalive timeout {}s must be < interval {}s",
                self.http2_keepalive_timeout_in_secs, self.http2_keepalive_interval_in_secs
            ))));
        }

        const MIN_WINDOW: u32 = 65535; // HTTP2 spec minimum
        if self.initial_stream_window_size < MIN_WINDOW {
            return Err(Error::Config(ConfigError::Message(format!(
                "server stream window size {} below minimum {}",
                self.initial_stream_window_size, MIN_WINDOW
            ))));
        }

        if self.initial_connection_window_size < self.initial_stream_window_size {
            return Err(Error::Config(ConfigError::Message(format!(
                "server connection window {} smaller than stream window {}",
                self.initial_connection_window_size, self.initial_stream_window_size
            ))));
        }

        if self.max_decoding_message_size == 0 {
            return Err(Error::Config(ConfigError::Message(
                "server max_decoding_message_size must be > 0".to_string(),
            )));
        }

        Ok(())
    }
}

impl Default for ServerTransportParams {
    fn default() -> Self {
        Self {
            concurrency_limit_per_connection: default_server_concurrency_limit(),
            max_concurrent_streams: default_server_max_concurrent_streams(),
            max_pending_accept_reset_streams: default_max_pending_accept_reset_streams(),
            http2_keepalive_interval_in_secs: default_server_h2_keepalive_interval(),
            http2_keepalive_timeout_in_secs: default_server_h2_keepalive_timeout(),
            initial_connection_window_size: default_conn_window_size(),
            initial_stream_window_size: default_stream_window_size(),
            max_decoding_message_size: default_max_decoding_message_size(),
        }
    }
}

fn default_server_concurrency_limit() -> usize {
    1024
}
fn default_server_max_concurrent_streams() -> u32 {
    1024
}
fn default_server_h2_keepalive_interval() -> u64 {
    60
}
fn default_server_h2_keepalive_timeout() -> u64 {
    30
}

fn default_server_transport_params() -> ServerTransportParams {
    ServerTransportParams {
        concurrency_limit_per_connection: default_server_concurrency_limit(),
        max_concurrent_streams: default_server_max_concurrent_streams(),
        max_pending_accept_reset_streams: default_max_pending_accept_reset_streams(),
        http2_keepalive_interval_in_secs: default_server_h2_keepalive_interval(),
        http2_keepalive_timeout_in_secs: default_server_h2_keepalive_timeout(),
        initial_connection_window_size: default_conn_window_size(),
        initial_stream_window_size: default_stream_window_size(),
        max_decoding_message_size: default_max_decoding_message_size(),
    }
}

impl NetworkConfig {
    /// Validates configuration sanity across all connection types
    pub fn validate(&self) -> Result<()> {
        // Validate common parameters
        if self.buffer_size < 1024 {
            return Err(Error::Config(ConfigError::Message(format!(
                "Buffer size {} too small, minimum 1024 bytes",
                self.buffer_size
            ))));
        }

        // Validate per-connection type parameters
        self.control.validate("control")?;
        self.data.validate("data")?;
        self.bulk.validate("bulk")?;
        self.server.validate()?;

        Ok(())
    }
}

impl ConnectionParams {
    /// Type-specific validation with context for error messages
    pub(crate) fn validate(
        &self,
        conn_type: &str,
    ) -> Result<()> {
        // Timeout validation
        if self.connect_timeout_in_ms == 0 {
            return Err(Error::Config(ConfigError::Message(format!(
                "{conn_type} connection timeout must be > 0",
            ))));
        }

        if self.request_timeout_in_ms != 0
            && self.request_timeout_in_ms <= self.connect_timeout_in_ms
        {
            return Err(Error::Config(ConfigError::Message(format!(
                "{} request timeout {}ms must exceed connect timeout {}ms",
                conn_type, self.request_timeout_in_ms, self.connect_timeout_in_ms
            ))));
        }

        // HTTP2 keepalive validation
        if self.http2_keep_alive_timeout_in_secs >= self.http2_keep_alive_interval_in_secs {
            return Err(Error::Config(ConfigError::Message(format!(
                "{} keepalive timeout {}s must be < interval {}s",
                conn_type,
                self.http2_keep_alive_timeout_in_secs,
                self.http2_keep_alive_interval_in_secs
            ))));
        }

        // Window size validation when not using adaptive windows
        const MIN_WINDOW: u32 = 65535; // HTTP2 spec minimum
        if self.stream_window_size < MIN_WINDOW {
            return Err(Error::Config(ConfigError::Message(format!(
                "{} stream window size {} below minimum {}",
                conn_type, self.stream_window_size, MIN_WINDOW
            ))));
        }

        if self.connection_window_size < self.stream_window_size {
            return Err(Error::Config(ConfigError::Message(format!(
                "{} connection window {} smaller than stream window {}",
                conn_type, self.connection_window_size, self.stream_window_size
            ))));
        }

        Ok(())
    }
}

// Default configuration profiles for each connection type

fn default_control_params() -> ConnectionParams {
    ConnectionParams {
        connect_timeout_in_ms: 20,             // Fast failure for leader elections
        request_timeout_in_ms: 100,            // Strict heartbeat timing
        tcp_keepalive_in_secs: 300,            // 5 minute TCP keepalive
        http2_keep_alive_interval_in_secs: 30, // Frequent pings
        http2_keep_alive_timeout_in_secs: 5,   // Short timeout
        connection_window_size: 1_048_576,     // 1MB connection window
        stream_window_size: 262_144,           // 256KB stream window
    }
}

fn default_data_params() -> ConnectionParams {
    ConnectionParams {
        connect_timeout_in_ms: 50,              // Balance speed and reliability
        request_timeout_in_ms: 500,             // Accommodate log batches
        tcp_keepalive_in_secs: 600,             // 10 minute TCP keepalive
        http2_keep_alive_interval_in_secs: 120, // Moderate ping interval
        http2_keep_alive_timeout_in_secs: 30,   // Longer grace period
        connection_window_size: 6_291_456,      // 6MB connection window
        stream_window_size: 1_048_576,          // 1MB stream window
    }
}

fn default_bulk_params() -> ConnectionParams {
    ConnectionParams {
        connect_timeout_in_ms: 500000,  // Allow for slow bulk connections
        request_timeout_in_ms: 5000000, // Disable request timeout
        tcp_keepalive_in_secs: 3600,    // Long-lived connections
        http2_keep_alive_interval_in_secs: 600, // 10 minute pings
        http2_keep_alive_timeout_in_secs: 60, // 1 minute timeout
        connection_window_size: 67_108_864, // 64MB connection window
        stream_window_size: 16_777_216, // 16MB stream window
    }
}

// Preserve existing default helpers for fallback
fn default_connect_timeout() -> u64 {
    20
}
fn default_request_timeout() -> u64 {
    100
}
fn default_tcp_nodelay() -> bool {
    true
}
fn default_tcp_keepalive() -> u64 {
    3600
}
fn default_h2_keepalive_interval() -> u64 {
    300
}
fn default_h2_keepalive_timeout() -> u64 {
    20
}
fn default_conn_window_size() -> u32 {
    20_971_520 // 20MB
}
fn default_stream_window_size() -> u32 {
    10_485_760 // 10MB
}
fn default_buffer_size() -> usize {
    65_536
}
fn default_max_pending_accept_reset_streams() -> usize {
    1000
}

fn default_max_decoding_message_size() -> usize {
    67_108_864
}
