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
#[allow(dead_code)]
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

    /// Common TCP setting for all connection types
    #[serde(default = "default_tcp_nodelay")]
    pub tcp_nodelay: bool,

    /// I/O buffer size in bytes for all connections
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
}

/// Low-level network parameters for a specific connection type
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ConnectionParams {
    /// TCP connect timeout in milliseconds
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_in_ms: u64,

    /// gRPC request completion timeout in milliseconds
    #[serde(default = "default_request_timeout")]
    pub request_timeout_in_ms: u64,

    /// Max concurrent requests per connection
    #[serde(default = "default_concurrency_limit")]
    pub concurrency_limit: usize,

    /// HTTP2 SETTINGS_MAX_CONCURRENT_STREAMS
    #[serde(default = "default_max_streams")]
    pub max_concurrent_streams: u32,

    /// TCP keepalive in seconds (None to disable)
    #[serde(default = "default_tcp_keepalive")]
    pub tcp_keepalive_in_secs: u64,

    /// HTTP2 keepalive ping interval in seconds
    #[serde(default = "default_h2_keepalive_interval")]
    pub http2_keep_alive_interval_in_secs: u64,

    /// HTTP2 keepalive timeout in seconds
    #[serde(default = "default_h2_keepalive_timeout")]
    pub http2_keep_alive_timeout_in_secs: u64,

    /// HTTP2 max frame size in bytes
    #[serde(default = "default_max_frame_size")]
    pub max_frame_size: u32,

    /// Initial connection-level flow control window in bytes
    #[serde(default = "default_conn_window_size")]
    pub connection_window_size: u32,

    /// Initial stream-level flow control window in bytes
    #[serde(default = "default_stream_window_size")]
    pub stream_window_size: u32,

    /// Enable HTTP2 adaptive window sizing
    #[serde(default = "default_adaptive_window")]
    pub adaptive_window: bool,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            control: default_control_params(),
            data: default_data_params(),
            bulk: default_bulk_params(),
            tcp_nodelay: default_tcp_nodelay(),
            buffer_size: default_buffer_size(),
        }
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
        if !self.adaptive_window {
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
        }

        Ok(())
    }
}

// Default configuration profiles for each connection type

fn default_control_params() -> ConnectionParams {
    ConnectionParams {
        connect_timeout_in_ms: 20,             // Fast failure for leader elections
        request_timeout_in_ms: 100,            // Strict heartbeat timing
        concurrency_limit: 1024,               // Moderate concurrency
        max_concurrent_streams: 100,           // Stream limit for control operations
        tcp_keepalive_in_secs: 300,            // 5 minute TCP keepalive
        http2_keep_alive_interval_in_secs: 30, // Frequent pings
        http2_keep_alive_timeout_in_secs: 5,   // Short timeout
        max_frame_size: default_max_frame_size(),
        connection_window_size: 1_048_576, // 1MB connection window
        stream_window_size: 262_144,       // 256KB stream window
        adaptive_window: false,            // Predictable behavior
    }
}

fn default_data_params() -> ConnectionParams {
    ConnectionParams {
        connect_timeout_in_ms: 50,              // Balance speed and reliability
        request_timeout_in_ms: 500,             // Accommodate log batches
        concurrency_limit: 8192,                // High concurrency for parallel logs
        max_concurrent_streams: 500,            // More streams for pipelining
        tcp_keepalive_in_secs: 600,             // 10 minute TCP keepalive
        http2_keep_alive_interval_in_secs: 120, // Moderate ping interval
        http2_keep_alive_timeout_in_secs: 30,   // Longer grace period
        max_frame_size: default_max_frame_size(),
        connection_window_size: 6_291_456, // 6MB connection window
        stream_window_size: 1_048_576,     // 1MB stream window
        adaptive_window: true,             // Optimize for varying loads
    }
}

fn default_bulk_params() -> ConnectionParams {
    ConnectionParams {
        connect_timeout_in_ms: 500000,  // Allow for slow bulk connections
        request_timeout_in_ms: 5000000, // Disable request timeout
        concurrency_limit: 4,           // Limit parallel bulk transfers
        max_concurrent_streams: 2,      // Minimal stream concurrency
        tcp_keepalive_in_secs: 3600,    // Long-lived connections
        http2_keep_alive_interval_in_secs: 600, // 10 minute pings
        http2_keep_alive_timeout_in_secs: 60, // 1 minute timeout
        max_frame_size: 16_777_215,     // Max allowed frame size
        connection_window_size: 67_108_864, // 64MB connection window
        stream_window_size: 16_777_216, // 16MB stream window
        adaptive_window: false,         // Stable throughput
    }
}

// Preserve existing default helpers for fallback
fn default_connect_timeout() -> u64 {
    20
}
fn default_request_timeout() -> u64 {
    100
}
fn default_concurrency_limit() -> usize {
    256
}
fn default_max_streams() -> u32 {
    500
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
fn default_max_frame_size() -> u32 {
    16_777_215
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
fn default_adaptive_window() -> bool {
    false
}
