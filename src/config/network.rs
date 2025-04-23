use config::ConfigError;
use serde::Deserialize;
use serde::Serialize;

use crate::Error;
use crate::Result;

/// Network communication configuration for gRPC/HTTP2 transport
///
/// Provides fine-grained control over low-level network parameters
#[derive(Debug, Serialize, Deserialize, Clone)]
#[allow(dead_code)]
pub struct NetworkConfig {
    /// Timeout for establishing TCP connections in milliseconds
    /// Default: 20ms (suitable for LAN environments)
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_in_ms: u64,

    /// Maximum duration for completing gRPC requests in milliseconds
    /// Default: 100ms (adjust based on RPC complexity)
    #[serde(default = "default_request_timeout")]
    pub request_timeout_in_ms: u64,

    /// Maximum concurrent requests per connection
    /// Default: 8192 (matches typical gRPC server settings)
    #[serde(default = "default_concurrency_limit")]
    pub concurrency_limit_per_connection: usize,

    /// HTTP2 SETTINGS_MAX_CONCURRENT_STREAMS value
    /// Default: 100 (controls concurrent streams per connection)
    #[serde(default = "default_max_streams")]
    pub max_concurrent_streams: u32,

    /// Enable TCP_NODELAY to disable Nagle's algorithm
    /// Default: true (recommended for low-latency scenarios)
    #[serde(default = "default_tcp_nodelay")]
    pub tcp_nodelay: bool,

    /// TCP keepalive duration in seconds
    /// Default: 3600s (1 hour, OS may enforce minimum values)
    #[serde(default = "default_tcp_keepalive")]
    pub tcp_keepalive_in_secs: u64,

    /// HTTP2 keepalive ping interval in seconds
    /// Default: 300s (5 minutes)
    #[serde(default = "default_h2_keepalive_interval")]
    pub http2_keep_alive_interval_in_secs: u64,

    /// HTTP2 keepalive timeout in seconds
    /// Default: 20s (must be < interval)
    #[serde(default = "default_h2_keepalive_timeout")]
    pub http2_keep_alive_timeout_in_secs: u64,

    /// Maximum HTTP2 frame size in bytes
    /// Default: 12MB (12582912 bytes)
    #[serde(default = "default_max_frame_size")]
    pub max_frame_size: u32,

    /// Initial connection-level flow control window in bytes
    /// Default: 12MB (12582912 bytes)
    #[serde(default = "default_conn_window_size")]
    pub initial_connection_window_size: u32,

    /// Initial per-stream flow control window in bytes
    /// Default: 2MB (2097152 bytes)
    #[serde(default = "default_stream_window_size")]
    pub initial_stream_window_size: u32,

    /// I/O buffer size in bytes
    /// Default: 64KB (65536 bytes)
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,

    /// Enable adaptive flow control window sizing
    /// When enabled, overrides initial window size settings
    /// Default: false (use fixed window sizes)
    #[serde(default = "default_adaptive_window")]
    pub http2_adaptive_window: bool,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            connect_timeout_in_ms: default_connect_timeout(),
            request_timeout_in_ms: default_request_timeout(),
            concurrency_limit_per_connection: default_concurrency_limit(),
            max_concurrent_streams: default_max_streams(),
            tcp_nodelay: default_tcp_nodelay(),
            http2_adaptive_window: default_adaptive_window(),
            tcp_keepalive_in_secs: default_tcp_keepalive(),
            http2_keep_alive_interval_in_secs: default_h2_keepalive_interval(),
            http2_keep_alive_timeout_in_secs: default_h2_keepalive_timeout(),
            max_frame_size: default_max_frame_size(),
            initial_connection_window_size: default_conn_window_size(),
            initial_stream_window_size: default_stream_window_size(),
            buffer_size: default_buffer_size(),
        }
    }
}
impl NetworkConfig {
    /// Validates network configuration consistency and safety
    /// Returns Error::InvalidConfig with detailed message if any rule fails
    pub fn validate(&self) -> Result<()> {
        // 1. Validate timeouts
        if self.connect_timeout_in_ms == 0 {
            return Err(Error::Config(ConfigError::Message(
                "Connection timeout must be greater than 0".into(),
            )));
        }

        if self.request_timeout_in_ms <= self.connect_timeout_in_ms {
            return Err(Error::Config(ConfigError::Message(format!(
                "Request timeout {}ms must exceed connection timeout {}ms",
                self.request_timeout_in_ms, self.connect_timeout_in_ms
            ))));
        }

        // 2. Validate HTTP2 keepalive relationship
        if self.http2_keep_alive_timeout_in_secs >= self.http2_keep_alive_interval_in_secs {
            return Err(Error::Config(ConfigError::Message(format!(
                "HTTP2 keepalive timeout {}s must be shorter than interval {}s",
                self.http2_keep_alive_timeout_in_secs, self.http2_keep_alive_interval_in_secs
            ))));
        }

        // 3. Validate concurrency limits
        if self.concurrency_limit_per_connection == 0 {
            return Err(Error::Config(ConfigError::Message(
                "Concurrency limit per connection must be > 0".into(),
            )));
        }

        if self.max_concurrent_streams == 0 {
            return Err(Error::Config(ConfigError::Message(
                "Max concurrent streams must be > 0".into(),
            )));
        }

        // 4. Validate HTTP2 frame size limits
        const MAX_FRAME_SIZE_LIMIT: u32 = 16_777_215; // 2^24-1 per spec
        if self.max_frame_size > MAX_FRAME_SIZE_LIMIT {
            return Err(Error::Config(ConfigError::Message(format!(
                "Max frame size {} exceeds protocol limit {}",
                self.max_frame_size, MAX_FRAME_SIZE_LIMIT
            ))));
        }

        // 5. Validate window sizes when adaptive window is disabled
        if !self.http2_adaptive_window {
            const MIN_INITIAL_WINDOW: u32 = 65_535; // HTTP2 minimum
            if self.initial_stream_window_size < MIN_INITIAL_WINDOW {
                return Err(Error::Config(ConfigError::Message(format!(
                    "Initial stream window size {} below minimum {}",
                    self.initial_stream_window_size, MIN_INITIAL_WINDOW
                ))));
            }

            if self.initial_connection_window_size < self.initial_stream_window_size {
                return Err(Error::Config(ConfigError::Message(format!(
                    "Connection window {} smaller than stream window {}",
                    self.initial_connection_window_size, self.initial_stream_window_size
                ))));
            }
        }

        // 6. Validate buffer sizing
        if self.buffer_size < 1024 {
            return Err(Error::Config(ConfigError::Message(format!(
                "Buffer size {} too small, minimum 1024 bytes",
                self.buffer_size
            ))));
        }

        Ok(())
    }
}

// Default value implementations
fn default_connect_timeout() -> u64 {
    20
}
fn default_request_timeout() -> u64 {
    100
}
fn default_concurrency_limit() -> usize {
    8192
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
    16777215
}
fn default_conn_window_size() -> u32 {
    12_582_912
}
fn default_stream_window_size() -> u32 {
    2_097_152
}
fn default_buffer_size() -> usize {
    65_536
}
fn default_adaptive_window() -> bool {
    false
}
