use std::time::Duration;

use crate::time::get_now_as_u32;

/// Client configuration parameters for network connection management
///
/// Encapsulates all tunable settings for establishing and maintaining
/// network connections, including timeouts, keepalive policies,
/// and performance-related options.
///
/// # Key Configuration Areas
/// - Connection establishment (TCP handshake timeout)
/// - Request/response lifecycle control
/// - HTTP/2 protocol optimization
/// - Network efficiency settings (compression, frame sizing)
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Client id representation
    pub id: u32,

    /// Maximum time to wait for establishing a TCP connection
    /// Default: 1 second
    pub connect_timeout: Duration,

    /// Maximum time to wait for a complete RPC response
    /// Default: 3 seconds
    pub request_timeout: Duration,

    /// TCP keepalive duration for idle connections
    /// Default: 5 minutes (300s)
    pub tcp_keepalive: Duration,

    /// Interval for HTTP/2 keepalive pings
    /// Default: 1 minute (60s)
    pub http2_keepalive_interval: Duration,

    /// Timeout for HTTP/2 keepalive pings
    /// Default: 20 seconds
    pub http2_keepalive_timeout: Duration,

    /// Maximum size of a single HTTP/2 frame in bytes
    /// Recommended: 1MB (1048576) to 16MB (16777215)
    /// Default: 1MB
    pub max_frame_size: u32,

    /// Enable Gzip compression for network traffic
    /// Tradeoff: Reduces bandwidth usage at the cost of CPU
    /// Default: true (enabled)
    pub enable_compression: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            id: get_now_as_u32(),
            connect_timeout: Duration::from_millis(1000),
            request_timeout: Duration::from_millis(3000),
            tcp_keepalive: Duration::from_secs(300),
            http2_keepalive_interval: Duration::from_secs(60),
            http2_keepalive_timeout: Duration::from_secs(20),
            max_frame_size: 1 << 20, // 1MB
            enable_compression: true,
        }
    }
}
