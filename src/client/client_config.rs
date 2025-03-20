#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub bootstrap_urls: Vec<String>,

    pub connect_timeout_in_ms: u64,
    pub request_timeout_in_ms: u64,
    pub concurrency_limit_per_connection: usize,
    pub tcp_keepalive_in_secs: u64,
    pub http2_keep_alive_interval_in_secs: u64,
    pub http2_keep_alive_timeout_in_secs: u64,
    pub max_frame_size: usize,
    pub initial_connection_window_size: u32,
    pub initial_stream_window_size: u32,
    pub buffer_size: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            bootstrap_urls: vec![format!("http://127.0.0.1:9081")],
            connect_timeout_in_ms: 100,
            request_timeout_in_ms: 100,
            concurrency_limit_per_connection: 8192,
            tcp_keepalive_in_secs: 3600,
            http2_keep_alive_interval_in_secs: 300,
            http2_keep_alive_timeout_in_secs: 20,
            max_frame_size: 12582912,
            initial_connection_window_size: 12582912,
            initial_stream_window_size: 12582912,
            buffer_size: 65536,
        }
    }
}
