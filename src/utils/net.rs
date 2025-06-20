use tokio::net::TcpStream;

/// accept ip either like 127.0.0.1 or docker host name: node1
pub(crate) fn address_str(addr: &str) -> String {
    // Strip existing "http://" or "https://" prefixes if duplicated.
    let normalized = addr.trim_start_matches("http://").trim_start_matches("https://");
    // Re-add a single "http://" prefix (or use HTTPS if needed).
    format!("http://{}", normalized)
}

pub(crate) async fn is_server_ready(addr: &str) -> bool {
    TcpStream::connect(addr).await.is_ok()
}
