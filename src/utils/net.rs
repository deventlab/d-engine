use tokio::net::TcpStream;

/// accept ip either like 127.0.0.1 or docker host name: node1
pub(crate) fn address_str(addr: &str) -> String {
    format!("http://{addr}")
}

pub(crate) async fn is_server_ready(addr: &str) -> bool {
    TcpStream::connect(addr).await.is_ok()
}
