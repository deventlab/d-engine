use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::ToSocketAddrs;
use std::str::FromStr;

use tokio::net::TcpStream;

/// accept ip either like 127.0.0.1 or docker host name: node1
pub(crate) fn address_str(
    ip: &str,
    port: u16,
) -> String {
    let addr: SocketAddr = match Ipv4Addr::from_str(ip) {
        Ok(ipv4) => SocketAddr::V4(SocketAddrV4::new(ipv4, port)),
        Err(_) => {
            // If parsing fails, try resolving the hostname
            (ip, port)
                .to_socket_addrs()
                .map_err(|e| {
                    panic!("Failed to resolve hostname {}: {:?}", ip, e);
                })
                .unwrap()
                .next()
                .expect("No addresses found")
            // panic!("error to call Ipv4Addr::from_str({:?}), {:?}", ip, e);
        }
    };
    format!("http://{}", addr)
}

pub(crate) async fn is_server_ready(addr: &str) -> bool {
    TcpStream::connect(addr).await.is_ok()
}
