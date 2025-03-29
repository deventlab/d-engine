use std::{net::SocketAddr, path::PathBuf};

use serde::Deserialize;

use crate::grpc::rpc_service::NodeMeta;

#[derive(Debug, Deserialize, Clone)]
pub struct ClusterConfig {
    #[serde(default = "default_node_id")]
    pub node_id: u32,

    #[serde(default = "default_listen_addr")]
    pub listen_address: SocketAddr,

    #[serde(default)]
    pub initial_cluster: Vec<NodeMeta>,

    #[serde(default = "default_db_dir")]
    pub db_root_dir: PathBuf,

    #[serde(default = "default_log_dir")]
    pub log_dir: PathBuf,
}

fn default_node_id() -> u32 {
    1
}
fn default_listen_addr() -> SocketAddr {
    "127.0.0.1:9081".parse().unwrap()
}
fn default_db_dir() -> PathBuf {
    PathBuf::from("/tmp/db")
}
fn default_log_dir() -> PathBuf {
    PathBuf::from("/tmp/logs")
}
