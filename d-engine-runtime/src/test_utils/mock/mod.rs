mod mock_node_builder;
mod mock_rpc_services;

pub use mock_node_builder::*;
pub use mock_rpc_services::*;

use crate::Node;
use d_engine_core::{MockTypeConfig, Raft};
use d_engine_proto::server::cluster::NodeMeta;
use tokio::sync::watch;

use super::node_config;

pub fn mock_node(
    db_path: &str,
    shutdown_signal: watch::Receiver<()>,
    peers_meta_option: Option<Vec<NodeMeta>>,
) -> Node<MockTypeConfig> {
    let mut node_config = node_config(db_path);
    if let Some(peers_meta) = peers_meta_option {
        node_config.cluster.initial_cluster = peers_meta;
    }
    // Initializing Shutdown Signal
    // let (graceful_tx, graceful_rx) = watch::channel(());
    MockBuilder::new(shutdown_signal).with_node_config(node_config).build_node()
}

pub fn mock_raft(
    db_path: &str,
    shutdown_signal: watch::Receiver<()>,
    peers_meta_option: Option<Vec<NodeMeta>>,
) -> Raft<MockTypeConfig> {
    let mut node_config = node_config(db_path);
    // Set batch_threshold=0, means the replication will be triggered immediatelly.
    node_config.raft.replication.rpc_append_entries_in_batch_threshold = 0;
    if let Some(peers_meta) = peers_meta_option {
        node_config.cluster.initial_cluster = peers_meta;
    }

    MockBuilder::new(shutdown_signal).with_node_config(node_config).build_raft()
}
