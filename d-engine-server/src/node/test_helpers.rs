//! Test helpers for Node unit tests
//!
//! Provides reusable fixtures and utilities to avoid code duplication
//! across Node test suites.

#[cfg(test)]
use std::sync::Arc;
use tokio::sync::watch;

use crate::node::Node;
use crate::test_utils::MockBuilder;
use d_engine_core::RaftNodeConfig;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::NodeMeta;

/// Creates a test Node with minimal setup for unit testing
///
/// Returns (Node, shutdown_tx) where shutdown_tx can be used to trigger graceful shutdown
#[cfg(test)]
pub fn create_test_node() -> (Node<d_engine_core::MockTypeConfig>, watch::Sender<()>) {
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    let node = MockBuilder::new(shutdown_rx.clone())
        .with_node_config({
            let mut config = RaftNodeConfig::new().expect("Default config");
            config.cluster.initial_cluster = vec![
                NodeMeta {
                    id: 1,
                    address: "127.0.0.1:9001".to_string(),
                    role: Follower as i32,
                    status: NodeStatus::Active as i32,
                },
            ];
            config
        })
        .build_node();

    (node, shutdown_tx)
}

/// Creates a test Node with custom node_id
#[cfg(test)]
pub fn create_test_node_with_id(node_id: u32) -> (Node<d_engine_core::MockTypeConfig>, watch::Sender<()>) {
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    let node = MockBuilder::new(shutdown_rx.clone())
        .id(node_id)
        .with_node_config({
            let mut config = RaftNodeConfig::new().expect("Default config");
            config.cluster.node_id = node_id;
            config.cluster.initial_cluster = vec![
                NodeMeta {
                    id: node_id,
                    address: format!("127.0.0.1:{}", 9000 + node_id),
                    role: Follower as i32,
                    status: NodeStatus::Active as i32,
                },
            ];
            config
        })
        .build_node();

    (node, shutdown_tx)
}

/// Creates a test Node wrapped in Arc for concurrent testing
#[cfg(test)]
pub fn create_test_node_arc() -> (Arc<Node<d_engine_core::MockTypeConfig>>, watch::Sender<()>) {
    let (node, shutdown_tx) = create_test_node();
    (Arc::new(node), shutdown_tx)
}
