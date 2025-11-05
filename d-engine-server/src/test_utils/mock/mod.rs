//! Mock implementations for Raft components
//!
//! Provides convenient builders and factory functions for creating
//! mock Raft instances with sensible defaults.

mod mock_node_builder;

pub use mock_node_builder::*;

use crate::Node;
use d_engine_core::node_config;
use d_engine_core::{MockTypeConfig, Raft};
use d_engine_proto::server::cluster::NodeMeta;
use tokio::sync::watch;

/// Create a mock Raft node for testing
///
/// Constructs a Node instance with mock components and optional peer metadata.
///
/// # Arguments
///
/// * `db_path` - Path for database configuration
/// * `shutdown_signal` - Watch receiver for graceful shutdown
/// * `peers_meta_option` - Optional list of peer node metadata
///
/// # Example
///
/// ```rust,ignore
/// use d_engine_server::test_utils::mock_node;
/// use tokio::sync::watch;
///
/// let (tx, rx) = watch::channel(());
/// let node = mock_node("/tmp/test_db", rx, None);
/// ```
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

/// Create a mock Raft instance for testing
///
/// Constructs a Raft instance with all components mocked and optimized
/// for testing (e.g., batch threshold set to 0 for immediate replication).
///
/// # Arguments
///
/// * `db_path` - Path for database configuration
/// * `shutdown_signal` - Watch receiver for graceful shutdown
/// * `peers_meta_option` - Optional list of peer node metadata
///
/// # Example
///
/// ```rust,ignore
/// use d_engine_server::test_utils::mock_raft;
/// use tokio::sync::watch;
///
/// let (tx, rx) = watch::channel(());
/// let raft = mock_raft("/tmp/test_db", rx, None);
/// ```
///
/// # Note
///
/// This sets `rpc_append_entries_in_batch_threshold=0` to ensure
/// replication happens immediately (useful for testing).
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
