//! Unit test module for isolating RaftContext components via mocked
//! dependencies.
//!
//! This module provides mock implementations of storage, network and handler
//! components using the [mockall] framework. Designed for granular testing of
//! Raft consensus algorithm components with the following characteristics:
//!
//! - Uses **mocked storage interfaces** with in-memory state control
//! - Simulates network transport with deterministic responses
//! - Allows precise behavior injection for handlers
//! - Enables isolated testing of component interactions
//!
//! The [`TestContext`] struct encapsulates a controlled testing environment
//! containing:
//! - Mock storage implementations (raft log, state machine)
//! - Simulated network layer
//! - Configurable cluster membership
//! - Instrumented handler implementations
//!
//! The [`setup_mock_context`] function initializes a test environment with:
//! - Auto-generated mock objects via [mockall] attributes
//! - Preconfigured peer responses
//! - Deterministic transport simulation
//! - Component interaction tracking
//!
//! This differs from integration tests in that:
//! - I/O operations use mocked storage with ephemeral state
//! - Network communication is simulated without actual ports binding
//! - Component states reset between tests
//! - Specific interaction patterns can be enforced
//!
//! Typical usage scenarios:
//! - Unit testing of individual Raft components
//! - Validation of state machine edge cases
//! - Network partition simulation
//! - Protocol violation testing
//! - Fast feedback during development iterations
//!
//! [mockall]: https://docs.rs/mockall/latest/mockall/

mod mock_builder;
mod mock_rpc;
mod mock_rpc_service;

pub use mock_builder::*;
pub use mock_rpc::*;
pub use mock_rpc_service::*;
use tokio::sync::watch;

use super::enable_logger;
use super::node_config;
use super::MockTypeConfig;
use crate::proto::NodeMeta;
use crate::Node;
use crate::Raft;
use crate::RaftContext;

pub(crate) fn mock_node(
    db_path: &str,
    shutdown_signal: watch::Receiver<()>,
    peers_meta_option: Option<Vec<NodeMeta>>,
) -> Node<MockTypeConfig> {
    enable_logger();

    let mut node_config = node_config(db_path);
    if peers_meta_option.is_some() {
        node_config.cluster.initial_cluster = peers_meta_option.unwrap();
    }
    // Initializing Shutdown Signal
    // let (graceful_tx, graceful_rx) = watch::channel(());
    MockBuilder::new(shutdown_signal)
        .wiht_node_config(node_config)
        .build_node()
}

pub(crate) fn mock_raft(
    db_path: &str,
    shutdown_signal: watch::Receiver<()>,
    peers_meta_option: Option<Vec<NodeMeta>>,
) -> Raft<MockTypeConfig> {
    enable_logger();

    let mut node_config = node_config(db_path);
    // Set batch_threshold=0, means the replication will be triggered immediatelly.
    node_config.raft.replication.rpc_append_entries_in_batch_threshold = 0;
    if peers_meta_option.is_some() {
        node_config.cluster.initial_cluster = peers_meta_option.unwrap();
    }

    MockBuilder::new(shutdown_signal)
        .wiht_node_config(node_config)
        .build_raft()
}

pub(crate) fn mock_raft_context(
    db_path: &str,
    shutdown_signal: watch::Receiver<()>,
    peers_meta_option: Option<Vec<NodeMeta>>,
) -> RaftContext<MockTypeConfig> {
    enable_logger();

    let mut node_config = node_config(db_path);
    if peers_meta_option.is_some() {
        node_config.cluster.initial_cluster = peers_meta_option.unwrap();
    }

    MockBuilder::new(shutdown_signal)
        .wiht_node_config(node_config)
        .build_context()
}
