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
//! The mock context encapsulates a controlled testing environment containing:
//! - Mock storage implementations (raft log, state machine)
//! - Simulated network layer
//! - Configurable cluster membership
//! - Instrumented handler implementations
//!
//! Mock initialization provides a test environment with:
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

pub mod mock_raft_builder;
mod mock_rpc;
mod mock_rpc_service;
mod mock_storage_engine;
mod mock_type_config;

use d_engine_proto::server::cluster::NodeMeta;
pub use mock_raft_builder::*;
pub use mock_rpc::*;
pub use mock_rpc_service::*;
pub use mock_storage_engine::*;
pub use mock_type_config::*;
use tokio::sync::watch;

use super::node_config;
use crate::RaftContext;

pub fn mock_raft_context(
    db_path: &str,
    shutdown_signal: watch::Receiver<()>,
    peers_meta_option: Option<Vec<NodeMeta>>,
) -> RaftContext<MockTypeConfig> {
    let mut node_config = node_config(db_path);
    if let Some(peers_meta) = peers_meta_option {
        node_config.cluster.initial_cluster = peers_meta;
    }
    node_config.raft.replication.rpc_append_entries_in_batch_threshold = 1;
    // Reduce timeout for test
    node_config.retry.auto_discovery.timeout_ms = 10;

    MockBuilder::new(shutdown_signal).with_node_config(node_config).build_context()
}

/// Creates mock RaftContext with automatic TempDir cleanup
///
/// This is the recommended helper for unit tests. It automatically manages
/// temporary directories and provides a clean testing environment.
///
/// Returns:
/// - RaftContext configured for testing
/// - TempDir that auto-cleans on drop
///
/// Example:
/// ```rust,ignore
/// let (_graceful_tx, graceful_rx) = watch::channel(());
/// let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);
/// // Use context in test...
/// // _temp_dir automatically cleaned up when dropped
/// ```
pub fn mock_raft_context_with_temp(
    shutdown_signal: watch::Receiver<()>,
    peers_meta_option: Option<Vec<NodeMeta>>,
) -> (RaftContext<MockTypeConfig>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let ctx = mock_raft_context(
        temp_dir.path().to_str().unwrap(),
        shutdown_signal,
        peers_meta_option,
    );
    (ctx, temp_dir)
}
