//! # d-engine-server
//!
//! Complete Raft server with gRPC and storage - batteries included
//!
//! ## When to use this crate directly
//!
//! - ✅ Embedding server in a larger Rust application
//! - ✅ Need programmatic access to server APIs
//! - ✅ Building custom tooling around d-engine
//! - ✅ Already have your own client implementation
//!
//! ## When to use `d-engine` instead
//!
//! Most users should use [`d-engine`](https://crates.io/crates/d-engine):
//!
//! ```toml
//! [dependencies]
//! d-engine = { version = "0.2", features = ["server"] }
//! ```
//!
//! It re-exports this crate plus optional client libraries with simpler dependency management.
//!
//! ## Quick Start
//!
//! **Embedded Mode** (zero-overhead local client):
//!
//! ```rust,ignore
//! use d_engine_server::EmbeddedEngine;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let engine = EmbeddedEngine::start_with("config.toml").await?;
//!     engine.wait_ready(Duration::from_secs(5)).await?;
//!
//!     let client = engine.client();
//!     client.put(b"key".to_vec(), b"value".to_vec()).await?;
//!
//!     engine.stop().await?;
//!     Ok(())
//! }
//! ```
//!
//! **Standalone Mode** (independent server):
//!
//! ```rust,ignore
//! use d_engine_server::StandaloneServer;
//! use tokio::sync::watch;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     std::env::set_var("CONFIG_PATH", "config.toml");
//!     let (_shutdown_tx, shutdown_rx) = watch::channel(());
//!     StandaloneServer::run(shutdown_rx).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Features
//!
//! This crate provides:
//! - **gRPC Server** - Production-ready Raft RPC implementation
//! - **Storage Backends** - File-based and RocksDB storage
//! - **Cluster Orchestration** - Node lifecycle and membership management
//! - **Snapshot Coordination** - Automatic log compaction
//! - **Watch API** - Real-time state change notifications
//!
//! ## Custom Storage
//!
//! Implement the [`StateMachine`] and [`StorageEngine`] traits:
//!
//! ```rust,ignore
//! use d_engine_server::{StateMachine, StorageEngine};
//!
//! struct MyStateMachine;
//! impl StateMachine for MyStateMachine {
//!     // Apply committed entries to your application state
//! }
//!
//! struct MyStorageEngine;
//! impl StorageEngine for MyStorageEngine {
//!     // Persist Raft logs and metadata
//! }
//! ```
//!
//! ## Documentation
//!
//! For comprehensive guides:
//! - [Customize Storage Engine](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/customize_storage_engine/index.html)
//! - [Customize State Machine](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/customize_state_machine/index.html)
//! - [Watch Feature Guide](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/watch_feature/index.html)

#![warn(missing_docs)]

// ==================== Core Public API ====================

/// Node lifecycle management
///
/// Contains [`Node`] and [`NodeBuilder`] for server setup.
pub mod node;

/// Public API layer for different deployment modes
///
/// Contains [`EmbeddedEngine`] and [`StandaloneServer`].
pub mod api;

// Re-export LeaderInfo from d-engine-core
pub use d_engine_core::LeaderInfo;

/// Storage layer implementations
///
/// Provides file-based and RocksDB storage backends.
pub mod storage;

// -------------------- Primary Entry Points --------------------
pub use api::EmbeddedEngine;
pub use api::StandaloneServer;
// -------------------- Error Types --------------------
/// Unified error type for all d-engine operations
pub use d_engine_core::Error;
/// Hard state (Raft persistent state: term, voted_for, log)
pub use d_engine_core::HardState;
/// Log storage trait
pub use d_engine_core::LogStore;
/// Metadata storage trait
pub use d_engine_core::MetaStore;
/// Protocol buffer error type
pub use d_engine_core::ProstError;
/// Unified result type (equivalent to Result<T, Error>)
pub use d_engine_core::Result;
/// Snapshot operation error type
pub use d_engine_core::SnapshotError;
/// Storage-specific error type
pub use d_engine_core::StorageError;
// -------------------- Extension Traits --------------------
/// Storage trait for implementing custom storage backends
///
/// Implement this trait to create your own storage engine.
pub use d_engine_core::{StateMachine, StorageEngine};
pub use node::LocalClientError;
pub use node::LocalKvClient;
pub use node::Node;
pub use node::NodeBuilder;
// Re-export storage implementations
pub use storage::{
    FileStateMachine,
    // File-based storage
    FileStorageEngine,
};
// Conditional RocksDB exports
#[cfg(feature = "rocksdb")]
pub use storage::{RocksDBStateMachine, RocksDBStorageEngine};

// -------------------- Data Types --------------------

/// Common Raft protocol types
pub mod common {
    // Basic types used in Raft consensus protocol
    pub use d_engine_proto::common::Entry;
    pub use d_engine_proto::common::EntryPayload;
    pub use d_engine_proto::common::LogId;
    pub use d_engine_proto::common::entry_payload;
}

/// Client protocol types
pub mod client {
    // Client write command types for custom business logic
    pub use d_engine_proto::client::WriteCommand;
    pub use d_engine_proto::client::write_command;
}

/// Server storage protocol types
pub mod server_storage {
    // Server storage protocol types (snapshots, replication)
    pub use d_engine_proto::server::storage::SnapshotMetadata;
}

// ==================== Internal API (Hidden) ====================
mod membership;
mod network;
mod utils;

#[doc(hidden)]
pub use d_engine_core::Raft;

// ==================== Test Utilities ====================

/// Test utilities for d-engine-server
///
/// This module is only available when the `test-utils` feature is enabled
/// or when running tests.
///
/// Contains mock implementations and test helpers.
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
