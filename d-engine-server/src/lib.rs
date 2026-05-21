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
//! use d_engine_server::StandaloneEngine;
//! use tokio::sync::watch;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let (_shutdown_tx, shutdown_rx) = watch::channel(());
//!     StandaloneEngine::run("./data", shutdown_rx).await?;
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
/// Contains [`EmbeddedEngine`] and [`StandaloneEngine`].
pub mod api;

// Re-export LeaderInfo from d-engine-core
pub use d_engine_core::LeaderInfo;

/// Storage layer implementations
///
/// Provides file-based and RocksDB storage backends.
pub mod storage;

// -------------------- Primary Entry Points --------------------
pub use api::EmbeddedEngine;
pub use api::StandaloneEngine;
pub use membership::MembershipSnapshot;
// -------------------- Error Types --------------------
/// Unified error type for all d-engine operations
pub use d_engine_core::Error;
/// Log storage trait
pub use d_engine_core::LogStore;
/// Metadata storage trait
pub use d_engine_core::MetaStore;
/// Unified result type (equivalent to Result<T, Error>)
pub use d_engine_core::Result;
/// Prefix scan result: matching entries + revision anchor for watch deduplication
pub use d_engine_core::ScanResult;
/// Storage-specific error type
pub use d_engine_core::StorageError;
// Internal types required by storage implementations — not part of user API
#[doc(hidden)]
pub use d_engine_core::HardState;
#[doc(hidden)]
pub use d_engine_core::ProstError;
#[doc(hidden)]
pub use d_engine_core::SnapshotError;
// -------------------- Client API --------------------
pub use api::EmbeddedClient;
/// Unified client operations trait (put/get/delete/CAS/watch).
/// Re-exported here so embedded-mode users don't need a separate d-engine-client dependency.
pub use d_engine_core::client::{ClientApi, ClientApiError, ClientApiResult};
/// Storage trait for implementing custom storage backends
///
/// Implement this trait to create your own storage engine.
pub use d_engine_core::{StateMachine, StorageEngine};
/// Error code discriminator returned by [`ClientApiError::code()`].
/// Use this to pattern-match error categories without inspecting message strings:
/// `e.code() == ErrorCode::NotLeader`
pub use d_engine_proto::error::ErrorCode;
pub use node::Node;
pub use node::NodeBuilder;
// Re-export storage implementations
pub use storage::{FileStateMachine, FileStorageEngine};
// Re-export core types needed by applications
pub use d_engine_core::ApplyResult;
pub use d_engine_core::{ApplyEntry, Command};
// Conditional RocksDB exports
#[cfg(feature = "rocksdb")]
pub use storage::{RocksDBStateMachine, RocksDBStorageEngine, RocksDBUnifiedEngine};

// -------------------- Watch API Types --------------------
/// Watch event types and handles — available when the `watch` feature is enabled.
///
/// Use these when implementing embedded watch handlers:
/// ```rust,ignore
/// use d_engine::{WatchEventType, WatcherHandle, WatchEvent};
/// ```
#[cfg(feature = "watch")]
pub use d_engine_core::{WatchError, WatchEvent, WatchEventType, WatcherHandle};

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

// ==================== Test Utilities ====================

/// Standardized test suite for custom [`StateMachine`] implementations.
///
/// Enable the `__test_support` feature in your `[dev-dependencies]` to access this module:
/// ```toml
/// [dev-dependencies]
/// d-engine = { version = "...", features = ["server", "__test_support"] }
/// ```
#[cfg(feature = "__test_support")]
pub use d_engine_core::state_machine_test;

/// Standardized test suite for custom [`StorageEngine`] implementations.
///
/// Enable the `__test_support` feature in your `[dev-dependencies]` to access this module:
/// ```toml
/// [dev-dependencies]
/// d-engine = { version = "...", features = ["server", "__test_support"] }
/// ```
#[cfg(feature = "__test_support")]
pub use d_engine_core::storage_engine_test;

/// Test utilities for d-engine-server
///
/// This module is only available when running tests.
///
/// Contains mock implementations and test helpers.
#[cfg(test)]
#[doc(hidden)]
pub(crate) mod test_utils;
