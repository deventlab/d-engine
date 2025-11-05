//! # d-engine-server
//!
//! Production-ready Raft consensus server implementation.
//!
//! ## Architecture
//! - [`node`] - Node lifecycle management
//! - [`storage`] - Pluggable storage backends
//! - Core Raft protocol (re-exported from `d-engine-core`)
//! - Protocol definitions (re-exported from `d-engine-proto`)
//!
//! ## Quick Start
//! ```ignore
//! use d_engine_server::{NodeBuilder, FileStorageEngine, FileStateMachine};
//! use std::sync::Arc;
//! use std::path::PathBuf;
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let (tx, rx) = tokio::sync::watch::channel(());
//!
//!     let storage = Arc::new(FileStorageEngine::new(PathBuf::from("/tmp/storage"))?);
//!     let state_machine = Arc::new(FileStateMachine::new(PathBuf::from("/tmp/sm")).await?);
//!
//!     let node = NodeBuilder::new(None, rx)
//!         .storage_engine(storage)
//!         .state_machine(state_machine)
//!         .build()
//!         .start_rpc_server()
//!         .await
//!         .ready()?;
//!
//!     node.run().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Custom Storage Backend
//!
//! Implement the [`StateMachine`] and [`StorageEngine`] traits:
//!
//! ```rust,ignore
//! use d_engine_server::{StateMachine, StorageEngine};
//!
//! struct MyStateMachine { /* ... */ }
//! impl StateMachine for MyStateMachine { /* ... */ }
//!
//! struct MyStorageEngine { /* ... */ }
//! impl StorageEngine for MyStorageEngine { /* ... */ }
//! ```
//!
//! See the [d-engine-docs](https://docs.rs/d-engine-docs) for architecture details.

#![doc = include_str!("../../d-engine-docs/src/docs/overview.md")]
#![warn(missing_docs)]

// ==================== Core Public API ====================

/// Node lifecycle management
///
/// Contains [`Node`] and [`NodeBuilder`] for server setup.
pub mod node;

/// Storage layer implementations
///
/// Provides file-based and RocksDB storage backends.
pub mod storage;

// -------------------- Primary Entry Points --------------------
pub use node::{Node, NodeBuilder};

// Re-export storage implementations
pub use storage::{
    FileStateMachine,
    // File-based storage
    FileStorageEngine,
};

// Conditional RocksDB exports
#[cfg(feature = "rocksdb")]
pub use storage::{RocksDBStateMachine, RocksDBStorageEngine};

// -------------------- Extension Traits --------------------

/// Storage trait for implementing custom storage backends
///
/// Implement this trait to create your own storage engine.
pub use d_engine_core::{StateMachine, StorageEngine};

/// Log storage trait
pub use d_engine_core::LogStore;

/// Metadata storage trait
pub use d_engine_core::MetaStore;

// -------------------- Error Types --------------------

/// Unified error type for all d-engine operations
pub use d_engine_core::Error;

/// Unified result type (equivalent to Result<T, Error>)
pub use d_engine_core::Result;

/// Storage-specific error type
pub use d_engine_core::StorageError;

/// Snapshot operation error type
pub use d_engine_core::SnapshotError;

/// Protocol buffer error type
pub use d_engine_core::ProstError;

/// Hard state (Raft persistent state: term, voted_for, log)
pub use d_engine_core::HardState;

// -------------------- Data Types --------------------

/// Common Raft protocol types
pub mod common {
    // Basic types used in Raft consensus protocol
    pub use d_engine_proto::common::{Entry, EntryPayload, LogId, entry_payload};
}

/// Client protocol types
pub mod client {
    // Client write command types for custom business logic
    pub use d_engine_proto::client::{WriteCommand, write_command};
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
