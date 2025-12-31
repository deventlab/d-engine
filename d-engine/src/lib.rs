//! # d-engine
//!
//! A lightweight and strongly consistent Raft consensus engine written in Rust.
//! Build reliable and scalable distributed systems by embedding d-engine into your applications.
//!
//! ## What's New in v0.2.0
//!
//! - **Modular workspace**: Use `features = ["client"]` to reduce dependencies
//! - **TTL/Lease**: Automatic key expiration for distributed locks
//! - **Watch API**: Real-time key change notifications
//! - **EmbeddedEngine**: Single-node start, scale to 3 nodes with zero code changes
//! - **LocalKvClient**: Zero-overhead in-process access (<0.1ms latency)
//!
//! See the [CHANGELOG](https://github.com/deventlab/d-engine/blob/main/CHANGELOG.md) for full details.
//!
//! ## Quick Start
//!
//! Add d-engine to your `Cargo.toml` with features you need:
//!
//! ```toml
//! # For client-only applications
//! d-engine = { version = "0.2", features = ["client"] }
//!
//! # For server/embedded applications
//! d-engine = { version = "0.2", features = ["server"] }
//!
//! # For applications that need both
//! d-engine = { version = "0.2", features = ["full"] }
//! ```
//!
//! ## Features
//!
//! - **`client`** - Client library for connecting to d-engine servers
//! - **`server`** - Embed a d-engine Raft server into your application
//! - **`full`** - Both client and server capabilities
//!
//! ## Client Example
//!
//! ```rust,ignore
//! use d_engine::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::builder(vec![
//!         "http://localhost:9081".into(),
//!         "http://localhost:9082".into(),
//!     ])
//!     .build()
//!     .await?;
//!
//!     // Write data
//!     client.kv().put("key", "value").await?;
//!
//!     // Read data
//!     let value = client.kv().get("key").await?;
//!     println!("Value: {:?}", value);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Server Example (Embedded)
//!
//! ```rust,ignore
//! use d_engine::{NodeBuilder, FileStorageEngine, FileStateMachine};
//! use std::sync::Arc;
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let (tx, rx) = tokio::sync::watch::channel(());
//!
//!     let storage = Arc::new(FileStorageEngine::new(PathBuf::from("/tmp/storage"))?);
//!     let state_machine = Arc::new(FileStateMachine::new(PathBuf::from("/tmp/sm")).await?);
//!
//!     let node = NodeBuilder::new(None, rx)
//!         .storage_engine(storage)
//!         .state_machine(state_machine)
//!         .start_server()
//!         .await?;
//!
//!     node.run().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Architecture
//!
//! d-engine is designed with a modular architecture:
//!
//! - **d-engine-proto**: Protocol definitions (Protobuf/gRPC)
//! - **d-engine-core**: Core Raft algorithm implementation
//! - **d-engine-client**: Client library for cluster communication
//! - **d-engine-server**: Production-ready server runtime
//!
//! This main crate re-exports the public APIs from these components based on
//! enabled features, providing a unified interface for developers.

#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

// ==================== Client API ====================

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub use d_engine_client::{
    // Main entry points
    Client,
    // Error types
    ClientApiError,
    ClientBuilder,
    // Configuration
    ClientConfig,
    // Specialized clients
    ClusterClient,
    GrpcKvClient,
};

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
/// Protocol types for client operations
pub mod protocol {
    pub use d_engine_client::protocol::ClientResult;
    pub use d_engine_client::protocol::ReadConsistencyPolicy;
    pub use d_engine_client::protocol::WriteCommand;
}

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
/// Cluster management types
pub mod cluster_types {
    pub use d_engine_client::cluster_types::NodeMeta;
    pub use d_engine_client::cluster_types::NodeStatus;
}

// ==================== Core API ====================

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub use d_engine_client::KvClient;
#[cfg(all(feature = "server", feature = "rocksdb"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "server", feature = "rocksdb"))))]
pub use d_engine_server::RocksDBStateMachine;
#[cfg(all(feature = "server", feature = "rocksdb"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "server", feature = "rocksdb"))))]
pub use d_engine_server::RocksDBStorageEngine;
// ==================== Server API ====================
#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
pub use d_engine_server::{
    // Embedded mode
    EmbeddedEngine,
    // Error types
    Error,
    // Storage implementations
    FileStateMachine,
    FileStorageEngine,
    // Data types
    HardState,
    LeaderInfo,
    // Embedded KV client (zero-overhead, same process)
    LocalKvClient,
    // Extension traits for custom implementations
    LogStore,
    MetaStore,
    // Main entry points
    Node,
    NodeBuilder,
    ProstError,
    Result,
    SnapshotError,
    // Standalone mode
    StandaloneServer,
    StateMachine,
    StorageEngine,
    StorageError,
};

#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
/// Common Raft protocol types
pub mod common {
    pub use d_engine_server::common::Entry;
    pub use d_engine_server::common::EntryPayload;
    pub use d_engine_server::common::LogId;
    pub use d_engine_server::common::entry_payload;
}

#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
/// Client write command types
pub mod write_command {
    pub use d_engine_server::client::WriteCommand;
    pub use d_engine_server::client::write_command;
}

#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
/// Server storage types
pub mod storage {
    pub use d_engine_server::server_storage::SnapshotMetadata;
}

// ==================== Convenience Re-exports ====================

/// Convenient prelude for quick imports
///
/// Import everything you need with:
/// ```rust,ignore
/// use d_engine::prelude::*;
/// ```
pub mod prelude {
    #[cfg(feature = "client")]
    pub use d_engine_client::Client;
    #[cfg(feature = "client")]
    pub use d_engine_client::ClientBuilder;
    #[cfg(feature = "client")]
    pub use d_engine_client::GrpcKvClient;
    #[cfg(feature = "client")]
    pub use d_engine_client::KvClient;
    #[cfg(feature = "server")]
    pub use d_engine_server::EmbeddedEngine;
    #[cfg(feature = "server")]
    pub use d_engine_server::FileStateMachine;
    #[cfg(feature = "server")]
    pub use d_engine_server::FileStorageEngine;
    #[cfg(feature = "server")]
    pub use d_engine_server::LeaderInfo;
    #[cfg(feature = "server")]
    pub use d_engine_server::LocalClientError;
    #[cfg(feature = "server")]
    pub use d_engine_server::LocalKvClient;
    #[cfg(feature = "server")]
    pub use d_engine_server::Node;
    #[cfg(feature = "server")]
    pub use d_engine_server::NodeBuilder;
    #[cfg(feature = "server")]
    pub use d_engine_server::StandaloneServer;
    #[cfg(feature = "server")]
    pub use d_engine_server::StateMachine;
    #[cfg(feature = "server")]
    pub use d_engine_server::StorageEngine;
}
