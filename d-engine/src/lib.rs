#![warn(missing_docs)]

//! # d-engine
//!
//! 🚀 Lightweight Raft consensus engine - recommended entry point for most users
//!
//! ## Quick Start
//!
//! ```toml
//! [dependencies]
//! d-engine = { version = "0.2", features = ["server", "client"] }
//! ```
//!
//! ## When to use which crate?
//!
//! | Your use case | Recommended crate |
//! |---------------|------------------|
//! | **Most users - full application** | `d-engine` (this crate) ⭐ |
//! | **Client-only application** | `d-engine` with `features = ["client"]` |
//! | **Server-only application** | `d-engine` with `features = ["server"]` |
//! | **Custom Raft integration** | [`d-engine-core`](https://crates.io/crates/d-engine-core) |
//! | **Non-Rust client (Python/Go/Java)** | [`d-engine-proto`](https://crates.io/crates/d-engine-proto) for `.proto` files |
//!
//! ## Architecture
//!
//! d-engine supports two integration modes:
//!
//! ### Embedded Mode (Rust)
//! - Your app + Raft engine in **one process**
//! - Zero-overhead `LocalKvClient` (memory-only, <0.1ms)
//! - Use `d-engine` crate with `features = ["server"]`
//!
//! ### Standalone Mode (Any Language)
//! - Raft server as **separate process**
//! - Client connects via **gRPC**
//! - Go/Python/Java: use `d-engine-proto` to generate client code
//! - Rust: use `d-engine-client` crate
//!
//! ## Crate Organization
//!
//! ```text
//! d-engine          ← Start here (Rust apps)
//! ├── client        ← gRPC client (optional)
//! └── server        ← Raft server + LocalKvClient
//!     └── core      ← Pure Raft algorithm
//!
//! d-engine-proto    ← Protocol definitions (Go/Python/Java)
//! ```
//!
//! ## 📚 Full Documentation
//!
//! For comprehensive guides, see:
//! - [Quick Start - Embedded Mode](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/quick-start-5min.md)
//! - [Quick Start - Standalone Mode](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/quick-start-standalone.md)
//! - [Integration Modes Guide](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/integration-modes.md)
//! - [Examples Directory](https://github.com/deventlab/d-engine/tree/main/examples)
//!
//! ## Features
//!
//! - `server` - Server runtime (enabled by default)
//! - `client` - Client library (enabled by default)
//! - `rocksdb` - RocksDB storage backend (enabled by default)
//! - `watch` - Real-time key monitoring
//!
//! ## Examples
//!
//! ### Embedded Mode (Single Binary)
//!
//! ```rust,ignore
//! use d_engine::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Start embedded engine with RocksDB
//!     let engine = EmbeddedEngine::with_rocksdb("./data", None).await?;
//!
//!     // Wait for node initialization
//!     engine.ready().await;
//!
//!     // Wait for leader election
//!     engine.wait_leader(std::time::Duration::from_secs(5)).await?;
//!
//!     // Get KV client
//!     let client = engine.client();
//!
//!     // Store and retrieve data
//!     client.put(b"hello".to_vec(), b"world".to_vec()).await?;
//!
//!     if let Some(value) = client.get(b"hello".to_vec()).await? {
//!         println!("Retrieved: hello = {}", String::from_utf8_lossy(&value));
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ### Standalone Mode (Separate Client/Server)
//!
//! Server:
//! ```rust,ignore
//! use d_engine::prelude::*;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
//!
//!     let storage = Arc::new(RocksDBStorageEngine::new("./storage")?);
//!     let state_machine = Arc::new(RocksDBStateMachine::new("./state_machine").await?);
//!
//!     let node = NodeBuilder::new(None, shutdown_rx)
//!         .storage_engine(storage)
//!         .state_machine(state_machine)
//!         .build()
//!         .start_rpc_server()
//!         .await
//!         .ready()
//!         .expect("Failed to start node");
//!
//!     node.run().await?;
//!     Ok(())
//! }
//! ```
//!
//! Client:
//! ```rust,ignore
//! use d_engine::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::connect(vec!["http://localhost:50051"]).await?;
//!
//!     client.put(b"key".to_vec(), b"value".to_vec()).await?;
//!
//!     Ok(())
//! }
//! ```

// Re-export server components when server feature is enabled
#[cfg(feature = "server")]
pub use d_engine_server::*;

// Re-export client components when client feature is enabled
#[cfg(feature = "client")]
pub use d_engine_client::*;

/// Convenient prelude for importing common types
///
/// ```rust,ignore
/// use d_engine::prelude::*;
/// ```
pub mod prelude {
    #[cfg(feature = "server")]
    pub use d_engine_server::{
        EmbeddedEngine, Error, FileStateMachine, FileStorageEngine, LocalKvClient, Node,
        NodeBuilder, Result, StateMachine, StorageEngine,
    };

    #[cfg(feature = "rocksdb")]
    pub use d_engine_server::{RocksDBStateMachine, RocksDBStorageEngine};

    #[cfg(feature = "client")]
    pub use d_engine_client::{Client, KvClient};
}
