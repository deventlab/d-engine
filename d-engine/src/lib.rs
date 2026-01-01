#![warn(missing_docs)]

//! # d-engine - Distributed Coordination Engine
//!
//! **Lightweight Raft consensus engine for building distributed systems**
//!
//! ## 🚀 New to d-engine? Start Here
//!
//! Follow this learning path to get started quickly:
//!
//! ```text
//! 1. Is d-engine Right for You? (1 minute)
//!    ↓
//! 2. Choose Integration Mode (1 minute)
//!    ↓
//! 3a. Quick Start - Embedded (5 minutes)
//!    OR
//! 3b. Quick Start - Standalone (5 minutes)
//!    ↓
//! 4. Scale to Cluster (optional)
//! ```
//!
//! **→ Start: [Is d-engine Right for You?](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/use-cases.md)**
//!
//! ## Quick Start
//!
//! ```toml
//! [dependencies]
//! d-engine = "0.2"
//! ```
//!
//! ### Embedded Mode (Single Process)
//!
//! ```rust,ignore
//! use d_engine::prelude::*;
//!
//! let engine = EmbeddedEngine::start_with("config.toml").await?;
//! engine.wait_ready(Duration::from_secs(5)).await?;
//! let client = engine.client();
//! client.put(b"key".to_vec(), b"value".to_vec()).await?;
//! ```
//!
//! **→ [5-Minute Embedded Guide](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/quick-start-5min.md)**
//!
//! ### Standalone Mode (Client-Server)
//!
//! ```rust,ignore
//! use d_engine::prelude::*;
//!
//! let client = Client::builder(vec!["http://localhost:50051".to_string()])
//!     .build()
//!     .await?;
//! client.kv().put(b"key".to_vec(), b"value".to_vec()).await?;
//! ```
//!
//! **→ [Standalone Guide](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/quick-start-standalone.md)**
//!
//! ## Crate Organization
//!
//! | Crate | Purpose | Use When |
//! |-------|---------|----------|
//! | **`d-engine`** | Unified API (this crate) | **Start here** ⭐ |
//! | [`d-engine-server`] | Server runtime | Server-only apps |
//! | [`d-engine-client`] | Client library | Client-only apps |
//! | [`d-engine-core`] | Pure Raft algorithm | Custom integrations |
//! | [`d-engine-proto`] | Protocol definitions | Non-Rust clients |
//!
//! [`d-engine-server`]: https://crates.io/crates/d-engine-server
//! [`d-engine-client`]: https://crates.io/crates/d-engine-client
//! [`d-engine-core`]: https://crates.io/crates/d-engine-core
//! [`d-engine-proto`]: https://crates.io/crates/d-engine-proto
//!
//! ## Features
//!
//! - `server` (default) - Raft server runtime
//! - `client` (default) - gRPC client library
//! - `rocksdb` (default) - Production storage backend
//! - `watch` - Real-time key monitoring
//!
//! ## Documentation Index
//!
//! ### Getting Started
//! - [Is d-engine Right for You?](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/use-cases.md) - Common use cases
//! - [Integration Modes](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/integration-modes.md) - Embedded vs Standalone
//! - [Quick Start - Embedded](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/quick-start-5min.md)
//! - [Quick Start - Standalone](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/quick-start-standalone.md)
//!
//! ### Guides by Role
//!
//! #### Client Developers
//! - [Read Consistency](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/client_guide/read-consistency.md) - Choosing consistency policies
//! - [Error Handling](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/client_guide/error-handling.md)
//! - [Go Client Guide](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/client_guide/go-client.md)
//!
//! #### Server Operators
//! - [Customize Storage Engine](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/server_guide/customize-storage-engine.md)
//! - [Customize State Machine](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/server_guide/customize-state-machine.md)
//! - [Consistency Tuning](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/server_guide/consistency-tuning.md)
//! - [Watch Feature](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/server_guide/watch-feature.md)
//!
//! ### Examples & Performance
//! - [Examples Directory](https://github.com/deventlab/d-engine/tree/main/examples) - Working code examples
//! - [Single Node Expansion](https://github.com/deventlab/d-engine/tree/main/examples/single-node-expansion) - Scale from 1 to 3 nodes
//! - [Throughput Optimization](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/performance/throughput-optimization-guide.md)
//!
//! ## License
//!
//! MIT or Apache-2.0

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
        NodeBuilder, Result, StandaloneServer, StateMachine, StorageEngine,
    };

    #[cfg(feature = "rocksdb")]
    pub use d_engine_server::{RocksDBStateMachine, RocksDBStorageEngine};

    #[cfg(feature = "client")]
    pub use d_engine_client::{Client, ClientBuilder, KvClient};
}
