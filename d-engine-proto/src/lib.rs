//! # d-engine-proto
//!
//! gRPC protocol definitions for d-engine - foundation for all client implementations
//!
//! ## When to use this crate
//!
//! - ✅ Building Go/Python/Java clients
//! - ✅ Need raw `.proto` files for code generation
//! - ✅ Custom protocol extensions
//! - ✅ Contributing to d-engine protocol development
//!
//! ## For Rust users
//!
//! If you're writing Rust code, use [`d-engine`](https://crates.io/crates/d-engine) or
//! [`d-engine-client`](https://crates.io/crates/d-engine-client) instead - they provide
//! higher-level APIs on top of these protocol definitions.
//!
//! ```toml
//! [dependencies]
//! d-engine = { version = "0.2", features = ["client"] }
//! ```
//!
//! ## For Non-Rust Developers
//!
//! The `.proto` files are included in this crate. Generate client code for your language:
//!
//! ```bash
//! # Go (✅ Tested - see examples/quick-start-standalone)
//! protoc -I. \
//!   --go_out=./go \
//!   --go_opt=module=github.com/deventlab/d-engine/proto \
//!   --go-grpc_out=./go \
//!   --go-grpc_opt=module=github.com/deventlab/d-engine/proto \
//!   proto/common.proto \
//!   proto/error.proto \
//!   proto/client/client_api.proto
//!
//! # Python (⚠️ Command verified, end-to-end integration not yet tested)
//! protoc --python_out=./out \
//!   proto/common.proto \
//!   proto/error.proto \
//!   proto/client/client_api.proto
//! ```
//!
//! **Language Support Status:**
//! - ✅ **Go** - Production-ready with working example
//! - ⚠️ **Python** - Proto generation verified, client integration pending
//! - 🔜 **Other languages** - Community contributions welcome
//!
//! Protocol files are located in the source repository under `d-engine-proto/proto/`.
//!
//! ## Documentation
//!
//! For language-specific integration guides:
//! - [Go Client Guide](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/client_guide/go-client.md)
//!
//! ## Protocol Modules
//!
//! This crate provides protobuf-generated Rust types organized by service area:

pub mod common {
    include!("generated/d_engine.common.rs");
}

pub mod error {
    include!("generated/d_engine.common.error.rs");
}
pub mod server {
    pub mod cluster {
        include!("generated/d_engine.server.cluster.rs");
    }

    pub mod replication {
        include!("generated/d_engine.server.replication.rs");
    }

    pub mod election {
        include!("generated/d_engine.server.election.rs");
    }

    pub mod storage {
        include!("generated/d_engine.server.storage.rs");
    }
}

pub mod client {
    include!("generated/d_engine.client.rs");
}

pub mod exts;
