//! # d-engine-proto
//!
//! gRPC protocol definitions - for building non-Rust d-engine clients
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
//! ## For non-Rust developers
//!
//! The `.proto` files are included in this crate. Generate client code for your language:
//!
//! ```bash
//! # Python example
//! protoc --python_out=. --grpc_python_out=. d-engine.proto
//!
//! # Go example
//! protoc --go_out=. --go-grpc_out=. d-engine.proto
//!
//! # Java example
//! protoc --java_out=. --grpc-java_out=. d-engine.proto
//! ```
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
