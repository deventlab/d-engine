//! Protocol Buffer definitions and generated code for RPC services.
//!
//! This module contains auto-generated Rust types from Protobuf definitions,
//! typically created using [`tonic-build`] or `protoc` compiler plugins.

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
