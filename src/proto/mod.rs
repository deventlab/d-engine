//! Protocol Buffer definitions and generated code for RPC services.
//!
//! This module contains auto-generated Rust types from Protobuf definitions,
//! typically created using [`tonic-build`] or `protoc` compiler plugins.

pub mod common {
    include!("../generated/raft.common.rs");
}

pub mod error {
    include!("../generated/raft.error.rs");
}

pub mod cluster {
    include!("../generated/raft.cluster.rs");
}

pub mod replication {
    include!("../generated/raft.replication.rs");
}

pub mod election {
    include!("../generated/raft.election.rs");
}

pub mod storage {
    include!("../generated/raft.storage.rs");
}

pub mod client {
    include!("../generated/raft.client.rs");
}
