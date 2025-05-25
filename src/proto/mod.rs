//! Protocol Buffer definitions and generated code for RPC services.
//!
//! This module contains auto-generated Rust types from Protobuf definitions,
//! typically created using [`tonic-build`] or `protoc` compiler plugins.

pub mod common {
    tonic::include_proto!("raft.common");
}

pub mod error {
    tonic::include_proto!("raft.error");
}

pub mod cluster {
    tonic::include_proto!("raft.cluster");
}

pub mod replication {
    tonic::include_proto!("raft.replication");
}

pub mod election {
    tonic::include_proto!("raft.election");
}

pub mod storage {
    tonic::include_proto!("raft.storage");
}

pub mod client {
    tonic::include_proto!("raft.client");
}
