//! Client abstractions for d-engine.
//!
//! Core traits and types for KV client operations, shared by both
//! embedded (local) and standalone (remote) implementations.

pub mod client_api;
pub mod client_api_error;
pub mod types;

pub use client_api::ClientApi;
pub use client_api_error::{ClientApiError, ClientApiResult};
pub use types::{
    ClientReadRequest, ClientResponse, ClientResponsePayload, ClientWriteRequest, ErrorCode,
    KvEntry, LeaderHint, ReadResults, WriteOperation, WriteResult,
};
// ReadConsistencyPolicy lives in config but is part of the public client API surface.
pub use crate::config::ReadConsistencyPolicy;

#[cfg(test)]
mod client_api_error_test;
#[cfg(test)]
mod types_test;
