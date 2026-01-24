//! Client abstractions for d-engine.
//!
//! Core traits and types for KV client operations, shared by both
//! embedded (local) and standalone (remote) implementations.

pub mod client_api;
pub mod client_api_error;

pub use client_api::ClientApi;
pub use client_api_error::{ClientApiError, ClientApiResult};

#[cfg(test)]
mod client_api_error_test;
