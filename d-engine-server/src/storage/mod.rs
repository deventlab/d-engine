//! Storage layer for d-engine.
//!
//! This module provides pluggable and extensible storage components for
//! persisting Raft state and application data. It defines core traits,
//! adapters, and utilities to support different backend implementations.
//!
//! Key responsibilities include:
//! - Managing Raft log entries and snapshots.
//! - Providing an abstraction layer (`StorageEngine`) for persistence.
//! - Supporting in-memory buffering and disk-backed storage (e.g., via Sled).
//! - Coordinating state machine application and snapshot lifecycle.
//! - Managing key expiration through lease-based lifecycle management.
//!
//! This module is designed so developers can easily implement custom
//! storage backends without changing the Raft protocol logic.
mod adaptors;
mod buffered;
mod lease;

pub use adaptors::*;
pub use buffered::*;
// Re-export Lease trait from core for convenience
pub use d_engine_core::Lease;
pub use lease::DefaultLease;

#[cfg(test)]
mod lease_integration_test;
#[cfg(test)]
mod lease_unit_test;
