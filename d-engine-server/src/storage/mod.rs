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
//!
//! This module is designed so developers can easily implement custom
//! storage backends without changing the Raft protocol logic.
mod adaptors;
mod buffered;

pub use adaptors::*;
pub use buffered::*;
