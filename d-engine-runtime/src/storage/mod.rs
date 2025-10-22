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
mod raft_log;
mod snapshot_path_manager;
mod state_machine;
mod storage_engine;

pub use adaptors::*;
pub(crate) use buffered::*;
pub(crate) use snapshot_path_manager::*;
pub use state_machine::*;
pub use storage_engine::*;

#[doc(hidden)]
pub use raft_log::*;
#[doc(hidden)]
pub mod state_machine_test;
#[doc(hidden)]
pub mod storage_engine_test;
