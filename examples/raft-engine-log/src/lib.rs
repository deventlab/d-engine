//! raft-engine-backed `StorageEngine` for d-engine.
//!
//! The log and the hard state live in raft-engine, which writes into
//! preallocated segment files instead of appending to a growing file.

mod entry_record;
mod options;
mod raft_engine_storage;
#[cfg(test)]
mod raft_engine_storage_test;
#[cfg(test)]
mod storage_engine_suite_test;
#[cfg(test)]
mod options_test;

pub use options::RaftEngineOptions;
pub use raft_engine_storage::{RaftEngineLogStore, RaftEngineMetaStore, RaftEngineStorage};
