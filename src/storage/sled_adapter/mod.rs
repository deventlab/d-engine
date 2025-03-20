// Submodule declaration
// -----------------------------------------------------------------------------
mod raft_log;
mod state_machine;
mod state_storage;

// Re-export
// -----------------------------------------------------------------------------
pub(crate) use raft_log::*;
pub use state_machine::*;
pub(crate) use state_storage::*;

// -----------------------------------------------------------------------------
// Database namespaces
/// Sled database tree namespaces
const RAFT_LOG_NAMESPACE: &str = "raft_log";
const STATE_MACHINE_NAMESPACE: &str = "state_machine";
const STATE_STORAGE_NAMESPACE: &str = "state_storage";

pub const HARD_STATE_KEY: &str = "HARD_STATE_KEY";
