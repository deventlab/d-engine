// Submodule declaration
// -----------------------------------------------------------------------------
mod raft_log;
// mod snapshot;
mod state_machine;
mod state_storage;

// Re-export
// -----------------------------------------------------------------------------
pub use raft_log::*;
// pub use snapshot::*;
pub use state_machine::*;
pub use state_storage::*;
// -----------------------------------------------------------------------------
// Database namespaces
/// Sled database tree namespaces
const RAFT_LOG_NAMESPACE: &str = "raft_log";
const STATE_STORAGE_NAMESPACE: &str = "state_storage";
