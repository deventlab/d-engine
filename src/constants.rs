/// Internal client id which is used when new term Leader send an no-op proposal
pub const INTERNAL_CLIENT_ID: u32 = 0;

// -
// Database namespaces

/// Sled database tree namespaces
pub(crate) const STATE_MACHINE_NAMESPACE: &str = "_state_machine";
pub(crate) const STATE_MACHINE_META_NAMESPACE: &str = "_state_machine_meta";

/// Sled entry key namespaces
pub(crate) const STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX: &str = "__raft_last_applied_index";
pub(crate) const STATE_MACHINE_META_KEY_LAST_APPLIED_TERM: &str = "__raft_last_applied_term";

pub(crate) const STATE_STORAGE_HARD_STATE_KEY: &str = "_state_storage_hard_state";

/// Snapshot dir
pub(crate) const SNAPSHOT_DIR: &str = "_snapshot";
