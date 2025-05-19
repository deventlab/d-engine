/// Internal client id which is used when new term Leader send an no-op proposal
pub const INTERNAL_CLIENT_ID: u32 = 0;

// -
// Database namespaces

/// Sled database tree namespaces
pub(crate) const STATE_MACHINE_TREE: &str = "_state_machine_tree";
pub(crate) const STATE_MACHINE_META_NAMESPACE: &str = "_state_machine_metadata";
pub(crate) const STATE_SNAPSHOT_METADATA_TREE: &str = "_snapshot_metadata";

/// Sled entry key namespaces
pub(crate) const STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX: &str = "_raft_last_applied_index";
pub(crate) const STATE_MACHINE_META_KEY_LAST_APPLIED_TERM: &str = "_raft_last_applied_term";
pub(crate) const SNAPSHOT_METADATA_KEY_LAST_INCLUDED_INDEX: &str = "_raft_last_included_index";
pub(crate) const SNAPSHOT_METADATA_KEY_LAST_INCLUDED_TERM: &str = "_raft_last_included_term";
pub(crate) const SNAPSHOT_METADATA_KEY_LAST_SNAPSHOT_CHECKSUM: &str = "_raft_last_snapshot_checksum";

pub(crate) const STATE_STORAGE_HARD_STATE_KEY: &str = "_state_storage_hard_state";

/// Snapshot dir
pub(crate) const SNAPSHOT_DIR_PREFIX: &str = "snapshot-";
