// -
// Database namespaces

/// Sled database tree namespaces
pub(crate) const STATE_MACHINE_TREE: &str = "_state_machine_tree";
pub(crate) const STATE_MACHINE_META_NAMESPACE: &str = "_state_machine_metadata";
pub(crate) const STATE_SNAPSHOT_METADATA_TREE: &str = "_snapshot_metadata";

/// Sled entry key namespaces
pub(crate) const STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX: &str = "_raft_last_applied_index";
pub(crate) const STATE_MACHINE_META_KEY_LAST_APPLIED_TERM: &str = "_raft_last_applied_term";

pub(crate) const LAST_SNAPSHOT_METADATA_KEY: &str = "_raft_last_snapshot_metadata";

pub(crate) const STATE_STORAGE_HARD_STATE_KEY: &str = "_state_storage_hard_state";

/// Snapshot dir
pub(crate) const SNAPSHOT_DIR_PREFIX: &str = "snapshot-";
