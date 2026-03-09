use serde::Deserialize;
use serde::Serialize;

use crate::Result;

/// Storage engine infrastructure configuration.
///
/// Controls low-level storage backend behavior, independent of Raft protocol
/// semantics. Sits at the same level as `network` and `tls` — infrastructure,
/// not protocol behavior.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct StorageConfig {
    /// Use a single shared RocksDB instance (4 column families) instead of two
    /// separate DB instances (one for Raft log + meta, one for state machine).
    ///
    /// When `true`: one 128 MB block cache, one background thread pool, one
    /// file-descriptor budget — halves resource usage on developer machines.
    ///
    /// When `false` (default): each DB instance manages its own resources,
    /// providing stronger isolation between the log and state machine workloads.
    #[serde(default)]
    pub unified_db: bool,
}

impl StorageConfig {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}
