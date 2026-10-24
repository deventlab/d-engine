//! Tunables for the raft-engine log store.
//!
//! They are read from an optional `[raft_engine]` table in the node's config
//! file, next to the d-engine settings. d-engine itself ignores that table.

use std::path::Path;

use d_engine::{Error, StorageError};
use serde::Deserialize;

/// Table name in the node config file.
const TABLE: &str = "raft_engine";

/// Log-store settings. Every field is optional in the file; a missing field (or
/// a missing table) keeps its default. A misspelled key is rejected.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RaftEngineOptions {
    /// Size at which the active log file is closed and the next one is started.
    pub target_file_size_mib: u64,
    /// Log files are reclaimed once the retained log grows past this size.
    pub purge_threshold_mib: u64,
    /// Zero-filled files prepared at startup for reuse as future log files.
    /// Writes into these files overwrite already-allocated blocks.
    pub prefill_limit_mib: u64,
    /// How often expired log files are reclaimed.
    pub purge_interval_secs: u64,
}

impl Default for RaftEngineOptions {
    fn default() -> Self {
        Self {
            target_file_size_mib: 64,
            purge_threshold_mib: 512,
            prefill_limit_mib: 256,
            purge_interval_secs: 10,
        }
    }
}

impl RaftEngineOptions {
    /// Reads the `[raft_engine]` table of the config file at `path`.
    pub fn from_toml_file(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(StorageError::IoError)?;
        let doc: toml::Table = toml::from_str(&text).map_err(|e| {
            StorageError::LogStorage(format!("invalid config file {}: {e}", path.display()))
        })?;
        let options = match doc.get(TABLE) {
            None => Self::default(),
            Some(table) => table.clone().try_into().map_err(|e| {
                StorageError::LogStorage(format!(
                    "invalid [{TABLE}] in {}: {e}",
                    path.display()
                ))
            })?,
        };
        options.validate()?;
        Ok(options)
    }

    /// Rejects values that would make the store misbehave instead of failing
    /// later in a less obvious place.
    pub fn validate(&self) -> Result<(), Error> {
        let positive = [
            ("target_file_size_mib", self.target_file_size_mib),
            ("purge_threshold_mib", self.purge_threshold_mib),
            ("prefill_limit_mib", self.prefill_limit_mib),
            // A zero interval would make the purge thread spin.
            ("purge_interval_secs", self.purge_interval_secs),
        ];
        for (name, value) in positive {
            if value == 0 {
                return Err(StorageError::LogStorage(format!(
                    "[{TABLE}] {name} must be greater than 0"
                ))
                .into());
            }
        }
        if self.purge_threshold_mib < self.target_file_size_mib {
            return Err(StorageError::LogStorage(format!(
                "[{TABLE}] purge_threshold_mib ({}) must not be smaller than target_file_size_mib ({})",
                self.purge_threshold_mib, self.target_file_size_mib
            ))
            .into());
        }
        Ok(())
    }
}
