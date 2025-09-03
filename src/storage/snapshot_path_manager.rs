use std::path::PathBuf;

use crate::proto::common::LogId;

/// Centralized manager for all snapshot-related path generation and naming conventions.
///
/// Encapsulates all path construction logic to ensure:
/// - Consistent naming patterns across the system
/// - Easy maintenance of naming conventions
/// - Type-safe path operations
/// - Clear separation of temporary vs final paths

#[derive(Debug)]
pub(crate) struct SnapshotPathManager {
    /// Base directory where snapshots are stored
    pub(crate) base_dir: PathBuf,
    /// Prefix for finalized snapshot files
    pub(crate) snapshot_prefix: String,
    /// Prefix for temporary working files
    pub(crate) temp_prefix: String,
}

impl SnapshotPathManager {
    /// Creates a new path manager with default prefixes
    pub(crate) fn new(
        base_dir: PathBuf,
        snapshot_prefix: String,
    ) -> Self {
        Self {
            base_dir,
            snapshot_prefix,
            temp_prefix: "temp-".to_string(),
        }
    }

    /// Unified temporary file path for snapshot assembly
    pub(crate) fn temp_assembly_file(&self) -> PathBuf {
        self.base_dir.join(format!("{}snapshot.part.tar.gz", self.temp_prefix))
    }

    /// Creates a new path manager with custom prefixes
    #[allow(unused)]
    pub(crate) fn with_prefixes(
        base_dir: PathBuf,
        snapshot_prefix: String,
        temp_prefix: String,
    ) -> Self {
        Self {
            base_dir,
            snapshot_prefix,
            temp_prefix,
        }
    }

    /// Generates the final path for a compressed snapshot file
    pub(crate) fn final_snapshot_path(
        &self,
        log_id: &LogId,
    ) -> PathBuf {
        self.base_dir.join(format!(
            "{}{}-{}.tar.gz",
            self.snapshot_prefix, log_id.index, log_id.term
        ))
    }

    /// Generates a temporary working path for snapshot creation
    pub(crate) fn temp_work_path(
        &self,
        log_id: &LogId,
    ) -> PathBuf {
        self.base_dir.join(format!(
            "{}{}-{}",
            self.temp_prefix, log_id.index, log_id.term
        ))
    }

    /// Generates a versioned temporary path (for atomic writes)
    #[allow(unused)]
    pub(crate) fn versioned_temp_path(
        &self,
        version: u64,
    ) -> PathBuf {
        self.base_dir.join(format!(".temp-{version}"))
    }

    /// Generates a versioned final path (for atomic writes)
    #[allow(unused)]
    pub(crate) fn versioned_final_path(
        &self,
        version: u64,
        log_id: &LogId,
    ) -> PathBuf {
        self.base_dir.join(format!(
            "{}{}-{}-{}",
            self.snapshot_prefix, version, log_id.index, log_id.term
        ))
    }

    /// Extracts metadata from a snapshot filename
    #[allow(unused)]
    pub(crate) fn parse_snapshot_filename(
        &self,
        filename: &str,
    ) -> Option<(u64, u64)> {
        let patterns = [
            &self.snapshot_prefix, // Final snapshot pattern
            &self.temp_prefix,     // Temporary pattern
            "snapshot.part",       // Assembly pattern
        ];

        for pattern in patterns {
            if let Some(stripped) = filename.strip_prefix(pattern) {
                let parts: Vec<&str> = stripped.split('-').collect();
                if parts.len() >= 2 {
                    if let (Ok(index), Ok(term)) =
                        (parts[0].parse(), parts[1].split('.').next()?.parse())
                    {
                        return Some((index, term));
                    }
                }
            }
        }
        None
    }
}
