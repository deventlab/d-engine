//! This module provides the core abstractions for:
//! - Snapshot data streaming
//! - Snapshot metadata handling
//! - Persistent snapshot storage

use std::path::PathBuf;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncRead;
use tonic::async_trait;

use crate::Result;

/// Represents a snapshot data source that supports streaming read
///
/// Key characteristics:
/// - Provides asynchronous streaming access to snapshot bytes
/// - Guarantees thread-safe access through Send + Sync
/// - Avoids loading entire data into memory
#[async_trait]
pub trait Snapshot: Send + Sync {
    /// Returns an asynchronous byte stream reader
    ///
    /// Implementations must:
    /// - Read data in chunks to prevent memory exhaustion
    /// - Maintain read position across multiple poll calls
    /// - Handle IO errors gracefully
    async fn data_stream(&self) -> Result<Box<dyn AsyncRead + Unpin + Send>>;

    /// Returns critical Raft protocol metadata for the snapshot.
    ///
    /// # Invariants
    /// - `last_included_index` must correspond to the last log entry included in the snapshot
    /// - `last_included_term` must match the term of the last included entry
    /// - These values must be atomically captured with the snapshot data
    fn metadata(&self) -> SnapshotMetadata;
}

/// Metadata required by Raft protocol for snapshot consistency.
///
/// # Cross-Node Compatibility
/// - Must use network byte order for serialization
/// - All cluster members must agree on serialization format
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// The index of the last log entry included in the snapshot
    ///
    /// # Raft Spec
    /// - Must be greater than or equal to any log index reported by followers during leader
    ///   elections
    /// - Used to determine log entry compaction points
    pub last_included_index: u64,

    /// The term of the last log entry included in the snapshot
    ///
    /// # Raft Spec
    /// - Critical for leader election consistency checks
    /// - Must match the term of the log entry at `last_included_index`
    pub last_included_term: u64,
}

/// Persistent storage management for snapshots.
///
/// # Implementation Requirements
/// - Must guarantee atomic snapshot writes (complete or nothing)
/// - Should optimize for large binary files (GB-range)
/// - Must handle concurrent read/write access safely
pub trait SnapshotStore: Send + Sync {
    /// Persists a snapshot to durable storage.
    ///
    /// # Parameters
    /// - `snapshot`: Complete snapshot data with valid metadata
    ///
    /// # Returns
    /// - `PathBuf` to the persisted snapshot file for debugging
    ///
    /// # Error Conditions
    /// - Insufficient disk space
    /// - Filesystem permission issues
    /// - Invalid snapshot metadata
    async fn save(
        &self,
        snapshot: &dyn Snapshot,
    ) -> Result<PathBuf>;

    /// Loads the most recent valid snapshot.
    ///
    /// # Selection Criteria
    /// - Highest `last_included_index` value
    /// - Valid metadata and data integrity
    /// - Latest modification timestamp (if available)
    async fn load_latest(&self) -> crate::Result<Option<Box<dyn crate::Snapshot>>>;

    /// Removes obsolete snapshots to reclaim storage.
    ///
    /// # Parameters
    /// - `before_version`: All snapshots with version numbers less than this value should be
    ///   deleted
    ///
    /// # Retention Strategy
    /// - Recommend keeping at least 2 recent snapshots for recovery
    /// - Should delete incrementally (oldest first)
    async fn cleanup(
        &self,
        before_version: u64,
    ) -> Result<()>;
}
