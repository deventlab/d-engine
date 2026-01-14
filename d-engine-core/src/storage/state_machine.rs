//! State machine trait for Raft consensus.
//!
//! See the [server customization guide](https://github.com/deventlab/d-engine/blob/master/d-engine/src/docs/server_guide/customize-state-machine.md) for details.

use bytes::Bytes;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;
#[cfg(any(test, feature = "__test_support"))]
use mockall::automock;
use tonic::async_trait;

use crate::Error;

/// State machine trait for Raft consensus
///
/// # Thread Safety Requirements
///
/// **CRITICAL**: Implementations MUST be thread-safe.
///
/// - Read methods (`get()`, `len()`) may be called concurrently
/// - Write methods should use internal synchronization
/// - No assumptions about caller's threading model
#[cfg_attr(any(test, feature = "__test_support"), automock)]
#[async_trait]
pub trait StateMachine: Send + Sync + 'static {
    /// Starts the state machine service.
    ///
    /// This method:
    /// 1. Flips internal state flags
    /// 2. Loads persisted data (e.g., TTL state from disk)
    ///
    /// Called once during node startup. Performance is not critical.
    async fn start(&self) -> Result<(), Error>;

    /// Stops the state machine service gracefully.
    /// This is typically a sync operation for state management.
    fn stop(&self) -> Result<(), Error>;

    /// Checks if the state machine is currently running.
    /// Sync operation as it just checks an atomic boolean.
    fn is_running(&self) -> bool;

    /// Retrieves a value by key from the state machine.
    /// Sync operation as it accesses in-memory data structures.
    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Bytes>, Error>;

    /// Returns the term of a specific log entry by its ID.
    /// Sync operation as it queries in-memory data.
    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64>;

    /// Applies a chunk of log entries to the state machine.
    /// Async operation as it may involve disk I/O for persistence.
    async fn apply_chunk(
        &self,
        chunk: Vec<Entry>,
    ) -> Result<(), Error>;

    /// Returns the number of entries in the state machine.
    /// NOTE: This may be expensive for some implementations.
    /// Sync operation but should be used cautiously.
    fn len(&self) -> usize;

    /// Checks if the state machine is empty.
    /// Sync operation that typically delegates to len().
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Updates the last applied index in memory.
    /// Sync operation as it just updates atomic variables.
    fn update_last_applied(
        &self,
        last_applied: LogId,
    );

    /// Gets the last applied log index and term.
    /// Sync operation as it reads from atomic variables.
    fn last_applied(&self) -> LogId;

    /// Persists the last applied index to durable storage.
    /// Should be async as it involves disk I/O.
    fn persist_last_applied(
        &self,
        last_applied: LogId,
    ) -> Result<(), Error>;

    /// Updates snapshot metadata in memory.
    /// Sync operation as it updates in-memory structures.
    fn update_last_snapshot_metadata(
        &self,
        snapshot_metadata: &SnapshotMetadata,
    ) -> Result<(), Error>;

    /// Retrieves the current snapshot metadata.
    /// Sync operation as it reads from in-memory structures.
    fn snapshot_metadata(&self) -> Option<SnapshotMetadata>;

    /// Persists snapshot metadata to durable storage.
    /// Should be async as it involves disk I/O.
    fn persist_last_snapshot_metadata(
        &self,
        snapshot_metadata: &SnapshotMetadata,
    ) -> Result<(), Error>;

    /// Applies a snapshot received from the Raft leader to the local state machine
    ///
    /// # Critical Security and Integrity Measures
    /// 1. Checksum Validation: Verifies snapshot integrity before application
    /// 2. Version Validation: Ensures snapshot is newer than current state
    /// 3. Atomic Application: Uses locking to prevent concurrent modifications
    /// 4. File Validation: Confirms compressed format before decompression
    ///
    /// # Workflow
    /// 1. Validate snapshot metadata and version
    /// 2. Verify compressed file format
    /// 3. Decompress to temporary directory
    /// 4. Validate checklsum
    /// 5. Initialize new state machine database
    /// 6. Atomically replace current database
    /// 7. Update Raft metadata and indexes
    async fn apply_snapshot_from_file(
        &self,
        metadata: &SnapshotMetadata,
        snapshot_path: std::path::PathBuf,
    ) -> Result<(), Error>;

    /// Generates a snapshot of the state machine's current key-value entries
    /// up to the specified `last_included_index`.
    ///
    /// This function:
    /// 1. Creates a new database at `temp_snapshot_path`.
    /// 2. Copies all key-value entries from the current state machine's database where the key
    ///    (interpreted as a log index) does not exceed `last_included_index`.
    /// 3. Uses batch writes for efficiency, committing every 100 records.
    /// 4. Will update last_included_index and last_included_term in memory
    /// 5. Will persist last_included_index and last_included_term into current database and new
    ///    database specified by `temp_snapshot_path`
    ///
    /// # Arguments
    /// * `new_snapshot_dir` - Temporary path to store the snapshot data.
    /// * `last_included_index` - Last log index included in the snapshot.
    /// * `last_included_term` - Last log term included in the snapshot.
    ///
    /// # Returns
    /// * if success, checksum will be returned
    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
        last_included: LogId,
    ) -> Result<Bytes, Error>;

    /// Saves the hard state of the state machine.
    /// Sync operation as it typically just delegates to other persistence methods.
    fn save_hard_state(&self) -> Result<(), Error>;

    /// Flushes any pending writes to durable storage.
    /// Sync operation that may block but provides a synchronous interface.
    fn flush(&self) -> Result<(), Error>;

    /// Flushes any pending writes to durable storage.
    /// Async operation as it involves disk I/O.
    async fn flush_async(&self) -> Result<(), Error>;

    /// Resets the state machine to its initial state.
    /// Async operation as it may involve cleaning up files and data.
    async fn reset(&self) -> Result<(), Error>;

    /// Background lease cleanup hook.
    ///
    /// Called periodically by the background cleanup task (if enabled).
    /// Returns keys that were cleaned up.
    ///
    /// # Default Implementation
    /// No-op, suitable for state machines without lease support.
    ///
    /// # Called By
    /// Framework calls this from background cleanup task spawned in NodeBuilder::build().
    /// Only called when cleanup_strategy = "background".
    ///
    /// # Returns
    /// - Vec of deleted keys (for logging/metrics)
    ///
    /// # Example (d-engine built-in state machines)
    /// ```ignore
    /// async fn lease_background_cleanup(&self) -> Result<Vec<Bytes>, Error> {
    ///     if let Some(ref lease) = self.lease {
    ///         let expired_keys = lease.get_expired_keys(SystemTime::now());
    ///         if !expired_keys.is_empty() {
    ///             self.delete_batch(&expired_keys).await?;
    ///         }
    ///         return Ok(expired_keys);
    ///     }
    ///     Ok(vec![])
    /// }
    /// ```
    async fn lease_background_cleanup(&self) -> Result<Vec<bytes::Bytes>, Error> {
        Ok(vec![])
    }
}
