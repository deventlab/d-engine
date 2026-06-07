//! State machine trait for Raft consensus.
//!
//! See the [server customization guide](https://github.com/deventlab/d-engine/blob/master/d-engine/src/docs/server_guide/customize-state-machine.md) for details.

use async_trait::async_trait;
use bytes::Bytes;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;

use crate::ApplyEntry;
#[cfg(any(test, feature = "__test_support"))]
use mockall::automock;

use crate::Error;
use crate::StorageError;

/// All `(key, value)` pairs returned by a prefix scan, plus the revision anchor.
///
/// `revision` equals `last_applied_index` at scan time — clients use it as the
/// filter threshold when draining a watch buffer: skip events where
/// `event.revision <= scan_result.revision` to avoid double-applying.
#[derive(Debug, Clone)]
pub struct ScanResult {
    pub entries: Vec<(Bytes, Bytes)>,
    pub revision: u64,
}

/// Result of applying a single log entry to the state machine
///
/// Returned by `StateMachine::apply_chunk()` for each entry in the chunk.
/// Enables operations like CAS to communicate their execution result back to clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyResult {
    /// Log index of the applied entry
    pub index: u64,

    /// Whether the operation succeeded
    ///
    /// Semantics by operation type:
    /// - CAS: `true` if compare succeeded and value was updated, `false` otherwise
    /// - PUT/DELETE: always `true` (failures return `Err` from `apply_chunk`)
    pub succeeded: bool,
}

impl ApplyResult {
    /// Create a successful result for the given index
    pub fn success(index: u64) -> Self {
        Self {
            index,
            succeeded: true,
        }
    }

    /// Create a failed result for the given index
    pub fn failure(index: u64) -> Self {
        Self {
            index,
            succeeded: false,
        }
    }
}

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

    /// Permanently close underlying storage resources (e.g. DB handle, file descriptors).
    ///
    /// Called by `EmbeddedEngine::stop()` before the Raft loop exits to release
    /// OS-level resources (e.g. RocksDB LOCK file) without waiting for all
    /// `Arc<SM>` clones to drop.
    ///
    /// Default is a no-op — custom state machine implementations do not need to
    /// override this unless they hold exclusive OS resources that must be released
    /// before the process exits or a new engine instance is started.
    fn close_storage(&self) {}

    /// Checks if the state machine is currently running.
    /// Sync operation as it just checks an atomic boolean.
    fn is_running(&self) -> bool;

    /// Retrieves a value by key from the state machine.
    /// Sync operation as it accesses in-memory data structures.
    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Bytes>, Error>;

    /// Retrieves multiple values by key from the state machine in a single call.
    ///
    /// # Why this belongs in the protocol layer
    ///
    /// `get()` is already part of the protocol interface because the ReadActor hot path
    /// requires direct SM access. `get_multi` is its natural batch extension — the same
    /// reasoning applies. An Iterator's `fold` has a default impl; `Vec` overrides it for
    /// performance. Users don't need to know which path executes.
    ///
    /// # Read coherence requirement
    ///
    /// The consistency unit is the **request**, not the key. All returned values must
    /// come from the same applied state. A torn read — key A from apply index 100,
    /// key B from apply index 105 — is a correctness violation in coordinator workloads:
    ///
    /// - Service registry: addr=v2 + version=v2.2 routes traffic to the wrong instance
    /// - Leader election: leader=node-3 + term=43 is a phantom state that never existed
    /// - Quota management: used=850 with a reset window_start blocks valid requests
    ///
    /// etcd (Range + MVCC), TiKV RawKV (BatchGet + RocksDB snapshot), and Consul stale
    /// reads all guarantee snapshot consistency even in eventual/stale modes.
    ///
    /// # Default implementation
    ///
    /// Calls `get()` sequentially. Correct when the caller holds no write locks, but
    /// does not guarantee snapshot isolation under concurrent writes. Override in
    /// implementations that support it (RocksDB `db.snapshot()`, FileStateMachine
    /// read-lock held for the full batch).
    ///
    /// # Position contract
    ///
    /// `result[i]` corresponds to `keys[i]`. Missing keys produce `None` at their
    /// position; the result length always equals `keys.len()`.
    fn get_multi(
        &self,
        keys: &[Bytes],
    ) -> Result<Vec<Option<Bytes>>, Error> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    /// Returns the term of a specific log entry by its ID.
    /// Sync operation as it queries in-memory data.
    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64>;

    /// Applies a batch of decoded log entries to the state machine.
    ///
    /// Receives `&[ApplyEntry]` (already decoded by the framework) instead of raw
    /// proto `Entry`.  The framework decodes `bytes → Command` exactly once in
    /// `DefaultStateMachineHandler`; implementors never touch proto or wire format.
    ///
    /// The returned `Vec` MUST have the same length as `chunk` and preserve order.
    /// Pre-allocate with `Vec::with_capacity(chunk.len())` on the hot path.
    async fn apply_chunk(
        &self,
        chunk: &[ApplyEntry],
    ) -> Result<Vec<ApplyResult>, Error>;

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

    /// Returns all `(key, value)` pairs whose key starts with `prefix`, plus
    /// the `last_applied_index` at the time of the scan.
    ///
    /// Clients use `revision` as the filter anchor when draining watch events
    /// after reconnection: skip events where `event.revision <= revision`.
    ///
    /// # Default implementation
    /// Returns an error. Implementations that support prefix scan must override.
    fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<ScanResult, Error> {
        let _ = prefix;
        Err(Error::System(crate::SystemError::Storage(
            StorageError::StateMachineError(
                "scan_prefix not supported by this state machine".into(),
            ),
        )))
    }

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
