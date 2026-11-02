//! State machine trait for Raft consensus.
//!
//! See the [server customization guide](https://github.com/deventlab/d-engine/blob/master/d-engine/src/docs/server_guide/customize-state-machine.md) for details.

use async_trait::async_trait;
use bytes::Bytes;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;
#[cfg(any(test, feature = "__test_support"))]
use mockall::automock;

use crate::ApplyEntry;
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

/// Outcome of `StateMachine::apply_snapshot_from_file`. Not a plain `Result<()>`:
/// a snapshot can be safely skipped (stale/duplicate) without that being an
/// error, and callers (StateMachineWorker) need to know which happened for
/// logging/metrics.
#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotApplyResult {
    /// The destructive install actually ran.
    Applied { last_included: LogId },
    /// incoming.index < current applied index — no-op, not an error.
    IgnoredStale { current: LogId },
    /// incoming.index == current index, same term — no-op, not an error.
    IgnoredDuplicate { current: LogId },
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
/// - Read methods (`get()`) may be called concurrently
/// - Write methods should use internal synchronization
/// - No assumptions about caller's threading model
///
/// # Durability contract
///
/// The engine never asks a state machine to restore itself; it only calls the methods below. An
/// implementation therefore has to restore its own state whenever it is opened:
///
/// - After reopening the same storage, [`last_applied`](Self::last_applied) returns the position of
///   the last entry whose effects are durable. Persist it together with the data it describes (in
///   one transaction or write batch where the storage allows), so a restart never replays an entry
///   that was already applied.
/// The engine keeps its own record of the snapshots it holds, so a state machine stores nothing
/// about snapshots.
///
/// `StateMachineTestSuite` checks this rule; run it against your implementation.
///
/// # `NotServing`
///
/// While a state machine cannot answer reads correctly, `get` and `get_multi` return
/// `StorageError::NotServing`, for example
/// `Err(StorageError::NotServing("installing a snapshot".into()).into())`. This happens in two
/// situations: the state machine is replacing its state from a snapshot, and it has been closed.
///
/// It means "not now", not "the data is damaged". The engine answers that read through Raft
/// instead, from the state machine after the install, so the client still gets a correct answer.
/// A simple implementation keeps an `AtomicBool` that it clears just before it starts replacing
/// its state and sets again when it has finished.
#[cfg_attr(any(test, feature = "__test_support"), automock)]
#[async_trait]
pub trait StateMachine: Send + Sync + 'static {
    /// Permanently closes the state machine: stops serving, persists what must survive a
    /// restart (e.g. data, metadata), flushes, and releases exclusive resources
    /// (e.g. a DB handle or its lock file).
    ///
    /// Called once by the state machine worker as its last act, after everything queued for
    /// it has been applied. The engine owns the state machine, but a state machine that shares
    /// its storage handle with the developer (a cheap `Clone`) is not dropped when the engine
    /// stops, so this must not rely on `Drop`. Must be synchronous and idempotent; afterwards
    /// every operation returns `NotServing`.
    ///
    /// When it returns, reopening the same storage restores every applied entry and a
    /// `last_applied` that is not older than the one before the call, and every exclusive
    /// resource is released. If persisting fails, still release the resources and return the
    /// first error. A second call returns `Ok(())`.
    /// Default is a no-op, for implementations with nothing to persist or release.
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Retrieves a value by key from the state machine.
    ///
    /// Called concurrently from the read path, so it must be thread-safe and cheap. Return
    /// `StorageError::NotServing` while the state machine cannot answer (for example during a
    /// snapshot install, or after `close()`): the engine then routes the read through Raft.
    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Bytes>, Error>;

    /// Retrieves multiple values by key from the state machine in a single call.
    ///
    /// The engine reads through this method, so a request with several keys is answered in one
    /// call, from one consistent state.
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

    /// Gets the last applied log index and term.
    /// Sync operation as it reads from atomic variables.
    fn last_applied(&self) -> LogId;

    /// Installs a snapshot received from the Raft leader.
    ///
    /// The engine decides whether to install: this is called only for a snapshot that is newer
    /// than `last_applied()`. `snapshot_path` is a directory the engine has already decompressed
    /// the archive into and whose checksum it has verified against `metadata`.
    ///
    /// The implementation must:
    /// 1. Replace its state so that a concurrent reader sees the old state, the new state, or
    ///    `NotServing` (see the trait documentation), never a mix. Either swap the state in one
    ///    atomic step, or return `NotServing` from reads for as long as the replacement takes.
    /// 2. Set `last_applied()` to `metadata.last_included` and make that durable. The engine checks
    ///    this after the call and reports an error if it did not happen.
    async fn apply_snapshot_from_file(
        &self,
        metadata: &SnapshotMetadata,
        snapshot_path: std::path::PathBuf,
    ) -> Result<(), Error>;

    /// Generates a snapshot of the state machine's current key-value entries
    /// up to the specified `last_included_index`.
    ///
    /// Exports the current state (as of `last_included`) to `new_snapshot_dir`. The framework
    /// computes the checksum of that directory itself and records the snapshot only once the
    /// final archive is durably built, so a crash mid-snapshot never leaves a record pointing at
    /// a missing archive; the state machine does neither.
    ///
    /// # Concurrency contract (#436)
    /// May run concurrently with `apply_chunk` (never with `apply_snapshot_from_file` — the two
    /// are mutually exclusive by construction). Must still export an atomically-pinned view: if
    /// `apply_chunk` runs mid-export, the result must reflect strictly before or strictly after
    /// it, never a mix — a torn read across related keys silently corrupts data. Verified by
    /// `test_generate_snapshot_data_is_atomic_under_concurrent_apply`.
    ///
    /// # Arguments
    /// * `new_snapshot_dir` - Temporary path to store the snapshot data.
    /// * `last_included` - Index and term of the last log entry the snapshot includes.
    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
        last_included: LogId,
    ) -> Result<(), Error>;

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
}
