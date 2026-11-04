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
/// # Errors
///
/// Every method reports a failure with a plain [`std::io::Error`]. The engine does not look at its
/// kind: the message is for the log. Build one from a message, `io::Error::other("disk full")`, or
/// wrap the error of the storage library you use, `.map_err(io::Error::other)`; the original error
/// stays attached as its source.
///
/// What a failure does depends on the method. A failure in `apply_chunk` or
/// `apply_snapshot_from_file` stops the node, which then recovers from its replicas: an entry that
/// was committed cannot be skipped, so the node must not go on without it. A failed `get`,
/// `get_multi` or `scan_prefix` fails only that read.
///
/// # Reads while a snapshot is installed
///
/// A read can run while `apply_snapshot_from_file` replaces the state. It must return the old
/// state, the new state, or fail with an error; it must never return a mix of the two, because
/// that is a state that never existed.
#[cfg_attr(any(test, feature = "__test_support"), automock)]
#[async_trait]
pub trait StateMachine: Send + Sync + 'static {
    /// Persists what must survive a restart (e.g. data, metadata), flushes, and releases exclusive
    /// resources (e.g. a DB handle or its lock file).
    ///
    /// Called once by the state machine worker as its last act, after the engine has stopped its
    /// read path. The engine calls nothing on the state machine once `close()` has started, so you
    /// do not need to define what a closed state machine does when it is asked to read or write.
    /// The engine owns the state machine, but one that shares its storage handle with the
    /// developer (a cheap `Clone`) is not dropped when the engine stops, so this must not rely on
    /// `Drop`. Must be synchronous and may be called more than once; later calls return `Ok(())`.
    ///
    /// When it returns, reopening the same storage restores every applied entry and a
    /// `last_applied` that is not older than the one before the call, and every exclusive
    /// resource is released. If persisting fails, still release the resources and return the
    /// first error.
    ///
    /// Do not rely on `close()` for crash safety. When `shutdown_apply_timeout_ms` expires the
    /// node stops waiting for the worker: `close()` then runs after the node has returned, or not
    /// at all if the process exits first. A restart must be correct without it, which is why
    /// `last_applied` has to be persisted together with the data it describes.
    ///
    /// Default is a no-op, for implementations with nothing to persist or release.
    fn close(&self) -> std::io::Result<()> {
        Ok(())
    }

    /// Retrieves a value by key from the state machine.
    ///
    /// Called concurrently from the read path, so it must be thread-safe and cheap. Return an
    /// error if the value cannot be read; only this read fails.
    fn get(
        &self,
        key_buffer: &[u8],
    ) -> std::io::Result<Option<Bytes>>;

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
    ) -> std::io::Result<Vec<Option<Bytes>>> {
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
    ) -> std::io::Result<Vec<ApplyResult>>;

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
    /// 1. Replace its state so that a concurrent reader sees the old state, the new state, or a
    ///    failed read, never a mix (see "Reads while a snapshot is installed" in the trait
    ///    documentation).
    /// 2. Set `last_applied()` to `metadata.last_included` and make that durable. The engine checks
    ///    this after the call and reports an error if it did not happen.
    async fn apply_snapshot_from_file(
        &self,
        metadata: &SnapshotMetadata,
        snapshot_path: std::path::PathBuf,
    ) -> std::io::Result<()>;

    /// Exports the state machine's current state into `new_snapshot_dir` and returns the position
    /// of the last applied entry that the exported state contains.
    ///
    /// The engine checksums the directory itself, labels the snapshot with the returned position,
    /// and records the snapshot only once the final archive is durably built, so a crash
    /// mid-snapshot never leaves a record pointing at a missing archive; the state machine does
    /// none of that. `new_snapshot_dir` does not exist yet: create it (`create_dir_all`), then put
    /// any regular files in it, with names of your choice; the engine archives the whole directory
    /// and hands it back to
    /// [`apply_snapshot_from_file`](Self::apply_snapshot_from_file) unchanged.
    ///
    /// # The position must describe the exported data
    ///
    /// Read the position from the same consistent view the data is read from, never from a
    /// separate in-memory counter: `apply_chunk` keeps running during the export, so a counter read
    /// a moment earlier or later describes a different state. A snapshot whose position is ahead of
    /// its data makes the node that installs it skip entries for good; one whose position is
    /// behind makes it replay entries on top of data that already contains them, which gives a
    /// different result for operations such as compare-and-swap.
    ///
    /// The engine reads [`last_applied`](Self::last_applied) before and after the call and rejects
    /// a position outside that range; the snapshot is then discarded and the node keeps running.
    /// That check only catches a position that is plainly wrong, so it does not replace the
    /// consistency described above.
    ///
    /// # Exporting a consistent view
    ///
    /// Any of these works:
    /// - read everything inside one read transaction or snapshot of the storage engine, position
    ///   included (MVCC databases);
    /// - take a checkpoint of the storage engine and export from that frozen copy;
    /// - hold a read lock while copying the data and reading the position, with `apply_chunk`
    ///   taking the write lock and updating the position under it.
    ///
    /// # Concurrency contract (#436)
    /// May run concurrently with `apply_chunk` (never with `apply_snapshot_from_file` — the two
    /// are mutually exclusive by construction). Must still export an atomically-pinned view: if
    /// `apply_chunk` runs mid-export, the result must reflect strictly before or strictly after
    /// it, never a mix — a torn read across related keys silently corrupts data. Verified by
    /// `test_generate_snapshot_data_is_atomic_under_concurrent_apply`.
    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
    ) -> std::io::Result<LogId>;

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
    ) -> std::io::Result<ScanResult> {
        let _ = prefix;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "scan_prefix not supported by this state machine",
        ))
    }
}
