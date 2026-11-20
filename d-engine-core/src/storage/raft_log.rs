//! RaftLog trait with explicit safety contracts and documentation
//!
//! This trait defines the interface for Raft log storage with comprehensive
//! safety guarantees. Implementers MUST adhere to all documented invariants
//! to maintain Raft consensus protocol correctness.
//!
//! Key Safety Properties (from Raft paper):
//! 1. Log Matching: If two logs contain an entry with same index and term, then logs are identical
//!    in all preceding entries
//! 2. Leader Append-Only: Leaders never overwrite or delete entries
//! 3. State Machine Safety: If a server has applied a log entry at a given index, no other server
//!    will ever apply a different log entry for that index

use std::ops::RangeInclusive;

use async_trait::async_trait;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
#[cfg(any(test, feature = "__test_support"))]
use mockall::automock;

use crate::Result;

#[cfg_attr(any(test, feature = "__test_support"), automock)]
#[async_trait]
pub trait RaftLog: Send + Sync + 'static {
    // =========================================================================
    // READ OPERATIONS (Lock-free, no side effects)
    // =========================================================================

    /// Retrieves a log entry by index.
    ///
    /// # Returns
    /// - `Ok(Some(entry))` if entry exists
    /// - `Ok(None)` if index is out of range
    /// - `Err(_)` only for unrecoverable storage errors
    ///
    /// # Safety Invariants
    /// - MUST be thread-safe and lock-free for maximum performance
    /// - MUST NOT modify any state
    /// - MUST return consistent data within a single term
    fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>>;

    /// Returns the smallest log index (inclusive).
    ///
    /// # Returns
    /// - `0` if log is empty
    /// - First index otherwise (typically 1 after initialization)
    ///
    /// # Safety Invariants
    /// - MUST be monotonically non-decreasing (can only increase via purge)
    /// - MUST be <= last_entry_id()
    fn first_entry_id(&self) -> u64;

    /// Returns the largest log index (inclusive).
    ///
    /// # Returns
    /// - `0` if log is empty
    /// - Last index otherwise
    ///
    /// # Safety Invariants
    /// - MUST be monotonically non-decreasing during normal operation
    /// - Can decrease only during conflict resolution (filter_out_conflicts_and_append)
    /// - MUST be >= first_entry_id()
    fn last_entry_id(&self) -> u64;

    /// Returns the highest log index confirmed crash-safe on this node.
    ///
    /// Advanced only after the storage backend confirms durability (fsync complete).
    /// Used by the Raft core for quorum and commit decisions — never use `last_entry_id()`
    /// for this purpose, as it reflects in-memory state only.
    ///
    /// - MemFirst: lags `last_entry_id()` until `batch_processor` completes fsync.
    /// - DiskFirst: equals `last_entry_id()` (every append blocks until durable).
    fn durable_index(&self) -> u64;

    /// Content-validated durable-watermark advance. `index`/`term` describe
    /// what a completed fsync claims is now safe — rejected (`None`) if
    /// `entry_term(index) != Some(term)`, meaning the log content at that
    /// index has changed (truncated + replaced) since fsync started on it.
    /// `Some(new_value)` only when it actually advanced — callers use this
    /// to decide whether to fire `handle_log_flushed`.
    fn try_advance_durable_index(
        &self,
        mark: LogId,
    ) -> Option<u64>;

    /// Returns the LogId (term + index) of the last entry.
    ///
    /// # Returns
    /// - `None` if log is empty
    /// - `Some(LogId)` otherwise
    ///
    /// # Safety Invariants
    /// - MUST match last_entry().map(|e| LogId { term: e.term, index: e.index })
    /// - Critical for leader election and log matching
    fn last_log_id(&self) -> Option<LogId>;

    /// Returns the last log entry.
    ///
    /// # Safety Invariants
    /// - MUST be equivalent to entry(last_entry_id())
    /// - Performance optimization for common access pattern
    fn last_entry(&self) -> Option<Entry>;

    /// Checks if log is empty.
    ///
    /// # Safety Invariants
    /// - MUST be equivalent to (first_entry_id() == 0)
    fn is_empty(&self) -> bool;

    /// Returns the term of a specific entry.
    ///
    /// # Safety Invariants
    /// - MUST be equivalent to entry(entry_id).map(|e| e.term)
    /// - Critical for AppendEntries RPC consistency checks
    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64>;
    /// Finds the first index belonging to a specific term.
    ///
    /// # Returns
    /// - `None` if no entry with this term exists
    /// - `Some(index)` of the first entry with matching term
    ///
    /// # Safety Invariants
    /// - MUST scan forward from first_entry_id()
    /// - Result MUST be <= last_index_for_term(term)
    /// - Critical for calculating commit index
    fn first_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64>;

    /// Finds the last index belonging to a specific term.
    ///
    /// # Returns
    /// - `None` if no entry with this term exists
    /// - `Some(index)` of the last entry with matching term
    ///
    /// # Safety Invariants
    /// - MUST scan backward from last_entry_id()
    /// - Result MUST be >= first_index_for_term(term)
    /// - Critical for leader election and log compaction
    fn last_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64>;

    /// Retrieves a contiguous range of log entries.
    ///
    /// # Arguments
    /// - `range`: Inclusive range [start, end]
    ///
    /// # Returns
    /// - `Ok(Vec<Entry>)` with all entries in range
    /// - Empty vec if range is out of bounds
    ///
    /// # Safety Invariants
    /// - Returned entries MUST be ordered by index (ascending)
    /// - MUST include all entries in range without gaps
    /// - Critical for AppendEntries RPC replication
    fn get_entries_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>>;

    // =========================================================================
    // INDEX ALLOCATION (Thread-safe, atomic)
    // =========================================================================

    /// Pre-allocates the next log index atomically.
    ///
    /// # Returns
    /// - Next available index
    ///
    /// # Safety Invariants
    /// - MUST be thread-safe (atomic increment)
    /// - MUST return unique, monotonically increasing values
    /// - Each call MUST return a different value across all threads
    /// - Used by leader to assign indexes to client proposals
    fn pre_allocate_raft_logs_next_index(&self) -> u64;

    /// Pre-allocates a contiguous range of log indices atomically.
    ///
    /// # Arguments
    /// - `count`: Number of indices to allocate
    ///
    /// # Returns
    /// - Inclusive range [start, end] where (end - start + 1) == count
    /// - Empty range (u64::MAX..=u64::MAX) if count == 0
    ///
    /// # Safety Invariants
    /// - MUST be atomic across all threads
    /// - MUST NOT return overlapping ranges
    /// - MUST prevent overflow (panic if allocation would exceed u64::MAX)
    /// - Used for batch proposal optimization
    fn pre_allocate_id_range(
        &self,
        count: u64,
    ) -> RangeInclusive<u64>;

    // =========================================================================
    // WRITE OPERATIONS (CRITICAL: Durability and Atomicity Required)
    // =========================================================================

    /// Appends `entries` to the end of the log without waiting for them to reach disk.
    ///
    /// On return the entries are visible to every read method (`entry`, `last_log_id`, ...)
    /// and have been written to the storage engine, but they are not yet crash-safe: the
    /// fsync runs in the background. When it completes, `durable_index()` advances and the
    /// log sends `InternalEvent::LogFlushed`. Nothing that promises durability to another node
    /// (an AppendEntries success, a commit) may rely on this call alone.
    ///
    /// An empty `entries` is a no-op.
    ///
    /// # Contract
    /// - The caller passes entries in index order, contiguous, not overlapping existing
    ///   entries. They are not re-checked; use `filter_out_conflicts_and_append` when the new
    ///   entries may overlap or conflict with the log.
    ///
    /// # Errors
    /// Returns a fatal error if the log is poisoned or the storage engine write fails. The
    /// log stays poisoned; the caller must stop, not retry.
    async fn append_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()>;

    /// Applies an AppendEntries batch: checks the entry before it, drops the local entries
    /// that conflict with it, and appends what is new.
    ///
    /// `new_entries` must start at `prev_log_index + 1`. If the local log has no entry at
    /// `prev_log_index` with `prev_log_term`, nothing changes and the current `last_log_id()`
    /// is returned. `prev_log_index == 0 && prev_log_term == 0` stands for the position before
    /// the first entry and always matches; it does not clear the log.
    ///
    /// Otherwise the log is cut at the first new entry whose index is already present with a
    /// different term, and every new entry from there on is appended. New entries that already
    /// match are skipped, so a repeated batch changes nothing. Local entries are removed only
    /// where a new entry conflicts: an empty or stale batch never shortens the log. On success
    /// the id of the last new entry is returned.
    ///
    /// Like `append_entries`, this does not wait for disk; the truncation becomes durable with
    /// the fsync of the appended entries. The caller must not acknowledge the batch before
    /// `durable_index()` covers it.
    ///
    /// # Errors
    /// - `NonContiguousAppend` if `new_entries` does not start at `prev_log_index + 1` or has a gap.
    /// - A fatal error if the log is poisoned or the storage engine write fails.
    async fn filter_out_conflicts_and_append(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        new_entries: Vec<Entry>,
    ) -> Result<Option<LogId>>;

    /// Returns the highest index a majority of voters hold durably, if the leader may commit
    /// up to it now.
    ///
    /// The leader's own `durable_index()` is added to `matched_ids`, and the median of the
    /// result is the highest index held by a majority. It is returned only if it is above
    /// `commit_index` and the entry there belongs to `current_term`: a leader never commits
    /// an entry of an earlier term by counting replicas (Raft §5.4.2); such entries commit
    /// together with a later entry of its own term.
    ///
    /// # Contract
    /// - `matched_ids` holds exactly one value per voter other than the leader: its match
    ///   index, or 0 if it has confirmed nothing. A missing voter or an extra learner
    ///   moves the median, and the leader may then commit an entry most voters do not hold.
    /// - The leader's contribution is its durable index, never its in-memory tail, so a
    ///   commit never depends on entries the leader could still lose in a crash.
    fn calculate_majority_matched_index(
        &self,
        current_term: u64,
        commit_index: u64,
        matched_ids: Vec<u64>,
    ) -> Option<u64>;

    /// Removes every entry up to and including `cutoff`, because a snapshot already covers them.
    ///
    /// Afterwards `cutoff` is the purge boundary: `entry_term(cutoff.index)` returns
    /// `cutoff.term`, so an AppendEntries whose previous entry is the boundary still matches,
    /// and `durable_index()` counts the boundary as durable because the snapshot holds it.
    ///
    /// Only the prefix is removed. Entries after `cutoff` are kept exactly as they are, whether
    /// or not they agree with the snapshot; a caller that installs a snapshot conflicting with
    /// the local log must drop those entries itself first (see `truncate_from`).
    ///
    /// # Contract
    /// - The caller holds a durable snapshot that covers `cutoff`; nothing up to `cutoff` is
    ///   needed again.
    /// - The purge is durable when this returns `Ok`.
    /// - A crash during the call leaves the log either exactly as before or fully purged: the
    ///   removed entries and the boundary always agree, and entries after `cutoff` are never lost.
    ///
    /// # Errors
    /// Returns a fatal error if the log is poisoned or the storage engine purge fails.
    async fn purge_logs_up_to(
        &self,
        cutoff_index: LogId,
    ) -> Result<()>;

    /// Waits until every entry appended so far is on disk.
    ///
    /// Writes out entries not yet handed to the storage engine, asks for an fsync up to the
    /// current last entry, and returns once it completes; returns at once if everything is
    /// already durable. It covers entries only: a log that only shrank (truncation) is not
    /// synced by this call.
    ///
    /// The normal write path does not use it: durability is reported through `durable_index()`
    /// and `InternalEvent::LogFlushed`. It is used when the log is closed.
    ///
    /// # Errors
    /// Returns an error if the storage engine write or the fsync fails, or if the fsync worker
    /// has stopped.
    async fn flush(&self) -> Result<()>;

    /// Resets the entire log storage (destructive operation).
    ///
    /// # Safety Invariants
    /// - MUST clear all log entries
    /// - MUST clear all metadata
    /// - MUST reset first_entry_id() and last_entry_id() to 0
    /// - MUST clear term indexes
    /// - MUST be atomic (no partial state visible)
    /// - MUST NOT be called during normal operation (only during initialization/recovery)
    ///
    /// # Durability Contract
    /// - Changes MUST be durable before returning Ok(())
    async fn reset(&self) -> Result<()>;

    // =========================================================================
    // PERSISTENT STATE (Raft Paper §5.2)
    // =========================================================================

    /// Returns the term and vote saved by the last successful `save_hard_state`, or `None` on a
    /// node that has never saved one.
    ///
    /// Read once at startup to restore the node's term and vote.
    ///
    /// # Errors
    /// Returns an error if the stored state cannot be read or decoded. Startup treats it as fatal.
    fn load_hard_state(&self) -> Result<Option<crate::HardState>>;

    /// Saves the current term and vote, and returns only once they are on disk.
    ///
    /// A node that votes, restarts and forgets the vote can vote again in the same term, and two
    /// candidates can then both win it. Role code changes the term or the vote only through
    /// `commit_hard_state`, which calls this before the change is acted on (for example before a
    /// vote is sent). It is also called once more on shutdown.
    ///
    /// # Contract
    /// - Durable on return: after `Ok(())`, `load_hard_state` returns this state, even after a crash.
    /// - The caller never lowers the term; this method does not check it.
    ///
    /// # Errors
    /// Returns a fatal error if the log is poisoned or the metadata store write or flush fails.
    /// The log is then marked poisoned and the node stops.
    fn save_hard_state(
        &self,
        hard_state: &crate::HardState,
    ) -> Result<()>;

    /// Gracefully closes the log, ensuring all pending IO completes and any
    /// background IO threads have exited before returning.
    ///
    /// Called during node shutdown. Default is a no-op for implementations
    /// without a dedicated background IO thread.
    async fn close(&self) {}
}
