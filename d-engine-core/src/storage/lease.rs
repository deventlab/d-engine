//! Lease (Time-based key expiration) trait for d-engine.
//!
//! Provides the interface for automatic key expiration through lease-based lifecycle management.
//! This is a framework-level abstraction that allows custom lease implementations.
//!
//! # Design Philosophy
//!
//! Inspired by etcd's lease concept, d-engine provides lease management as a framework-level
//! feature that all state machines (including custom implementations) can leverage.
//!
//! # Architecture
//!
//! - **Trait-based**: `Lease` trait defines the interface
//! - **Implementation**: d-engine-server provides `DefaultLease` with high-performance dual-index design
//! - **Zero overhead**: Completely disabled when not used
//! - **Snapshot support**: Full persistence through Raft snapshots
//!
//! # Concurrency Model
//!
//! Implementations should follow these guidelines:
//! - **Read path (hot)**: Lock-free, supports high concurrency
//! - **Write path (cold)**: Single-threaded (CommitHandler), Mutex acceptable
//! - **Read-write**: Concurrent safe, reads don't block on cleanup

use std::time::SystemTime;

use bytes::Bytes;

use crate::Result;

/// Lease (租约) management interface for key expiration.
///
/// Manages key lifecycles through time-based leases. d-engine provides a default
/// implementation (`DefaultLease` in d-engine-server), but developers can implement
/// custom lease management strategies.
///
/// # Thread Safety
///
/// All methods must be thread-safe as they will be called concurrently from:
/// - Read path: Multiple concurrent client reads
/// - Write path: Single-threaded apply operations
///
/// # Performance Requirements
///
/// - `is_expired()`: Must be O(1) and lock-free (hot path, called on every read)
/// - `register()` / `unregister()`: Should be O(log N) or better
/// - `get_expired_keys()`: Should be O(K log N) where K = expired keys
///
/// # Example Implementation
///
/// ```ignore
/// use d_engine_core::storage::Lease;
/// use bytes::Bytes;
/// use std::time::SystemTime;
///
/// struct MyCustomLease {
///     // Your data structures
/// }
///
/// impl Lease for MyCustomLease {
///     fn register(&self, key: Bytes, ttl_secs: u64) {
///         // Your implementation
///     }
///
///     fn is_expired(&self, key: &[u8]) -> bool {
///         // Your implementation
///     }
///
///     // ... other methods
/// }
/// ```
pub trait Lease: Send + Sync + 'static {
    /// Register a lease for a key.
    ///
    /// If the key already has a lease, updates to the new expiration time.
    ///
    /// # Arguments
    /// * `key` - Key to set expiration for
    /// * `ttl_secs` - Time-to-live in seconds from now
    ///
    /// # Performance
    /// Should be O(log N) or better. Called on every insert with TTL.
    fn register(
        &self,
        key: Bytes,
        ttl_secs: u64,
    );

    /// Remove lease for a key (on update/delete).
    ///
    /// Called when a key is updated without TTL or explicitly deleted.
    ///
    /// # Performance
    /// Should be O(log N) or better.
    fn unregister(
        &self,
        key: &[u8],
    );

    /// Check if a key's lease has expired.
    ///
    /// # Returns
    /// * `true` - Key has expired and should be treated as non-existent
    /// * `false` - Key has not expired or has no lease
    ///
    /// # Performance
    /// **CRITICAL**: Must be O(1) and lock-free. This is the hot path,
    /// called on every read operation.
    fn is_expired(
        &self,
        key: &[u8],
    ) -> bool;

    /// Get all keys with expired leases.
    ///
    /// Removes returned keys from internal lease indexes.
    ///
    /// # Arguments
    /// * `now` - Current time to check expiration against
    ///
    /// # Returns
    /// List of expired keys (keys are removed from lease tracking)
    ///
    /// # Performance
    /// Should be O(K log N) where K = number of expired keys.
    fn get_expired_keys(
        &self,
        now: SystemTime,
    ) -> Vec<Bytes>;

    /// Called on every apply operation (piggyback cleanup).
    ///
    /// The lease implementation decides internally whether to perform cleanup
    /// based on its configuration (e.g., piggyback strategy with frequency control).
    ///
    /// # Returns
    /// List of expired keys that were cleaned up (may be empty)
    ///
    /// # Performance
    /// Should be O(1) most of the time (fast path when not cleaning).
    fn on_apply(&self) -> Vec<Bytes>;

    /// Check if any key has ever been registered with a lease.
    ///
    /// Used for fast-path optimization to skip lease logic entirely when
    /// leases are not used.
    ///
    /// # Returns
    /// * `true` - At least one key has been registered (even if expired)
    /// * `false` - No keys have ever been registered
    ///
    /// # Performance
    /// Must be O(1) - simple flag check.
    fn has_lease_keys(&self) -> bool;

    /// Fast check: returns true if there might be expired keys.
    ///
    /// This is an O(1) optimization to avoid full expired key scan.
    ///
    /// # Returns
    /// * `false` - Definitely no expired keys (fast path)
    /// * `true` - Maybe has expired keys (need full check)
    ///
    /// # Performance
    /// Should be O(1) or O(log N) at most.
    fn may_have_expired_keys(
        &self,
        now: SystemTime,
    ) -> bool;

    /// Get number of keys with active leases.
    ///
    /// # Performance
    /// Should be O(1).
    fn len(&self) -> usize;

    /// Check if no keys have active leases.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Serialize lease state for snapshot.
    ///
    /// Used by Raft snapshot mechanism to persist lease metadata.
    ///
    /// # Returns
    /// Serialized bytes (format is implementation-defined)
    fn to_snapshot(&self) -> Vec<u8>;

    /// Reload lease state from snapshot data.
    ///
    /// Used during snapshot restoration. Should filter out already-expired leases.
    ///
    /// # Arguments
    /// * `data` - Serialized snapshot data
    ///
    /// # Errors
    /// Returns error if deserialization fails.
    fn reload(
        &self,
        data: &[u8],
    ) -> Result<()>;
}
