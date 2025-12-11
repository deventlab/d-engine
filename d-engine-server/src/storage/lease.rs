//! Default lease implementation for d-engine.
//!
//! Provides high-performance lease management with single-index lock-free architecture.
//! The `Lease` trait is defined in d-engine-core for framework-level abstraction.
//!
//! # Architecture
//!
//! - **Single index**: DashMap<key, expiration_time> (completely lock-free for register/unregister)
//! - **Cleanup**: O(N) iteration with time limit and shard read locks (rare operation)
//!
//! # Concurrency Model
//!
//! - **Register**: O(1) lock-free (single shard write lock)
//! - **Unregister**: O(1) lock-free (single shard write lock)
//! - **Cleanup**: O(N) with shard read locks (frequency: 1/1000 applies, duration: 1ms max)
//! - **No global Mutex** - Eliminates lock contention under high concurrency
//!
//! # Performance vs Dual-Index Design
//!
//! Old design (BTreeMap + Mutex):
//! - Register: O(log N) + Mutex lock → contention under concurrency
//! - Cleanup: O(K log N) + Mutex lock
//!
//! New design (DashMap only):
//! - Register: O(1) lock-free → zero contention
//! - Cleanup: O(N) with read locks → no write blocking, rare execution

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use d_engine_core::Lease;

use crate::Result;

/// Default lease implementation with single-index lock-free architecture.
///
/// # Performance Characteristics
///
/// - `is_expired()`: O(1), ~10-20ns, lock-free
/// - `register()`: O(1), ~20ns, lock-free (single shard write lock)
/// - `unregister()`: O(1), ~20ns, lock-free (single shard write lock)
/// - `cleanup()`: O(N), time-limited, shard read locks (rare: 1/1000 applies)
///
/// # Memory Usage
///
/// - Per-key overhead: ~50 bytes (single DashMap entry)
/// - Expired keys are removed automatically during cleanup
#[derive(Debug)]
pub struct DefaultLease {
    /// Lease cleanup configuration (immutable after creation)
    config: d_engine_core::config::LeaseConfig,

    /// Apply counter for piggyback cleanup frequency
    apply_counter: AtomicU64,

    /// ✅ Single index: key → expiration_time (completely lock-free)
    /// - Register/Unregister: O(1), single shard lock
    /// - Cleanup: O(N) iteration with shard read locks
    key_to_expiry: DashMap<Bytes, SystemTime>,

    /// Whether any lease has ever been registered
    /// Once true, stays true forever (optimization flag)
    has_keys: AtomicBool,
}

impl DefaultLease {
    /// Creates a new default lease manager with the given configuration.
    ///
    /// # Arguments
    /// * `config` - Lease cleanup strategy configuration
    pub fn new(config: d_engine_core::config::LeaseConfig) -> Self {
        Self {
            config,
            apply_counter: AtomicU64::new(0),
            key_to_expiry: DashMap::new(),
            has_keys: AtomicBool::new(false),
        }
    }

    /// Cleanup expired keys with time limit (方案 2: iter + remove_if).
    ///
    /// Uses DashMap::iter() which acquires read locks on all shards.
    /// Read locks allow concurrent writes to proceed (only blocks on same shard).
    ///
    /// # Performance
    ///
    /// - Iteration: O(N) with shard read locks
    /// - Removal: O(K) where K = expired keys, lock-free per-key
    /// - Time limit: Stops after max_duration_ms to prevent long pauses
    /// - Frequency: Only called every N applies (default: 1/1000)
    ///
    /// # Arguments
    /// * `max_duration_ms` - Maximum duration in milliseconds
    fn cleanup_expired_with_limit(
        &self,
        max_duration_ms: u64,
    ) -> Vec<Bytes> {
        let start = Instant::now();
        let now = SystemTime::now();

        // Phase 1: collect expired keys (read-only, with time limit)
        let to_remove: Vec<Bytes> = self
            .key_to_expiry
            .iter()
            .take_while(|_| start.elapsed().as_millis() <= max_duration_ms as u128)
            .filter(|entry| *entry.value() <= now)
            .map(|entry| entry.key().clone())
            .collect();

        // Phase 2: remove after dropping iter (avoids deadlock)
        to_remove
            .into_iter()
            .filter_map(|key| self.key_to_expiry.remove_if(&key, |_, v| *v <= now).map(|(k, _)| k))
            .collect()
    }

    /// Get expiration time for a specific key.
    ///
    /// # Performance
    /// O(1) - DashMap lookup, lock-free
    #[allow(dead_code)]
    pub fn get_expiration(
        &self,
        key: &[u8],
    ) -> Option<SystemTime> {
        self.key_to_expiry.get(key).map(|entry| *entry.value())
    }

    /// Restore from snapshot data (used during initialization).
    ///
    /// Filters out already-expired keys during restoration.
    ///
    /// # Arguments
    /// * `data` - Serialized snapshot data
    /// * `config` - Lease configuration to use
    pub fn from_snapshot(
        data: &[u8],
        config: d_engine_core::config::LeaseConfig,
    ) -> Self {
        let snapshot: LeaseSnapshot = match bincode::deserialize(data) {
            Ok(s) => s,
            Err(_) => return Self::new(config),
        };

        let now = SystemTime::now();
        let manager = Self::new(config);

        // Rebuild single index, skipping expired keys
        for (key, expire_at) in snapshot.key_to_expiry {
            if expire_at > now {
                let key_bytes = Bytes::from(key);
                manager.key_to_expiry.insert(key_bytes, expire_at);
            }
        }

        // Restore has_keys flag if we have any keys
        if !manager.key_to_expiry.is_empty() {
            manager.has_keys.store(true, Ordering::Relaxed);
        }

        manager
    }
}

impl Lease for DefaultLease {
    /// Register a key with TTL (Time-To-Live).
    ///
    /// # TTL Semantics
    ///
    /// - **Absolute expiration time**: The expiration time is calculated as
    ///   `expire_at = SystemTime::now() + Duration::from_secs(ttl_secs)` and stored internally.
    /// - **Crash-safe**: The absolute expiration time survives node restarts. After crash recovery,
    ///   expired keys remain expired (no TTL reset).
    /// - **Persistent**: The expiration time is persisted to disk during snapshot generation
    ///   and graceful shutdown.
    ///
    /// # Example
    ///
    /// ```text
    /// T0:  register(key="foo", ttl=10) → expire_at = T0 + 10 = T10
    /// T5:  CRASH
    /// T12: RESTART
    ///      → WAL replay: expire_at = T10 < T12 (already expired)
    ///      → Key is NOT restored (correctly expired)
    /// ```
    ///
    /// # Arguments
    ///
    /// * `key` - The key to register expiration for
    /// * `ttl_secs` - Time-to-live in seconds from now
    ///
    /// # Performance
    ///
    /// O(1) - Completely lock-free, only acquires single shard write lock
    fn register(
        &self,
        key: Bytes,
        ttl_secs: u64,
    ) {
        // Mark that lease is being used (lazy activation)
        self.has_keys.store(true, Ordering::Relaxed);

        // Calculate absolute expiration time
        let expire_at = SystemTime::now() + Duration::from_secs(ttl_secs);

        // Single index update (overwrites old value if exists)
        // DashMap::insert is lock-free (only single shard write lock)
        self.key_to_expiry.insert(key, expire_at);
    }

    /// Unregister a key's TTL.
    ///
    /// # Performance
    ///
    /// O(1) - Completely lock-free, only acquires single shard write lock
    fn unregister(
        &self,
        key: &[u8],
    ) {
        // Single index removal (lock-free)
        self.key_to_expiry.remove(key);
    }

    /// Check if a key is expired.
    ///
    /// # Performance
    ///
    /// O(1) - Lock-free DashMap lookup
    fn is_expired(
        &self,
        key: &[u8],
    ) -> bool {
        if let Some(expire_at) = self.key_to_expiry.get(key) {
            *expire_at <= SystemTime::now()
        } else {
            false
        }
    }

    /// Get all expired keys (without time limit).
    ///
    /// This method is rarely used directly. Most cleanup happens via `on_apply()`.
    ///
    /// # Performance
    ///
    /// O(N) - Iterates all keys with shard read locks
    fn get_expired_keys(
        &self,
        now: SystemTime,
    ) -> Vec<Bytes> {
        // Phase 1: collect expired keys (read-only)
        let to_remove: Vec<Bytes> = self
            .key_to_expiry
            .iter()
            .filter(|entry| *entry.value() <= now)
            .map(|entry| entry.key().clone())
            .collect();

        // Phase 2: remove after dropping iter (avoids deadlock)
        to_remove
            .into_iter()
            .filter_map(|key| self.key_to_expiry.remove_if(&key, |_, v| *v <= now).map(|(k, _)| k))
            .collect()
    }

    /// Piggyback cleanup on apply operations.
    ///
    /// Called every N applies (configured via piggyback_frequency).
    /// Returns expired keys that were removed.
    ///
    /// # Performance
    ///
    /// - Fast path (no cleanup): O(1) atomic check
    /// - Cleanup path: O(N) with time limit, shard read locks
    fn on_apply(&self) -> Vec<Bytes> {
        // Fast path: if piggyback is not enabled, return immediately
        if !self.config.is_piggyback() {
            return vec![];
        }

        // Piggyback cleanup: check if it's time to cleanup
        let count = self.apply_counter.fetch_add(1, Ordering::Relaxed);
        if count % self.config.piggyback_frequency == 0 {
            self.cleanup_expired_with_limit(self.config.max_cleanup_duration_ms)
        } else {
            vec![]
        }
    }

    /// Check if any keys have been registered.
    ///
    /// # Performance
    ///
    /// O(1) - Single atomic load
    fn has_lease_keys(&self) -> bool {
        self.has_keys.load(Ordering::Relaxed)
    }

    /// Quick check if there might be expired keys.
    ///
    /// This is a heuristic check - samples first 10 entries.
    /// May return false negatives but never false positives.
    ///
    /// # Performance
    ///
    /// O(1) - Checks first few entries only
    fn may_have_expired_keys(
        &self,
        now: SystemTime,
    ) -> bool {
        if !self.has_lease_keys() {
            return false;
        }

        // Quick check: iterate first 10 entries
        // DashMap::iter().take(10) is cheap (early termination)
        for entry in self.key_to_expiry.iter().take(10) {
            if *entry.value() <= now {
                return true;
            }
        }

        false
    }

    /// Get total number of keys with active leases.
    ///
    /// # Performance
    ///
    /// O(1) - DashMap maintains internal count
    fn len(&self) -> usize {
        self.key_to_expiry.len()
    }

    /// Serialize current lease state to snapshot.
    ///
    /// # Performance
    ///
    /// O(N) - Iterates all keys with shard read locks
    fn to_snapshot(&self) -> Vec<u8> {
        let snapshot = LeaseSnapshot {
            key_to_expiry: self
                .key_to_expiry
                .iter()
                .map(|entry| (entry.key().to_vec(), *entry.value()))
                .collect(),
        };
        bincode::serialize(&snapshot).unwrap_or_default()
    }

    /// Reload lease state from snapshot.
    ///
    /// Filters out already-expired keys during restoration.
    ///
    /// # Performance
    ///
    /// O(N) - Rebuilds single index
    fn reload(
        &self,
        data: &[u8],
    ) -> Result<()> {
        let snapshot: LeaseSnapshot = bincode::deserialize(data).map_err(|e| {
            crate::Error::System(d_engine_core::SystemError::Storage(
                d_engine_core::StorageError::StateMachineError(format!(
                    "Failed to deserialize lease snapshot: {e}"
                )),
            ))
        })?;

        let now = SystemTime::now();

        // Clear existing data
        self.key_to_expiry.clear();
        self.apply_counter.store(0, Ordering::Relaxed);

        // Rebuild single index, skipping expired keys
        for (key, expire_at) in snapshot.key_to_expiry {
            if expire_at > now {
                let key_bytes = Bytes::from(key);
                self.key_to_expiry.insert(key_bytes, expire_at);
            }
        }

        // Update has_keys flag
        if !self.key_to_expiry.is_empty() {
            self.has_keys.store(true, Ordering::Relaxed);
        }

        Ok(())
    }
}

/// Snapshot-serializable lease state.
#[derive(Debug, Serialize, Deserialize)]
struct LeaseSnapshot {
    key_to_expiry: HashMap<Vec<u8>, SystemTime>,
}
