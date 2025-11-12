//! Default lease implementation for d-engine.
//!
//! Provides high-performance lease management with dual-index architecture.
//! The `Lease` trait is defined in d-engine-core for framework-level abstraction.
//!
//! # Architecture
//!
//! - **Hot path (read)**: DashMap for O(1) lock-free expiration checks
//! - **Cold path (cleanup)**: BTreeMap for O(K log N) range-based cleanup
//!
//! # Concurrency Model
//!
//! - **Read path**: Lock-free via DashMap, supports high concurrency
//! - **Write path**: Single-threaded (CommitHandler), Mutex acceptable
//! - **Read-write**: Concurrent safe, reads don't block on cleanup

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use d_engine_core::Lease;

use crate::Result;

/// Default lease implementation with dual-index architecture.
///
/// Optimized for d-engine's access patterns:
/// - **Hot path** (read): DashMap for O(1) lock-free expiration checks
/// - **Cold path** (cleanup): BTreeMap for O(K log N) range-based cleanup
///
/// # Performance Characteristics
///
/// - `is_expired()`: O(1), ~10-20ns, lock-free
/// - `register()`: O(log N), ~30ns + mutex
/// - `unregister()`: O(log N), ~30ns + mutex
/// - `get_expired_keys()`: O(K log N), K = number of expired keys
///
/// # Concurrency Design
///
/// - Read path uses only DashMap (lock-free)
/// - Write path acquires Mutex on BTreeMap
/// - No contention between reads and writes
/// - Write path is single-threaded (CommitHandler), so Mutex is efficient
///
/// # Memory Usage
///
/// - Per-key overhead: ~100 bytes (DashMap entry + BTreeMap vector entry)
/// - Expired keys are removed automatically during cleanup
#[derive(Debug)]
pub struct DefaultLease {
    /// Lease cleanup configuration (immutable after creation)
    config: d_engine_core::config::LeaseConfig,

    /// Apply counter for piggyback cleanup frequency
    apply_counter: AtomicU64,

    /// ✅ HOT PATH: Lock-free concurrent reads
    /// Maps key → expiration_time for O(1) expiration checks
    /// Performance: O(1), ~10-20ns per lookup, lock-free
    key_to_expiry: DashMap<Bytes, SystemTime>,

    /// ⚠️ COLD PATH: Range queries for cleanup
    /// Maps expiration_time → Vec<keys> for efficient cleanup
    /// Performance: O(K log N), K = number of expired keys
    /// Mutex is acceptable because:
    /// - Cleanup is rare (every N applies, max 1ms duration)
    /// - Only called from single-threaded write path (CommitHandler)
    /// - No contention with concurrent reads
    expirations: Mutex<BTreeMap<SystemTime, Vec<Bytes>>>,

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
            expirations: Mutex::new(BTreeMap::new()),
            has_keys: AtomicBool::new(false),
        }
    }

    /// Cleanup expired keys with time limit.
    ///
    /// Internal method used by piggyback cleanup.
    ///
    /// # Performance
    /// O(K log N) where K = number of expired keys
    fn cleanup_expired_with_limit(
        &self,
        _max_duration_ms: u64,
    ) -> Vec<Bytes> {
        // For now, simple implementation without time limiting
        // TODO: Add duration-based limiting in future optimization
        self.get_expired_keys(SystemTime::now())
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

        // Rebuild indexes, skipping expired keys
        for (key, expire_at) in snapshot.key_to_expiry {
            if expire_at > now {
                let key_bytes = Bytes::from(key);
                manager.key_to_expiry.insert(key_bytes.clone(), expire_at);

                let mut expirations = manager.expirations.lock();
                expirations.entry(expire_at).or_default().push(key_bytes);
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
    /// O(log N) - Acquires mutex and updates BTreeMap
    fn register(
        &self,
        key: Bytes,
        ttl_secs: u64,
    ) {
        // Mark that lease is being used (lazy activation)
        self.has_keys.store(true, Ordering::Relaxed);

        // Remove old lease if exists
        if let Some((_, old_expire_at)) = self.key_to_expiry.remove(&key) {
            let mut expirations = self.expirations.lock();
            if let Some(keys) = expirations.get_mut(&old_expire_at) {
                keys.retain(|k| k != &key);
                if keys.is_empty() {
                    expirations.remove(&old_expire_at);
                }
            }
        }

        // Calculate absolute expiration time
        // This is stored in WAL and persisted to disk
        let expire_at = SystemTime::now() + Duration::from_secs(ttl_secs);

        // Insert into both indexes
        self.key_to_expiry.insert(key.clone(), expire_at);

        let mut expirations = self.expirations.lock();
        expirations.entry(expire_at).or_default().push(key);
    }

    fn unregister(
        &self,
        key: &[u8],
    ) {
        if let Some((_, expire_at)) = self.key_to_expiry.remove(key) {
            let mut expirations = self.expirations.lock();
            if let Some(keys) = expirations.get_mut(&expire_at) {
                keys.retain(|k| k.as_ref() != key);
                if keys.is_empty() {
                    expirations.remove(&expire_at);
                }
            }
        }
    }

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

    fn get_expired_keys(
        &self,
        now: SystemTime,
    ) -> Vec<Bytes> {
        let mut expirations = self.expirations.lock();
        let mut expired_keys = Vec::new();

        // Collect all expiration times <= now
        let expired_times: Vec<SystemTime> =
            expirations.range(..=now).map(|(time, _)| *time).collect();

        // Remove expired entries from both indexes
        for time in expired_times {
            if let Some(keys) = expirations.remove(&time) {
                for key in &keys {
                    self.key_to_expiry.remove(key);
                }
                expired_keys.extend(keys);
            }
        }

        expired_keys
    }

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

    fn has_lease_keys(&self) -> bool {
        self.has_keys.load(Ordering::Relaxed)
    }

    fn may_have_expired_keys(
        &self,
        now: SystemTime,
    ) -> bool {
        if !self.has_lease_keys() {
            return false;
        }

        let expirations = self.expirations.lock();
        expirations
            .keys()
            .next()
            .map(|first_expiry| *first_expiry <= now)
            .unwrap_or(false)
    }

    fn len(&self) -> usize {
        self.key_to_expiry.len()
    }

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
        self.expirations.lock().clear();
        self.apply_counter.store(0, Ordering::Relaxed);

        // Rebuild indexes, skipping expired keys
        for (key, expire_at) in snapshot.key_to_expiry {
            if expire_at > now {
                let key_bytes = Bytes::from(key);
                self.key_to_expiry.insert(key_bytes.clone(), expire_at);

                let mut expirations = self.expirations.lock();
                expirations.entry(expire_at).or_default().push(key_bytes);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    fn default_config() -> d_engine_core::config::LeaseConfig {
        d_engine_core::config::LeaseConfig::default()
    }

    #[test]
    fn test_register_and_is_expired() {
        let lease = DefaultLease::new(default_config());

        lease.register(Bytes::from("key1"), 1);
        assert!(!lease.is_expired(b"key1"));

        sleep(Duration::from_secs(2));
        assert!(lease.is_expired(b"key1"));
    }

    #[test]
    fn test_unregister() {
        let lease = DefaultLease::new(default_config());

        lease.register(Bytes::from("key1"), 10);
        assert_eq!(lease.len(), 1);

        lease.unregister(b"key1");
        assert_eq!(lease.len(), 0);
        assert!(!lease.is_expired(b"key1"));
    }

    #[test]
    fn test_get_expired_keys() {
        let lease = DefaultLease::new(default_config());

        lease.register(Bytes::from("key1"), 1);
        lease.register(Bytes::from("key2"), 1);

        sleep(Duration::from_secs(2));

        let expired = lease.get_expired_keys(SystemTime::now());
        assert_eq!(expired.len(), 2);
        assert_eq!(lease.len(), 0);
    }

    #[test]
    fn test_has_lease_keys() {
        let lease = DefaultLease::new(default_config());
        assert!(!lease.has_lease_keys());

        lease.register(Bytes::from("key1"), 10);
        assert!(lease.has_lease_keys());

        lease.unregister(b"key1");
        assert!(lease.has_lease_keys()); // Still true (once set, stays set)
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let config = default_config();
        let lease1 = DefaultLease::new(config.clone());

        lease1.register(Bytes::from("key1"), 3600);
        lease1.register(Bytes::from("key2"), 7200);

        let snapshot = lease1.to_snapshot();
        let lease2 = DefaultLease::from_snapshot(&snapshot, config);

        assert_eq!(lease2.len(), 2);
        assert!(!lease2.is_expired(b"key1"));
        assert!(!lease2.is_expired(b"key2"));
    }

    #[test]
    fn test_reload() {
        let lease = DefaultLease::new(default_config());

        lease.register(Bytes::from("key1"), 3600);
        let snapshot = lease.to_snapshot();

        lease.register(Bytes::from("key2"), 100);
        assert_eq!(lease.len(), 2);

        lease.reload(&snapshot).unwrap();
        assert_eq!(lease.len(), 1);
        assert!(!lease.is_expired(b"key1"));
        assert!(!lease.is_expired(b"key2"));
    }
}
