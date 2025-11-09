//! TTL (Time-To-Live) manager for automatic key expiration.
//!
//! Maintains an in-memory index of key expiration times and provides
//! efficient lookup of expired keys. All expiration times are persisted
//! through state machine snapshots to survive restarts.
//!
//! # Design
//!
//! - Uses `SystemTime` for wall-clock expiration (not monotonic `Instant`)
//! - Two-way index: time→keys and key→time for fast operations
//! - No background threads - expiration checked during `apply()`
//! - Snapshot serialization includes all active TTL metadata

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Manages TTL expiration for keys
///
/// Thread-safe when wrapped in appropriate synchronization primitive.
/// Designed for embedding in StateMachine implementations.
#[derive(Debug, Default)]
pub struct TtlManager {
    /// Index: expiration_time -> list of keys expiring at that time
    /// BTreeMap allows efficient range queries for expired keys
    expirations: BTreeMap<SystemTime, Vec<Bytes>>,

    /// Reverse index: key -> expiration_time
    /// Enables fast TTL cancellation on key update/delete
    key_to_expiry: HashMap<Bytes, SystemTime>,

    /// Whether any TTL key has ever been registered
    /// Once true, stays true forever (even if all TTLs expire)
    /// Used for fast-path optimization: skip cleanup if no TTL ever used
    /// Note: Not serialized in snapshots, reconstructed from key_to_expiry on load
    has_ttl_keys: AtomicBool,
}

impl TtlManager {
    /// Creates a new empty TTL manager
    pub fn new() -> Self {
        Self {
            expirations: BTreeMap::new(),
            key_to_expiry: HashMap::new(),
            has_ttl_keys: AtomicBool::new(false),
        }
    }

    /// Registers a key with TTL in seconds
    ///
    /// If key already has TTL, updates to new expiration time.
    ///
    /// # Arguments
    /// * `key` - Key to set expiration for
    /// * `ttl_secs` - Time-to-live in seconds from now
    ///
    /// # Performance
    /// O(log N) for BTreeMap insertion
    pub fn register(
        &mut self,
        key: Bytes,
        ttl_secs: u64,
    ) {
        // Mark that TTL is being used (lazy activation)
        self.has_ttl_keys.store(true, Ordering::Relaxed);

        // Remove old TTL if exists
        self.unregister(&key);

        // Calculate absolute expiration time
        let expire_at = SystemTime::now() + Duration::from_secs(ttl_secs);

        // Add to indexes
        self.expirations.entry(expire_at).or_default().push(key.clone());
        self.key_to_expiry.insert(key, expire_at);
    }

    /// Removes TTL for a key (on update/delete)
    ///
    /// Called when key is updated without TTL or explicitly deleted.
    ///
    /// # Performance
    /// O(log N) for BTreeMap access + O(K) for vec removal where K = keys at same expiry time
    pub fn unregister(
        &mut self,
        key: &[u8],
    ) {
        if let Some(expire_at) = self.key_to_expiry.remove(key) {
            // Remove from expiration index
            if let Some(keys) = self.expirations.get_mut(&expire_at) {
                keys.retain(|k| k.as_ref() != key);
                // Clean up empty time slots
                if keys.is_empty() {
                    self.expirations.remove(&expire_at);
                }
            }
        }
    }

    /// Returns all keys expired at or before the given time
    ///
    /// Removes returned keys from internal indexes.
    ///
    /// # Performance
    /// O(log N + K) where K = number of expired keys
    pub fn get_expired_keys(
        &mut self,
        now: SystemTime,
    ) -> Vec<Bytes> {
        let mut expired_keys = Vec::new();

        // Collect all expiration times <= now
        let expired_times: Vec<SystemTime> =
            self.expirations.range(..=now).map(|(time, _)| *time).collect();

        // Remove expired entries
        for time in expired_times {
            if let Some(keys) = self.expirations.remove(&time) {
                for key in &keys {
                    self.key_to_expiry.remove(key);
                }
                expired_keys.extend(keys);
            }
        }

        expired_keys
    }

    /// Returns number of keys with active TTL
    pub fn len(&self) -> usize {
        self.key_to_expiry.len()
    }

    /// Returns true if no keys have TTL
    pub fn is_empty(&self) -> bool {
        self.key_to_expiry.is_empty()
    }

    /// Returns true if any TTL key has ever been registered
    ///
    /// This is a fast-path check for lazy activation optimization.
    /// Once true, stays true forever (even if all keys expire).
    ///
    /// Use this to skip cleanup logic entirely if TTL is never used.
    ///
    /// # Performance
    /// O(1) - single atomic load (~5ns)
    pub fn has_ttl_keys(&self) -> bool {
        self.has_ttl_keys.load(Ordering::Relaxed)
    }

    /// Fast check: returns true if there might be expired keys
    ///
    /// This is an O(1) optimization to avoid full expired key scan.
    ///
    /// # Returns
    /// - `false`: Definitely no expired keys (fast path)
    /// - `true`: Maybe has expired keys (need to check with get_expired_keys)
    ///
    /// # Performance
    /// O(1) - single BTreeMap peek (~30ns)
    pub fn may_have_expired_keys(
        &self,
        now: SystemTime,
    ) -> bool {
        if !self.has_ttl_keys() {
            return false; // Never used TTL
        }

        // Check if earliest expiration time <= now
        self.expirations
            .keys()
            .next()
            .map(|first_expiry| *first_expiry <= now)
            .unwrap_or(false)
    }

    /// Returns expiration time for a specific key
    ///
    /// Used for passive expiration check on read access.
    ///
    /// # Performance
    /// O(1) - HashMap lookup
    pub fn get_expiration(
        &self,
        key: &[u8],
    ) -> Option<SystemTime> {
        self.key_to_expiry.get(key).copied()
    }

    /// Serializes TTL state for snapshot
    ///
    /// Uses bincode for compact binary encoding.
    pub fn to_snapshot(&self) -> Vec<u8> {
        let snapshot = TtlSnapshot {
            key_to_expiry: self.key_to_expiry.iter().map(|(k, v)| (k.to_vec(), *v)).collect(),
        };
        bincode::serialize(&snapshot).unwrap_or_default()
    }

    /// Restores TTL state from snapshot
    ///
    /// Filters out already-expired keys during restoration.
    pub fn from_snapshot(data: &[u8]) -> Self {
        let snapshot: TtlSnapshot = match bincode::deserialize(data) {
            Ok(s) => s,
            Err(_) => return Self::new(),
        };

        let now = SystemTime::now();
        let mut manager = Self::new();

        // Rebuild indexes, skipping expired keys
        for (key, expire_at) in snapshot.key_to_expiry {
            if expire_at > now {
                let key_bytes = Bytes::from(key);
                manager.expirations.entry(expire_at).or_default().push(key_bytes.clone());
                manager.key_to_expiry.insert(key_bytes, expire_at);
            }
        }

        // Restore has_ttl_keys flag if we have any TTL keys
        if !manager.key_to_expiry.is_empty() {
            manager.has_ttl_keys.store(true, Ordering::Relaxed);
        }

        manager
    }
}

/// Snapshot-serializable TTL state
#[derive(Debug, Serialize, Deserialize)]
struct TtlSnapshot {
    key_to_expiry: HashMap<Vec<u8>, SystemTime>,
}
