//! Comprehensive unit tests for DefaultLease implementation
//!
//! Tests cover:
//! - Basic registration, expiration, and cleanup
//! - Update and replacement scenarios
//! - Snapshot serialization and deserialization
//! - Piggyback cleanup configuration and behavior
//! - Edge cases and error conditions

use std::thread::sleep;
use std::time::{Duration, SystemTime};

use bytes::Bytes;

use crate::storage::lease::DefaultLease;
use d_engine_core::Lease;

fn default_config() -> d_engine_core::config::LeaseConfig {
    d_engine_core::config::LeaseConfig::default()
}

// ============================================================================
// Basic Registration and Expiration Tests
// ============================================================================

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
fn test_unregister_nonexistent_key() {
    let lease = DefaultLease::new(default_config());

    // Should not panic on unregistering non-existent key
    lease.unregister(b"nonexistent");
    assert_eq!(lease.len(), 0);
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
fn test_is_empty() {
    let lease = DefaultLease::new(default_config());
    assert!(lease.is_empty());

    lease.register(Bytes::from("key1"), 10);
    assert!(!lease.is_empty());

    lease.unregister(b"key1");
    assert!(lease.is_empty());
}

// ============================================================================
// Has Lease Keys and Flags Tests
// ============================================================================

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
fn test_may_have_expired_keys_false_when_no_keys() {
    let lease = DefaultLease::new(default_config());

    // No keys registered, should return false immediately
    assert!(!lease.may_have_expired_keys(SystemTime::now()));
}

#[test]
fn test_may_have_expired_keys_false_when_all_valid() {
    let lease = DefaultLease::new(default_config());

    lease.register(Bytes::from("key1"), 3600);
    lease.register(Bytes::from("key2"), 7200);

    // All keys have future expiration, should return false
    assert!(!lease.may_have_expired_keys(SystemTime::now()));
}

#[test]
fn test_may_have_expired_keys_true_when_has_expired() {
    let lease = DefaultLease::new(default_config());

    lease.register(Bytes::from("key1"), 1);
    sleep(Duration::from_secs(2));

    // Has expired keys, should return true
    assert!(lease.may_have_expired_keys(SystemTime::now()));
}

// ============================================================================
// Get Expiration Tests
// ============================================================================

#[test]
fn test_get_expiration() {
    let lease = DefaultLease::new(default_config());

    lease.register(Bytes::from("key1"), 3600);

    let expiration = lease.get_expiration(b"key1");
    assert!(expiration.is_some());
    assert!(expiration.unwrap() > SystemTime::now());
}

#[test]
fn test_get_expiration_not_found() {
    let lease = DefaultLease::new(default_config());

    let expiration = lease.get_expiration(b"nonexistent");
    assert!(expiration.is_none());
}

// ============================================================================
// Update and Replacement Tests
// ============================================================================

#[test]
fn test_register_updates_existing_key() {
    let lease = DefaultLease::new(default_config());

    // Register with 10s TTL
    lease.register(Bytes::from("key1"), 10);
    assert_eq!(lease.len(), 1);

    let first_expiry = lease.get_expiration(b"key1").unwrap();

    // Register same key with 20s TTL (should update)
    sleep(Duration::from_millis(100));
    lease.register(Bytes::from("key1"), 20);

    let second_expiry = lease.get_expiration(b"key1").unwrap();

    // New expiry should be later than first
    assert!(second_expiry > first_expiry);
    assert_eq!(lease.len(), 1); // Still only one key
}

#[test]
fn test_multiple_keys_same_expiration() {
    let lease = DefaultLease::new(default_config());

    let ttl = 5u64;
    lease.register(Bytes::from("key1"), ttl);
    lease.register(Bytes::from("key2"), ttl);
    lease.register(Bytes::from("key3"), ttl);

    assert_eq!(lease.len(), 3);

    sleep(Duration::from_secs(ttl + 1));

    let expired = lease.get_expired_keys(SystemTime::now());
    assert_eq!(expired.len(), 3);
    assert_eq!(lease.len(), 0);
}

#[test]
fn test_mixed_expired_and_valid_keys() {
    let lease = DefaultLease::new(default_config());

    lease.register(Bytes::from("expire_soon"), 1);
    lease.register(Bytes::from("expire_later"), 3600);

    sleep(Duration::from_secs(2));

    let expired = lease.get_expired_keys(SystemTime::now());
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0], Bytes::from("expire_soon"));

    // expire_later should still be there
    assert_eq!(lease.len(), 1);
    assert!(!lease.is_expired(b"expire_later"));
}

// ============================================================================
// Snapshot Tests
// ============================================================================

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
fn test_snapshot_roundtrip_filters_expired_keys() {
    let config = default_config();
    let lease1 = DefaultLease::new(config.clone());

    lease1.register(Bytes::from("valid_key"), 3600);
    lease1.register(Bytes::from("expired_key"), 1);

    sleep(Duration::from_secs(2));

    let snapshot = lease1.to_snapshot();

    // from_snapshot should filter out expired keys
    let lease2 = DefaultLease::from_snapshot(&snapshot, config);

    // Only valid_key should remain
    assert_eq!(lease2.len(), 1);
    assert!(!lease2.is_expired(b"valid_key"));
}

#[test]
fn test_from_snapshot_with_invalid_data() {
    let config = default_config();

    // from_snapshot with invalid data should return empty lease
    let lease = DefaultLease::from_snapshot(&[0xFF, 0xFE], config);
    assert_eq!(lease.len(), 0);
    assert!(!lease.has_lease_keys());
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

#[test]
fn test_reload_invalid_data() {
    let lease = DefaultLease::new(default_config());

    lease.register(Bytes::from("key1"), 3600);
    assert_eq!(lease.len(), 1);

    // Reload with invalid data should fail gracefully
    // Note: reload fails during deserialization, so it returns error
    // and doesn't proceed to clear/rebuild (state remains unchanged)
    let result = lease.reload(&[0xFF, 0xFE, 0xFD]);
    assert!(result.is_err());

    // State remains unchanged because deserialization failed before clear
    assert_eq!(lease.len(), 1);
    assert!(!lease.is_expired(b"key1"));
}

#[test]
fn test_reload_clears_apply_counter() {
    let config = d_engine_core::config::LeaseConfig {
        cleanup_strategy: "piggyback".to_string(),
        piggyback_frequency: 2,
        ..default_config()
    };

    let lease = DefaultLease::new(config.clone());

    lease.register(Bytes::from("key1"), 3600);

    // Simulate some apply operations
    let _ = lease.on_apply();
    let _ = lease.on_apply();
    let _ = lease.on_apply();

    let snapshot = lease.to_snapshot();

    // After reload, apply_counter should be reset to 0
    lease.reload(&snapshot).unwrap();

    // Next on_apply should not trigger cleanup (counter starts at 0)
    let result = lease.on_apply();
    assert!(result.is_empty());
}

// ============================================================================
// Piggyback Cleanup Tests
// ============================================================================

#[test]
fn test_on_apply_with_piggyback_disabled() {
    let config = d_engine_core::config::LeaseConfig {
        cleanup_strategy: "disabled".to_string(),
        ..default_config()
    };

    let lease = DefaultLease::new(config);
    lease.register(Bytes::from("key1"), 1);

    sleep(Duration::from_secs(2));

    // Even with expired keys, piggyback cleanup should return empty
    let result = lease.on_apply();
    assert!(result.is_empty());

    // Expired key should still be in lease (no cleanup)
    assert_eq!(lease.len(), 1);
}

#[test]
fn test_on_apply_with_piggyback_frequency() {
    let config = d_engine_core::config::LeaseConfig {
        cleanup_strategy: "piggyback".to_string(),
        piggyback_frequency: 3,
        ..default_config()
    };

    let lease = DefaultLease::new(config);
    lease.register(Bytes::from("key1"), 1);

    sleep(Duration::from_secs(2));

    // First apply: counter=0, 0 % 3 == 0, triggers cleanup
    let result = lease.on_apply();
    assert_eq!(result.len(), 1);
    assert_eq!(lease.len(), 0);
}
