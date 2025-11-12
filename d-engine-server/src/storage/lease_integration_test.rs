#![allow(clippy::field_reassign_with_default)]
#![allow(clippy::uninlined_format_args)]

//! Integration tests for Lease functionality across the full stack
//!
//! This module contains comprehensive tests for:
//! - DefaultLease unit tests (moved from ttl_manager.rs)
//! - FileStateMachine Lease integration tests
//! - RocksDBStateMachine Lease integration tests

mod lease_tests {
    use crate::storage::DefaultLease;
    use bytes::Bytes;
    use d_engine_core::Lease;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_register_and_get_expired() {
        let config = d_engine_core::config::LeaseConfig::default();
        let manager = DefaultLease::new(config);

        // Register keys with 1 second TTL
        manager.register(Bytes::from("key1"), 1);
        manager.register(Bytes::from("key2"), 1);

        assert_eq!(manager.len(), 2);

        // No keys expired yet
        let expired = manager.get_expired_keys(std::time::SystemTime::now());
        assert_eq!(expired.len(), 0);

        // Wait for expiration
        sleep(Duration::from_secs(2));

        let expired = manager.get_expired_keys(std::time::SystemTime::now());
        assert_eq!(expired.len(), 2);
        assert_eq!(manager.len(), 0);
    }

    #[test]
    fn test_unregister() {
        let config = d_engine_core::config::LeaseConfig::default();
        let manager = DefaultLease::new(config);

        manager.register(Bytes::from("key1"), 10);
        assert_eq!(manager.len(), 1);

        manager.unregister(b"key1");
        assert_eq!(manager.len(), 0);
    }

    #[test]
    fn test_update_ttl() {
        let config = d_engine_core::config::LeaseConfig::default();
        let manager = DefaultLease::new(config);

        // Register with 10 seconds
        manager.register(Bytes::from("key1"), 10);

        // Update to 20 seconds
        manager.register(Bytes::from("key1"), 20);

        // Should only have one TTL entry (old one replaced)
        assert_eq!(manager.len(), 1);
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let config = d_engine_core::config::LeaseConfig::default();
        let manager = DefaultLease::new(config.clone());

        manager.register(Bytes::from("key1"), 3600);
        manager.register(Bytes::from("key2"), 7200);

        let snapshot = manager.to_snapshot();
        let restored = DefaultLease::from_snapshot(&snapshot, config);

        assert_eq!(restored.len(), 2);
    }

    #[test]
    fn test_snapshot_filters_expired() {
        let config = d_engine_core::config::LeaseConfig::default();
        let manager = DefaultLease::new(config.clone());

        // Register key with 1 second TTL
        manager.register(Bytes::from("key1"), 1);
        manager.register(Bytes::from("key2"), 3600);

        sleep(Duration::from_secs(2));

        let snapshot = manager.to_snapshot();
        let restored = DefaultLease::from_snapshot(&snapshot, config);

        // Only key2 should be restored
        assert_eq!(restored.len(), 1);
    }
}

mod file_state_machine_tests {
    use bytes::Bytes;
    use prost::Message;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::time::sleep;

    use crate::storage::{DefaultLease, FileStateMachine};
    use d_engine_core::StateMachine;
    use d_engine_proto::client::{
        WriteCommand,
        write_command::{Insert, Operation},
    };
    use d_engine_proto::common::{Entry, EntryPayload, entry_payload::Payload};

    /// Helper to create a FileStateMachine with lease injected for testing
    async fn create_file_state_machine_with_lease(
        path: std::path::PathBuf,
        lease_config: d_engine_core::config::LeaseConfig,
    ) -> FileStateMachine {
        let mut sm = FileStateMachine::new(path).await.unwrap();
        let lease = std::sync::Arc::new(DefaultLease::new(lease_config));
        sm.set_lease(lease);
        sm.load_lease_data().await.unwrap();
        sm
    }

    /// Helper to create an entry with Insert command
    fn create_insert_entry(
        index: u64,
        term: u64,
        key: &[u8],
        value: &[u8],
        ttl_secs: Option<u64>,
    ) -> Entry {
        let insert = Insert {
            key: Bytes::from(key.to_vec()),
            value: Bytes::from(value.to_vec()),
            ttl_secs,
        };
        let write_cmd = WriteCommand {
            operation: Some(Operation::Insert(insert)),
        };
        let payload = Payload::Command(write_cmd.encode_to_vec().into());

        Entry {
            index,
            term,
            payload: Some(EntryPayload {
                payload: Some(payload),
            }),
        }
    }

    #[tokio::test]
    async fn test_ttl_expiration_after_apply() {
        let temp_dir = TempDir::new().unwrap();
        let mut lease_config = d_engine_core::config::LeaseConfig::default();
        lease_config.cleanup_strategy = "piggyback".to_string();
        let sm =
            create_file_state_machine_with_lease(temp_dir.path().to_path_buf(), lease_config).await;

        // Insert key with 2 second TTL
        let entry = create_insert_entry(1, 1, b"ttl_key", b"ttl_value", Some(2));
        sm.apply_chunk(vec![entry]).await.unwrap();

        // Key should exist immediately
        let value = sm.get(b"ttl_key").unwrap();
        assert_eq!(value, Some(Bytes::from("ttl_value")));

        // Wait for expiration
        sleep(Duration::from_secs(3)).await;

        // Apply another entry to trigger expiration check
        let entry2 = create_insert_entry(2, 1, b"other_key", b"other_value", None);
        sm.apply_chunk(vec![entry2]).await.unwrap();

        // Key should be expired
        let value = sm.get(b"ttl_key").unwrap();
        assert_eq!(value, None);

        // Other key should still exist
        let value = sm.get(b"other_key").unwrap();
        assert_eq!(value, Some(Bytes::from("other_value")));
    }

    #[tokio::test]
    async fn test_ttl_snapshot_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();
        let sm = FileStateMachine::new(temp_dir.path().to_path_buf()).await.unwrap();

        // Insert key with 3600 second TTL (won't expire during test)
        let entry = create_insert_entry(1, 1, b"persistent_key", b"persistent_value", Some(3600));
        sm.apply_chunk(vec![entry]).await.unwrap();

        // Create snapshot
        let snapshot_dir = temp_dir.path().join("snapshot");
        sm.generate_snapshot_data(
            snapshot_dir.clone(),
            d_engine_proto::common::LogId { index: 1, term: 1 },
        )
        .await
        .unwrap();

        // Create new state machine and restore snapshot
        let temp_dir2 = TempDir::new().unwrap();
        let sm2 = FileStateMachine::new(temp_dir2.path().to_path_buf()).await.unwrap();

        sm2.apply_snapshot_from_file(
            &d_engine_proto::server::storage::SnapshotMetadata {
                last_included: Some(d_engine_proto::common::LogId { index: 1, term: 1 }),
                checksum: Bytes::new(),
            },
            snapshot_dir,
        )
        .await
        .unwrap();

        // Key should exist in restored state machine
        let value = sm2.get(b"persistent_key").unwrap();
        assert_eq!(value, Some(Bytes::from("persistent_value")));
    }

    #[tokio::test]
    async fn test_file_state_machine_ttl_persistence_across_restart() {
        let temp_dir = TempDir::new().unwrap();
        let state_machine_path = temp_dir.path().to_path_buf();
        let mut lease_config = d_engine_core::config::LeaseConfig::default();
        lease_config.cleanup_strategy = "piggyback".to_string();

        // Phase 1: Create state machine and insert keys with TTL
        {
            let sm = create_file_state_machine_with_lease(
                state_machine_path.clone(),
                lease_config.clone(),
            )
            .await;

            let entry1 = create_insert_entry(1, 1, b"short_ttl_key", b"value1", Some(2));
            let entry2 = create_insert_entry(2, 1, b"long_ttl_key", b"value2", Some(3600));
            let entry3 = create_insert_entry(3, 1, b"no_ttl_key", b"value3", None);

            sm.apply_chunk(vec![entry1, entry2, entry3]).await.unwrap();

            // Verify all keys exist
            assert_eq!(
                sm.get(b"short_ttl_key").unwrap(),
                Some(Bytes::from("value1"))
            );
            assert_eq!(
                sm.get(b"long_ttl_key").unwrap(),
                Some(Bytes::from("value2"))
            );
            assert_eq!(sm.get(b"no_ttl_key").unwrap(), Some(Bytes::from("value3")));

            // Gracefully stop state machine to persist TTL data
            sm.stop().unwrap();
            // State machine drops here
        }

        // Phase 2: Restart - create new state machine from same directory
        {
            let sm = create_file_state_machine_with_lease(state_machine_path.clone(), lease_config)
                .await;

            // Verify all keys still exist after restart
            assert_eq!(
                sm.get(b"short_ttl_key").unwrap(),
                Some(Bytes::from("value1"))
            );
            assert_eq!(
                sm.get(b"long_ttl_key").unwrap(),
                Some(Bytes::from("value2"))
            );
            assert_eq!(sm.get(b"no_ttl_key").unwrap(), Some(Bytes::from("value3")));

            // Wait for short TTL to expire
            sleep(Duration::from_secs(3)).await;

            // Trigger expiration check
            let entry4 = create_insert_entry(4, 1, b"trigger", b"trigger", None);
            sm.apply_chunk(vec![entry4]).await.unwrap();

            // short_ttl_key should be expired
            assert_eq!(sm.get(b"short_ttl_key").unwrap(), None);
            // long_ttl_key should still exist
            assert_eq!(
                sm.get(b"long_ttl_key").unwrap(),
                Some(Bytes::from("value2"))
            );
            // no_ttl_key should still exist
            assert_eq!(sm.get(b"no_ttl_key").unwrap(), Some(Bytes::from("value3")));
        }
    }

    #[tokio::test]
    async fn test_ttl_update_cancels_previous() {
        let temp_dir = TempDir::new().unwrap();
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();
        let sm = FileStateMachine::new(temp_dir.path().to_path_buf()).await.unwrap();

        // Insert key with 2 second TTL
        let entry1 = create_insert_entry(1, 1, b"update_key", b"value1", Some(2));
        sm.apply_chunk(vec![entry1]).await.unwrap();

        // Immediately update with longer TTL
        let entry2 = create_insert_entry(2, 1, b"update_key", b"value2", Some(10));
        sm.apply_chunk(vec![entry2]).await.unwrap();

        // Wait past original TTL
        sleep(Duration::from_secs(3)).await;

        // Trigger expiration check
        let entry3 = create_insert_entry(3, 1, b"trigger", b"trigger", None);
        sm.apply_chunk(vec![entry3]).await.unwrap();

        // Key should still exist (new TTL not expired)
        let value = sm.get(b"update_key").unwrap();
        assert_eq!(value, Some(Bytes::from("value2")));
    }

    #[tokio::test]
    async fn test_passive_deletion_on_get() {
        let temp_dir = TempDir::new().unwrap();
        let mut lease_config = d_engine_core::config::LeaseConfig::default();
        lease_config.cleanup_strategy = "piggyback".to_string();
        let sm =
            create_file_state_machine_with_lease(temp_dir.path().to_path_buf(), lease_config).await;

        // Insert key with 1 second TTL
        let entry = create_insert_entry(1, 1, b"passive_key", b"passive_value", Some(1));
        sm.apply_chunk(vec![entry]).await.unwrap();

        // Key should exist immediately
        let value = sm.get(b"passive_key").unwrap();
        assert_eq!(value, Some(Bytes::from("passive_value")));

        // Wait for expiration
        sleep(Duration::from_secs(2)).await;

        // Passive deletion: key should be deleted on get() without apply_chunk
        let value = sm.get(b"passive_key").unwrap();
        assert_eq!(value, None);

        // Verify key is truly deleted (not just hidden)
        let value = sm.get(b"passive_key").unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_piggyback_cleanup_frequency() {
        let temp_dir = TempDir::new().unwrap();
        let mut lease_config = d_engine_core::config::LeaseConfig::default();
        lease_config.cleanup_strategy = "piggyback".to_string();
        let sm =
            create_file_state_machine_with_lease(temp_dir.path().to_path_buf(), lease_config).await;

        // Insert 5 keys with 1 second TTL
        for i in 0..5 {
            let key = format!("piggyback_key_{i}");
            let entry = create_insert_entry(i + 1, 1, key.as_bytes(), b"value", Some(1));
            sm.apply_chunk(vec![entry]).await.unwrap();
        }

        // Wait for all keys to expire
        sleep(Duration::from_secs(2)).await;

        // Apply 100 entries to trigger piggyback cleanup (frequency=100)
        for i in 100..200 {
            let entry = create_insert_entry(i, 1, b"dummy", b"dummy", None);
            sm.apply_chunk(vec![entry]).await.unwrap();
        }

        // Expired keys should be cleaned up by piggyback mechanism
        for i in 0..5 {
            let key = format!("piggyback_key_{i}");
            let value = sm.get(key.as_bytes()).unwrap();
            assert_eq!(
                value, None,
                "Key {} should be deleted by piggyback cleanup",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_lazy_activation_no_ttl_overhead() {
        let temp_dir = TempDir::new().unwrap();
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();
        let sm = FileStateMachine::new(temp_dir.path().to_path_buf()).await.unwrap();

        // Insert keys WITHOUT TTL
        for i in 0..10 {
            let key = format!("no_ttl_key_{i}");
            let entry = create_insert_entry(i + 1, 1, key.as_bytes(), b"value", None);
            sm.apply_chunk(vec![entry]).await.unwrap();
        }

        // Apply 150 more entries (should trigger piggyback cleanup check)
        for i in 10..160 {
            let entry = create_insert_entry(i + 1, 1, b"dummy", b"dummy", None);
            sm.apply_chunk(vec![entry]).await.unwrap();
        }

        // All keys should still exist (no TTL, lazy activation should skip cleanup)
        for i in 0..10 {
            let key = format!("no_ttl_key_{i}");
            let value = sm.get(key.as_bytes()).unwrap();
            assert_eq!(value, Some(Bytes::from("value")));
        }
    }
}

#[cfg(all(test, feature = "rocksdb"))]
mod rocksdb_state_machine_tests {
    use bytes::Bytes;
    use prost::Message;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::time::sleep;

    use crate::storage::RocksDBStateMachine;
    use d_engine_core::StateMachine;
    use d_engine_proto::client::{
        WriteCommand,
        write_command::{Delete, Insert, Operation},
    };
    use d_engine_proto::common::{Entry, EntryPayload, entry_payload::Payload};

    /// Helper to create a RocksDBStateMachine with lease injected for testing
    async fn create_rocksdb_state_machine_with_lease(
        path: std::path::PathBuf,
        lease_config: d_engine_core::config::LeaseConfig,
    ) -> RocksDBStateMachine {
        let mut sm = RocksDBStateMachine::new(path).unwrap();
        let lease = std::sync::Arc::new(crate::storage::DefaultLease::new(lease_config));
        sm.set_lease(lease);
        sm.load_lease_data().await.unwrap();
        sm
    }

    /// Helper to create an entry with Insert command
    fn create_insert_entry(
        index: u64,
        term: u64,
        key: &[u8],
        value: &[u8],
        ttl_secs: Option<u64>,
    ) -> Entry {
        let insert = Insert {
            key: Bytes::from(key.to_vec()),
            value: Bytes::from(value.to_vec()),
            ttl_secs,
        };
        let write_cmd = WriteCommand {
            operation: Some(Operation::Insert(insert)),
        };
        let payload = Payload::Command(write_cmd.encode_to_vec().into());

        Entry {
            index,
            term,
            payload: Some(EntryPayload {
                payload: Some(payload),
            }),
        }
    }

    /// Helper to create an entry with Delete command
    fn create_delete_entry(
        index: u64,
        term: u64,
        key: &[u8],
    ) -> Entry {
        let delete = Delete {
            key: Bytes::from(key.to_vec()),
        };
        let write_cmd = WriteCommand {
            operation: Some(Operation::Delete(delete)),
        };
        let payload = Payload::Command(write_cmd.encode_to_vec().into());

        Entry {
            index,
            term,
            payload: Some(EntryPayload {
                payload: Some(payload),
            }),
        }
    }

    #[tokio::test]
    async fn test_rocksdb_ttl_expiration_after_apply() {
        let temp_dir = TempDir::new().unwrap();
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();
        let sm =
            create_rocksdb_state_machine_with_lease(temp_dir.path().join("rocksdb"), ttl_config)
                .await;

        // Insert key with 2 second TTL
        let entry = create_insert_entry(1, 1, b"ttl_key", b"ttl_value", Some(2));
        sm.apply_chunk(vec![entry]).await.unwrap();

        // Key should exist immediately
        let value = sm.get(b"ttl_key").unwrap();
        assert_eq!(value, Some(Bytes::from("ttl_value")));

        // Wait for expiration
        sleep(Duration::from_secs(3)).await;

        // Apply another entry to trigger expiration check
        let entry2 = create_insert_entry(2, 1, b"other_key", b"other_value", None);
        sm.apply_chunk(vec![entry2]).await.unwrap();

        // Key should be expired
        let value = sm.get(b"ttl_key").unwrap();
        assert_eq!(value, None);

        // Other key should still exist
        let value = sm.get(b"other_key").unwrap();
        assert_eq!(value, Some(Bytes::from("other_value")));
    }

    #[tokio::test]
    #[ignore] // TODO: Fix RocksDB lock contention in snapshot restoration
    async fn test_rocksdb_ttl_snapshot_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();
        let sm = create_rocksdb_state_machine_with_lease(
            temp_dir.path().join("rocksdb"),
            ttl_config.clone(),
        )
        .await;

        // Insert key with 3600 second TTL (won't expire during test)
        let entry = create_insert_entry(1, 1, b"persistent_key", b"persistent_value", Some(3600));
        sm.apply_chunk(vec![entry]).await.unwrap();

        // Create snapshot
        let snapshot_dir = temp_dir.path().join("snapshot");
        sm.generate_snapshot_data(
            snapshot_dir.clone(),
            d_engine_proto::common::LogId { index: 1, term: 1 },
        )
        .await
        .unwrap();

        // Verify TTL state file exists
        let ttl_file = snapshot_dir.join("ttl_state.bin");
        assert!(
            ttl_file.exists(),
            "TTL state file should be created in snapshot"
        );

        // Create new state machine and restore snapshot
        let temp_dir2 = TempDir::new().unwrap();
        let sm2 =
            create_rocksdb_state_machine_with_lease(temp_dir2.path().join("rocksdb"), ttl_config)
                .await;

        sm2.apply_snapshot_from_file(
            &d_engine_proto::server::storage::SnapshotMetadata {
                last_included: Some(d_engine_proto::common::LogId { index: 1, term: 1 }),
                checksum: Bytes::new(),
            },
            snapshot_dir,
        )
        .await
        .unwrap();

        // TTL manager should be restored (we can't directly check the key in RocksDB
        // since the checkpoint restoration is not fully implemented, but TTL state is restored)
    }

    #[tokio::test]
    async fn test_rocksdb_ttl_update_cancels_previous() {
        let temp_dir = TempDir::new().unwrap();
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();
        let sm =
            create_rocksdb_state_machine_with_lease(temp_dir.path().join("rocksdb"), ttl_config)
                .await;

        // Insert key with 2 second TTL
        let entry1 = create_insert_entry(1, 1, b"update_key", b"value1", Some(2));
        sm.apply_chunk(vec![entry1]).await.unwrap();

        // Immediately update with longer TTL
        let entry2 = create_insert_entry(2, 1, b"update_key", b"value2", Some(10));
        sm.apply_chunk(vec![entry2]).await.unwrap();

        // Wait past original TTL
        sleep(Duration::from_secs(3)).await;

        // Trigger expiration check
        let entry3 = create_insert_entry(3, 1, b"trigger", b"trigger", None);
        sm.apply_chunk(vec![entry3]).await.unwrap();

        // Key should still exist (new TTL not expired)
        let value = sm.get(b"update_key").unwrap();
        assert_eq!(value, Some(Bytes::from("value2")));
    }

    #[tokio::test]
    async fn test_rocksdb_ttl_delete_unregisters() {
        let temp_dir = TempDir::new().unwrap();
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();
        let sm =
            create_rocksdb_state_machine_with_lease(temp_dir.path().join("rocksdb"), ttl_config)
                .await;

        // Insert key with TTL
        let entry1 = create_insert_entry(1, 1, b"delete_key", b"delete_value", Some(3600));
        sm.apply_chunk(vec![entry1]).await.unwrap();

        // Delete the key
        let entry2 = create_delete_entry(2, 1, b"delete_key");
        sm.apply_chunk(vec![entry2]).await.unwrap();

        // Key should not exist
        let value = sm.get(b"delete_key").unwrap();
        assert_eq!(value, None);

        // Even after waiting, no expiration should occur (TTL was unregistered)
        sleep(Duration::from_secs(2)).await;
        let entry3 = create_insert_entry(3, 1, b"trigger", b"trigger", None);
        sm.apply_chunk(vec![entry3]).await.unwrap();
    }

    #[tokio::test]
    async fn test_rocksdb_multiple_keys_with_different_ttls() {
        let temp_dir = TempDir::new().unwrap();
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();
        let sm =
            create_rocksdb_state_machine_with_lease(temp_dir.path().join("rocksdb"), ttl_config)
                .await;

        // Insert multiple keys with different TTLs
        let entry1 = create_insert_entry(1, 1, b"key_1sec", b"value1", Some(1));
        let entry2 = create_insert_entry(2, 1, b"key_5sec", b"value2", Some(5));
        let entry3 = create_insert_entry(3, 1, b"key_no_ttl", b"value3", None);

        sm.apply_chunk(vec![entry1, entry2, entry3]).await.unwrap();

        // All keys should exist initially
        assert_eq!(sm.get(b"key_1sec").unwrap(), Some(Bytes::from("value1")));
        assert_eq!(sm.get(b"key_5sec").unwrap(), Some(Bytes::from("value2")));
        assert_eq!(sm.get(b"key_no_ttl").unwrap(), Some(Bytes::from("value3")));

        // Wait 2 seconds
        sleep(Duration::from_secs(2)).await;

        // Trigger expiration check
        let entry4 = create_insert_entry(4, 1, b"trigger", b"trigger", None);
        sm.apply_chunk(vec![entry4]).await.unwrap();

        // key_1sec should be expired
        assert_eq!(sm.get(b"key_1sec").unwrap(), None);
        // key_5sec and key_no_ttl should still exist
        assert_eq!(sm.get(b"key_5sec").unwrap(), Some(Bytes::from("value2")));
        assert_eq!(sm.get(b"key_no_ttl").unwrap(), Some(Bytes::from("value3")));

        // Wait another 4 seconds (total 6 seconds)
        sleep(Duration::from_secs(4)).await;

        // Trigger expiration check again
        let entry5 = create_insert_entry(5, 1, b"trigger2", b"trigger2", None);
        sm.apply_chunk(vec![entry5]).await.unwrap();

        // key_5sec should now be expired too
        assert_eq!(sm.get(b"key_5sec").unwrap(), None);
        // key_no_ttl should still exist
        assert_eq!(sm.get(b"key_no_ttl").unwrap(), Some(Bytes::from("value3")));
    }

    #[tokio::test]
    async fn test_rocksdb_ttl_persistence_across_restart() {
        let temp_dir = TempDir::new().unwrap();
        let state_machine_path = temp_dir.path().join("rocksdb");
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();

        // Phase 1: Create state machine and insert keys with TTL
        {
            let sm = create_rocksdb_state_machine_with_lease(
                state_machine_path.clone(),
                ttl_config.clone(),
            )
            .await;

            let entry1 = create_insert_entry(1, 1, b"short_ttl_key", b"value1", Some(2));
            let entry2 = create_insert_entry(2, 1, b"long_ttl_key", b"value2", Some(3600));
            let entry3 = create_insert_entry(3, 1, b"no_ttl_key", b"value3", None);

            sm.apply_chunk(vec![entry1, entry2, entry3]).await.unwrap();

            // Verify all keys exist
            assert_eq!(
                sm.get(b"short_ttl_key").unwrap(),
                Some(Bytes::from("value1"))
            );
            assert_eq!(
                sm.get(b"long_ttl_key").unwrap(),
                Some(Bytes::from("value2"))
            );
            assert_eq!(sm.get(b"no_ttl_key").unwrap(), Some(Bytes::from("value3")));

            // Gracefully stop state machine to persist TTL data
            sm.stop().unwrap();
            // State machine drops here
        }

        // Phase 2: Restart - create new state machine from same directory
        {
            let sm =
                create_rocksdb_state_machine_with_lease(state_machine_path.clone(), ttl_config)
                    .await;

            // Verify all keys still exist after restart
            assert_eq!(
                sm.get(b"short_ttl_key").unwrap(),
                Some(Bytes::from("value1"))
            );
            assert_eq!(
                sm.get(b"long_ttl_key").unwrap(),
                Some(Bytes::from("value2"))
            );
            assert_eq!(sm.get(b"no_ttl_key").unwrap(), Some(Bytes::from("value3")));

            // Wait for short TTL to expire
            sleep(Duration::from_secs(3)).await;

            // Trigger expiration check
            let entry4 = create_insert_entry(4, 1, b"trigger", b"trigger", None);
            sm.apply_chunk(vec![entry4]).await.unwrap();

            // short_ttl_key should be expired
            assert_eq!(sm.get(b"short_ttl_key").unwrap(), None);
            // long_ttl_key should still exist
            assert_eq!(
                sm.get(b"long_ttl_key").unwrap(),
                Some(Bytes::from("value2"))
            );
            // no_ttl_key should still exist
            assert_eq!(sm.get(b"no_ttl_key").unwrap(), Some(Bytes::from("value3")));
        }
    }

    #[tokio::test]
    async fn test_rocksdb_reset_clears_ttl_manager() {
        let temp_dir = TempDir::new().unwrap();
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();
        let sm =
            create_rocksdb_state_machine_with_lease(temp_dir.path().join("rocksdb"), ttl_config)
                .await;

        // Insert keys with TTL
        let entry1 = create_insert_entry(1, 1, b"key1", b"value1", Some(3600));
        let entry2 = create_insert_entry(2, 1, b"key2", b"value2", Some(7200));
        sm.apply_chunk(vec![entry1, entry2]).await.unwrap();

        // Reset the state machine
        sm.reset().await.unwrap();

        // All data should be cleared
        assert_eq!(sm.get(b"key1").unwrap(), None);
        assert_eq!(sm.get(b"key2").unwrap(), None);
        assert_eq!(sm.len(), 0);
    }

    #[tokio::test]
    async fn test_rocksdb_passive_deletion_on_get() {
        let temp_dir = TempDir::new().unwrap();
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();
        let sm =
            create_rocksdb_state_machine_with_lease(temp_dir.path().join("rocksdb"), ttl_config)
                .await;

        // Insert key with 1 second TTL
        let entry = create_insert_entry(1, 1, b"passive_key", b"passive_value", Some(1));
        sm.apply_chunk(vec![entry]).await.unwrap();

        // Key should exist immediately
        let value = sm.get(b"passive_key").unwrap();
        assert_eq!(value, Some(Bytes::from("passive_value")));

        // Wait for expiration
        sleep(Duration::from_secs(2)).await;

        // Passive deletion: key should be deleted on get() without apply_chunk
        let value = sm.get(b"passive_key").unwrap();
        assert_eq!(value, None);

        // Verify key is truly deleted (not just hidden)
        let value = sm.get(b"passive_key").unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_rocksdb_piggyback_cleanup_frequency() {
        let temp_dir = TempDir::new().unwrap();
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();
        let sm =
            create_rocksdb_state_machine_with_lease(temp_dir.path().join("rocksdb"), ttl_config)
                .await;

        // Insert 5 keys with 1 second TTL
        for i in 0..5 {
            let key = format!("piggyback_key_{i}");
            let entry = create_insert_entry(i + 1, 1, key.as_bytes(), b"value", Some(1));
            sm.apply_chunk(vec![entry]).await.unwrap();
        }

        // Wait for all keys to expire
        sleep(Duration::from_secs(2)).await;

        // Apply 100 entries to trigger piggyback cleanup (frequency=100)
        for i in 100..200 {
            let entry = create_insert_entry(i, 1, b"dummy", b"dummy", None);
            sm.apply_chunk(vec![entry]).await.unwrap();
        }

        // Expired keys should be cleaned up by piggyback mechanism
        for i in 0..5 {
            let key = format!("piggyback_key_{i}");
            let value = sm.get(key.as_bytes()).unwrap();
            assert_eq!(
                value, None,
                "Key {} should be deleted by piggyback cleanup",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_rocksdb_lazy_activation_no_ttl_overhead() {
        let temp_dir = TempDir::new().unwrap();
        let mut ttl_config = d_engine_core::config::LeaseConfig::default();
        ttl_config.cleanup_strategy = "piggyback".to_string();
        let sm =
            create_rocksdb_state_machine_with_lease(temp_dir.path().join("rocksdb"), ttl_config)
                .await;

        // Insert keys WITHOUT TTL
        for i in 0..10 {
            let key = format!("no_ttl_key_{i}");
            let entry = create_insert_entry(i + 1, 1, key.as_bytes(), b"value", None);
            sm.apply_chunk(vec![entry]).await.unwrap();
        }

        // Apply 150 more entries (should trigger piggyback cleanup check)
        for i in 10..160 {
            let entry = create_insert_entry(i + 1, 1, b"dummy", b"dummy", None);
            sm.apply_chunk(vec![entry]).await.unwrap();
        }

        // All keys should still exist (no TTL, lazy activation should skip cleanup)
        for i in 0..10 {
            let key = format!("no_ttl_key_{i}");
            let value = sm.get(key.as_bytes()).unwrap();
            assert_eq!(value, Some(Bytes::from("value")));
        }
    }
}
