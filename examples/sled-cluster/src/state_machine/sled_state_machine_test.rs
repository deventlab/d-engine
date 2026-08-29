//! Validates SledStateMachine against the standard StateMachine test suite.
//!
//! All custom state machines must pass StateMachineTestSuite. This file wires
//! SledStateMachine to that suite and adds sled-specific scenarios not covered
//! by the standard suite.

use async_trait::async_trait;
use bytes::Bytes;
use d_engine::common::LogId;
use d_engine::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};
use d_engine::{ApplyEntry, Command, Error, StateMachine};
use std::sync::Arc;
use tempfile::TempDir;

use crate::SledStateMachine;

// ── Builder ───────────────────────────────────────────────────────────────────

/// Builder that creates SledStateMachine instances sharing the same path.
///
/// Persistence tests (test_drop_flushes_data, test_data_survives_reopen, etc.)
/// call build() multiple times and rely on all instances sharing the same path
/// so that data written by the first instance is readable by the second.
struct SledBuilder {
    temp_dir: TempDir,
}

impl SledBuilder {
    fn new() -> Self {
        Self {
            temp_dir: TempDir::new().expect("create TempDir"),
        }
    }
}

#[async_trait]
impl StateMachineBuilder for SledBuilder {
    async fn build(&self) -> Result<Arc<dyn StateMachine>, Error> {
        let sm = SledStateMachine::new(self.temp_dir.path(), 1)?;
        Ok(Arc::new(sm))
    }

    async fn cleanup(&self) -> Result<(), Error> {
        Ok(())
    }
}

// ── Standard suite ────────────────────────────────────────────────────────────

/// Full coverage via the shared StateMachineTestSuite.
///
/// Covers: start/stop, KV ops, CAS, apply_chunk, last_applied tracking,
/// snapshot generate+apply, persistence, drop flush, crash recovery, reset.
#[tokio::test]
async fn test_standard_suite() {
    StateMachineTestSuite::run_all_tests(SledBuilder::new()).await.unwrap();
}

/// Optional performance regression check (excluded from normal CI).
#[tokio::test]
#[ignore]
async fn test_performance_suite() {
    StateMachineTestSuite::run_performance_tests(SledBuilder::new()).await.unwrap();
}

// ── Sled-specific: snapshot content correctness ───────────────────────────────

/// Snapshot must capture all current SM data as arbitrary byte keys.
///
/// Verifies that generate_snapshot_data correctly copies all key-value pairs
/// regardless of key encoding — sled keys are not log indices.
#[tokio::test]
async fn test_snapshot_captures_all_current_data() {
    let temp = TempDir::new().unwrap();
    let sm = SledStateMachine::new(temp.path(), 1).unwrap();

    let entries: Vec<_> = (1u64..=5)
        .map(|i| ApplyEntry {
            index: i,
            term: 1,
            command: Command::Insert {
                key: Bytes::from(format!("snap_key_{i}")),
                value: Bytes::from(format!("snap_val_{i}")),
                ttl_secs: None,
            },
        })
        .collect();
    sm.apply_chunk(&entries).await.unwrap();

    let snap_dir = temp.path().join("snapshot");
    sm.generate_snapshot_data(snap_dir.clone(), LogId { index: 5, term: 1 })
        .await
        .unwrap();

    // Open the snapshot as a fresh SM and verify all 5 keys were captured.
    let snap_sm = SledStateMachine::new(&snap_dir, 1).unwrap();
    for i in 1u64..=5 {
        let key = format!("snap_key_{i}");
        let val = format!("snap_val_{i}");
        assert_eq!(
            snap_sm.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(val)),
            "key {key} missing from snapshot"
        );
    }
}

/// Snapshot must only include data that was in the SM at generation time.
/// Entries inserted AFTER generate_snapshot_data must NOT appear in the snapshot.
#[tokio::test]
async fn test_snapshot_does_not_include_post_snapshot_entries() {
    let temp = TempDir::new().unwrap();
    let sm = SledStateMachine::new(temp.path(), 1).unwrap();

    // Write two keys before snapshot
    let pre_entries = vec![ApplyEntry {
        index: 1,
        term: 1,
        command: Command::Insert {
            key: Bytes::from_static(b"before"),
            value: Bytes::from_static(b"v1"),
            ttl_secs: None,
        },
    }];
    sm.apply_chunk(&pre_entries).await.unwrap();

    let snap_dir = temp.path().join("partial_snapshot");
    sm.generate_snapshot_data(snap_dir.clone(), LogId { index: 1, term: 1 })
        .await
        .unwrap();

    // Write another key AFTER snapshot
    let post_entries = vec![ApplyEntry {
        index: 2,
        term: 1,
        command: Command::Insert {
            key: Bytes::from_static(b"after"),
            value: Bytes::from_static(b"v2"),
            ttl_secs: None,
        },
    }];
    sm.apply_chunk(&post_entries).await.unwrap();

    // Snapshot SM must have "before" but not "after"
    let snap_sm = SledStateMachine::new(&snap_dir, 1).unwrap();
    assert_eq!(
        snap_sm.get(b"before").unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
    assert!(snap_sm.get(b"after").unwrap().is_none());
}

// ── Sled-specific: snapshot install survives a restart ────────────────────────

/// Install must relocate content into this node's own data directory, not just swap
/// the live `db` pointer at the caller's temp decompression dir — otherwise it looks
/// correct until the next restart, when `SledStateMachine::new` reopens its own path
/// and finds none of it. Node B installs a snapshot, gets dropped (simulating a
/// restart), then reopens from the same path with no reference to the temp dir.
#[tokio::test]
async fn test_snapshot_install_content_survives_restart() {
    let source_dir = TempDir::new().unwrap();
    let source_sm = SledStateMachine::new(source_dir.path(), 1).unwrap();

    let entries: Vec<_> = (1u64..=3)
        .map(|i| ApplyEntry {
            index: i,
            term: 1,
            command: Command::Insert {
                key: Bytes::from(format!("install_key_{i}")),
                value: Bytes::from(format!("install_val_{i}")),
                ttl_secs: None,
            },
        })
        .collect();
    source_sm.apply_chunk(&entries).await.unwrap();

    let last_included = LogId { index: 3, term: 1 };
    let export_dir = TempDir::new().unwrap();
    let checksum = source_sm
        .generate_snapshot_data(export_dir.path().to_path_buf(), last_included)
        .await
        .unwrap();

    let metadata = d_engine::server_storage::SnapshotMetadata {
        last_included: Some(last_included),
        checksum: Bytes::from(checksum.to_vec()),
    };

    // Node B: fresh, empty, behind the purge boundary — installs the snapshot.
    let node_b_dir = TempDir::new().unwrap();
    let node_b_path = node_b_dir.path().to_path_buf();
    {
        let node_b = SledStateMachine::new(&node_b_path, 2).unwrap();
        let result = node_b
            .apply_snapshot_from_file(&metadata, export_dir.path().to_path_buf())
            .await
            .unwrap();
        assert!(
            matches!(result, d_engine::SnapshotApplyResult::Applied { .. }),
            "expected Applied, got: {result:?}"
        );
        assert_eq!(
            node_b.get(b"install_key_1").unwrap(),
            Some(Bytes::from_static(b"install_val_1")),
            "content must be readable immediately after install"
        );
    } // node_b dropped — simulates process exit; nothing keeps `export_dir` in scope

    // export_dir (the temp decompression directory) is still on disk here, but a real
    // restart would not carry it forward at all — reopening below only ever knows
    // about `node_b_path`, exactly like `SledStateMachine::new` on real process start.
    let node_b_reopened = SledStateMachine::new(&node_b_path, 2).unwrap();

    assert_eq!(
        node_b_reopened.last_applied(),
        last_included,
        "BUG: last_applied reverted after restart — the snapshot install must have \
         swapped the live db to point at the temp decompression directory instead of \
         relocating its content into node_b_path"
    );
    assert_eq!(
        node_b_reopened.get(b"install_key_1").unwrap(),
        Some(Bytes::from_static(b"install_val_1")),
        "BUG: installed data lost after restart"
    );
    assert_eq!(
        node_b_reopened.get(b"install_key_3").unwrap(),
        Some(Bytes::from_static(b"install_val_3")),
        "BUG: installed data lost after restart"
    );
}

/// A crash between the two renames `apply_snapshot_from_file` uses to move a snapshot
/// into place must recover on next startup, not silently open an empty database.
/// Reproduces the on-disk state directly rather than running a real install.
#[tokio::test]
async fn test_new_recovers_backup_after_interrupted_install() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let sm = SledStateMachine::new(&path, 1).unwrap();
        sm.apply_chunk(&[ApplyEntry {
            index: 1,
            term: 1,
            command: Command::Insert {
                key: Bytes::from_static(b"pre_crash_key"),
                value: Bytes::from_static(b"pre_crash_val"),
                ttl_secs: None,
            },
        }])
        .await
        .unwrap();
    } // dropped — releases sled's file lock so the rename below can succeed

    // Reproduce exactly the on-disk state a crash between the two renames in
    // `apply_snapshot_from_file` would leave: `state_machine` renamed aside, nothing
    // renamed into its place yet.
    let db_dir = path.join("state_machine");
    let backup_dir = path.join("state_machine.pre-install-backup");
    std::fs::rename(&db_dir, &backup_dir).unwrap();
    assert!(!db_dir.exists());
    assert!(backup_dir.exists());

    let recovered = SledStateMachine::new(&path, 1).unwrap();

    assert!(
        !backup_dir.exists(),
        "backup must be restored (renamed back), not left alongside a freshly \
         initialized empty database"
    );
    assert_eq!(
        recovered.get(b"pre_crash_key").unwrap(),
        Some(Bytes::from_static(b"pre_crash_val")),
        "BUG: SledStateMachine::new did not recover the pre-install backup — data \
         from before the interrupted install is gone"
    );
}
