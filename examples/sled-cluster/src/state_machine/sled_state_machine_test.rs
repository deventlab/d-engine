//! Validates SledStateMachine against the standard StateMachine test suite.
//!
//! All custom state machines must pass StateMachineTestSuite. This file wires
//! SledStateMachine to that suite and adds sled-specific scenarios not covered
//! by the standard suite.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use d_engine::{ApplyEntry, Command, Error, StateMachine};
use d_engine::common::LogId;
use d_engine::state_machine_test::{StateMachineBuilder, StateMachineTestSuite};
use tempfile::TempDir;

use crate::{SledStateMachine, init_sled_state_machine_db, STATE_SNAPSHOT_METADATA_TREE};

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
        Self { temp_dir: TempDir::new().expect("create TempDir") }
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

    // Snapshot metadata must reflect the requested last_included index.
    let meta = sm.snapshot_metadata().unwrap();
    assert_eq!(meta.last_included.unwrap().index, 5);
}

/// Snapshot DB must contain the snapshot metadata tree with the correct index,
/// so a joining follower can verify the snapshot before applying it.
#[tokio::test]
async fn test_snapshot_db_contains_metadata_tree() {
    let temp = TempDir::new().unwrap();
    let sm = SledStateMachine::new(temp.path(), 1).unwrap();

    let snap_dir = temp.path().join("meta_snapshot");
    sm.generate_snapshot_data(snap_dir.clone(), LogId { index: 42, term: 5 })
        .await
        .unwrap();

    let snap_db = init_sled_state_machine_db(&snap_dir).unwrap();
    let metadata_tree = snap_db.open_tree(STATE_SNAPSHOT_METADATA_TREE).unwrap();

    let stored = SledStateMachine::load_snapshot_metadata(&metadata_tree)
        .unwrap()
        .expect("snapshot metadata must be present in snapshot DB");
    let li = stored.last_included.unwrap();
    assert_eq!(li.index, 42);
    assert_eq!(li.term, 5);
}

/// Snapshot must only include data that was in the SM at generation time.
/// Entries inserted AFTER generate_snapshot_data must NOT appear in the snapshot.
#[tokio::test]
async fn test_snapshot_does_not_include_post_snapshot_entries() {
    let temp = TempDir::new().unwrap();
    let sm = SledStateMachine::new(temp.path(), 1).unwrap();

    // Write two keys before snapshot
    let pre_entries = vec![
        ApplyEntry {
            index: 1,
            term: 1,
            command: Command::Insert {
                key: Bytes::from_static(b"before"),
                value: Bytes::from_static(b"v1"),
                ttl_secs: None,
            },
        },
    ];
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
