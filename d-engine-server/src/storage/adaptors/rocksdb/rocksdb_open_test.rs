//! `open_rocksdb` opens the Raft log and the state machine of one node as two separate RocksDB
//! databases under one directory.
//!
//! The two directories are the layout a node's default start has always used, so a data directory
//! written by an earlier start is found again. They are separate databases so that the write load
//! of the log and of the state machine cannot stall each other.

use bytes::Bytes;
use d_engine_core::ApplyEntry;
use d_engine_core::Command;
use d_engine_core::HardState;
use d_engine_core::StorageEngine;
use d_engine_proto::common::LogId;
use tempfile::TempDir;

use super::open_rocksdb;
use crate::StateMachine;

/// open_rocksdb() on a fresh path succeeds and creates the databases.
#[test]
fn test_open_rocksdb_creates_the_databases_at_a_fresh_path() {
    let dir = TempDir::new().expect("temp dir");

    let result = open_rocksdb(dir.path());

    assert!(
        result.is_ok(),
        "open_rocksdb() must succeed on a fresh path"
    );
}

/// The data directory may not exist yet, nor its parents: `open_rocksdb` creates them, so a caller
/// does not have to prepare the directory first.
#[test]
fn test_open_rocksdb_creates_missing_parent_directories() {
    let dir = TempDir::new().expect("temp dir");
    let data_dir = dir.path().join("not").join("there").join("yet");

    let result = open_rocksdb(&data_dir);

    assert!(
        result.is_ok(),
        "open_rocksdb() must create a data directory that does not exist"
    );
}

/// The log and the state machine are two databases in `storage` and `state_machine`. RocksDB
/// writes a `CURRENT` file into every database directory it creates.
#[test]
fn test_open_rocksdb_keeps_the_log_and_the_state_machine_in_two_databases() {
    let dir = TempDir::new().expect("temp dir");

    let _handles = open_rocksdb(dir.path()).expect("open");

    assert!(
        dir.path().join("storage").join("CURRENT").exists(),
        "the Raft log must be a database of its own in `storage`"
    );
    assert!(
        dir.path().join("state_machine").join("CURRENT").exists(),
        "the state machine must be a database of its own in `state_machine`"
    );
}

/// After all handles are dropped, re-opening the same path succeeds (RocksDB lock released).
#[test]
fn test_open_rocksdb_reopen_after_drop_succeeds() {
    let dir = TempDir::new().expect("temp dir");
    {
        let _handles = open_rocksdb(dir.path()).expect("first open");
    } // both storage and state machine dropped, so both databases are closed

    let result = open_rocksdb(dir.path());

    assert!(
        result.is_ok(),
        "reopen after all handles dropped must succeed"
    );
}

/// Opening the same path twice while the first instance is alive must fail.
#[test]
fn test_open_rocksdb_concurrent_open_of_the_same_path_fails() {
    let dir = TempDir::new().expect("temp dir");
    let _first = open_rocksdb(dir.path()).expect("first open");

    let second = open_rocksdb(dir.path());

    assert!(
        second.is_err(),
        "second open on a live database must fail with a lock error"
    );
}

/// What a node wrote through the two handles is found again when the directory is opened again:
/// the hard state in the log database and the data and position in the state machine database.
#[tokio::test]
async fn test_open_rocksdb_finds_what_the_previous_open_wrote() {
    let dir = TempDir::new().expect("temp dir");
    {
        let (storage, sm) = open_rocksdb(dir.path()).expect("first open");
        storage
            .save_hard_state(&HardState {
                current_term: 5,
                voted_for: None,
            })
            .expect("save hard state");
        storage.flush().expect("flush hard state");
        sm.apply_chunk(&[ApplyEntry {
            index: 1,
            term: 5,
            command: Command::Insert {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
            },
        }])
        .await
        .expect("apply");
        sm.close().expect("close");
    }

    let (storage, sm) = open_rocksdb(dir.path()).expect("second open");

    let hard_state = storage.load_hard_state().expect("load hard state");
    assert_eq!(hard_state.map(|s| s.current_term), Some(5));
    assert_eq!(sm.get(b"k").expect("get"), Some(Bytes::from_static(b"v")));
    assert_eq!(sm.last_applied(), LogId { index: 1, term: 5 });
}
