use std::path::Path;

use tempfile::TempDir;

use crate::options::RaftEngineOptions;
use crate::raft_engine_storage::RaftEngineStorage;

fn write_config(
    dir: &TempDir,
    body: &str,
) -> std::path::PathBuf {
    let path = dir.path().join("node.toml");
    std::fs::write(&path, body).unwrap();
    path
}

fn count_log_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".raftlog"))
        .count()
}

/// A node config with no `[raft_engine]` table is the common case and must keep
/// the defaults, while the d-engine settings around it are not an error.
#[test]
fn test_options_missing_table_uses_defaults() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, "[cluster]\nnode_id = 1\n");

    assert_eq!(
        RaftEngineOptions::from_toml_file(path).unwrap(),
        RaftEngineOptions::default()
    );
}

/// Only the keys present in the file change; the rest stay at their defaults.
#[test]
fn test_options_partial_table_overrides_only_given_keys() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, "[cluster]\nnode_id = 1\n[raft_engine]\ntarget_file_size_mib = 8\n");

    let options = RaftEngineOptions::from_toml_file(path).unwrap();

    assert_eq!(options.target_file_size_mib, 8);
    assert_eq!(
        options.purge_threshold_mib,
        RaftEngineOptions::default().purge_threshold_mib
    );
}

/// A misspelled key must fail loudly; silently ignoring it would run the
/// benchmark with a segment size nobody asked for.
#[test]
fn test_options_unknown_key_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, "[raft_engine]\ntarget_file_size = 8\n");

    assert!(RaftEngineOptions::from_toml_file(path).is_err());
}

/// Values that would stall or break the store are refused at load time.
#[test]
fn test_options_invalid_values_are_rejected() {
    let dir = TempDir::new().unwrap();
    for body in [
        "[raft_engine]\ntarget_file_size_mib = 0\n",
        "[raft_engine]\npurge_interval_secs = 0\n",
        "[raft_engine]\ntarget_file_size_mib = 64\npurge_threshold_mib = 32\n",
    ] {
        let path = write_config(&dir, body);
        assert!(
            RaftEngineOptions::from_toml_file(path).is_err(),
            "accepted: {body}"
        );
    }
}

/// The configured segment size is the one raft-engine rotates at: a 1 MiB
/// segment must be rotated after about 3 MiB of entries, whereas the 64 MiB
/// default keeps them all in one file.
#[tokio::test]
async fn test_options_target_file_size_controls_rotation() {
    use d_engine::common::{Entry, EntryPayload, entry_payload};
    use d_engine::{LogStore, StorageEngine};
    use prost::bytes::Bytes;

    let entries = |range: std::ops::RangeInclusive<u64>| -> Vec<Entry> {
        range
            .map(|i| Entry {
                index: i,
                term: 1,
                payload: Some(EntryPayload {
                    payload: Some(entry_payload::Payload::Command(Bytes::from(vec![7u8; 1024]))),
                }),
            })
            .collect()
    };

    let small_dir = TempDir::new().unwrap();
    let small = RaftEngineStorage::open_with(
        small_dir.path().join("log"),
        &RaftEngineOptions {
            target_file_size_mib: 1,
            purge_threshold_mib: 8,
            prefill_limit_mib: 4,
            ..Default::default()
        },
    )
    .unwrap();
    for chunk in 0..3 {
        small
            .log_store()
            .persist_entries(entries(chunk * 1000 + 1..=chunk * 1000 + 1000))
            .await
            .unwrap();
    }

    let default_dir = TempDir::new().unwrap();
    let default = RaftEngineStorage::open_with(
        default_dir.path().join("log"),
        &RaftEngineOptions {
            prefill_limit_mib: 4,
            ..Default::default()
        },
    )
    .unwrap();
    for chunk in 0..3 {
        default
            .log_store()
            .persist_entries(entries(chunk * 1000 + 1..=chunk * 1000 + 1000))
            .await
            .unwrap();
    }

    assert!(count_log_files(&small_dir.path().join("log")) >= 2);
    assert_eq!(count_log_files(&default_dir.path().join("log")), 1);
}
