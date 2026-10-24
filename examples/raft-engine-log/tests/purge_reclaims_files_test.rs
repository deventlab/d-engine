//! The background purge thread must actually release log files once their
//! entries have been purged. raft-engine never does this by itself, and the unit
//! tests only prove that the thread stops promptly, not that it does its job.

use std::path::Path;
use std::time::{Duration, Instant};

use d_engine::common::{Entry, EntryPayload, LogId, entry_payload};
use d_engine::{LogStore, StorageEngine};
use prost::bytes::Bytes;
use raft_engine_log::{RaftEngineOptions, RaftEngineStorage};
use tempfile::TempDir;

/// Sequence numbers of the log files on disk (`<seq>.raftlog`), oldest first. A purged file that is
/// kept for reuse keeps its name, so only the number of files says how much space was released.
fn log_file_seqs(dir: &Path) -> Vec<u64> {
    let mut seqs: Vec<u64> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.strip_suffix(".raftlog").and_then(|stem| stem.parse().ok())
        })
        .collect();
    seqs.sort_unstable();
    seqs
}

fn entry(index: u64) -> Entry {
    Entry {
        index,
        term: 1,
        payload: Some(EntryPayload {
            payload: Some(entry_payload::Payload::Command(Bytes::from(vec![7u8; 1024]))),
        }),
    }
}

/// About 6 MiB of entries span several 1 MiB files. After `purge` covers all but
/// the last 100 entries, the oldest files hold nothing live, and the purge thread
/// (1 s interval, retained log above the 2 MiB threshold) must release them: files
/// beyond the recycle capacity are deleted, so fewer files remain on disk.
#[tokio::test]
async fn test_purge_thread_releases_files_after_purge() {
    let dir = TempDir::new().unwrap();
    let log_dir = dir.path().join("log");
    let storage = RaftEngineStorage::open_with(
        &log_dir,
        &RaftEngineOptions {
            target_file_size_mib: 1,
            purge_threshold_mib: 2,
            prefill_limit_mib: 4,
            purge_interval_secs: 1,
        },
    )
    .unwrap();
    let log = storage.log_store();

    for chunk in 0..60u64 {
        let batch: Vec<Entry> = (1..=100).map(|i| entry(chunk * 100 + i)).collect();
        log.persist_entries(batch).await.unwrap();
        log.flush().unwrap();
    }
    let before = log_file_seqs(&log_dir);
    assert!(before.len() >= 4, "expected several log files, got {before:?}");

    log.purge(LogId {
        term: 1,
        index: 5_900,
    })
    .await
    .unwrap();
    log.flush().unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let after = log_file_seqs(&log_dir);
        if after.len() < before.len() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "purge thread did not release any file: before {before:?}, after {after:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
