//! Recovery after a crash that loses what `flush()` had not yet made durable.
//!
//! Dropping the store and reopening it does not model a power loss: the page cache survives. Each
//! test therefore drops the store and then edits the files the way a power loss leaves them (the
//! unsynced tail torn or missing, a never-synced file gone), reopens, and checks that every entry
//! acknowledged by `flush()` is still there, byte for byte, and that the store accepts new writes.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use d_engine::common::{Entry, EntryPayload, LogId, entry_payload};
use d_engine::{LogStore, StorageEngine};
use prost::bytes::Bytes;
use raft_engine_log::{RaftEngineOptions, RaftEngineStorage};
use tempfile::TempDir;

const PAYLOAD_LEN: usize = 1024;
/// Entries per `persist_entries` call. raft-engine starts a new log file only between calls.
const BATCH: u64 = 10;

fn options() -> RaftEngineOptions {
    RaftEngineOptions {
        target_file_size_mib: 1,
        purge_threshold_mib: 2,
        prefill_limit_mib: 1,
        purge_interval_secs: 1,
    }
}

fn entry(index: u64, marker: u8) -> Entry {
    Entry {
        index,
        term: 1,
        payload: Some(EntryPayload {
            payload: Some(entry_payload::Payload::Command(Bytes::from(vec![marker; PAYLOAD_LEN]))),
        }),
    }
}

fn marker_of(entry: &Entry) -> u8 {
    match entry.payload.as_ref().and_then(|p| p.payload.as_ref()) {
        Some(entry_payload::Payload::Command(bytes)) => {
            assert!(bytes.iter().all(|b| *b == bytes[0]), "payload of {} is mixed", entry.index);
            bytes[0]
        }
        other => panic!("entry {} has no command payload: {other:?}", entry.index),
    }
}

/// Appends entries `from..=to` with `marker` in `BATCH`-sized calls and makes each call durable.
async fn write_acked(
    log: &impl LogStore,
    from: u64,
    to: u64,
    marker: u8,
) {
    let mut next = from;
    while next <= to {
        let end = (next + BATCH - 1).min(to);
        log.persist_entries((next..=end).map(|i| entry(i, marker)).collect()).await.unwrap();
        log.flush().unwrap();
        next = end + 1;
    }
}

/// Every index in `from..=to` is present, carries `expected(index)`, and nothing follows `to`.
async fn assert_log(
    log: &impl LogStore,
    from: u64,
    to: u64,
    expected: impl Fn(u64) -> u8,
) {
    assert_eq!(log.last_index(), to, "last_index after recovery");
    for index in from..=to {
        let e = log
            .entry(index)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("acknowledged entry {index} is missing after recovery"));
        assert_eq!(marker_of(&e), expected(index), "entry {index} has the wrong payload");
    }
    assert!(log.entry(to + 1).await.unwrap().is_none(), "entry {} must not exist", to + 1);
}

/// `.raftlog` files, oldest first.
fn log_files(dir: &Path) -> Vec<(u64, PathBuf)> {
    let mut files: Vec<(u64, PathBuf)> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let seq = name.strip_suffix(".raftlog")?.parse().ok()?;
            Some((seq, e.path()))
        })
        .collect();
    files.sort();
    files
}

fn active_file(dir: &Path) -> PathBuf {
    log_files(dir).pop().expect("no log file").1
}

fn open(dir: &Path) -> RaftEngineStorage {
    RaftEngineStorage::open_with(dir, &options()).unwrap()
}

/// Entries 1..=50 are acknowledged, 51..=60 were written but not flushed, and the power loss
/// kept only part of the unflushed bytes: the file ends in the middle of an entry. Recovery must
/// keep exactly the acknowledged entries, and a rewritten tail must survive one more restart.
#[tokio::test]
async fn test_recovery_after_torn_unflushed_tail_keeps_acked_entries() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("log");
    let acked_len;
    {
        let storage = open(&path);
        let log = storage.log_store();
        write_acked(&*log, 1, 50, 1).await;
        acked_len = std::fs::metadata(active_file(&path)).unwrap().len();
        log.persist_entries((51..=60).map(|i| entry(i, 2)).collect()).await.unwrap();
    }
    let file = active_file(&path);
    assert!(std::fs::metadata(&file).unwrap().len() > acked_len + 2_000, "unflushed bytes missing");
    std::fs::OpenOptions::new().write(true).open(&file).unwrap().set_len(acked_len + 1_500).unwrap();

    {
        let storage = open(&path);
        let log = storage.log_store();
        assert_log(&*log, 1, 50, |_| 1).await;
        write_acked(&*log, 51, 55, 3).await;
    }
    let storage = open(&path);
    assert_log(&*storage.log_store(), 1, 55, |i| if i <= 50 { 1 } else { 3 }).await;
}

/// Same unflushed entries, but the disk wrote their later blocks and not the earlier ones: the
/// file keeps its full length and a stretch inside the unflushed region is garbage.
#[tokio::test]
async fn test_recovery_after_garbage_in_unflushed_tail_keeps_acked_entries() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("log");
    let acked_len;
    {
        let storage = open(&path);
        let log = storage.log_store();
        write_acked(&*log, 1, 50, 1).await;
        acked_len = std::fs::metadata(active_file(&path)).unwrap().len() as usize;
        log.persist_entries((51..=60).map(|i| entry(i, 2)).collect()).await.unwrap();
    }
    let file = active_file(&path);
    let mut bytes = std::fs::read(&file).unwrap();
    assert!(bytes.len() > acked_len + 2_000, "unflushed bytes missing");
    bytes[acked_len + 100..acked_len + 600].fill(0xAB);
    std::fs::write(&file, bytes).unwrap();

    {
        let storage = open(&path);
        let log = storage.log_store();
        assert_log(&*log, 1, 50, |_| 1).await;
        write_acked(&*log, 51, 55, 3).await;
    }
    let storage = open(&path);
    assert_log(&*storage.log_store(), 1, 55, |i| if i <= 50 { 1 } else { 3 }).await;
}

/// A log file taken from the reuse pool still holds the records of its previous life after the
/// new header and the first new entries. Recovery must stop at the end of the new entries and not
/// read those old records as part of the log.
#[tokio::test]
async fn test_recovery_in_reused_file_ignores_stale_records_after_the_tail() {
    const STALE: u8 = 0xEE;
    const FRESH: u8 = 0x11;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("log");
    let mut last_acked;
    {
        let storage = open(&path);
        let log = storage.log_store();
        // First life: ~6 MiB, then release the oldest files into the reuse pool.
        write_acked(&*log, 1, 6_000, STALE).await;
        let files_before = log_files(&path).len();
        log.purge(LogId { term: 1, index: 5_900 }).await.unwrap();
        log.flush().unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while log_files(&path).len() >= files_before {
            assert!(Instant::now() < deadline, "purge thread did not release any log file");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Second life: write until a new log file whose bytes after the header are stale.
        last_acked = 6_000;
        let started_at = log_files(&path).last().unwrap().0;
        let mut stale_found = false;
        while last_acked < 6_000 + 12_000 {
            write_acked(&*log, last_acked + 1, last_acked + BATCH, FRESH).await;
            last_acked += BATCH;
            let (seq, file) = log_files(&path).pop().unwrap();
            if seq > started_at {
                stale_found = contains_run(&file, STALE);
                if stale_found {
                    break;
                }
            }
        }
        assert!(stale_found, "precondition not met: no log file with stale records after its tail");
        write_acked(&*log, last_acked + 1, last_acked + 5, FRESH).await;
        last_acked += 5;
    }
    let storage = open(&path);
    let log = storage.log_store();
    assert_log(&*log, 5_901, last_acked, |i| if i <= 6_000 { STALE } else { FRESH }).await;
    write_acked(&*log, last_acked + 1, last_acked + 5, FRESH).await;
}

/// True when the file holds a run of `marker` bytes. The fresh entries carry a different marker,
/// so such a run is a record left over from the file's previous life.
fn contains_run(
    file: &Path,
    marker: u8,
) -> bool {
    std::fs::read(file).unwrap().windows(512).any(|w| w.iter().all(|b| *b == marker))
}

/// How the newest log file looks after a power loss right after it was created.
#[derive(Clone, Copy, Debug)]
enum NewestFile {
    /// The directory entry never reached the disk.
    Missing,
    /// The entry exists but nothing was written into it.
    Empty,
    /// Only the first bytes of the header were written.
    TornHeader,
}

/// Writes acknowledged batches until the log starts a new file, then damages that file as a power
/// loss between creating it and syncing its first entries would. The batch that opened the file was
/// not acknowledged. Recovery must keep every earlier entry and accept new writes.
async fn recover_after_new_file_crash(newest: NewestFile) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("log");
    let mut acked = 0;
    {
        let storage = open(&path);
        let log = storage.log_store();
        let files = log_files(&path).len();
        loop {
            let batch_end = acked + BATCH;
            log.persist_entries((acked + 1..=batch_end).map(|i| entry(i, 1)).collect()).await.unwrap();
            if log_files(&path).len() > files {
                break; // this batch opened the new file and stays unacknowledged
            }
            log.flush().unwrap();
            acked = batch_end;
            assert!(acked < 5_000, "log never started a second file");
        }
    }
    let (_, file) = log_files(&path).pop().unwrap();
    match newest {
        NewestFile::Missing => std::fs::remove_file(&file).unwrap(),
        NewestFile::Empty => {
            std::fs::OpenOptions::new().write(true).open(&file).unwrap().set_len(0).unwrap()
        }
        NewestFile::TornHeader => {
            std::fs::OpenOptions::new().write(true).open(&file).unwrap().set_len(7).unwrap()
        }
    }

    {
        let storage = open(&path);
        let log = storage.log_store();
        assert_log(&*log, 1, acked, |_| 1).await;
        // Enough new data to open further files on top of the recovered state.
        write_acked(&*log, acked + 1, acked + 3_000, 2).await;
    }
    let storage = open(&path);
    assert_log(&*storage.log_store(), 1, acked + 3_000, |i| if i <= acked { 1 } else { 2 }).await;
}

#[tokio::test]
async fn test_recovery_when_newest_log_file_is_missing_keeps_acked_entries() {
    recover_after_new_file_crash(NewestFile::Missing).await;
}

#[tokio::test]
async fn test_recovery_when_newest_log_file_is_empty_keeps_acked_entries() {
    recover_after_new_file_crash(NewestFile::Empty).await;
}

#[tokio::test]
async fn test_recovery_when_newest_log_file_has_torn_header_keeps_acked_entries() {
    recover_after_new_file_crash(NewestFile::TornHeader).await;
}

/// The zero-filled files prepared at startup are never synced, so after a power loss they can be
/// absent or empty. They hold no entries; the log must still recover, and later rotations (which
/// would reuse them) must still work.
async fn recover_after_prefilled_files_lost(delete: bool) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("log");
    {
        let storage = open(&path);
        let log = storage.log_store();
        write_acked(&*log, 1, 50, 1).await;
    }
    let spare: Vec<PathBuf> = std::fs::read_dir(&path)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with(".raftlog.reserved"))
        .collect();
    assert!(!spare.is_empty(), "precondition not met: no prefilled file on disk");
    for file in &spare {
        if delete {
            std::fs::remove_file(file).unwrap();
        } else {
            std::fs::OpenOptions::new().write(true).open(file).unwrap().set_len(0).unwrap();
        }
    }

    {
        let storage = open(&path);
        let log = storage.log_store();
        assert_log(&*log, 1, 50, |_| 1).await;
        // About 4 MiB: several rotations, which take prefilled files first when there are any.
        write_acked(&*log, 51, 4_050, 2).await;
    }
    let storage = open(&path);
    assert_log(&*storage.log_store(), 1, 4_050, |i| if i <= 50 { 1 } else { 2 }).await;
}

#[tokio::test]
async fn test_recovery_when_prefilled_files_are_deleted_keeps_acked_entries() {
    recover_after_prefilled_files_lost(true).await;
}

#[tokio::test]
async fn test_recovery_when_prefilled_files_are_empty_keeps_acked_entries() {
    recover_after_prefilled_files_lost(false).await;
}
