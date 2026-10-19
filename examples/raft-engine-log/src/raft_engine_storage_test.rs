use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use d_engine::common::{Entry, EntryPayload, LogId, entry_payload};
use d_engine::{HardState, LogStore, MetaStore, StorageEngine};
use prost::bytes::Bytes;

use crate::options::RaftEngineOptions;
use crate::raft_engine_storage::RaftEngineStorage;

/// Unique scratch directory, removed on drop.
struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "raft-engine-log-{}-{}-{name}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn entry(
    index: u64,
    term: u64,
    cmd: &str,
) -> Entry {
    Entry {
        index,
        term,
        payload: Some(EntryPayload {
            payload: Some(entry_payload::Payload::Command(Bytes::from(cmd.to_owned()))),
        }),
    }
}

fn entries(
    range: std::ops::RangeInclusive<u64>,
    term: u64,
) -> Vec<Entry> {
    range.map(|i| entry(i, term, &format!("cmd-{i}"))).collect()
}

/// Persist entries and make them durable, as the fsync worker does.
async fn append_durable(
    storage: &RaftEngineStorage,
    batch: Vec<Entry>,
) {
    let log = storage.log_store();
    log.persist_entries(batch).await.unwrap();
    log.flush().unwrap();
}

/// Entries that were persisted and flushed must be fully readable after the
/// engine is closed and reopened, including their payload bytes.
#[tokio::test]
async fn test_log_flushed_entries_survive_reopen() {
    let dir = TestDir::new("reopen");
    let written = entries(1..=10, 3);
    {
        let storage = RaftEngineStorage::open(dir.path()).unwrap();
        append_durable(&storage, written.clone()).await;
    }

    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    let log = storage.log_store();
    assert_eq!(log.last_index(), 10);
    assert_eq!(log.get_entries(1..=10).unwrap(), written);
    assert_eq!(log.entry(7).await.unwrap(), Some(written[6].clone()));
}

/// An entry without a payload must round-trip unchanged: the on-disk bytes are
/// the entry's own encoding, so an empty field must not turn into a default.
#[tokio::test]
async fn test_log_entry_without_payload_roundtrips() {
    let dir = TestDir::new("nopayload");
    let bare = Entry {
        index: 1,
        term: 2,
        payload: None,
    };
    {
        let storage = RaftEngineStorage::open(dir.path()).unwrap();
        append_durable(&storage, vec![bare.clone()]).await;
    }

    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    assert_eq!(storage.log_store().entry(1).await.unwrap(), Some(bare));
}

/// A term conflict replaces the tail from `from_index` in one atomic write: the
/// old suffix beyond the new entries must be gone, before and after a restart.
#[tokio::test]
async fn test_log_replace_range_drops_old_tail_after_reopen() {
    let dir = TestDir::new("replace");
    let replacement = vec![entry(5, 2, "new-5"), entry(6, 2, "new-6")];
    {
        let storage = RaftEngineStorage::open(dir.path()).unwrap();
        append_durable(&storage, entries(1..=10, 1)).await;
        let log = storage.log_store();

        let last = log.replace_range(5, replacement.clone()).await.unwrap();
        log.flush().unwrap();

        assert_eq!(last, 6);
        assert_eq!(log.last_index(), 6);
        assert_eq!(log.entry(7).await.unwrap(), None);
    }

    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    let log = storage.log_store();
    assert_eq!(log.last_index(), 6);
    assert_eq!(log.entry(4).await.unwrap(), Some(entry(4, 1, "cmd-4")));
    assert_eq!(log.get_entries(5..=6).unwrap(), replacement);
    assert_eq!(log.entry(7).await.unwrap(), None);
    assert_eq!(log.entry(10).await.unwrap(), None);
}

/// `replace_range` must refuse the cases that would corrupt the index space
/// instead of reaching raft-engine's internal panics.
#[tokio::test]
async fn test_log_replace_range_invalid_input_returns_error() {
    let dir = TestDir::new("replace-invalid");
    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    let log = storage.log_store();
    append_durable(&storage, entries(1..=5, 1)).await;
    log.purge(LogId { term: 1, index: 2 }).await.unwrap();

    // Empty replacement.
    assert!(log.replace_range(3, vec![]).await.is_err());
    // Starts inside the purged prefix.
    assert!(log.replace_range(2, vec![entry(2, 2, "x")]).await.is_err());
    // Leaves a hole after the last index.
    assert!(log.replace_range(8, vec![entry(8, 2, "x")]).await.is_err());
    // The failed calls left the log untouched.
    assert_eq!(log.last_index(), 5);
}

/// The purge boundary is written together with the compaction, so after a
/// restart both the removed prefix and the boundary must be visible.
#[tokio::test]
async fn test_log_purge_boundary_survives_reopen() {
    let dir = TestDir::new("purge");
    let boundary = LogId { term: 4, index: 5 };
    {
        let storage = RaftEngineStorage::open(dir.path()).unwrap();
        append_durable(&storage, entries(1..=10, 4)).await;
        let log = storage.log_store();
        assert_eq!(log.load_purge_boundary().unwrap(), None);

        log.purge(boundary).await.unwrap();
        log.flush().unwrap();
    }

    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    let log = storage.log_store();
    assert_eq!(log.load_purge_boundary().unwrap(), Some(boundary));
    assert_eq!(log.entry(5).await.unwrap(), None);
    assert_eq!(log.entry(6).await.unwrap(), Some(entry(6, 4, "cmd-6")));
    assert_eq!(log.last_index(), 10);
}

/// Reads past either end of the stored range are clamped, never an error.
#[tokio::test]
async fn test_log_get_entries_clamps_to_stored_range() {
    let dir = TestDir::new("clamp");
    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    let log = storage.log_store();
    assert!(log.get_entries(1..=5).unwrap().is_empty());

    append_durable(&storage, entries(3..=6, 1)).await;
    let got = log.get_entries(1..=100).unwrap();
    assert_eq!(got.iter().map(|e| e.index).collect::<Vec<_>>(), vec![3, 4, 5, 6]);
    assert!(log.get_entries(7..=9).unwrap().is_empty());
}

/// `reset` drops every entry and returns `last_index` to 0.
#[tokio::test]
async fn test_log_reset_clears_all_entries() {
    let dir = TestDir::new("reset");
    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    let log = storage.log_store();
    append_durable(&storage, entries(1..=5, 1)).await;

    log.reset().await.unwrap();

    assert_eq!(log.last_index(), 0);
    assert_eq!(log.entry(1).await.unwrap(), None);
}

/// Truncating a suffix must drop it durably: after a restart the log still ends
/// at the entry before `from_index`, and the kept prefix is unchanged.
#[tokio::test]
async fn test_log_truncate_drops_suffix_after_reopen() {
    let dir = TestDir::new("truncate");
    {
        let storage = RaftEngineStorage::open(dir.path()).unwrap();
        append_durable(&storage, entries(1..=10, 1)).await;
        let log = storage.log_store();

        log.truncate(6).await.unwrap();
        log.flush().unwrap();

        assert_eq!(log.last_index(), 5);
    }

    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    let log = storage.log_store();
    assert_eq!(log.last_index(), 5);
    assert_eq!(log.get_entries(1..=10).unwrap(), entries(1..=5, 1));
    assert_eq!(log.entry(6).await.unwrap(), None);
}

/// After a truncate the log accepts new entries from `from_index` again, which
/// is how a follower resumes after a conflict.
#[tokio::test]
async fn test_log_truncate_then_append_continues_from_cut() {
    let dir = TestDir::new("truncate-append");
    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    let log = storage.log_store();
    append_durable(&storage, entries(1..=10, 1)).await;

    log.truncate(4).await.unwrap();
    // Checked before the append: appending index 4 would also drop the old tail
    // by itself, so only this assertion proves `truncate` did the cut.
    assert_eq!(log.last_index(), 3);
    append_durable(&storage, vec![entry(4, 2, "new-4")]).await;

    assert_eq!(log.last_index(), 4);
    assert_eq!(log.entry(4).await.unwrap(), Some(entry(4, 2, "new-4")));
    assert_eq!(log.entry(3).await.unwrap(), Some(entry(3, 1, "cmd-3")));
}

/// Truncating from the first stored index empties the log, including across a
/// restart, and a later append starts the log again.
#[tokio::test]
async fn test_log_truncate_from_first_index_empties_log() {
    let dir = TestDir::new("truncate-all");
    {
        let storage = RaftEngineStorage::open(dir.path()).unwrap();
        append_durable(&storage, entries(1..=5, 1)).await;
        let log = storage.log_store();

        log.truncate(1).await.unwrap();
        log.flush().unwrap();
        assert_eq!(log.last_index(), 0);
    }

    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    let log = storage.log_store();
    assert_eq!(log.last_index(), 0);
    assert_eq!(log.entry(1).await.unwrap(), None);

    append_durable(&storage, entries(1..=2, 3)).await;
    assert_eq!(log.last_index(), 2);
}

/// Truncating at or past the end changes nothing.
#[tokio::test]
async fn test_log_truncate_beyond_last_index_is_noop() {
    let dir = TestDir::new("truncate-noop");
    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    let log = storage.log_store();
    assert!(log.truncate(1).await.is_ok());

    append_durable(&storage, entries(1..=3, 1)).await;
    log.truncate(4).await.unwrap();
    log.truncate(100).await.unwrap();

    assert_eq!(log.get_entries(1..=3).unwrap(), entries(1..=3, 1));
}

/// `reset` clears the log only. The hard state and the purge boundary live in
/// the same engine and must survive it, in memory and after a restart.
#[tokio::test]
async fn test_log_reset_keeps_hard_state_and_purge_boundary() {
    let dir = TestDir::new("reset-meta");
    let boundary = LogId { term: 2, index: 3 };
    {
        let storage = RaftEngineStorage::open(dir.path()).unwrap();
        append_durable(&storage, entries(1..=5, 2)).await;
        let meta = storage.meta_store();
        meta.save_hard_state(&HardState {
            current_term: 9,
            voted_for: None,
        })
        .unwrap();
        let log = storage.log_store();
        log.purge(boundary).await.unwrap();

        log.reset().await.unwrap();
        log.flush().unwrap();

        assert_eq!(log.last_index(), 0);
        assert_eq!(meta.load_hard_state().unwrap().unwrap().current_term, 9);
        assert_eq!(log.load_purge_boundary().unwrap(), Some(boundary));
    }

    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    assert_eq!(storage.log_store().last_index(), 0);
    assert_eq!(storage.meta_store().load_hard_state().unwrap().unwrap().current_term, 9);
    assert_eq!(storage.log_store().load_purge_boundary().unwrap(), Some(boundary));
}

/// `persist_entries` only reaches the OS cache. The store must report that, so
/// the core runs `flush()` before it advances `durable_index`; a wrong `true`
/// would acknowledge writes that a power loss can still discard.
#[tokio::test]
async fn test_log_write_is_not_reported_durable_without_flush() {
    let dir = TestDir::new("durable-flag");
    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    assert!(!storage.log_store().is_write_durable());
}

/// The hard state saved before a restart is the one loaded after it.
#[tokio::test]
async fn test_meta_hard_state_survives_reopen() {
    let dir = TestDir::new("hard-state");
    {
        let storage = RaftEngineStorage::open(dir.path()).unwrap();
        let meta = storage.meta_store();
        assert!(meta.load_hard_state().unwrap().is_none());

        meta.save_hard_state(&HardState {
            current_term: 42,
            voted_for: None,
        })
        .unwrap();
        meta.flush().unwrap();
    }

    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    let loaded = storage.meta_store().load_hard_state().unwrap().unwrap();
    assert_eq!(loaded.current_term, 42);
    assert!(loaded.voted_for.is_none());
}

/// Log entries and hard state share one engine; writing one must not disturb
/// the other across a restart.
#[tokio::test]
async fn test_log_and_hard_state_are_independent_after_reopen() {
    let dir = TestDir::new("shared-engine");
    {
        let storage = RaftEngineStorage::open(dir.path()).unwrap();
        append_durable(&storage, entries(1..=3, 7)).await;
        let meta = storage.meta_store();
        meta.save_hard_state(&HardState {
            current_term: 7,
            voted_for: None,
        })
        .unwrap();
        meta.flush().unwrap();
        storage.log_store().purge(LogId { term: 7, index: 1 }).await.unwrap();
        storage.log_store().flush().unwrap();
    }

    let storage = RaftEngineStorage::open(dir.path()).unwrap();
    assert_eq!(storage.meta_store().load_hard_state().unwrap().unwrap().current_term, 7);
    assert_eq!(storage.log_store().last_index(), 3);
}

/// Dropping the storage must stop the purge thread right away. A worker that
/// sleeps out its interval would make shutdown hang for that whole interval.
#[tokio::test]
async fn test_purge_worker_stops_promptly_on_drop() {
    let dir = TestDir::new("purge-stop");
    let storage = RaftEngineStorage::open_with(
        dir.path(),
        &RaftEngineOptions {
            purge_interval_secs: 3600,
            ..Default::default()
        },
    )
    .unwrap();

    let started = std::time::Instant::now();
    drop(storage);

    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "drop waited for the purge interval: {:?}",
        started.elapsed()
    );
}

/// A nested data directory that does not exist yet must be created, since a
/// fresh node is started with a path like `db/1/log` and no setup step.
#[tokio::test]
async fn test_open_creates_missing_nested_directory() {
    let dir = TestDir::new("nested");
    let nested = dir.path().join("a").join("b").join("log");

    let storage = RaftEngineStorage::open(&nested).unwrap();

    assert!(nested.is_dir());
    drop(storage);
}
