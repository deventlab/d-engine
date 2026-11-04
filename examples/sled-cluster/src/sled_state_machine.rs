//! It works as KV storage for client business CRUDs.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use bytes::Bytes;
use d_engine::ApplyEntry;
use d_engine::ApplyResult;
use d_engine::BatchOp;
use d_engine::Command;
use d_engine::Result;
use d_engine::ScanResult;
use d_engine::SnapshotError;
use d_engine::StateMachine;
use d_engine::StateMachineError;
use d_engine::StorageError;
use d_engine::common::LogId;
use d_engine::server_storage::SnapshotMetadata;
use sled::Transactional;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use crate::safe_kv;
use crate::safe_vk_ivec;

/// Sled database tree namespaces
pub(crate) const STATE_MACHINE_TREE: &str = "_state_machine_tree";
pub(crate) const STATE_MACHINE_META_NAMESPACE: &str = "_state_machine_metadata";

/// Sled entry key namespaces
pub(crate) const STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX: &str = "_raft_last_applied_index";
pub(crate) const STATE_MACHINE_META_KEY_LAST_APPLIED_TERM: &str = "_raft_last_applied_term";

pub struct SledStateMachine {
    node_id: u32,

    /// This state machine's own configured data directory — the sled db actually lives
    /// at `path.join("state_machine")` (see `init_sled_state_machine_db`). Needed by
    /// `apply_snapshot_from_file` so an installed snapshot's content ends up living
    /// *here*, not in the caller's temporary decompression directory (#442 review).
    path: PathBuf,

    db: ArcSwapOption<sled::Db>,

    is_serving: Arc<AtomicBool>,

    /// Volatile state on all servers:
    /// index of highest log entry applied to state machine (initialized to 0,
    /// increases monotonically)
    /// The last submitted log index and term (atomic operation ensures lock-free)
    last_applied_index: AtomicU64,
    last_applied_term: AtomicU64,
}

impl std::fmt::Debug for SledStateMachine {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("SledStateMachine")
            .field("tree_len", &self.current_tree().map(|t| t.len()).ok())
            .finish()
    }
}

impl Drop for SledStateMachine {
    fn drop(&mut self) {
        let timer = Instant::now();

        // Save state into local database including flush operation
        match StateMachine::close(self) {
            Ok(()) => debug!("StateMachine closed in {:?}", timer.elapsed()),
            Err(e) => error!("Failed to close StateMachine: {}", e),
        }
    }
}

#[async_trait]
impl StateMachine for SledStateMachine {
    fn close(&self) -> std::result::Result<(), StateMachineError> {
        if self.db.load().is_none() {
            return Ok(());
        }
        self.is_serving.store(false, Ordering::Release);
        // Persist, then release the db handle so sled's file lock is freed even if persisting
        // failed. The lock is released once no other handle to the db is alive.
        let saved = self.save_hard_state();
        self.db.store(None);
        saved.map_err(StateMachineError::new)
    }

    fn last_applied(&self) -> LogId {
        LogId {
            index: self.last_applied_index.load(Ordering::SeqCst),
            term: self.last_applied_term.load(Ordering::SeqCst),
        }
    }

    fn get(
        &self,
        key_buffer: &[u8],
    ) -> std::result::Result<Option<Bytes>, StateMachineError> {
        match self.current_tree().map_err(StateMachineError::new)?.get(key_buffer) {
            Ok(Some(v)) => Ok(Some(Bytes::copy_from_slice(&v))),
            Ok(None) => Ok(None),
            Err(e) => {
                error!("state_machine get error: {}", e);
                Err(StateMachineError::new(e.to_string()))
            }
        }
    }

    async fn apply_chunk(
        &self,
        chunk: &[ApplyEntry],
    ) -> std::result::Result<Vec<ApplyResult>, StateMachineError> {
        trace!("Applying chunk: {} entries", chunk.len());
        let db = self.db().map_err(StateMachineError::new)?;

        let mut highest_log_id: Option<LogId> = None;
        for entry in chunk {
            if let Some(prev) = &highest_log_id {
                assert!(
                    entry.index > prev.index,
                    "apply_chunk: received unordered entry at index {} (prev={})",
                    entry.index,
                    prev.index
                );
            }
            highest_log_id = Some(LogId {
                index: entry.index,
                term: entry.term,
            });
        }
        let Some(position) = highest_log_id else {
            return Ok(Vec::new());
        };

        // The whole chunk and the new position are one transaction over the data tree and the
        // metadata tree: after a crash the position is exactly the last entry whose effects are in
        // the data. A compare-and-swap reads and writes inside the same transaction, so it sees
        // the entries applied before it in this chunk. sled may run the closure more than once on
        // a conflict, so it builds its results from scratch every time.
        let data_tree = self.current_tree().map_err(StateMachineError::new)?;
        let meta_tree = db
            .open_tree(STATE_MACHINE_META_NAMESPACE)
            .map_err(|e| StateMachineError::new(e.to_string()))?;

        let results = (&data_tree, &meta_tree)
            .transaction(|(data, meta)| {
                let mut results = Vec::with_capacity(chunk.len());
                for entry in chunk {
                    match &entry.command {
                        Command::Noop => {
                            debug!("NOOP at index {}", entry.index);
                            results.push(ApplyResult::success(entry.index));
                        }
                        Command::Insert { key, value, .. } => {
                            data.insert(key.as_ref(), value.as_ref())?;
                            results.push(ApplyResult::success(entry.index));
                        }
                        Command::Delete { key } => {
                            data.remove(key.as_ref())?;
                            results.push(ApplyResult::success(entry.index));
                        }
                        Command::CompareAndSwap {
                            key,
                            expected,
                            value: new_value,
                        } => {
                            let current = data.get(key.as_ref())?;
                            let cas_success = match (&current, expected) {
                                (None, None) => true,
                                (Some(current), Some(expected)) => {
                                    current.as_ref() == expected.as_ref()
                                }
                                _ => false,
                            };
                            if cas_success {
                                data.insert(key.as_ref(), new_value.as_ref())?;
                            }
                            debug!(
                                "CAS at index {}: key={:?}, success={}",
                                entry.index,
                                String::from_utf8_lossy(key),
                                cas_success
                            );
                            results.push(if cas_success {
                                ApplyResult::success(entry.index)
                            } else {
                                ApplyResult::failure(entry.index)
                            });
                        }
                        Command::Batch { ops } => {
                            for op in ops {
                                match op {
                                    BatchOp::Insert { key, value } => {
                                        data.insert(key.as_ref(), value.as_ref())?;
                                    }
                                    BatchOp::Delete { key } => {
                                        data.remove(key.as_ref())?;
                                    }
                                }
                            }
                            results.push(ApplyResult::success(entry.index));
                        }
                    }
                }
                meta.insert(
                    STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX.as_bytes(),
                    safe_kv(position.index),
                )?;
                meta.insert(
                    STATE_MACHINE_META_KEY_LAST_APPLIED_TERM.as_bytes(),
                    safe_kv(position.term),
                )?;
                Ok(results)
            })
            .map_err(|e: sled::transaction::TransactionError| {
                error!("state_machine apply_chunk failed: {}", e);
                StateMachineError::new(e.to_string())
            })?;

        self.update_last_applied(position);

        Ok(results)
    }

    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: PathBuf,
    ) -> std::result::Result<LogId, StateMachineError> {
        // Known limits of this example: the tree is iterated while `apply_chunk` may run, so the
        // copy is not a point-in-time view, and the position below comes from memory, not from the
        // exported data. Reading it before the copy keeps it at or behind the data. A state machine
        // that must export a consistent view has to exclude `apply_chunk` while it copies and read
        // the position under the same lock (see `StateMachine::generate_snapshot_data`).
        let position = self.last_applied();

        // 2. Create a new state machine database instance
        let new_db = init_sled_state_machine_db(&new_snapshot_dir)
            .map_err(|e| StateMachineError::new(e.to_string()))?;

        let exist_db_tree = self.current_tree().map_err(StateMachineError::new)?;
        let new_state_machine_tree = new_tree(&new_db, STATE_MACHINE_TREE).map_err(StateMachineError::new)?;

        let mut batch = sled::Batch::default();
        let mut counter = 0;

        // Copy all current data. Keys are arbitrary bytes — we cannot interpret them
        // as log indices. The SM already reflects state at last_included.index, so
        // all current entries are valid snapshot content.
        for item in exist_db_tree.iter() {
            let (k, v) = item.map_err(|e| StateMachineError::new(e.to_string()))?;

            batch.insert(k, v);
            counter += 1;

            if counter % 100 == 0 {
                new_state_machine_tree
                    .apply_batch(batch)
                    .map_err(|e| StateMachineError::new(e.to_string()))?;
                batch = sled::Batch::default();
            }
        }

        new_state_machine_tree
            .apply_batch(batch)
            .map_err(|e| StateMachineError::new(e.to_string()))?;

        // Make sure flush into disk
        new_db.flush().map_err(|e| StateMachineError::new(e.to_string()))?;

        Ok(position)
    }

    async fn apply_snapshot_from_file(
        &self,
        metadata: &SnapshotMetadata,
        decompressed_snapshot_path: PathBuf,
    ) -> std::result::Result<(), StateMachineError> {
        // `StateMachineWorker` never runs this concurrently with `apply_chunk`, a snapshot
        // export or another install (#436).

        // The engine only calls this for a snapshot newer than `last_applied()`.
        let new_last_included = metadata
            .last_included
            .ok_or_else(|| StateMachineError::new("Missing last_included in snapshot metadata"))?;

        // `decompressed_snapshot_path` is a temporary directory owned by the caller
        // (see `PreparedSnapshot`), not this state machine's own `self.path`. Opening a
        // sled::Db directly there and swapping `self.db` to point at it — the previous
        // approach — left the live database permanently rooted in that ephemeral
        // storage: correct until the next restart, then silently gone, because
        // `SledStateMachine::new` only ever reopens `self.path`, which never received
        // the new content (#442 review). Move the content home to `self.path` first.
        debug!(
            ?decompressed_snapshot_path,
            "Moving decompressed snapshot into this node's own data directory"
        );
        let db_dir = self.path.join("state_machine");
        let backup_dir = self.path.join("state_machine.pre-install-backup");
        let incoming_db_dir = decompressed_snapshot_path.join("state_machine");

        // Release the old db's file lock before touching its directory — sled refuses
        // to open two instances at the same path concurrently.
        drop(self.db.load_full());

        tokio::task::spawn_blocking(move || -> std::result::Result<(), std::io::Error> {
            if backup_dir.exists() {
                std::fs::remove_dir_all(&backup_dir)?;
            }
            // Not fully crash-atomic: a crash between these two renames leaves
            // `db_dir` absent and the new content sitting at `backup_dir` — recovered
            // by the startup check in `SledStateMachine::new`, not rolled back here.
            // Sled has no cross-directory atomic-replace primitive; this two-step
            // rename is the simplest thing that survives the *normal* restart case,
            // which is the bug this fixes. This is an example adaptor, not the
            // production RocksDB/File ones.
            std::fs::rename(&db_dir, &backup_dir)?;
            if let Err(e) = std::fs::rename(&incoming_db_dir, &db_dir) {
                // Temp storage and the data directory can be on different
                // filesystems (e.g. `/tmp` vs a data disk) — rename can't hop
                // devices. Fall back to a recursive copy in that case only.
                if e.kind() == std::io::ErrorKind::CrossesDevices {
                    copy_dir_recursive(&incoming_db_dir, &db_dir)?;
                } else {
                    return Err(e);
                }
            }
            Ok(())
        })
        .await
        .map_err(|e| StateMachineError::new(format!("snapshot install task panicked: {e}")))?
        .map_err(|e| StateMachineError::new(format!("failed to move snapshot into place: {e}")))?;

        debug!("Reopening state machine database at its permanent path");
        let db = init_sled_state_machine_db(&self.path)
            .map_err(|e| StateMachineError::new(e.to_string()))?;
        self.db.store(Some(Arc::new(db)));

        // Best-effort: a leftover `*.pre-install-backup` is harmless (just disk space)
        // if this fails or the process dies here — it's never read again once
        // `db_dir` exists, only by the startup check when `db_dir` is absent.
        let _ = tokio::fs::remove_dir_all(self.path.join("state_machine.pre-install-backup")).await;

        debug!(
            ?new_last_included,
            ?metadata,
            "Updating Raft metadata and indexes"
        );
        self.update_last_applied(new_last_included);

        self.save_hard_state().map_err(StateMachineError::new)?;

        Ok(())
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> std::result::Result<ScanResult, StateMachineError> {
        let tree = self.current_tree().map_err(StateMachineError::new)?;
        let mut entries = Vec::new();
        for result in tree.scan_prefix(prefix) {
            let (k, v) = result.map_err(|e| StateMachineError::new(e.to_string()))?;
            entries.push((Bytes::copy_from_slice(&k), Bytes::copy_from_slice(&v)));
        }
        let revision = self.last_applied_index.load(Ordering::SeqCst);
        Ok(ScanResult { entries, revision })
    }
}

impl SledStateMachine {
    pub fn new<P: AsRef<Path> + std::fmt::Debug>(
        path: P,
        node_id: u32,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Recovery from a crash mid-install (see `apply_snapshot_from_file`): if the
        // live `state_machine` directory is missing but a pre-install backup is still
        // there, the previous process died between renaming the old content aside and
        // renaming the new content into place. Restore the backup instead of silently
        // opening (and then re-initializing) an empty database.
        let db_dir = path.join("state_machine");
        let backup_dir = path.join("state_machine.pre-install-backup");
        if !db_dir.exists() && backup_dir.exists() {
            warn!(
                ?backup_dir,
                "Recovering from an interrupted snapshot install: restoring pre-install backup"
            );
            std::fs::rename(&backup_dir, &db_dir)
                .map_err(|e| StorageError::DbError(e.to_string()))?;
        }

        let db = Arc::new(
            init_sled_state_machine_db(&path).map_err(|e| StorageError::DbError(e.to_string()))?,
        );

        let state_machine_meta_tree = db
            .open_tree(STATE_MACHINE_META_NAMESPACE)
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        let (last_applied_index, last_applied_term) =
            Self::load_state_machine_metadata(&state_machine_meta_tree)?;

        let sm = SledStateMachine {
            db: ArcSwapOption::from(Some(db)),
            path,
            is_serving: Arc::new(AtomicBool::new(true)),
            node_id,

            last_applied_index: AtomicU64::new(0),
            last_applied_term: AtomicU64::new(0),
        };

        // Important to sync the last applied index into memory
        sm.update_last_applied(LogId {
            index: last_applied_index,
            term: last_applied_term,
        });

        Ok(sm)
    }

    fn load_state_machine_metadata(tree: &sled::Tree) -> Result<(u64, u64)> {
        let index = tree
            .get(STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX)
            .map_err(|e| StorageError::DbError(e.to_string()))?
            .map(safe_vk_ivec)
            .unwrap_or(Ok(0))?;

        let term = tree
            .get(STATE_MACHINE_META_KEY_LAST_APPLIED_TERM)
            .map_err(|e| StorageError::DbError(e.to_string()))?
            .map(safe_vk_ivec)
            .unwrap_or(Ok(0))?;

        Ok((index, term))
    }

    // Dynamically obtain the current Tree (lock-free cache optimization)
    #[inline]
    fn current_tree(&self) -> Result<sled::Tree> {
        self.db()?
            .open_tree(STATE_MACHINE_TREE)
            .map_err(|e| StorageError::DbError(e.to_string()).into())
    }

    /// The live db, or `NotServing` once `close()` has released it.
    fn db(&self) -> Result<Arc<sled::Db>> {
        self.db
            .load_full()
            .ok_or_else(|| StorageError::NotServing("state machine stopped".into()).into())
    }

    pub(crate) fn update_last_applied(
        &self,
        last_applied: LogId,
    ) {
        debug!(%self.node_id, ?last_applied, "update_last_applied");
        self.last_applied_index.store(last_applied.index, Ordering::SeqCst);
        self.last_applied_term.store(last_applied.term, Ordering::SeqCst);
    }

    pub(crate) fn persist_last_applied(
        &self,
        last_applied: LogId,
    ) -> Result<()> {
        debug!(%self.node_id, ?last_applied, "persist_last_applied");
        let db = self.db()?;
        let tree = db
            .open_tree(STATE_MACHINE_META_NAMESPACE)
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        tree.insert(
            STATE_MACHINE_META_KEY_LAST_APPLIED_INDEX,
            safe_kv(last_applied.index),
        )
        .map_err(|e| StorageError::DbError(e.to_string()))?;
        tree.insert(
            STATE_MACHINE_META_KEY_LAST_APPLIED_TERM,
            safe_kv(last_applied.term),
        )
        .map_err(|e| StorageError::DbError(e.to_string()))?;

        tree.flush().map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }

    pub(crate) fn save_hard_state(&self) -> Result<()> {
        let last_applied = self.last_applied();
        self.persist_last_applied(last_applied)?;

        self.flush()?;
        Ok(())
    }

    pub(crate) fn flush(&self) -> Result<()> {
        let db = self.db()?;
        let bytes = db.flush().map_err(|e| StorageError::DbError(e.to_string()))?;
        info!(
            "Successfully flushed State Machine, bytes flushed: {}",
            bytes
        );
        Ok(())
    }
}

/// Recursively copies `src` into `dst`, creating `dst` if needed. Fallback for
/// `apply_snapshot_from_file` when the decompressed snapshot's temp directory and this
/// node's data directory are on different filesystems, so `std::fs::rename` can't just
/// hop them across devices.
fn copy_dir_recursive(
    src: &std::path::Path,
    dst: &std::path::Path,
) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let dst_path = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &dst_path)?;
        } else {
            std::fs::copy(entry.path(), &dst_path)?;
        }
    }
    Ok(())
}

/// TODO: how to refactor with `current_tree`
fn new_tree(
    db: &sled::Db,
    key: &str,
) -> Result<sled::Tree> {
    db.open_tree(key)
        .map_err(|e| SnapshotError::OperationFailed(format!("{e:?}")).into())
}

pub fn init_sled_state_machine_db(
    sled_db_root_path: impl AsRef<std::path::Path> + std::fmt::Debug
) -> std::result::Result<sled::Db, sled::Error> {
    tracing::debug!(
        "init_sled_state_machine_db from path: {:?}",
        sled_db_root_path
    );

    let path = sled_db_root_path.as_ref();
    let state_machine_db_path = path.join("state_machine");

    sled::Config::default()
        .path(&state_machine_db_path)
        .cache_capacity(10 * 1024 * 1024) //10MB
        .flush_every_ms(Some(3))
        .use_compression(true)
        .compression_factor(1)
        // .segment_size(256)
        // .print_profile_on_drop(true)
        .open()
}
