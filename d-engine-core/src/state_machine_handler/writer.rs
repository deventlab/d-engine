use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use d_engine_proto::common::Entry;
use d_engine_proto::server::storage::SnapshotMetadata;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::StateMachineWriterOps;
use crate::ApplyResult;
use crate::CapturedLocalSnapshot;
use crate::OwnedSnapshotDir;
use crate::Result;
use crate::SnapshotApplyResult;
use crate::SnapshotError;
use crate::SnapshotPathManager;
use crate::StateMachine;
use crate::TypeConfig;
use crate::alias::SMOF;
use crate::convert::classify_error;
use crate::decode_entries;
use crate::file_io::compute_checksum_from_folder_path;
use crate::scoped_timer::ScopedTimer;
use crate::state_machine_handler::applied_state::AppliedState;
use crate::state_machine_handler::snapshot_catalog::SnapshotCatalog;

/// The only type allowed to run destructive state-machine mutations — held exclusively
/// by `StateMachineWorker`. Shares `state_machine`/`applied` with its paired
/// `DefaultStateMachineHandler` (constructed together, see `builder.rs`), but owns the
/// watch-broadcast fields exclusively: nothing on the reader side touches them.
#[derive(Debug)]
pub struct DefaultStateMachineWriter<T>
where
    T: TypeConfig,
{
    node_id: u32,
    state_machine: Arc<SMOF<T>>,
    applied: Arc<AppliedState>,

    /// Shared with the paired `DefaultStateMachineHandler` — same `Arc`, same naming
    /// convention, so temp/final paths computed by capture (here) and by
    /// build/cleanup (reader side) never drift apart.
    path_mgr: Arc<SnapshotPathManager>,

    /// Shared with the paired handler: records the snapshots this node holds.
    catalog: Arc<SnapshotCatalog>,

    #[cfg(feature = "watch")]
    watch_event_tx: Option<tokio::sync::broadcast::Sender<d_engine_proto::client::WatchResponse>>,
    #[cfg(feature = "watch")]
    prev_kv_watcher_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl<T: TypeConfig> DefaultStateMachineWriter<T> {
    pub(crate) fn new(
        node_id: u32,
        state_machine: Arc<SMOF<T>>,
        applied: Arc<AppliedState>,
        path_mgr: Arc<SnapshotPathManager>,
        catalog: Arc<SnapshotCatalog>,
        #[cfg_attr(not(feature = "watch"), allow(unused_variables))] watch_event_tx: Option<
            tokio::sync::broadcast::Sender<d_engine_proto::client::WatchResponse>,
        >,
        #[cfg_attr(not(feature = "watch"), allow(unused_variables))] prev_kv_watcher_count: Arc<
            std::sync::atomic::AtomicUsize,
        >,
    ) -> Self {
        Self {
            node_id,
            state_machine,
            applied,
            path_mgr,
            catalog,
            #[cfg(feature = "watch")]
            watch_event_tx,
            #[cfg(feature = "watch")]
            prev_kv_watcher_count,
        }
    }
}

#[async_trait]
impl<T: TypeConfig> StateMachineWriterOps<T> for DefaultStateMachineWriter<T> {
    async fn apply_chunk(
        &self,
        chunk: Vec<Entry>,
    ) -> Result<Vec<ApplyResult>> {
        let _timer = ScopedTimer::new("apply_chunk");
        let start = std::time::Instant::now();
        let chunk_size = chunk.len();

        metrics::counter!("core.state_machine.apply_chunk.count", &[(
            "node_id",
            self.node_id.to_string()
        )])
        .increment(1);

        let last_index = chunk.last().map(|entry| entry.index);
        trace!(
            "[node-{}] apply_chunk::entry={:?} last_index: {:?}",
            self.node_id, &chunk, last_index
        );

        let sm = self.state_machine.clone();
        let apply_entries = decode_entries(chunk)?;

        #[cfg(feature = "watch")]
        let prev_values: Option<Vec<Option<bytes::Bytes>>> = if self.watch_event_tx.is_some()
            && self.prev_kv_watcher_count.load(std::sync::atomic::Ordering::Relaxed) > 0
        {
            Some(super::default_state_machine_handler::read_prev_values(
                &*sm,
                &apply_entries,
            ))
        } else {
            None
        };

        let apply_t0 = std::time::Instant::now();
        let apply_result = sm.apply_chunk(&apply_entries).await;
        let apply_elapsed = apply_t0.elapsed();
        metrics::counter!("core.state_machine.apply.busy_nanos_total", &[(
            "node_id",
            self.node_id.to_string()
        )])
        .increment(apply_elapsed.as_nanos() as u64);

        #[cfg(feature = "watch")]
        if let Ok(ref results) = apply_result
            && let Some(ref tx) = self.watch_event_tx
        {
            super::default_state_machine_handler::broadcast_watch_events(
                &apply_entries,
                results,
                tx,
                prev_values.as_deref(),
            );
        }

        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        metrics::histogram!("core.state_machine.apply_chunk.duration_ms", &[(
            "node_id",
            self.node_id.to_string()
        )])
        .record(duration_ms);
        metrics::histogram!("core.state_machine.apply_chunk.batch_size", &[(
            "node_id",
            self.node_id.to_string()
        )])
        .record(chunk_size as f64);

        match &apply_result {
            Ok(_) => {
                if let Some(idx) = last_index {
                    self.applied.advance(idx);
                    metrics::gauge!("core.raft.apply_index", &[(
                        "node_id",
                        self.node_id.to_string()
                    )])
                    .set(idx as f64);
                }
                metrics::counter!("core.state_machine.apply_chunk.success", &[(
                    "node_id",
                    self.node_id.to_string()
                )])
                .increment(1);
            }
            Err(e) => {
                let error_type = classify_error(e);
                metrics::counter!("core.state_machine.apply_chunk.error", &[
                    ("node_id", self.node_id.to_string()),
                    ("error_type", error_type)
                ])
                .increment(1);
            }
        }

        apply_result
    }

    async fn install_prepared_snapshot(
        &self,
        metadata: SnapshotMetadata,
        dir: PathBuf,
    ) -> Result<SnapshotApplyResult> {
        // The engine, not the state machine, decides whether a snapshot is installed. A snapshot
        // without a boundary cannot be classified at all, so it is rejected rather than guessed.
        let incoming = metadata.last_included.ok_or_else(|| {
            SnapshotError::OperationFailed("Missing last_included in snapshot metadata".into())
        })?;
        let current = self.state_machine.last_applied();
        match incoming.index.cmp(&current.index) {
            std::cmp::Ordering::Less => {
                info!(?incoming, ?current, "Ignoring stale snapshot");
                return Ok(SnapshotApplyResult::IgnoredStale { current });
            }
            std::cmp::Ordering::Equal if incoming.term == current.term => {
                info!(?incoming, ?current, "Ignoring duplicate snapshot");
                return Ok(SnapshotApplyResult::IgnoredDuplicate { current });
            }
            // The same log index with a different term means the two histories disagree. That
            // is a real inconsistency: it must be neither ignored nor installed over.
            std::cmp::Ordering::Equal => {
                return Err(SnapshotError::BoundaryConflict {
                    index: current.index,
                    local_term: current.term,
                    incoming_term: incoming.term,
                }
                .into());
            }
            std::cmp::Ordering::Greater => {}
        }

        // Only a snapshot that will be installed is read and hashed. The checksum guarantees a
        // state machine only ever receives a directory that matches what the leader exported.
        let computed = compute_checksum_from_folder_path(&dir).await?;
        if metadata.checksum.as_ref() != computed.as_ref() {
            error!(
                "Snapshot checksum mismatch! Computed: {:?}, Expected: {:?}",
                computed, metadata.checksum
            );
            return Err(SnapshotError::ChecksumMismatch.into());
        }

        self.state_machine.apply_snapshot_from_file(&metadata, dir).await?;

        // The state machine must now be at the snapshot's boundary. If it is not, the engine
        // would believe the node is still behind and replay entries that the installed state
        // already contains, so this is reported instead of carried on from.
        let reached = self.state_machine.last_applied();
        if reached != incoming {
            return Err(SnapshotError::OperationFailed(format!(
                "state machine reports last_applied {reached:?} after installing a snapshot that \
                 ends at {incoming:?}"
            ))
            .into());
        }

        self.applied.advance(incoming.index);

        // The snapshot is installed, so this node now holds it: the archive was kept at its final
        // path when it was received. A failure to record it does not undo the install.
        let archive = self.path_mgr.final_snapshot_path(&incoming);
        if tokio::fs::try_exists(&archive).await.unwrap_or(false) {
            if let Err(e) = self.catalog.publish(&metadata).await {
                error!(?e, "Failed to record the installed snapshot");
            }
        } else {
            warn!(
                ?archive,
                "The installed snapshot's archive is missing; not recorded"
            );
        }

        Ok(SnapshotApplyResult::Applied {
            last_included: incoming,
        })
    }
    async fn capture_local_snapshot(&self) -> Result<CapturedLocalSnapshot> {
        let last_included = self.state_machine.last_applied();
        let temp_path = self.path_mgr.temp_work_path(&last_included);

        // A crashed/killed previous attempt at this exact index can leave temp_path
        // behind; without removing it first, generate_snapshot_data fails with
        // "File exists" forever.
        if let Err(e) = tokio::fs::remove_dir_all(&temp_path).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            return Err(SnapshotError::OperationFailed(format!(
                "Failed to remove stale temp directory: {e}"
            ))
            .into());
        }

        // generate_snapshot_data only exports the state; it does not publish snapshot
        // metadata. The metadata is published later, once the archive is durably built.
        if let Err(e) = self
            .state_machine
            .generate_snapshot_data(temp_path.clone(), last_included)
            .await
        {
            let _ = tokio::fs::remove_dir_all(&temp_path).await;
            return Err(e);
        }

        // The framework, not the state machine, checksums the exported directory.
        let checksum = match compute_checksum_from_folder_path(&temp_path).await {
            Ok(sum) => Bytes::copy_from_slice(&sum),
            Err(e) => {
                let _ = tokio::fs::remove_dir_all(&temp_path).await;
                return Err(e);
            }
        };

        Ok(CapturedLocalSnapshot {
            metadata: SnapshotMetadata {
                last_included: Some(last_included),
                checksum,
            },
            temp_dir: OwnedSnapshotDir::from_existing(temp_path)?,
        })
    }

    fn close(&self) -> Result<()> {
        self.state_machine.close()
    }
}
