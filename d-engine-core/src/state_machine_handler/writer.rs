use async_trait::async_trait;
use d_engine_proto::common::Entry;
use d_engine_proto::server::storage::SnapshotMetadata;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::trace;

use super::StateMachineWriterOps;
use crate::ApplyResult;
use crate::CapturedLocalSnapshot;
use crate::Result;
use crate::SnapshotApplyResult;
use crate::SnapshotError;
use crate::SnapshotPathManager;
use crate::StateMachine;
use crate::TypeConfig;
use crate::alias::SMOF;
use crate::convert::classify_error;
use crate::decode_entries;
use crate::scoped_timer::ScopedTimer;
use crate::state_machine_handler::applied_state::AppliedState;

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

        metrics::counter!(
            "core.state_machine.apply_chunk.count",
            &[("node_id", self.node_id.to_string())]
        )
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
        metrics::counter!(
            "core.state_machine.apply.busy_nanos_total",
            &[("node_id", self.node_id.to_string())]
        )
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
        metrics::histogram!(
            "core.state_machine.apply_chunk.duration_ms",
            &[("node_id", self.node_id.to_string())]
        )
        .record(duration_ms);
        metrics::histogram!(
            "core.state_machine.apply_chunk.batch_size",
            &[("node_id", self.node_id.to_string())]
        )
        .record(chunk_size as f64);

        match &apply_result {
            Ok(_) => {
                if let Some(idx) = last_index {
                    self.applied.advance(idx);
                    metrics::gauge!(
                        "core.raft.apply_index",
                        &[("node_id", self.node_id.to_string())]
                    )
                    .set(idx as f64);
                }
                metrics::counter!(
                    "core.state_machine.apply_chunk.success",
                    &[("node_id", self.node_id.to_string())]
                )
                .increment(1);
            }
            Err(e) => {
                let error_type = classify_error(e);
                metrics::counter!(
                    "core.state_machine.apply_chunk.error",
                    &[
                        ("node_id", self.node_id.to_string()),
                        ("error_type", error_type)
                    ]
                )
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
        let result = self.state_machine.apply_snapshot_from_file(&metadata, dir).await?;
        if let SnapshotApplyResult::Applied { last_included } = &result {
            self.applied.advance(last_included.index);
        }
        Ok(result)
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

        // generate_snapshot_data commits SnapshotMetadata as the state machine's
        // current snapshot internally (each StateMachine adapter does this as part
        // of the same call) — by the time this returns, the boundary is durable,
        // not just captured in memory.
        let checksum = match self
            .state_machine
            .generate_snapshot_data(temp_path.clone(), last_included)
            .await
        {
            Ok(bytes) => bytes,
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
            temp_dir: temp_path,
        })
    }
}
