use std::ops::RangeInclusive;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use autometrics::autometrics;
use tokio::fs;
use tokio::fs::remove_dir_all;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tonic::async_trait;
use tonic::IntoRequest;
use tonic::Status;
use tonic::Streaming;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::SnapshotAssembler;
use super::SnapshotContext;
use super::SnapshotPolicy;
use super::StateMachineHandler;
use crate::alias::ROF;
use crate::alias::SMOF;
use crate::alias::SNP;
use crate::constants::SNAPSHOT_DIR_PREFIX;
use crate::file_io::move_directory;
use crate::file_io::validate_checksum;
use crate::proto::client_command::Command;
use crate::proto::ClientCommand;
use crate::proto::ClientResult;
use crate::proto::LogId;
use crate::proto::PurgeLogRequest;
use crate::proto::PurgeLogResponse;
use crate::proto::SnapshotChunk;
use crate::proto::SnapshotMetadata;
use crate::proto::SnapshotResponse;
use crate::utils::cluster::error;
use crate::MaybeCloneOneshotSender;
use crate::NewCommitData;
use crate::RaftLog;
use crate::Result;
use crate::SnapshotConfig;
use crate::SnapshotGuard;
use crate::StateMachine;
use crate::StorageError;
use crate::TypeConfig;
use crate::API_SLO;

#[derive(Debug)]
pub struct DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    // last_applied, as an application progress indicator, may fall under the responsibility of the
    // Handler, as it manages the application process.
    last_applied: AtomicU64,   // The last applied log index
    pending_commit: AtomicU64, // The highest pending commit index
    max_entries_per_chunk: usize,
    state_machine: Arc<SMOF<T>>,

    // current_snapshot_version: AtomicU64,
    snapshot_config: SnapshotConfig,
    snapshot_policy: SNP<T>,
    snapshot_in_progress: AtomicBool,

    /// Temporary lock when snapshot is generated (to prevent concurrent snapshot generation)
    snapshot_lock: RwLock<()>,
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
pub(crate) struct CleanupSnapshotMeta {
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) path: PathBuf,
}

#[async_trait]
impl<T> StateMachineHandler<T> for DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    /// Update pending commit index
    fn update_pending(
        &self,
        new_commit: u64,
    ) {
        let mut current = self.pending_commit.load(Ordering::Acquire);
        while new_commit > current {
            match self
                .pending_commit
                .compare_exchange_weak(current, new_commit, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(e) => current = e,
            }
        }
    }

    /// Batch application log
    async fn apply_batch(
        &self,
        raft_log: Arc<ROF<T>>,
    ) -> Result<()> {
        if let Some(range) = self.pending_range() {
            // Read logs in batches
            let range_end = *range.end();
            let entries = raft_log.get_entries_between(range);

            // Apply in parallel
            let chunks = entries.into_iter().collect::<Vec<_>>();
            let handles: Vec<_> = chunks
                .chunks(self.max_entries_per_chunk)
                .map(|chunk| {
                    let sm = self.state_machine.clone();
                    let chunk = chunk.to_vec(); // Transfer ownership of chunk to the closure
                    tokio::spawn(async move { sm.apply_chunk(chunk) })
                })
                .collect();

            // Wait for all batches to complete
            for h in handles {
                if let Err(e) = h.await {
                    error("apply_batch", &e);
                }
            }

            // Atomic update last_applied
            self.last_applied.store(range_end, Ordering::Release);
        }
        Ok(())
    }

    /// TODO: decouple client related commands with RAFT internal logic
    #[autometrics(objective = API_SLO)]
    fn read_from_state_machine(
        &self,
        client_command: Vec<ClientCommand>,
    ) -> Option<Vec<ClientResult>> {
        let mut result = Vec::new();
        for c in client_command {
            match c.command {
                Some(Command::Get(key)) => {
                    if let Ok(Some(value)) = self.state_machine.get(&key) {
                        result.push(ClientResult { key, value });
                    }
                }
                _ => {
                    error!("might be a bug while receiving none GET command, ignore.")
                }
            }
        }

        if !result.is_empty() {
            Some(result)
        } else {
            None
        }
    }

    async fn install_snapshot_chunk(
        &self,
        current_term: u64,
        stream_request: Box<Streaming<SnapshotChunk>>,
        sender: MaybeCloneOneshotSender<std::result::Result<SnapshotResponse, tonic::Status>>,
    ) -> Result<()> {
        debug!(%current_term, ?stream_request, "install_snapshot_chunk");

        // 1. Init SnapshotAssembler
        let mut assembler = SnapshotAssembler::new(&self.snapshot_config.snapshots_dir).await?;

        // 2. Block processing loop (including network fault tolerance)
        let mut term_check = None;
        let mut metadata = None;
        let mut total_chunks = None;

        let mut stream = stream_request.into_request().into_inner();

        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(e) => {
                    let _ = sender.send(Err(e));
                    return Ok(());
                }
            };

            // Verify the legitimacy of Leader
            if let Some((term, leader_id)) = &term_check {
                if chunk.term != *term || chunk.leader_id != *leader_id {
                    sender
                        .send(Err(Status::aborted("Leader changed")))
                        .map_err(|e| StorageError::Snapshot(format!("Send snapshot error: {:?}", e)))?;
                    return Ok(());
                }
            } else {
                term_check = Some((chunk.term, chunk.leader_id));
                metadata = chunk.metadata;
                total_chunks = Some(chunk.total);
            }

            // Checksum validation
            if !validate_checksum(&chunk.data, &chunk.checksum) {
                sender
                    .send(Ok(SnapshotResponse {
                        term: current_term,
                        success: false,
                        next_chunk: chunk.seq,
                    }))
                    .map_err(|e| StorageError::Snapshot(format!("Send snapshot error: {:?}", e)))?;
                return Ok(());
            }

            // Write to temporary file
            if let Err(e) = assembler.write_chunk(chunk.seq, chunk.data).await {
                sender
                    .send(Err(Status::internal(e.to_string())))
                    .map_err(|e| StorageError::Snapshot(format!("Send snapshot error: {:?}", e)))?;
                return Ok(());
            }
        }

        let total = total_chunks.unwrap_or(0);
        if assembler.received_chunks() != total {
            sender
                .send(Err(Status::internal(format!(
                    "Received chunks({}) != total({})",
                    assembler.received_chunks(),
                    total
                ))))
                .map_err(|e| {
                    warn!("Failed to send response: {:?}", e);
                    StorageError::Snapshot(format!("Send snapshot error: {:?}", e))
                })?;
            return Ok(());
        }

        if metadata.is_none() {
            sender
                .send(Err(Status::internal(
                    "Received snapshot chunk does not include metadata information".to_string(),
                )))
                .map_err(|e| {
                    warn!("Failed to send response: {:?}", e);
                    StorageError::Snapshot(format!("Send snapshot error: {:?}", e))
                })?;
            return Ok(());
        }
        // Application snapshot
        let metadata = metadata.unwrap();
        self.state_machine
            .apply_snapshot_from_file(&metadata, assembler.finalize(&metadata).await?)
            .await?;

        // Final confirmation
        sender
            .send(Ok(SnapshotResponse {
                term: current_term,
                success: true,
                next_chunk: 0,
            }))
            .map_err(|e| StorageError::Snapshot(format!("Send snapshot error: {:?}", e)))?;

        Ok(())
    }

    #[inline]
    fn should_snapshot(
        &self,
        new_commit_data: NewCommitData,
    ) -> bool {
        if self.snapshot_in_progress.load(Ordering::Relaxed) {
            return false;
        }

        let last_applied = self.state_machine.last_applied();
        let (last_included, _) = self.state_machine.last_included();

        self.snapshot_policy.should_trigger(&SnapshotContext {
            role: new_commit_data.role,
            last_included,
            last_applied,
            current_term: new_commit_data.current_term,
        })
    }

    async fn create_snapshot(&self) -> Result<(SnapshotMetadata, PathBuf)> {
        // 0. Create a guard (automatically manage state)
        let _guard = SnapshotGuard::new(&self.snapshot_in_progress)?;

        // 1: Get write lock
        debug!("create_snapshot 1: Get write lock");
        let _guard = self.snapshot_lock.write().await;

        // 2: Prepare temp snapshot file and final snapshot file
        debug!("create_snapshot 2: Prepare temp snapshot file and final snapshot file");
        let last_included = self.state_machine.last_applied();
        let temp_dir = self
            .snapshot_config
            .snapshots_dir
            .join(format!("temp-{}-{}", last_included.index, last_included.term));
        let final_dir = self.snapshot_config.snapshots_dir.join(format!(
            "{}{}-{}",
            SNAPSHOT_DIR_PREFIX, last_included.index, last_included.term
        ));

        // 3: Create snapshot based on the temp path
        debug!(?temp_dir, "create_snapshot 3: Create snapshot based on the temp path");

        let checksum = match self
            .state_machine
            .generate_snapshot_data(temp_dir.clone(), last_included)
            .await
        {
            Err(e) => {
                error!(?e, "state_machine.generate_snapshot_data failed");
                return Err(e);
            }
            Ok(checksum) => checksum,
        };

        // 4: Atomic rename to final snapshot file path
        debug!(
            ?final_dir,
            "create_snapshot 4: Atomic rename to final snapshot file path"
        );

        move_directory(&temp_dir, &final_dir).await?;

        // 5: cleanup old versions
        debug!(%self.snapshot_config.cleanup_retain_count, "create_snapshot 6: cleanup old versions");
        if let Err(e) = self
            .cleanup_snapshot(
                self.snapshot_config.cleanup_retain_count,
                &self.snapshot_config.snapshots_dir,
            )
            .await
        {
            error!(%e, "clean up old snapshot file failed");
        }

        Ok((
            SnapshotMetadata {
                last_included: Some(last_included),
                checksum: checksum.to_vec(),
            },
            final_dir,
        ))
    }

    #[tracing::instrument]
    async fn cleanup_snapshot(
        &self,
        retain_count: u64,
        snapshot_dir: &Path,
    ) -> Result<()> {
        // Phase 1: Collect and parse snapshots
        let mut snapshots = Vec::new();

        let mut entries = fs::read_dir(snapshot_dir).await.map_err(|e| StorageError::PathError {
            path: snapshot_dir.to_path_buf(),
            source: e,
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(StorageError::IoError)? {
            let path = entry.path();
            debug!(?path, "cleanup_snapshot");

            if path.is_dir() {
                if let Some((index, term)) = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(parse_snapshot_dirname)
                {
                    info!("Index: {:>10} | Term: {:>10} | Path: {}", index, term, path.display());
                    snapshots.push(CleanupSnapshotMeta { index, term, path });
                }
            }
        }

        // Phase 2: Sorting and cleaning
        if snapshots.len() <= retain_count as usize {
            return Ok(()); // No need to clean
        }

        // Sort in ascending order by index (earlier snapshots are at the front)
        snapshots.sort_by_key(|m| m.index);

        // Phase 4: Difference: Take the remaining elements
        // Calculate the split point to be retained
        let split_point = snapshots.len() - retain_count as usize;

        for meta in &snapshots[..split_point] {
            info!(
                "Deleting old snapshot [index={}, term={}] at {}",
                meta.index,
                meta.term,
                meta.path.display()
            );
            if let Err(e) = remove_dir_all(&meta.path).await {
                error!("Failed to delete {}: {}", meta.path.display(), e);
            }
        }
        Ok(())
    }

    async fn handle_purge_request(
        &self,
        current_term: u64,
        leader_id: Option<u32>,
        last_purged: Option<LogId>,
        req: &PurgeLogRequest,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<PurgeLogResponse> {
        // Verification 1: Leader identity legitimacy
        if req.term < current_term || leader_id != Some(req.leader_id) {
            return Ok(PurgeLogResponse {
                term: current_term,
                success: false,
                last_purged,
            });
        }

        // Verification 2: Locally applied log index >= requested up_to_index
        if req.last_included.is_none() {
            return Ok(PurgeLogResponse {
                term: current_term,
                success: false,
                last_purged,
            });
        }

        if let Some(last_included) = req.last_included {
            if self.state_machine.last_applied().index < last_included.index {
                return Ok(PurgeLogResponse {
                    term: current_term,
                    success: false,
                    last_purged,
                });
            }
        }

        // Verification 3: Verify snapshot consistency (to prevent snapshot data corruption)
        if let (_, Some(local_checksum)) = self.state_machine.last_included() {
            if req.snapshot_checksum != local_checksum {
                return Ok(PurgeLogResponse {
                    term: current_term,
                    success: false,
                    last_purged,
                });
            }
        } else {
            // There is no corresponding snapshot locally, snapshot synchronization needs to be triggered
            return Ok(PurgeLogResponse {
                term: current_term,
                success: false,
                last_purged,
            });
        }

        // Perform physical deletion
        match raft_log.purge_logs_up_to(req.last_included.unwrap()) {
            Ok(_) => Ok(PurgeLogResponse {
                term: current_term,
                success: true,
                last_purged,
            }),
            Err(e) => {
                error!(?e, "raft_log.purge_logs_up_to");
                Ok(PurgeLogResponse {
                    term: current_term,
                    success: false,
                    last_purged,
                })
            }
        }
    }
}

impl<T> DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    pub fn new(
        last_applied_index: u64,
        max_entries_per_chunk: usize,
        state_machine: Arc<SMOF<T>>,
        snapshot_config: SnapshotConfig,
        snapshot_policy: SNP<T>,
    ) -> Self {
        Self {
            last_applied: AtomicU64::new(last_applied_index),
            pending_commit: AtomicU64::new(0),
            max_entries_per_chunk,
            state_machine,
            // current_snapshot_version: AtomicU64::new(0),
            snapshot_config,
            snapshot_policy,
            snapshot_lock: RwLock::new(()),
            snapshot_in_progress: AtomicBool::new(false),
        }
    }

    /// Get the interval to be processed
    pub fn pending_range(&self) -> Option<RangeInclusive<u64>> {
        let last_applied = self.last_applied.load(Ordering::Acquire);
        let pending_commit = self.pending_commit.load(Ordering::Acquire);

        if pending_commit > last_applied {
            Some((last_applied + 1)..=pending_commit)
        } else {
            None
        }
    }

    #[cfg(test)]
    pub fn pending_commit(&self) -> u64 {
        self.pending_commit.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub fn last_applied(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }
}

/// Manual parsing file name format: snapshot-{index}-{term}
fn parse_snapshot_dirname(name: &str) -> Option<(u64, u64)> {
    debug!(%name, "parse_snapshot_dirname");

    // Check prefix and suffix
    if !name.starts_with(SNAPSHOT_DIR_PREFIX) {
        return None;
    }

    // Remove fixed parts
    let core = &name[9..name.len()]; // "snapshot-".len() = 9,

    // Split version and index
    let parts: Vec<&str> = core.splitn(2, '-').collect();
    if parts.len() != 2 {
        return None;
    }

    // Parse numbers
    match (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
        (Ok(i), Ok(t)) => Some((i, t)),
        _ => None,
    }
}
