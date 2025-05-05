use std::ops::RangeInclusive;
use std::path::PathBuf;
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
use super::StateMachineHandler;
use crate::alias::ROF;
use crate::alias::SMOF;
use crate::constants::SNAPSHOT_DIR_PREFIX;
use crate::file_io::move_directory;
use crate::file_io::validate_checksum;
use crate::proto::client_command::Command;
use crate::proto::ClientCommand;
use crate::proto::ClientResult;
use crate::proto::SnapshotChunk;
use crate::proto::SnapshotResponse;
use crate::utils::cluster::error;
use crate::MaybeCloneOneshotSender;
use crate::RaftLog;
use crate::Result;
use crate::SnapshotConfig;
use crate::StateMachine;
use crate::StorageError;
use crate::TypeConfig;
use crate::API_SLO;

#[derive(Debug)]
pub struct DefaultStateMachineHandler<T>
where T: TypeConfig
{
    // last_applied, as an application progress indicator, may fall under the responsibility of the
    // Handler, as it manages the application process.
    last_applied: AtomicU64,   // The last applied log index
    pending_commit: AtomicU64, // The highest pending commit index
    max_entries_per_chunk: usize,
    state_machine: Arc<SMOF<T>>,

    current_snapshot_version: AtomicU64,
    snapshot_config: SnapshotConfig,

    /// Temporary lock when snapshot is generated (to prevent concurrent snapshot generation)
    snapshot_lock: RwLock<()>,
}

#[async_trait]
impl<T> StateMachineHandler<T> for DefaultStateMachineHandler<T>
where T: TypeConfig
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
        stream_request: Streaming<SnapshotChunk>,
        sender: MaybeCloneOneshotSender<std::result::Result<SnapshotResponse, tonic::Status>>,
    ) -> Result<()> {
        // 1. Create a temporary directory
        let temp_dir =
            tempfile::tempdir_in(&self.snapshot_config.snapshots_dir).map_err(|e| StorageError::PathError {
                path: self.snapshot_config.snapshots_dir.clone(),
                source: e,
            })?;
        let mut assembler = SnapshotAssembler::new(temp_dir.path()).await?;

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
                term_check = Some((chunk.term, chunk.leader_id.clone()));
                metadata = chunk.metadata.clone();
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

        // Application snapshot
        let metadata = metadata.unwrap();
        self.state_machine
            .apply_snapshot_from_file(metadata, assembler.finalize().await?)
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

    async fn create_snapshot(&self) -> Result<std::path::PathBuf> {
        // 1: Get write lock
        debug!("create_snapshot 1: Get write lock");
        let _guard = self.snapshot_lock.write().await;

        // 2: Prepare temp snapshot file and final snapshot file
        debug!("create_snapshot 2: Prepare temp snapshot file and final snapshot file");
        let (last_included_index, last_included_term) = self.state_machine.last_applied();
        let current_snapshot_version = self.current_snapshot_version();
        let new_snapshot_version = current_snapshot_version + 1;
        let temp_dir = self
            .snapshot_config
            .snapshots_dir
            .join(format!("temp-{}", new_snapshot_version));
        let final_dir = self.snapshot_config.snapshots_dir.join(format!(
            "{}{}-{}-{}",
            SNAPSHOT_DIR_PREFIX, new_snapshot_version, last_included_index, last_included_term
        ));

        // 3: Create snapshot based on the temp path
        debug!(?temp_dir, "create_snapshot 3: Create snapshot based on the temp path");
        if let Err(e) = self
            .state_machine
            .generate_snapshot_data(&temp_dir, last_included_index, last_included_term)
            .await
        {
            error!(?e, "state_machine.generate_snapshot_data failed");
            return Err(e);
        }

        // 4: Atomic rename to final snapshot file path
        debug!(
            ?final_dir,
            "create_snapshot 4: Atomic rename to final snapshot file path"
        );

        move_directory(&temp_dir, &final_dir).await?;

        // 5: Update snapshot version
        debug!(%current_snapshot_version, %new_snapshot_version, "create_snapshot 5: Update snapshot version");
        self.current_snapshot_version
            .compare_exchange(
                current_snapshot_version,
                new_snapshot_version,
                Ordering::Release,
                Ordering::Relaxed,
            )
            .map_err(|e| StorageError::Snapshot(format!("{:?}", e)))?;

        // 6: cleanup old versions
        let cleanup_version = current_snapshot_version.saturating_sub(self.snapshot_config.cleanup_version_offset);
        debug!(%cleanup_version, "create_snapshot 6: cleanup old versions");
        if let Err(e) = self
            .cleanup_snapshot(cleanup_version, &self.snapshot_config.snapshots_dir)
            .await
        {
            error!(%e, "clean up old snapshot file failed");
        }

        Ok(final_dir)
    }

    async fn cleanup_snapshot(
        &self,
        before_version: u64,
        snapshot_dir: &PathBuf,
    ) -> Result<()> {
        let mut entries = fs::read_dir(snapshot_dir.as_path())
            .await
            .map_err(|e| StorageError::PathError {
                path: snapshot_dir.clone(),
                source: e,
            })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| StorageError::IoError(e))? {
            let path = entry.path();
            debug!(?path, "cleanup_snapshot");

            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                    //File name parsing logic
                    if let Some((version, index, term)) = parse_snapshot_dirname(dir_name) {
                        info!(
                            "Version: {:>4} | Index: {:>10} | Term: {:>10} | Path: {}",
                            version,
                            index,
                            term,
                            path.display()
                        );

                        debug!(%version, %before_version, "cleanup_snapshot");

                        if version <= before_version {
                            info!(?version, "going to be deleted.");
                            if let Err(e) = remove_dir_all(&path).await {
                                error!(%e, "delete_file failed");
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn current_snapshot_version(&self) -> u64 {
        self.current_snapshot_version.load(Ordering::Acquire)
    }
}

impl<T> DefaultStateMachineHandler<T>
where T: TypeConfig
{
    pub fn new(
        last_applied_index: u64,
        max_entries_per_chunk: usize,
        state_machine: Arc<SMOF<T>>,
        snapshot_config: SnapshotConfig,
    ) -> Self {
        Self {
            last_applied: AtomicU64::new(last_applied_index),
            pending_commit: AtomicU64::new(0),
            max_entries_per_chunk,
            state_machine,
            current_snapshot_version: AtomicU64::new(0),
            snapshot_lock: RwLock::new(()),
            snapshot_config,
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

/// Manual parsing file name format: snapshot-{version}-{index}.bin
fn parse_snapshot_dirname(name: &str) -> Option<(u64, u64, u64)> {
    debug!(%name, "parse_snapshot_dirname");

    // Check prefix and suffix
    if !name.starts_with(SNAPSHOT_DIR_PREFIX) {
        return None;
    }

    // Remove fixed parts
    let core = &name[9..name.len()]; // "snapshot-".len() = 9, ".bin".len() = 4

    // Split version and index
    let parts: Vec<&str> = core.splitn(3, '-').collect();
    if parts.len() != 3 {
        return None;
    }

    // Parse numbers
    match (
        parts[0].parse::<u64>(),
        parts[1].parse::<u64>(),
        parts[2].parse::<u64>(),
    ) {
        (Ok(v), Ok(i), Ok(t)) => Some((v, i, t)),
        _ => None,
    }
}
