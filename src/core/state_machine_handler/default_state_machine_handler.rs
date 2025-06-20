use super::SnapshotAssembler;
use super::SnapshotContext;
use super::SnapshotPolicy;
use super::StateMachineHandler;
use crate::alias::ROF;
use crate::alias::SMOF;
use crate::alias::SNP;
use crate::constants::SNAPSHOT_DIR_PREFIX;
use crate::file_io::validate_checksum;
use crate::proto::client::ClientResult;
use crate::proto::common::LogId;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotChunk;
use crate::proto::storage::SnapshotMetadata;
use crate::proto::storage::SnapshotResponse;
use crate::scoped_timer::ScopedTimer;
use crate::utils::cluster::error;
use crate::MaybeCloneOneshotSender;
use crate::NewCommitData;
use crate::RaftLog;
use crate::Result;
use crate::SnapshotConfig;
use crate::SnapshotError;
use crate::SnapshotGuard;
use crate::SnapshotPathManager;
use crate::StateMachine;
use crate::StorageError;
use crate::TypeConfig;
use crate::API_SLO;
use async_compression::tokio::write::GzipEncoder;
use async_stream::try_stream;
use autometrics::autometrics;
use futures::stream::BoxStream;
use std::ops::RangeInclusive;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::fs::remove_dir_all;
use tokio::fs::remove_file;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tonic::async_trait;
use tonic::IntoRequest;
use tonic::Status;
use tonic::Streaming;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;

#[derive(Debug)]
pub struct DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    node_id: u32,
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

    path_mgr: Arc<SnapshotPathManager>,

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
        keys: Vec<Vec<u8>>,
    ) -> Option<Vec<ClientResult>> {
        let mut result = Vec::new();
        for key in keys {
            if let Ok(Some(value)) = self.state_machine.get(&key) {
                result.push(ClientResult { key, value });
            }
        }

        if !result.is_empty() {
            Some(result)
        } else {
            None
        }
    }

    #[instrument(skip(self))]
    async fn install_snapshot_chunk(
        &self,
        current_term: u64,
        stream_request: Box<Streaming<SnapshotChunk>>,
        sender: MaybeCloneOneshotSender<std::result::Result<SnapshotResponse, tonic::Status>>,
        config: &SnapshotConfig,
    ) -> Result<()> {
        let _timer = ScopedTimer::new("install_snapshot_chunk");
        debug!(%current_term, "install_snapshot_chunk");

        // 1. Init SnapshotAssembler
        let mut assembler = SnapshotAssembler::new(self.path_mgr.clone()).await?;

        // 2. Configuration
        let chunk_timeout = Duration::from_secs(10); // Timeout between chunks
        let mut last_received = Instant::now();

        // 3. Stream processing
        let mut stream = stream_request.into_request().into_inner();
        let mut term_check = None;
        let mut captured_metadata: Option<SnapshotMetadata> = None;
        let mut total_chunks = None;
        let mut count = 0;

        loop {
            // Receive next chunk with timeout
            let chunk = match timeout(chunk_timeout, stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    last_received = Instant::now();
                    chunk
                }
                Ok(Some(Err(e))) => {
                    error!("install_snapshot_chunk stream.next error: {:?}", e);
                    let _ = sender.send(Err(e));
                    return Ok(());
                }
                Ok(None) => {
                    // Stream ended normally
                    debug!("install_snapshot_chunk:: Stream ended normally");
                    break;
                }
                Err(_) => {
                    let elapsed = last_received.elapsed();
                    warn!("No chunk received for {} seconds", elapsed.as_secs());
                    let _ = sender.send(Err(Status::deadline_exceeded(format!(
                        "No chunk received for {} seconds",
                        elapsed.as_secs()
                    ))));
                    return Ok(());
                }
            };

            debug!(seq = %chunk.seq, ?chunk.metadata, "install_snapshot_chunk");

            // Verify leader legitimacy
            if let Some((term, leader_id)) = &term_check {
                if chunk.term != *term || chunk.leader_id != *leader_id {
                    sender
                        .send(Err(Status::aborted("Leader changed")))
                        .map_err(|e| SnapshotError::OperationFailed(format!("Send error: {e:?}")))?;
                    return Ok(());
                }
            } else {
                term_check = Some((chunk.term, chunk.leader_id));
                // Only capture metadata if present (should be first chunk only)
                let first_metadata = match chunk.metadata {
                    Some(m) => m,
                    None => {
                        // Handle missing metadata on first chunk immediately
                        sender
                            .send(Err(Status::internal(
                                "First snapshot chunk is missing metadata".to_string(),
                            )))
                            .map_err(|e| SnapshotError::OperationFailed(format!("Send error: {e:?}")))?;
                        return Ok(());
                    }
                };

                captured_metadata = Some(first_metadata);
                total_chunks = Some(chunk.total);

                debug!(
                    "Initial snapshot metadata: {:?}, total_chunks={}",
                    captured_metadata,
                    total_chunks.unwrap_or(0)
                );
            }

            debug!(?captured_metadata, "in process");
            // Checksum validation
            if !validate_checksum(&chunk.data, &chunk.checksum) {
                warn!("validate_checksum failed");
                sender
                    .send(Ok(SnapshotResponse {
                        term: current_term,
                        success: false,
                        next_chunk: chunk.seq,
                    }))
                    .map_err(|e| SnapshotError::OperationFailed(format!("Send error: {e:?}")))?;
                return Ok(());
            }

            // Write to temporary file
            if let Err(e) = assembler.write_chunk(chunk.seq, chunk.data).await {
                warn!("assembler.write_chunk failed");
                sender
                    .send(Err(Status::internal(e.to_string())))
                    .map_err(|e| SnapshotError::OperationFailed(format!("Send error: {e:?}")))?;
                return Ok(());
            }

            count += 1;

            // Yield every 1 chunks to prevent starvation
            if count % config.receiver_yield_every_n_chunks == 0 {
                debug!(%count, "install_snapshot_chunk yield_now");
                tokio::task::yield_now().await;
            }
        }

        let total = total_chunks.unwrap_or(0);

        debug!(%total, "install_snapshot_chunk");

        if assembler.received_chunks() != total {
            sender
                .send(Err(Status::internal(format!(
                    "Received chunks({}) != total({})",
                    assembler.received_chunks(),
                    total
                ))))
                .map_err(|e| {
                    warn!("Failed to send response: {:?}", e);
                    SnapshotError::OperationFailed(format!("Send snapshot error: {e:?}"))
                })?;
            return Ok(());
        }

        debug!(?captured_metadata, "done");
        let final_metadata = match captured_metadata {
            Some(meta) => meta.clone(),
            None => {
                sender
                    .send(Err(Status::internal("Missing metadata in snapshot stream".to_string())))
                    .map_err(|e| {
                        warn!("Failed to send response: {:?}", e);
                        SnapshotError::OperationFailed(format!("Send error: {e:?}"))
                    })?;
                return Ok(());
            }
        };

        let snapshot_path = match assembler.finalize(&final_metadata).await {
            Ok(path) => path,
            Err(e) => {
                sender
                    .send(Err(Status::internal(format!("Finalize snapshot path failed: {:?}", e))))
                    .map_err(|e| {
                        warn!("Failed to send response: {:?}", e);
                        SnapshotError::OperationFailed(format!("Send error: {e:?}"))
                    })?;
                return Ok(());
            }
        };

        debug!(?final_metadata, ?snapshot_path, "before apply_snapshot_from_file");

        if let Err(e) = self
            .state_machine
            .apply_snapshot_from_file(&final_metadata, snapshot_path)
            .await
        {
            sender
                .send(Err(Status::internal(format!(
                    "Apply snapshot from file failed: {:?}",
                    e
                ))))
                .map_err(|e| {
                    warn!("Failed to send response: {:?}", e);
                    SnapshotError::OperationFailed(format!("Send error: {e:?}"))
                })?;
            return Ok(());
        }

        // Final confirmation
        let response = Ok(SnapshotResponse {
            term: current_term,
            success: true,
            next_chunk: 0,
        });

        match sender.send(response) {
            Ok(_) => debug!("Snapshot response sent successfully"),
            Err(send_error) => {
                let error_msg = format!(
                    "[install_snapshot_chunk | {}]Failed to send response: {:?}",
                    self.node_id, send_error
                );
                warn!("{}", error_msg);

                return Err(SnapshotError::OperationFailed(error_msg.into()).into());
            }
        }
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
        let last_snapshot_metadata = self.state_machine.snapshot_metadata();

        let last_included = last_snapshot_metadata
            .and_then(|meta| meta.last_included)
            .unwrap_or_else(|| LogId { index: 0, term: 0 });

        self.snapshot_policy.should_trigger(&SnapshotContext {
            role: new_commit_data.role,
            last_included,
            last_applied,
            current_term: new_commit_data.current_term,
        })
    }

    async fn create_snapshot(&self) -> Result<(SnapshotMetadata, PathBuf)> {
        // 0. Create a guard (automatically manage state)
        let _guard1 = SnapshotGuard::new(&self.snapshot_in_progress)?;

        // 1: Get write lock
        debug!("create_snapshot 1: Get write lock");
        let _guard2 = self.snapshot_lock.write().await;

        // 2: Prepare temp snapshot file and final snapshot file
        debug!("create_snapshot 2: Prepare temp snapshot file and final snapshot file");
        let raw_last_included = self.state_machine.last_applied();

        // Apply retention policy
        let last_included = LogId {
            index: raw_last_included
                .index
                .saturating_sub(self.snapshot_config.retained_log_entries),
            term: self
                .state_machine
                .entry_term(
                    raw_last_included
                        .index
                        .saturating_sub(self.snapshot_config.retained_log_entries),
                )
                .unwrap_or(raw_last_included.term),
        };

        let temp_path = self.path_mgr.temp_work_path(&last_included);

        // 3: Create snapshot based on the temp path
        debug!(?temp_path, "create_snapshot 3: Create snapshot based on the temp path");
        let checksum = self
            .state_machine
            .generate_snapshot_data(temp_path.clone(), last_included)
            .await?;

        // 4: Compress the snapshot directory into a tar.gz archive
        debug!("create_snapshot 4: Compressing snapshot directory");
        let final_path = self.path_mgr.final_snapshot_path(&last_included);
        // let compressed_path = final_path.with_extension("tar.gz");
        let compressed_file = File::create(&final_path)
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to create compressed file: {}", e)))?;
        let gzip_encoder = GzipEncoder::new(compressed_file);
        let mut tar_builder = tokio_tar::Builder::new(gzip_encoder);

        // Add all files in temp_path to the archive
        tar_builder
            .append_dir_all(".", &temp_path)
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to create tar archive: {}", e)))?;

        // Finish writing and flush all data
        tar_builder
            .finish()
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to finish tar archive: {}", e)))?;

        // Get inner GzipEncoder and shutdown to ensure all data is written
        let mut gzip_encoder = tar_builder
            .into_inner()
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to get inner encoder: {}", e)))?;
        gzip_encoder
            .shutdown()
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to shutdown gzip encoder: {}", e)))?;

        // 6. Remove the original uncompressed directory
        remove_dir_all(&temp_path)
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to remove temp directory: {}", e)))?;

        // 7: cleanup old versions
        debug!(%self.snapshot_config.cleanup_retain_count, "create_snapshot 5: cleanup old versions");
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
            final_path,
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

            if path.extension().map_or(false, |ext| ext == "gz") {
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    let parsed = parse_snapshot_dirname(file_name).or_else(|| {
                        file_name
                            .strip_suffix(".tar.gz")
                            .and_then(|stripped| parse_snapshot_dirname(stripped))
                    });

                    let (index, term) = if let Some(pair) = parsed {
                        pair
                    } else {
                        continue;
                    };

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
            let file_type = tokio::fs::metadata(&meta.path)
                .await
                .map_err(StorageError::IoError)?
                .file_type();

            if file_type.is_file() {
                remove_file(&meta.path).await.map_err(StorageError::IoError)?;
            } else if file_type.is_dir() {
                remove_dir_all(&meta.path).await.map_err(StorageError::IoError)?;
            }
        }
        Ok(())
    }

    #[instrument(skip(self))]
    async fn validate_purge_request(
        &self,
        current_term: u64,
        leader_id: Option<u32>,
        req: &PurgeLogRequest,
    ) -> Result<bool> {
        // Verification 1: Leader identity legitimacy
        if req.term < current_term || leader_id != Some(req.leader_id) {
            return Ok(false);
        }

        // Verification 2: Locally applied log index >= requested up_to_index
        if req.last_included.is_none() {
            return Ok(false);
        }

        if let Some(last_included) = req.last_included {
            if self.state_machine.last_applied().index < last_included.index {
                return Ok(false);
            }
        }

        // Verification 3: Verify snapshot consistency (to prevent snapshot data corruption)
        if let Some(last_snapshot_metadata) = self.state_machine.snapshot_metadata() {
            if req.snapshot_checksum != last_snapshot_metadata.checksum {
                return Ok(false);
            }
        } else {
            // There is no corresponding snapshot locally, snapshot synchronization needs to be triggered
            return Ok(false);
        }

        Ok(true)
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
                node_id: self.node_id,
                term: current_term,
                success: false,
                last_purged,
            });
        }

        // Verification 2: Locally applied log index >= requested up_to_index
        if req.last_included.is_none() {
            return Ok(PurgeLogResponse {
                node_id: self.node_id,
                term: current_term,
                success: false,
                last_purged,
            });
        }

        if let Some(last_included) = req.last_included {
            if self.state_machine.last_applied().index < last_included.index {
                return Ok(PurgeLogResponse {
                    node_id: self.node_id,
                    term: current_term,
                    success: false,
                    last_purged,
                });
            }
        }

        // Verification 3: Verify snapshot consistency (to prevent snapshot data corruption)
        if let Some(last_snapshot_metadata) = self.state_machine.snapshot_metadata() {
            if req.snapshot_checksum != last_snapshot_metadata.checksum {
                return Ok(PurgeLogResponse {
                    node_id: self.node_id,
                    term: current_term,
                    success: false,
                    last_purged,
                });
            }
        } else {
            // There is no corresponding snapshot locally, snapshot synchronization needs to be triggered
            return Ok(PurgeLogResponse {
                node_id: self.node_id,
                term: current_term,
                success: false,
                last_purged,
            });
        }

        // Perform physical deletion
        match raft_log.purge_logs_up_to(req.last_included.unwrap()) {
            Ok(_) => Ok(PurgeLogResponse {
                node_id: self.node_id,
                term: current_term,
                success: true,
                last_purged,
            }),
            Err(e) => {
                error!(?e, "raft_log.purge_logs_up_to");
                Ok(PurgeLogResponse {
                    node_id: self.node_id,
                    term: current_term,
                    success: false,
                    last_purged,
                })
            }
        }
    }

    fn get_latest_snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        self.state_machine.snapshot_metadata()
    }

    /// Load snapshot data as a stream of chunks
    #[instrument(skip(self))]
    async fn load_snapshot_data(
        &self,
        metadata: SnapshotMetadata,
    ) -> Result<BoxStream<'static, Result<SnapshotChunk>>> {
        let _timer = ScopedTimer::new("install_snapshot_chunk");
        let last_included = metadata
            .last_included
            .ok_or_else(|| SnapshotError::OperationFailed("No last_included in metadata".to_string()))?;

        // Build path to compressed snapshot file
        let snapshot_file = self.path_mgr.final_snapshot_path(&last_included);

        debug!("Loading snapshot from file: {:?}", snapshot_file);
        self.create_snapshot_chunk_stream(snapshot_file, metadata).await
    }
}

impl<T> DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    pub fn new(
        node_id: u32,
        last_applied_index: u64,
        max_entries_per_chunk: usize,
        state_machine: Arc<SMOF<T>>,
        snapshot_config: SnapshotConfig,
        snapshot_policy: SNP<T>,
    ) -> Self {
        Self {
            node_id,
            last_applied: AtomicU64::new(last_applied_index),
            pending_commit: AtomicU64::new(0),
            max_entries_per_chunk,
            state_machine,
            snapshot_policy,
            path_mgr: Arc::new(SnapshotPathManager::new(
                snapshot_config.snapshots_dir.clone(),
                SNAPSHOT_DIR_PREFIX.to_string(),
            )),
            snapshot_config,

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

    #[instrument(skip(self))]
    async fn create_snapshot_chunk_stream(
        &self,
        snapshot_file: PathBuf,
        metadata: SnapshotMetadata,
    ) -> Result<BoxStream<'static, Result<SnapshotChunk>>> {
        let _timer = ScopedTimer::new("install_snapshot_chunk");
        let path_metadata = tokio::fs::metadata(&snapshot_file)
            .await
            .map_err(StorageError::IoError)?;

        if !path_metadata.is_file() {
            return Err(SnapshotError::OperationFailed(format!(
                "Invalid snapshot file type: {}",
                snapshot_file.display()
            ))
            .into());
        }

        let chunk_size = self.snapshot_config.chunk_size;
        let node_id = self.node_id;
        let term = metadata.last_included.map(|id| id.term).unwrap_or(0);

        // Open the compressed file
        let mut file = File::open(&snapshot_file).await.map_err(StorageError::IoError)?;
        let file_len = path_metadata.len();
        let total_chunks = ((file_len as usize) + chunk_size - 1) / chunk_size;

        debug!(%total_chunks);

        let stream = try_stream! {
            let mut seq = 0;
            let mut buffer = vec![0u8; chunk_size];

            while let Ok(bytes_read) = file.read(&mut buffer).await {
                if bytes_read == 0 {
                    break;
                }

                let chunk_data = buffer[..bytes_read].to_vec();
                let checksum = crc32fast::hash(&chunk_data).to_be_bytes().to_vec();

                // Only first chunk contains metadata
                let chunk_metadata = if seq == 0 {
                    Some(metadata.clone())
                } else {
                    None
                };

                debug!(%seq, ?chunk_metadata, "create_snapshot_chunk_stream");
                yield SnapshotChunk {
                    term,
                    leader_id: node_id,
                    metadata: chunk_metadata,
                    seq,
                    total: total_chunks as u32,
                    data: chunk_data,
                    checksum,
                };

                seq += 1;
            }
        };

        Ok(Box::pin(stream))
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
