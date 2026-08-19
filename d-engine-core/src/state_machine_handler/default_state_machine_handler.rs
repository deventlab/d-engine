#[cfg(feature = "watch")]
use crate::BatchOp;

use crate::CapturedLocalSnapshot;
use crate::PreparedSnapshot;
use crate::client::KvEntry;
use crate::state_machine_handler::applied_state::AppliedState;
use async_compression::tokio::bufread::GzipDecoder;
use async_compression::tokio::write::GzipEncoder;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;
use d_engine_proto::server::storage::snapshot_ack::ChunkStatus;
use futures::stream::BoxStream;
use memmap2::Mmap;
use memmap2::MmapOptions;
use std::ops::RangeInclusive;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::fs;
use tokio::fs::File;
use tokio::fs::remove_dir_all;
use tokio::fs::remove_file;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::time::timeout;
use tokio_tar::Archive;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;

use super::SnapshotAssembler;
use super::SnapshotContext;
use super::SnapshotPolicy;
use super::StateMachineHandler;
#[cfg(feature = "watch")]
use crate::ApplyEntry;
#[cfg(feature = "watch")]
use crate::ApplyResult;
#[cfg(feature = "watch")]
use crate::Command;
use crate::NewCommitData;
use crate::Result;
use crate::SnapshotConfig;
use crate::SnapshotError;
use crate::SnapshotPathManager;
use crate::StateMachine;
use crate::StorageError;
use crate::TypeConfig;
use crate::alias::SMOF;
use crate::alias::SNP;
use crate::file_io::validate_checksum;
use crate::file_io::validate_compressed_format;
use crate::scoped_timer::ScopedTimer;

/// Unified snapshot metadata with precomputed values
#[derive(Debug, Clone)]
pub struct SnapshotTransferMeta {
    pub metadata: SnapshotMetadata,
    pub total_chunks: u32,
    pub chunk_size: usize,
    #[allow(unused)]
    pub file_size: u64,
    pub file_path: PathBuf,
}

#[derive(Debug)]
pub struct DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    node_id: u32,

    pending_commit: AtomicU64, // The highest pending commit index
    state_machine: Arc<SMOF<T>>,

    applied: Arc<AppliedState>,
    applied_notify_rx: tokio::sync::watch::Receiver<u64>,

    // current_snapshot_version: AtomicU64,
    snapshot_config: SnapshotConfig,
    /// Explicit runtime path — always `data_dir/snapshots` (see #10). Not
    /// part of `snapshot_config`; `snapshots_dir` is a runtime identity, not
    /// a configurable setting.
    snapshots_dir: PathBuf,
    snapshot_policy: SNP<T>,
    snapshot_in_progress: AtomicBool,

    path_mgr: Arc<SnapshotPathManager>,
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
    fn last_applied(&self) -> u64 {
        self.applied.load()
    }

    /// Get the interval to be processed
    fn pending_range(&self) -> Option<RangeInclusive<u64>> {
        let last_applied = self.applied.load();
        let pending_commit = self.pending_commit.load(Ordering::Acquire);

        if pending_commit > last_applied {
            Some((last_applied + 1)..=pending_commit)
        } else {
            None
        }
    }

    /// Update pending commit index
    fn update_pending(
        &self,
        new_commit: u64,
    ) {
        let mut current = self.pending_commit.load(Ordering::Acquire);
        while new_commit > current {
            match self.pending_commit.compare_exchange_weak(
                current,
                new_commit,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(e) => current = e,
            }
        }
    }

    async fn wait_applied(
        &self,
        target_index: u64,
        timeout: Duration,
    ) -> Result<()> {
        let mut rx = self.applied_notify_rx.clone();

        // Fast path: check if already applied (avoid timeout future overhead)
        {
            let current = *rx.borrow();
            if current >= target_index {
                return Ok(());
            }
        }

        // Slow path: need to wait for notification
        tokio::time::timeout(timeout, async {
            loop {
                rx.changed()
                    .await
                    .map_err(|_| crate::Error::Fatal("apply notify channel closed".into()))?;

                let current = *rx.borrow();
                if current >= target_index {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(|_| {
            let current_applied = *rx.borrow();
            crate::Error::Fatal(format!(
                "Timeout waiting for state machine to apply index {target_index} \
                 (timeout: {timeout:?}, current_applied: {current_applied})"
            ))
        })?
    }

    fn read_from_state_machine(
        &self,
        keys: Vec<Bytes>,
    ) -> Option<Vec<KvEntry>> {
        let mut result = Vec::new();
        for key in keys {
            if let Ok(Some(value)) = self.state_machine.get(&key) {
                result.push(KvEntry { key, value });
            }
        }

        if !result.is_empty() {
            Some(result)
        } else {
            None
        }
    }

    async fn prepare_snapshot_stream(
        &self,
        first_chunk: SnapshotChunk,
        mut remaining_chunks: mpsc::Receiver<SnapshotChunk>,
        ack_tx: mpsc::Sender<SnapshotAck>,
        config: &SnapshotConfig,
    ) -> Result<PreparedSnapshot> {
        let mut assembler = SnapshotAssembler::new(self.path_mgr.clone()).await?;
        let chunk_timeout = Duration::from_secs(config.receive_chunk_timeout_in_sec);
        let mut last_received = Instant::now();

        // The caller already checked first_chunk.leader_term against its own current_term
        // (#436). (term, leader_id) here is purely an *internal* consistency
        // anchor — every later chunk must match, or the leader changed mid-transfer.
        let (term, leader_id) = (first_chunk.leader_term, first_chunk.leader_id);
        let captured_metadata = first_chunk.metadata.clone().ok_or_else(|| {
            SnapshotError::OperationFailed("Missing metadata in snapshot stream".to_string())
        })?;
        let total_chunks = first_chunk.total_chunks;

        let mut count = 0u32;
        Self::validate_and_write_chunk(&mut assembler, first_chunk, &ack_tx).await?;
        count += 1;

        loop {
            let chunk = match timeout(chunk_timeout, remaining_chunks.recv()).await {
                Ok(Some(chunk)) => {
                    debug!("receive new chunk.");
                    last_received = Instant::now();
                    chunk
                }
                Ok(None) => {
                    debug!("no more chunks available...");
                    break;
                }
                Err(_) => {
                    ack_tx
                        .send(SnapshotAck {
                            seq: 0,
                            status: ChunkStatus::Failed.into(),
                            next_requested: 0,
                        })
                        .await
                        .map_err(|e| {
                            SnapshotError::OperationFailed(format!("Failed to send ACK: {e}"))
                        })?;
                    let elapsed = last_received.elapsed();
                    return Err(SnapshotError::OperationFailed(format!(
                        "No chunk received for {} seconds",
                        elapsed.as_secs()
                    ))
                    .into());
                }
            };

            if chunk.leader_term != term || chunk.leader_id != leader_id {
                ack_tx
                    .send(SnapshotAck {
                        seq: chunk.seq,
                        status: ChunkStatus::OutOfOrder.into(),
                        next_requested: 0,
                    })
                    .await
                    .map_err(|e| {
                        SnapshotError::OperationFailed(format!("Failed to send ACK: {e}"))
                    })?;
                return Err(SnapshotError::OperationFailed(
                    "Leader changed during transfer".to_string(),
                )
                .into());
            }

            Self::validate_and_write_chunk(&mut assembler, chunk, &ack_tx).await?;

            count += 1;
            if count % config.receiver_yield_every_n_chunks as u32 == 0 {
                debug!(%count, %config.receiver_yield_every_n_chunks, "yield_now");
                tokio::task::yield_now().await;
            }
        }

        debug!(%total_chunks, "expected total chunks");
        if assembler.received_chunks() != total_chunks {
            ack_tx
                .send(SnapshotAck {
                    seq: assembler.received_chunks(),
                    status: ChunkStatus::Failed.into(),
                    next_requested: 0,
                })
                .await
                .map_err(|e| SnapshotError::OperationFailed(format!("Failed to send ACK: {e}")))?;
            return Err(SnapshotError::OperationFailed(format!(
                "Received chunks({}) != total({})",
                assembler.received_chunks(),
                total_chunks
            ))
            .into());
        }

        let snapshot_path = assembler.finalize(&captured_metadata).await?;

        let temp_dir = tempfile::tempdir()?;
        self.decompress_to_directory(&snapshot_path, temp_dir.path()).await?;

        Ok(PreparedSnapshot {
            metadata: captured_metadata,
            temp_dir,
        })
    }

    #[inline]
    fn should_snapshot(
        &self,
        new_commit_data: NewCommitData,
    ) -> bool {
        let _timer = ScopedTimer::new("should_snapshot");

        if self.snapshot_in_progress.load(Ordering::Relaxed) {
            trace!("Snapshot already in progress");
            return false;
        }

        let last_applied = self.state_machine.last_applied();
        let last_snapshot_metadata = self.state_machine.snapshot_metadata();

        let last_included = last_snapshot_metadata
            .and_then(|meta| meta.last_included)
            .unwrap_or(LogId { index: 0, term: 0 });

        self.snapshot_policy.should_trigger(&SnapshotContext {
            role: new_commit_data.role,
            last_included,
            last_applied,
            current_term: new_commit_data.current_term,
        })
    }

    fn try_begin_local_snapshot_capture(&self) -> Result<()> {
        self.snapshot_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .map_err(|_| {
                SnapshotError::OperationFailed("Snapshot already in progress".to_string())
            })?;
        Ok(())
    }

    fn end_local_snapshot_capture(&self) {
        self.snapshot_in_progress.store(false, Ordering::SeqCst);
    }

    async fn build_local_snapshot(
        &self,
        captured: CapturedLocalSnapshot,
    ) -> Result<(SnapshotMetadata, PathBuf)> {
        let CapturedLocalSnapshot { metadata, temp_dir } = captured;
        let last_included = metadata.last_included.ok_or_else(|| {
            SnapshotError::OperationFailed("captured snapshot has no last_included".to_string())
        })?;

        debug!(
            ?temp_dir,
            ?last_included,
            "build_local_snapshot: compressing"
        );
        let final_path = self.path_mgr.final_snapshot_path(&last_included);
        if let Err(e) = self.compress_directory(&temp_dir, &final_path).await {
            let _ = remove_dir_all(&temp_dir).await;
            return Err(e);
        }

        remove_dir_all(&temp_dir).await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to remove temp directory: {e}"))
        })?;

        debug!(%self.snapshot_config.cleanup_retain_count, "build_local_snapshot: cleanup old versions");
        if let Err(e) = self
            .cleanup_snapshot(
                self.snapshot_config.cleanup_retain_count,
                &self.snapshots_dir,
                &self.snapshot_config.snapshots_dir_prefix,
            )
            .await
        {
            error!(%e, "clean up old snapshot file failed");
        }

        info!(?final_path, "New local snapshot built");
        Ok((metadata, final_path))
    }

    async fn cleanup_snapshot(
        &self,
        retain_count: u64,
        snapshot_dir: &Path,
        snapshot_dir_prefix: &str,
    ) -> Result<()> {
        // Phase 1: Collect and parse snapshots
        let mut snapshots = Vec::new();

        let mut entries =
            fs::read_dir(snapshot_dir).await.map_err(|e| StorageError::PathError {
                path: snapshot_dir.to_path_buf(),
                source: e,
            })?;

        while let Some(entry) = entries.next_entry().await.map_err(StorageError::IoError)? {
            let path = entry.path();
            debug!(?path, "cleanup_snapshot");

            if path.extension().is_some_and(|ext| ext == "gz")
                && let Some(file_name) = path.file_name().and_then(|n| n.to_str())
            {
                let parsed = parse_snapshot_dirname(file_name, snapshot_dir_prefix).or_else(|| {
                    file_name
                        .strip_suffix(".tar.gz")
                        .and_then(|s| parse_snapshot_dirname(s, snapshot_dir_prefix))
                });

                let (index, term) = if let Some(pair) = parsed {
                    pair
                } else {
                    continue;
                };

                info!(
                    "Index: {:>10} | Term: {:>10} | Path: {}",
                    index,
                    term,
                    path.display()
                );
                snapshots.push(CleanupSnapshotMeta { index, term, path });
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

    fn get_latest_snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        self.state_machine.snapshot_metadata()
    }

    /// Load snapshot data as a stream of chunks (ZERO-COPY)
    async fn load_snapshot_data(
        &self,
        metadata: SnapshotMetadata,
        leader_term: u64,
    ) -> Result<BoxStream<'static, Result<SnapshotChunk>>> {
        let _timer = ScopedTimer::new("load_snapshot_data");
        let transfer_meta = self.prepare_transfer_meta(metadata).await?;

        debug!(
            "Loading snapshot from file: {:?} ({} chunks)",
            transfer_meta.file_path, transfer_meta.total_chunks
        );

        // Use zero-copy memory mapping for efficient reads
        let file = tokio::fs::File::open(&transfer_meta.file_path)
            .await
            .map_err(StorageError::IoError)?;
        let mmap = unsafe { MmapOptions::new().map(&file).map_err(StorageError::IoError)? };
        let mmap_arc = Arc::new(mmap);

        let node_id = self.node_id;
        let chunk_size = transfer_meta.chunk_size;
        let total_chunks = transfer_meta.total_chunks;
        let metadata = transfer_meta.metadata;

        let stream = try_stream! {
            for seq in 0..total_chunks {
                let start = (seq as usize) * chunk_size;
                let end = std::cmp::min(start + chunk_size, mmap_arc.len());

                if start >= mmap_arc.len() {
                    break;
                }

                // ZERO-COPY: Create Bytes that references the memory map
                let chunk_data = zero_copy_bytes_from_mmap(mmap_arc.clone(), start, end);

                let checksum = crc32fast::hash(&chunk_data).to_be_bytes();
                let chunk_checksum = Bytes::copy_from_slice(&checksum);

                yield SnapshotChunk {
                    leader_term,
                    leader_id: node_id,
                    metadata: if seq == 0 { Some(metadata.clone()) } else { None },
                    seq,
                    total_chunks,
                    data: chunk_data,
                    chunk_checksum,
                };
            }
        };

        Ok(Box::pin(stream))
    }
}

/// Read the current value for every write operation in `chunk`, before applying.
///
/// Returns one `Option<Bytes>` per entry in the same order:
/// - `Some(value)` for Insert / Delete / CAS entries (empty Bytes if key didn't exist)
/// - `None` for Noop entries (no prev_value needed)
///
/// Uses a per-batch overlay so two writes to the same key within one chunk produce
/// correct prev_values: the second write sees the first write's value, not the
/// pre-batch SM state.
#[cfg(feature = "watch")]
pub(crate) fn read_prev_values(
    sm: &dyn crate::StateMachine,
    chunk: &[ApplyEntry],
) -> Vec<Option<bytes::Bytes>> {
    use std::collections::HashMap;

    let mut overlay: HashMap<bytes::Bytes, Option<bytes::Bytes>> = HashMap::new();
    let mut result = Vec::with_capacity(chunk.len());

    for entry in chunk {
        let prev = match &entry.command {
            Command::Insert { key, value, .. } => {
                let prev = overlay.get(key).cloned().unwrap_or_else(|| sm.get(key).ok().flatten());
                overlay.insert(key.clone(), Some(value.clone()));
                Some(prev.unwrap_or_default())
            }
            Command::Delete { key } => {
                let prev = overlay.get(key).cloned().unwrap_or_else(|| sm.get(key).ok().flatten());
                overlay.insert(key.clone(), None);
                Some(prev.unwrap_or_default())
            }
            Command::CompareAndSwap { key, .. } => {
                let prev = overlay.get(key).cloned().unwrap_or_else(|| sm.get(key).ok().flatten());
                // CAS outcome is unknown before apply; skip overlay update.
                // Same-key CAS pairs in one batch are rare and semantically ambiguous.
                Some(prev.unwrap_or_default())
            }
            Command::Noop => None,
            Command::Batch { ops: _ } => None,
        };
        result.push(prev);
    }
    result
}

/// Broadcast watch events for applied chunk entries (fire-and-forget).
///
/// Receives already-decoded `&[ApplyEntry]` — no proto decode happens here.
/// Non-blocking: if channel is full, oldest events are dropped (lagging receivers).
///
/// `results` must have the same length as `chunk` (enforced by StateMachineHandler contract).
/// CAS entries where `results[i].succeeded == false` are skipped — no mutation occurred.
/// `prev_values`: optional slice of pre-apply values (None = no prev_kv watcher active).
#[cfg(feature = "watch")]
#[inline]
pub(crate) fn broadcast_watch_events(
    chunk: &[ApplyEntry],
    results: &[ApplyResult],
    tx: &tokio::sync::broadcast::Sender<d_engine_proto::client::WatchResponse>,
    prev_values: Option<&[Option<bytes::Bytes>]>,
) {
    use d_engine_proto::client::WatchEventType;
    use d_engine_proto::client::WatchResponse;

    for (i, entry) in chunk.iter().enumerate() {
        let prev_value =
            prev_values.and_then(|pv| pv.get(i)).and_then(|v| v.clone()).unwrap_or_default();

        let events: Vec<WatchResponse> = match &entry.command {
            Command::Insert { key, value, .. } => vec![WatchResponse {
                key: key.clone(),
                value: value.clone(),
                prev_value,
                event_type: WatchEventType::Put as i32,
                error: 0,
                revision: entry.index,
            }],
            Command::Delete { key } => vec![WatchResponse {
                key: key.clone(),
                value: bytes::Bytes::new(),
                prev_value,
                event_type: WatchEventType::Delete as i32,
                error: 0,
                revision: entry.index,
            }],
            Command::CompareAndSwap { key, value, .. } => {
                // Only broadcast if CAS actually mutated the value.
                // A failed CAS leaves the key unchanged — no watch event.
                if results.get(i).is_some_and(|r| r.succeeded) {
                    vec![WatchResponse {
                        key: key.clone(),
                        value: value.clone(),
                        prev_value,
                        event_type: WatchEventType::Put as i32,
                        error: 0,
                        revision: entry.index,
                    }]
                } else {
                    vec![]
                }
            }
            Command::Noop => vec![],
            // Batch: one event per op. prev_value is not supported per-op yet
            // (read_prev_values returns one value per ApplyEntry, not per BatchOp).
            Command::Batch { ops } => ops
                .iter()
                .map(|op| match op {
                    BatchOp::Insert { key, value } => WatchResponse {
                        key: key.clone(),
                        value: value.clone(),
                        prev_value: bytes::Bytes::new(),
                        event_type: WatchEventType::Put as i32,
                        error: 0,
                        revision: entry.index,
                    },
                    BatchOp::Delete { key } => WatchResponse {
                        key: key.clone(),
                        value: bytes::Bytes::new(),
                        prev_value: bytes::Bytes::new(),
                        event_type: WatchEventType::Delete as i32,
                        error: 0,
                        revision: entry.index,
                    },
                })
                .collect(),
        };

        for ev in events {
            // Fire-and-forget: ignore send errors (no receivers or lagging)
            let _ = tx.send(ev);
        }
    }
}

impl<T> DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    pub fn new(
        node_id: u32,
        last_applied_index: u64,
        state_machine: Arc<SMOF<T>>,
        snapshots_dir: PathBuf,
        snapshot_config: SnapshotConfig,
        snapshot_policy: SNP<T>,
    ) -> Self {
        let (applied, applied_notify_rx) = AppliedState::new(last_applied_index);

        Self {
            node_id,
            applied,
            applied_notify_rx,
            pending_commit: AtomicU64::new(0),
            state_machine,
            snapshot_policy,
            path_mgr: Arc::new(SnapshotPathManager::new(
                snapshots_dir.clone(),
                snapshot_config.snapshots_dir_prefix.clone(),
            )),
            snapshots_dir,
            snapshot_config,

            snapshot_in_progress: AtomicBool::new(false),
        }
    }

    /// Create transfer metadata with precomputed values
    async fn prepare_transfer_meta(
        &self,
        metadata: SnapshotMetadata,
    ) -> Result<SnapshotTransferMeta> {
        let last_included = metadata.last_included.ok_or_else(|| {
            SnapshotError::OperationFailed("No last_included in metadata".to_string())
        })?;

        let file_path = self.path_mgr.final_snapshot_path(&last_included);
        let file_meta = tokio::fs::metadata(&file_path).await.map_err(StorageError::IoError)?;
        let file_size = file_meta.len();
        let chunk_size = self.snapshot_config.chunk_size;

        // Precompute total chunks once
        let total_chunks = file_size.div_ceil(chunk_size as u64) as u32;

        Ok(SnapshotTransferMeta {
            metadata,
            total_chunks,
            chunk_size,
            file_size,
            file_path,
        })
    }

    async fn compress_directory(
        &self,
        source_dir: &Path,
        dest_path: &Path,
    ) -> Result<()> {
        let compressed_file = File::create(dest_path).await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to create compressed file: {e}"))
        })?;

        let gzip_encoder = GzipEncoder::new(compressed_file);
        let mut tar_builder = tokio_tar::Builder::new(gzip_encoder);

        // Add all files in source_dir to the archive
        tar_builder.append_dir_all(".", source_dir).await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to create tar archive: {e}"))
        })?;

        // Finish writing and flush all data
        tar_builder.finish().await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to finish tar archive: {e}"))
        })?;

        // Get inner GzipEncoder and shutdown to ensure all data is written
        let mut gzip_encoder = tar_builder.into_inner().await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to get inner encoder: {e}"))
        })?;
        gzip_encoder.shutdown().await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to shutdown gzip encoder: {e}"))
        })?;

        Ok(())
    }

    async fn decompress_to_directory(
        &self,
        compressed_path: &Path,
        target_dir: &Path,
    ) -> Result<()> {
        debug!(
            ?compressed_path,
            "Validate file format before processing (IMPROVEMENT ADDED)"
        );
        // Validate file format before processing (IMPROVEMENT ADDED) Verify the file is
        //    actually compressed using magic numbers or extension
        validate_compressed_format(compressed_path)?;

        let file = File::open(compressed_path).await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to open snapshot file: {e}"))
        })?;

        let buf_reader = BufReader::new(file);
        let gzip_decoder = GzipDecoder::new(buf_reader);
        let mut archive = Archive::new(gzip_decoder);

        archive.unpack(target_dir).await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to unpack snapshot: {e}"))
        })?;

        Ok(())
    }

    /// Shared by `prepare_snapshot_stream`'s first-chunk handling and its remaining-chunk
    /// loop: validate checksum, write to the assembler, ACK. Kept as one function so the
    /// two call sites can't silently drift apart.
    async fn validate_and_write_chunk(
        assembler: &mut SnapshotAssembler,
        chunk: SnapshotChunk,
        ack_tx: &mpsc::Sender<SnapshotAck>,
    ) -> Result<()> {
        if !validate_checksum(&chunk.data, &chunk.chunk_checksum) {
            ack_tx
                .send(SnapshotAck {
                    seq: chunk.seq,
                    status: ChunkStatus::ChecksumMismatch.into(),
                    next_requested: chunk.seq,
                })
                .await
                .map_err(|e| SnapshotError::OperationFailed(format!("Failed to send ACK: {e}")))?;
            return Err(
                SnapshotError::OperationFailed("Checksum validation failed".to_string()).into(),
            );
        }

        assembler.write_chunk(chunk.seq, chunk.data).await?;

        ack_tx
            .send(SnapshotAck {
                seq: chunk.seq,
                status: ChunkStatus::Accepted.into(),
                next_requested: chunk.seq + 1,
            })
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to send ACK: {e}")))?;

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn pending_commit(&self) -> u64 {
        self.pending_commit.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn snapshot_in_progress(&self) -> bool {
        self.snapshot_in_progress.load(Ordering::Acquire)
    }

    /// Test helper: simulate state machine applying to target index
    #[cfg(test)]
    pub(crate) fn test_simulate_apply(
        &self,
        target_index: u64,
    ) {
        self.applied.advance(target_index);
    }
}

/// Helper function to create zero-copy Bytes from memory map
fn zero_copy_bytes_from_mmap(
    mmap_arc: Arc<Mmap>,
    start: usize,
    end: usize,
) -> Bytes {
    // Get a slice of the memory map
    let slice = &mmap_arc[start..end];
    Bytes::copy_from_slice(slice)
}

/// Manual parsing file name format: snapshot-{index}-{term}
fn parse_snapshot_dirname(
    name: &str,
    snapshot_dir_prefix: &str,
) -> Option<(u64, u64)> {
    debug!(%name, "parse_snapshot_dirname");

    // Check prefix and suffix
    if !name.starts_with(snapshot_dir_prefix) {
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

/// Constructs a paired reader/writer sharing one `AppliedState` and one `state_machine`
/// handle. This is the only place `DefaultStateMachineWriter` should be built alongside
/// its reader — `StateMachineWorker` must be the sole holder of the writer half.
pub fn new_reader_writer_pair<T: TypeConfig>(
    node_id: u32,
    last_applied_index: u64,
    state_machine: Arc<SMOF<T>>,
    snapshots_dir: PathBuf,
    snapshot_config: SnapshotConfig,
    snapshot_policy: SNP<T>,
    watch_event_tx: Option<tokio::sync::broadcast::Sender<d_engine_proto::client::WatchResponse>>,
    prev_kv_watcher_count: Arc<std::sync::atomic::AtomicUsize>,
) -> (
    DefaultStateMachineHandler<T>,
    super::DefaultStateMachineWriter<T>,
) {
    let (applied, applied_notify_rx) = AppliedState::new(last_applied_index);
    let path_mgr = Arc::new(SnapshotPathManager::new(
        snapshots_dir.clone(),
        snapshot_config.snapshots_dir_prefix.clone(),
    ));
    let reader = DefaultStateMachineHandler {
        node_id,
        pending_commit: AtomicU64::new(0),
        state_machine: state_machine.clone(),
        applied: applied.clone(),
        applied_notify_rx,
        snapshot_policy,
        path_mgr: path_mgr.clone(),
        snapshots_dir,
        snapshot_config,
        snapshot_in_progress: AtomicBool::new(false),
    };
    let writer = super::DefaultStateMachineWriter::new(
        node_id,
        state_machine,
        applied,
        path_mgr,
        watch_event_tx,
        prev_kv_watcher_count,
    );
    (reader, writer)
}
