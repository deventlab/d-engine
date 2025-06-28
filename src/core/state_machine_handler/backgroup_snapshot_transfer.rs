use crate::proto::storage::snapshot_ack::ChunkStatus;
use crate::proto::storage::{snapshot_ack, SnapshotAck};
use crate::{proto::storage::snapshot_service_client::SnapshotServiceClient, Result, SnapshotError};
use crate::{proto::storage::SnapshotChunk, SnapshotConfig};
use crate::{NetworkError, TypeConfig};
use futures::StreamExt;
use futures::{stream::BoxStream, Stream};
use lru::LruCache;
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::marker::PhantomData;
use std::num::NonZero;
use std::pin::Pin;
use std::{
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{sleep, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::Status;
use tracing::{debug, trace, warn};

// Retry manager with exponential backoff and jitter
pub(super) struct RetryManager {
    max_retries: u32,
    retry_states: HashMap<u32, ChunkRetryState>,
}

pub(super) struct ChunkRetryState {
    retry_count: u32,
    last_retry: Instant,
}

impl RetryManager {
    pub(super) fn new(max_retries: u32) -> Self {
        RetryManager {
            max_retries,
            retry_states: HashMap::new(),
        }
    }

    pub(super) fn record_failure(
        &mut self,
        seq: u32,
    ) -> Result<()> {
        let state = self.retry_states.entry(seq).or_insert(ChunkRetryState {
            retry_count: 0,
            last_retry: Instant::now(),
        });

        state.retry_count += 1;
        state.last_retry = Instant::now();

        if state.retry_count > self.max_retries {
            Err(SnapshotError::TransferFailed.into())
        } else {
            Ok(())
        }
    }

    pub(super) fn record_success(
        &mut self,
        seq: u32,
    ) {
        self.retry_states.remove(&seq);
    }
}

pub(crate) struct BackgroundSnapshotTransfer<T> {
    _marker: PhantomData<T>,
}

#[allow(unused)]
impl<T> BackgroundSnapshotTransfer<T>
where
    T: TypeConfig,
{
    // Unified push transfer entry point
    pub(crate) async fn run_push_transfer(
        node_id: u32,
        data_stream: BoxStream<'static, Result<SnapshotChunk>>,
        channel: tonic::transport::Channel,
        config: SnapshotConfig,
    ) -> Result<()> {
        debug!(%node_id, "Starting push snapshot transfer");
        let config = Arc::new(config);

        // Preload the first chunk and verify
        let mut data_stream = data_stream;
        let first_chunk = match data_stream.next().await {
            Some(Ok(chunk)) if chunk.seq == 0 && chunk.metadata.is_some() => chunk,
            Some(Ok(_)) => return Err(SnapshotError::InvalidFirstChunk.into()),
            Some(Err(e)) => return Err(e),
            None => return Err(SnapshotError::EmptySnapshot.into()),
        };

        let client = SnapshotServiceClient::new(channel)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);

        Self::push_transfer_loop(client, first_chunk, data_stream, config).await?;

        debug!(%node_id, "Push snapshot transfer completed");
        Ok(())
    }

    // Dedicated push logic
    async fn push_transfer_loop(
        mut client: SnapshotServiceClient<Channel>,
        first_chunk: SnapshotChunk,
        mut data_stream: Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
        config: Arc<SnapshotConfig>,
    ) -> Result<()> {
        // 1. Create a transmission channel
        let (mut request_tx, request_rx) = mpsc::channel::<Arc<SnapshotChunk>>(config.push_queue_size);

        // 2. Send the first data block
        request_tx
            .send(Arc::new(first_chunk))
            .await
            .map_err(|e| NetworkError::SingalSendFailed(format!("{:?}", e)))?;

        // 3. Start the background task to send the remaining blocks
        tokio::spawn(async move {
            while let Some(chunk) = data_stream.next().await {
                if let Ok(chunk) = chunk {
                    if let Err(e) = Self::send_chunk_with_retry(&mut request_tx, Arc::new(chunk), &config).await {
                        break;
                    }
                }
            }
        });

        // 4. Create a gRPC request stream
        //  Lazy cloning - clone only when needed to send:
        let request_stream = ReceiverStream::new(request_rx).map(|arc_chunk| (*arc_chunk).clone());

        // 5. Initiate a gRPC call (key point!)
        let response = client
            .install_snapshot(request_stream)
            .await
            .map_err(|e| NetworkError::TonicStatusError(Box::new(e)))?;

        // 6. Check the response
        if response.into_inner().success {
            Ok(())
        } else {
            Err(SnapshotError::RemoteRejection.into())
        }
    }

    // Unified pull transfer entry point
    pub(crate) async fn run_pull_transfer(
        ack_stream: Box<tonic::Streaming<SnapshotAck>>,
        chunk_tx: mpsc::Sender<std::result::Result<Arc<SnapshotChunk>, Status>>,
        mut data_stream: BoxStream<'static, Result<SnapshotChunk>>,
        config: SnapshotConfig,
    ) -> Result<()> {
        debug!("Starting pull snapshot transfer");

        // Create processing pipeline
        let transfer_fut = Self::process_transfer(ack_stream, data_stream, chunk_tx.clone(), config.clone());

        // Run with timeout
        tokio::select! {
            res = transfer_fut => res,
            _ = sleep(Duration::from_secs(config.transfer_timeout_in_sec)) => {
                Err(SnapshotError::TransferTimeout.into())
            }
        }
    }

    // Dedicated pull logic
    async fn process_transfer(
        mut ack_stream: Box<tonic::Streaming<SnapshotAck>>,
        mut data_stream: Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
        chunk_tx: mpsc::Sender<std::result::Result<Arc<SnapshotChunk>, Status>>,
        config: SnapshotConfig,
    ) -> Result<()> {
        let mut chunk_cache = LruCache::new(NonZero::new(config.cache_size).unwrap());
        let mut pending_acks = HashSet::new();
        let mut retry_counts = HashMap::new();
        let mut next_seq = 0;
        let mut total_chunks = None;
        let max_bandwidth_mbps: u32 = config.max_bandwidth_mbps;

        // Unified chunk processing loop
        loop {
            tokio::select! {
                // Process incoming ACKs
                ack = ack_stream.next() => {
                    trace!("receive new ack");

                    match ack {
                        Some(Ok(ack)) => Self::handle_ack(
                            ack,
                            &mut pending_acks,
                            &mut retry_counts,
                            &config,
                        ).await?,

                        Some(Err(e)) => return Err(NetworkError::TonicStatusError(Box::new(e)).into()),

                        None => break, // ACK stream closed
                    }
                },

                // Process next data chunk
                chunk = data_stream.next(), if total_chunks.is_none() || next_seq < total_chunks.unwrap() => {
                    match chunk {
                        Some(Ok(chunk)) => {
                            // Validate and cache chunk
                            trace!(%chunk.seq, %next_seq, "Validate and cache chunk");
                            if chunk.seq != next_seq {
                                return Err(SnapshotError::OutOfOrderChunk.into());
                            }

                            if chunk.seq == 0 {
                                if chunk.metadata.is_none() {
                                    return Err(SnapshotError::MissingMetadata.into());
                                }
                                trace!(?chunk.total_chunks);
                                total_chunks = Some(chunk.total_chunks);
                            }

                            let arc_chunk = Arc::new(chunk);

                            chunk_cache.put(arc_chunk.seq, arc_chunk.clone());

                            trace!(%arc_chunk.seq, "-------- send chunk --------");

                            Self::send_chunk(&chunk_tx, arc_chunk.clone(), max_bandwidth_mbps).await?;
                            pending_acks.insert(arc_chunk.seq);
                            next_seq += 1;
                        }
                        Some(Err(e)) => return Err(e),
                        None => break, // Data stream exhausted
                    }
                },

                // Retry mechanism (separate timer)
                _ = sleep(Duration::from_millis(config.retry_interval_in_ms)), if !pending_acks.is_empty() => {
                    // Add filtering: only process blocks that need to be retried
                    let needs_retry: Vec<u32> = pending_acks.iter()
                    .filter(|&&seq| {
                        // Only process blocks whose retry times are not exceeded
                        retry_counts.get(&seq).map_or(true, |&c| c <= config.max_retries)
                    })
                    .copied()
                    .collect();

                    if !needs_retry.is_empty() {
                        trace!(?retry_counts, ?needs_retry, ?pending_acks);

                        Self::handle_retries(
                            &pending_acks,
                            &mut retry_counts,
                            &mut chunk_cache,
                            &chunk_tx,
                            &config,
                        ).await?;
                    }
                }
            }

            // Completion check
            debug!(?total_chunks, "------ total_chunks");

            if let Some(total) = total_chunks {
                if next_seq >= total && pending_acks.is_empty() {
                    break;
                }
            }
        }

        debug!("Pull snapshot transfer completed");
        Ok(())
    }

    // Handle ACK messages with proper error management
    async fn handle_ack(
        ack: SnapshotAck,
        pending_acks: &mut HashSet<u32>,
        retry_counts: &mut HashMap<u32, u32>,
        config: &SnapshotConfig,
    ) -> Result<()> {
        let seq = ack.seq;

        // Skip if this ACK is for a chunk that's already been processed
        if !pending_acks.contains(&seq) {
            trace!(%seq, "Received ACK for already-processed chunk, ignoring");
            return Ok(());
        }

        match ChunkStatus::try_from(ack.status) {
            Ok(ChunkStatus::Accepted) => {
                trace!(%seq, "remove");

                pending_acks.remove(&seq);
                retry_counts.remove(&seq);

                trace!(?pending_acks, ?retry_counts, "handle_ack");
            }
            Ok(ChunkStatus::ChecksumMismatch) => {
                trace!(?retry_counts, "ChecksumMismatch");

                let count = retry_counts.entry(seq).or_insert(0);
                *count += 1;

                if *count > config.max_retries {
                    trace!(%seq, "Max retries exceeded, removing from pending_acks");
                    pending_acks.remove(&seq); // Remove if the retry limit is exceeded
                    return Err(SnapshotError::TransferFailed.into());
                }

                // Will be resent in next retry cycle
            }
            _ => {
                warn!("Unknown chunk status for seq {}: {}", seq, ack.status);
                pending_acks.remove(&seq); // Unknown status is also removed
            }
        }

        Ok(())
    }

    // Handle periodic retries
    async fn handle_retries(
        pending_acks: &HashSet<u32>,
        retry_counts: &mut HashMap<u32, u32>,
        chunk_cache: &mut LruCache<u32, Arc<SnapshotChunk>>,
        chunk_tx: &mpsc::Sender<std::result::Result<Arc<SnapshotChunk>, Status>>,
        config: &SnapshotConfig,
    ) -> Result<()> {
        let max_bandwidth_mbps = config.max_bandwidth_mbps;

        // Create a snapshot of pending_acks to avoid concurrent modification issues
        let pending_snapshot: Vec<u32> = pending_acks.iter().copied().collect();

        for seq in pending_snapshot {
            // Double-check if still pending before sending
            if !pending_acks.contains(&seq) {
                trace!(%seq, "Skipping retry for already-acked chunk");
                continue;
            }

            // Skip if max retries exceeded
            let count = retry_counts.entry(seq).or_insert(0);
            if *count > config.max_retries {
                trace!(%seq, "Skipping retry for chunk with max retries exceeded");
                continue;
            }

            if let Some(chunk) = chunk_cache.get(&seq) {
                Self::send_chunk(chunk_tx, chunk.clone(), max_bandwidth_mbps).await?;
            } else {
                return Err(SnapshotError::ChunkNotCached(seq).into());
            }
        }
        Ok(())
    }

    // Send next chunk in sequence
    async fn send_next_chunk(
        seq: u32,
        data_stream: &mut Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
        chunk_tx: &mpsc::Sender<std::result::Result<Arc<SnapshotChunk>, Status>>,
        config: &SnapshotConfig,
        chunk_cache: &mut LruCache<u32, Arc<SnapshotChunk>>,
        total_chunks: u32,
        leader_term: u64,
        leader_id: u32,
    ) -> Result<()> {
        let mut chunk = match data_stream.next().await {
            Some(Ok(chunk)) => chunk,
            Some(Err(e)) => return Err(e),
            None => return Err(SnapshotError::IncompleteSnapshot.into()),
        };

        // Update chunk metadata
        chunk.seq = seq;
        chunk.leader_term = leader_term;
        chunk.leader_id = leader_id;
        chunk.total_chunks = total_chunks;

        // Calculate and set checksum
        let checksum = crc32fast::hash(&chunk.data);
        chunk.chunk_checksum = checksum.to_be_bytes().to_vec();

        let arc_chunk = Arc::new(chunk);
        chunk_cache.put(seq, arc_chunk.clone());
        Self::send_chunk(chunk_tx, arc_chunk.clone(), config.max_bandwidth_mbps).await?;
        Ok(())
    }

    // Send chunk with retry logic for push mode
    async fn send_chunk_with_retry(
        tx: &mut mpsc::Sender<Arc<SnapshotChunk>>,
        chunk: Arc<SnapshotChunk>,
        config: &SnapshotConfig,
    ) -> Result<()> {
        const MAX_RETRIES: u32 = 3;
        let mut attempt = 0;
        let mut backoff = Duration::from_millis(100);

        loop {
            debug!(?attempt);
            match tx.try_send(chunk.clone()) {
                Ok(_) => return Ok(()),
                Err(TrySendError::Full(_)) => {
                    if attempt >= MAX_RETRIES {
                        return Err(SnapshotError::Backpressure.into());
                    }

                    // Apply rate limiting before retry
                    Self::apply_rate_limit(&chunk, config.max_bandwidth_mbps).await;

                    sleep(backoff).await;
                    backoff = backoff.mul_f32(2.0).min(Duration::from_secs(1));
                    attempt += 1;
                }
                Err(_) => return Err(SnapshotError::ReceiverDisconnected.into()),
            }
        }
    }

    // Apply rate limiting for chunk transmission
    async fn apply_rate_limit(
        chunk: &SnapshotChunk,
        max_bandwidth_mbps: u32,
    ) {
        if max_bandwidth_mbps > 0 {
            let chunk_size_mb = chunk.data.len() as f64 / 1_000_000.0;
            let min_duration = Duration::from_secs_f64(chunk_size_mb / max_bandwidth_mbps as f64);
            debug!(?chunk_size_mb, ?min_duration);
            sleep(min_duration).await;
        }
    }

    // Send chunk with proper error handling
    async fn send_chunk(
        chunk_tx: &mpsc::Sender<std::result::Result<Arc<SnapshotChunk>, Status>>,
        chunk: Arc<SnapshotChunk>,
        max_bandwidth_mbps: u32,
    ) -> Result<()> {
        Self::apply_rate_limit(&chunk, max_bandwidth_mbps).await;

        chunk_tx
            .send(Ok(chunk))
            .await
            .map_err(|_| SnapshotError::ReceiverDisconnected.into())
    }

    // Load specific chunk from stream
    pub(super) async fn load_specific_chunk(
        data_stream: &mut Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
        seq: u32,
        leader_term: u64,
        leader_id: u32,
        total_chunks: u32,
    ) -> Result<Option<SnapshotChunk>> {
        while let Some(chunk) = data_stream.next().await {
            let mut chunk = chunk?;
            if chunk.seq == seq {
                chunk.leader_term = leader_term;
                chunk.leader_id = leader_id;
                chunk.total_chunks = total_chunks;
                return Ok(Some(chunk));
            }
        }
        Ok(None)
    }

    // pub(super) async fn send_chunks(
    //     mut stream: impl Stream<Item = Result<SnapshotChunk>> + Unpin,
    //     tx: mpsc::Sender<SnapshotChunk>,
    //     config: SnapshotConfig,
    // ) -> Result<()> {
    //     let mut yield_counter = 0;
    //     while let Some(chunk) = stream.next().await {
    //         let chunk = chunk?;

    //         //Apply rate limit
    //         if config.max_bandwidth_mbps > 0 {
    //             let chunk_size_mb = chunk.data.len() as f64 / 1_000_000.0;
    //             let min_duration = Duration::from_secs_f64(chunk_size_mb / config.max_bandwidth_mbps as f64);
    //             tokio::time::sleep(min_duration).await;
    //         }

    //         // Non-blocking send
    //         if tx.try_send(chunk).is_err() {
    //             warn!("Snapshot receiver lagging, dropping chunk");
    //             return Err(SnapshotError::Backpressure.into());
    //         }

    //         // Yield control every N chunks
    //         yield_counter += 1;
    //         if yield_counter % config.sender_yield_every_n_chunks == 0 {
    //             tokio::task::yield_now().await;
    //         }
    //     }
    //     Ok(())
    // }

    // /// Production-ready handler for streaming snapshot requests
    // pub(crate) async fn run_pull_transfer(
    //     leader_term: u64,
    //     leader_id: u32,
    //     mut ack_stream_request: Box<tonic::Streaming<SnapshotAck>>,
    //     chunk_tx: mpsc::Sender<std::result::Result<SnapshotChunk, Status>>,
    //     mut data_stream: BoxStream<'static, Result<SnapshotChunk>>,
    //     metadata: SnapshotMetadata,
    //     config: SnapshotConfig,
    // ) -> Result<()> {
    //     // Create transfer instance for tracking
    //     let transfer = Arc::new(Self {
    //         node_id: 0, // Not used in this context
    //         metadata: metadata.clone(),
    //         config: Arc::new(config.clone()),
    //         status: Arc::new(AtomicU32::new(SnapshotStatus::Pending as u32)),
    //         _marker: PhantomData,
    //     });

    //     // Preload the first chunk and verify
    //     let first_chunk = match data_stream.next().await {
    //         Some(Ok(chunk)) if chunk.seq == 0 && chunk.metadata.is_some() => {
    //             // Verify required fields
    //             if chunk.total_chunks == 0 || chunk.leader_id != 0 || chunk.leader_term != 0 {
    //                 return Err(SnapshotError::InvalidFirstChunk.into());
    //             }
    //             chunk
    //         }
    //         Some(Ok(_)) => return Err(SnapshotError::InvalidFirstChunk.into()),
    //         Some(Err(e)) => return Err(e),
    //         None => return Err(SnapshotError::EmptySnapshot.into()),
    //     };

    //     // Get total_chunks from the first chunk
    //     let total_chunks = first_chunk.total_chunks;

    //     // Send the first chunk (note to update the leader information)
    //     let mut first_chunk = first_chunk;
    //     first_chunk.leader_id = leader_id;
    //     first_chunk.leader_term = leader_term;
    //     let checksum = crc32fast::hash(&first_chunk.data);
    //     first_chunk.chunk_checksum = checksum.to_be_bytes().to_vec();
    //     Self::send_chunk(&chunk_tx, first_chunk, &config).await?;

    //     let mut next_chunk_index = 0; // Start from the second chunk
    //     let mut chunks_sent = 1; // 1 chunk has been sent (the first chunk)

    //     let mut last_ack_time = Instant::now();
    //     let mut retransmit_queue: VecDeque<u32> = VecDeque::new();

    //     let mut retry_states: HashMap<u32, ChunkRetryState> = HashMap::new();
    //     const MAX_RETRIES: u32 = 5;
    //     const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(100);
    //     // Main transfer loop
    //     while next_chunk_index < total_chunks || !retransmit_queue.is_empty() {
    //         tokio::select! {
    //             // Handle incoming ACKs
    //             ack = ack_stream_request.message() => match ack {
    //                 Ok(Some(ack)) => {
    //                     last_ack_time = Instant::now();

    //                     match ack.status() {
    //                         snapshot_ack::ChunkStatus::Accepted => {
    //                             debug!("Chunk {} accepted", ack.seq);
    //                         }
    //                         snapshot_ack::ChunkStatus::ChecksumMismatch => {
    //                             warn!("Checksum mismatch for chunk {}, resending", ack.seq);
    //                             retransmit_queue.push_back(ack.seq);
    //                         }
    //                         snapshot_ack::ChunkStatus::OutOfOrder => {
    //                             warn!("Out-of-order chunk {}, resetting to {}", ack.seq, ack.next_requested);
    //                             next_chunk_index = ack.next_requested;
    //                             retransmit_queue.clear();
    //                         }
    //                         snapshot_ack::ChunkStatus::Requested => {
    //                             debug!("Explicit request for chunk {}", ack.next_requested);
    //                             if ack.next_requested < next_chunk_index {
    //                                 // If the requested chunk has been sent, process it from the retransmit queue
    //                                 retransmit_queue.push_back(ack.next_requested);
    //                             } else {
    //                                 // If the requested chunk has not been sent, jump directly to that position
    //                                 next_chunk_index = ack.next_requested;
    //                                 retransmit_queue.clear();
    //                             }
    //                         }
    //                         snapshot_ack::ChunkStatus::Failed => {
    //                             let retry_state = retry_states.entry(ack.seq).or_insert(ChunkRetryState {
    //                                 retry_count: 0,
    //                                 last_retry: Instant::now() - INITIAL_RETRY_DELAY,
    //                             });

    //                             // Apply exponential backoff
    //                             let next_retry_time = retry_state.last_retry +
    //                                 INITIAL_RETRY_DELAY * 2u32.pow(retry_state.retry_count);

    //                             if Instant::now() >= next_retry_time {
    //                                 retry_state.retry_count += 1;
    //                                 retry_state.last_retry = Instant::now();

    //                                 if retry_state.retry_count > MAX_RETRIES {
    //                                     error!("Chunk {} failed after {} retries, aborting transfer", ack.seq, MAX_RETRIES);
    //                                     transfer.status.store(SnapshotStatus::Failed as u32, Ordering::Release);
    //                                     return Err(SnapshotError::TransferFailed.into());
    //                                 }

    //                                 warn!("Chunk {} failed (attempt {}/{}), resending",
    //                                 ack.seq, retry_state.retry_count, MAX_RETRIES);
    //                                 retransmit_queue.push_back(ack.seq);
    //                             } else {
    //                                 // Schedule for later retry
    //                                 retransmit_queue.push_back(ack.seq);
    //                             }
    //                         }
    //                     }
    //                 }
    //                 Ok(None) => break, // Stream closed by learner
    //                 Err(e) => {
    //                     transfer.status.store(SnapshotStatus::Failed as u32, Ordering::Release);
    //                     return Err(NetworkError::TonicStatusError(Box::new(e)).into());
    //                 }
    //             },

    //             // Send next chunk or retransmit
    //             _ = async {
    //                 if let Some(seq) = retransmit_queue.pop_front() {
    //                     // Retransmit specific chunk
    //                     if let Some(chunk) = Self::load_specific_chunk(
    //                         &mut data_stream,
    //                         seq,
    //                         leader_term,
    //                         leader_id,
    //                         total_chunks
    //                     ).await? {
    //                         Self::send_chunk(&chunk_tx, chunk, &config).await?;
    //                     }
    //                 } else if next_chunk_index < total_chunks {
    //                     // Send next chunk in sequence
    //                     if let Some(chunk) = Self::load_next_chunk(
    //                         &mut data_stream,
    //                         next_chunk_index,
    //                         leader_term,
    //                         leader_id,
    //                         &transfer,
    //                         total_chunks
    //                     ).await? {
    //                         Self::send_chunk(&chunk_tx, chunk, &config).await?;
    //                         next_chunk_index += 1;
    //                     }
    //                     chunks_sent += 1;
    //                 }
    //                 Ok::<(), Box<dyn Error + Send + Sync>>(())
    //             } => {},

    //             // Handle timeout
    //             _ = tokio::time::sleep(Duration::from_secs(5)) => {
    //                 if last_ack_time.elapsed() > Duration::from_secs(10) {
    //                     transfer.status.store(SnapshotStatus::Failed as u32, Ordering::Release);
    //                     return Err(SnapshotError::TransferTimeout.into());
    //                 }
    //             }
    //         }
    //     }

    //     transfer
    //         .status
    //         .store(SnapshotStatus::Completed as u32, Ordering::Release);
    //     Ok(())
    // }

    // async fn load_next_chunk(
    //     data_stream: &mut Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
    //     seq: u32,
    //     leader_term: u64,
    //     leader_id: u32,
    //     transfer: &Arc<Self>,
    //     total_chunks: u32,
    // ) -> Result<Option<SnapshotChunk>> {
    //     match data_stream.next().await {
    //         Some(Ok(mut chunk)) => {
    //             // Update chunk metadata
    //             chunk.leader_id = leader_id;
    //             chunk.leader_term = leader_term;
    //             chunk.seq = seq;
    //             chunk.total_chunks = total_chunks;

    //             // Add metadata only for first chunk
    //             if seq == 0 {
    //                 chunk.metadata = Some(transfer.metadata.clone());
    //             }

    //             // Calculate and set checksum
    //             chunk.chunk_checksum = crc32fast::hash(&chunk.data).to_be_bytes().to_vec();

    //             Ok(Some(chunk))
    //         }
    //         Some(Err(e)) => Err(e),
    //         None => Ok(None),
    //     }
    // }

    // pub(super) async fn load_specific_chunk(
    //     data_stream: &mut Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
    //     seq: u32,
    //     leader_term: u64,
    //     leader_id: u32,
    //     total_chunks: u32,
    // ) -> Result<Option<SnapshotChunk>> {
    //     while let Some(Ok(mut chunk)) = data_stream.next().await {
    //         if chunk.seq == seq {
    //             chunk.leader_term = leader_term;
    //             chunk.leader_id = leader_id;
    //             chunk.total_chunks = total_chunks;
    //             return Ok(Some(chunk));
    //         }
    //     }
    //     Ok(None)
    // }

    // async fn send_chunk(
    //     chunk_tx: &mpsc::Sender<std::result::Result<SnapshotChunk, Status>>,
    //     chunk: SnapshotChunk,
    //     config: &SnapshotConfig,
    // ) -> Result<()> {
    //     // Apply rate limiting
    //     if config.max_bandwidth_mbps > 0 {
    //         let chunk_size_mb = chunk.data.len() as f64 / 1_000_000.0;
    //         let min_duration = Duration::from_secs_f64(chunk_size_mb / config.max_bandwidth_mbps as f64);
    //         tokio::time::sleep(min_duration).await;
    //     }

    //     // Send with bounded channel backpressure
    //     match chunk_tx.send(Ok(chunk)).await {
    //         Ok(_) => Ok(()),
    //         Err(e) => {
    //             warn!(?e, "Failed to send chunk: receiver disconnected");
    //             Err(SnapshotError::ReceiverDisconnected.into())
    //         }
    //     }
    // }

    // / Create snapshot block
    // fn create_chunk(
    //     &self,
    //     leader_term: u64,
    //     leader_id: u32,
    //     seq: u32,
    //     data: Option<Vec<u8>>,
    //     is_first: bool,
    //     total_chunks: u32,
    // ) -> Result<SnapshotChunk> {
    //     let data = data.unwrap_or_default();

    //     // Calculate checksum
    //     let mut hasher = crc32fast::Hasher::new();
    //     hasher.update(&data);
    //     let checksum = hasher.finalize().to_be_bytes().to_vec();

    //     Ok(SnapshotChunk {
    //         leader_term,
    //         leader_id,
    //         seq,
    //         total_chunks,
    //         chunk_checksum: checksum,
    //         metadata: if is_first { Some(self.metadata.clone()) } else { None },
    //         data,
    //     })
    // }
}
