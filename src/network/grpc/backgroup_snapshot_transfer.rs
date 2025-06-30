use std::collections::HashMap;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::num::NonZero;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::BoxStream;
use futures::Stream;
use futures::StreamExt;
use lru::LruCache;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::sleep;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::Status;
use tracing::debug;
use tracing::trace;
use tracing::warn;

use crate::proto::storage::snapshot_ack::ChunkStatus;
use crate::proto::storage::snapshot_service_client::SnapshotServiceClient;
use crate::proto::storage::SnapshotAck;
use crate::proto::storage::SnapshotChunk;
use crate::NetworkError;
use crate::Result;
use crate::SnapshotConfig;
use crate::SnapshotError;
use crate::TypeConfig;

pub(crate) struct BackgroundSnapshotTransfer<T> {
    _marker: PhantomData<T>,
}

#[allow(unused)]
impl<T> BackgroundSnapshotTransfer<T>
where T: TypeConfig
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

        Self::push_transfer_loop(node_id, client, first_chunk, data_stream, config).await?;

        debug!(%node_id, "Push snapshot transfer completed");
        Ok(())
    }

    // Dedicated push logic
    async fn push_transfer_loop(
        node_id: u32,
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
            .map_err(|e| NetworkError::SingalSendFailed(format!("{e:?}")))?;

        // 3. Start the background task to send the remaining blocks
        let (error_tx, mut error_rx) = mpsc::channel(1);

        // # Bug fix: request_tx can not be cloned.
        // let mut bg_request_tx = request_tx.clone();
        let bg_config = config.clone();
        tokio::spawn(async move {
            let result = async {
                while let Some(chunk) = data_stream.next().await {
                    match chunk {
                        Ok(chunk) => {
                            Self::send_chunk_with_retry(&mut request_tx, Arc::new(chunk), &bg_config).await?;
                        }
                        Err(e) => return Err(e),
                    }
                }
                Ok(())
            }
            .await;

            debug!("finished send snapshot stream!");

            // Only send error if one occurred
            if let Err(e) = result {
                let _ = error_tx.send(e).await;
            }
            // Otherwise, let error_tx drop naturally
        });

        // 4. Create a gRPC request stream
        let request_stream = ReceiverStream::new(request_rx).map(|arc_chunk| (*arc_chunk).clone());

        // 5. Initiate gRPC call with timeout and error handling
        let grpc_fut = client.install_snapshot(request_stream);
        tokio::pin!(grpc_fut);

        debug!(config.push_timeout_in_ms);
        let timeout_duration = Duration::from_millis(config.push_timeout_in_ms);
        let timeout_fut = tokio::time::sleep(timeout_duration);
        tokio::pin!(timeout_fut);

        loop {
            tokio::select! {
                // Check for background errors
                bg_error = error_rx.recv() => {
                    match bg_error {
                        Some(e) => return Err(e),
                        None => continue, // No error yet, or background task completed without error, keep waiting
                    }
                }

                response = &mut grpc_fut => {
                    trace!("normal response ...");
                    match response {
                        Ok(response) => {
                            if response.into_inner().success {
                                return Ok(());
                            } else {
                                return Err(SnapshotError::RemoteRejection.into());
                            }
                        }
                        Err(e) => return Err(NetworkError::TonicStatusError(Box::new(e)).into()),
                    }
                }

                // Handle timeout
                 _ = &mut timeout_fut => {
                    trace!("timeout ...");
                    return Err(NetworkError::Timeout{node_id, duration: timeout_duration}.into());
                }
            }
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
                        retry_counts.get(&seq).is_none_or(|&c| c <= config.max_retries)
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

    // Send chunk with retry logic for push mode
    async fn send_chunk_with_retry(
        tx: &mut mpsc::Sender<Arc<SnapshotChunk>>,
        chunk: Arc<SnapshotChunk>,
        config: &SnapshotConfig,
    ) -> Result<()> {
        let mut attempt = 0;
        // let mut backoff = Duration::from_millis(config.snapshot_push_backoff_in_ms);
        let max_retry = config.snapshot_push_max_retry;

        loop {
            trace!(?attempt);
            match tx.try_send(chunk.clone()) {
                Ok(_) => {
                    trace!("send chunk.");
                    return Ok(());
                }
                Err(TrySendError::Full(_)) => {
                    trace!("queue is full!");
                    if attempt >= max_retry {
                        return Err(SnapshotError::Backpressure.into());
                    }
                    let start = Instant::now();
                    // Apply rate limiting before retry
                    Self::apply_rate_limit(&chunk, config.max_bandwidth_mbps).await;
                    let duration = start.elapsed();

                    attempt += 1;
                    trace!(?attempt, ?duration, "apply_rate_limit=");
                }
                Err(e) => {
                    trace!(?e, "unknown error");
                    return Err(SnapshotError::ReceiverDisconnected.into());
                }
            }
        }
    }

    // Apply rate limiting for chunk transmission
    async fn apply_rate_limit(
        chunk: &SnapshotChunk,
        max_bandwidth_mbps: u32,
    ) {
        if max_bandwidth_mbps > 0 {
            let chunk_size_bits = chunk.data.len() as f64 * 8.0;
            let bandwidth_bps = max_bandwidth_mbps as f64 * 1_000_000.0;
            let min_duration_secs = chunk_size_bits / bandwidth_bps;

            let min_duration = Duration::from_secs_f64(min_duration_secs);
            debug!(
                chunk_size_bytes = chunk.data.len(),
                max_bandwidth_mbps,
                min_duration_secs,
                min_duration = ?min_duration
            );
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
}
