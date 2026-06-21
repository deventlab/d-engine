use std::time::Duration;

use bytes::Bytes;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotResponse;
use futures::StreamExt;
use futures::stream;
use futures::stream::BoxStream;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tonic::Status;
use tracing::debug;

use super::*;
use crate::Error;
use crate::MockTypeConfig;
use crate::NetworkError;
use crate::SnapshotConfig;
use crate::SnapshotError;
use crate::StorageError;
use crate::create_test_chunk;
use crate::create_test_snapshot_stream;

/// Helper to create a valid snapshot stream
fn create_snapshot_stream(
    chunks: usize,
    chunk_size: usize,
) -> BoxStream<'static, Result<SnapshotChunk>> {
    let chunks: Vec<SnapshotChunk> = (0..chunks)
        .map(|seq| {
            let data = vec![seq as u8; chunk_size];
            create_test_chunk(
                seq as u32,
                &data,
                1, // term
                1, // leader_id
                chunks as u32,
            )
        })
        .collect();

    let stream = create_test_snapshot_stream(chunks);
    Box::pin(
        stream.map(|item| item.map_err(|s| NetworkError::TonicStatusError(Box::new(s)).into())),
    )
}

fn default_snapshot_config() -> SnapshotConfig {
    SnapshotConfig {
        max_bandwidth_mbps: 1,
        sender_yield_every_n_chunks: 2,
        transfer_timeout_in_sec: 1,
        push_timeout_in_ms: 100,
        ..Default::default()
    }
}

// Helper to create snapshot chunks for testing
fn create_snapshot_chunk(
    seq: u32,
    size: usize,
) -> SnapshotChunk {
    SnapshotChunk {
        leader_term: 1,
        leader_id: 1,
        seq,
        total_chunks: 1,
        chunk_checksum: Bytes::new(),
        metadata: if seq == 0 {
            Some(Default::default())
        } else {
            None
        },
        data: Bytes::from(vec![0; size]),
    }
}

#[cfg(test)]
mod run_push_transfer_test {
    use super::*;
    use crate::MockNode;
    use crate::SystemError;

    #[tokio::test]
    async fn test_push_transfer_success() {
        let config = default_snapshot_config();
        let stream = create_snapshot_stream(3, 1024); // 3 chunks of 1KB each

        // Start mock server
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let (channel, _port) = MockNode::simulate_snapshot_mock_server(
            Ok(SnapshotResponse {
                term: 1,
                success: true, // always succeed
                next_chunk: 1,
            }),
            shutdown_rx,
        )
        .await
        .unwrap();

        let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(
            1,
            Box::pin(stream),
            channel,
            config,
        )
        .await;

        debug!(?result);

        assert!(result.is_ok());
    }

    // Test: Transfer fails when stream returns an error
    #[tokio::test]
    async fn test_push_transfer_fails_on_stream_error() {
        let config = default_snapshot_config();
        let stream = create_snapshot_stream(2, 512);
        let error_stream = stream::once(async {
            Err(StorageError::IoError(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Test error",
            ))
            .into())
        });
        let combined_stream = stream.chain(error_stream).boxed();

        // Start mock server
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let (channel, _port) = MockNode::simulate_snapshot_mock_server(
            Err(Status::unavailable("Service is not ready")),
            shutdown_rx,
        )
        .await
        .unwrap();

        let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(
            1,
            combined_stream,
            channel,
            config,
        )
        .await;

        assert!(result.is_err());
    }

    // Test: Transfer respects bandwidth limit
    #[tokio::test]
    async fn test_push_transfer_respects_bandwidth_limit() {
        let mut config = default_snapshot_config();
        config.max_bandwidth_mbps = 1; // 1 MBps
        config.push_queue_size = 1;
        config.push_timeout_in_ms = 2500;

        let stream = create_snapshot_stream(3, 5 * 1024); // 2 chunks of 5KB each (1.25MB total)

        // Start mock server
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let (channel, _port) = MockNode::simulate_snapshot_mock_server(
            Ok(SnapshotResponse {
                term: 1,
                success: true, // always succeed
                next_chunk: 2,
            }),
            shutdown_rx,
        )
        .await
        .unwrap();

        let start = Instant::now();
        let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(
            1,
            Box::pin(stream),
            channel,
            config,
        )
        .await;

        debug!(?result);
        assert!(result.is_ok());
        let duration = start.elapsed();
        debug!(?duration);

        // Calculate expected minimum time:
        // Total data = 2 chunks * 5KB = 10KB = 80,000 bits
        // Bandwidth = 1 Mbps = 1,000,000 bps
        // Minimum time = 80,000 / 1,000,000 = 0.08 seconds
        assert!(duration >= Duration::from_micros(80));
    }

    // Test: First chunk validation fails when metadata is missing
    #[tokio::test]
    async fn test_push_transfer_fails_on_invalid_first_chunk_metadata() {
        let config = default_snapshot_config();

        // Create a stream with first chunk missing metadata
        let mut invalid_chunk = create_snapshot_chunk(0, 1024);
        invalid_chunk.metadata = None;
        let stream = stream::iter(vec![Ok(invalid_chunk)]).boxed();

        // Start mock server
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let (channel, _port) = MockNode::simulate_snapshot_mock_server(
            Ok(SnapshotResponse {
                term: 1,
                success: true,
                next_chunk: 1,
            }),
            shutdown_rx,
        )
        .await
        .unwrap();

        let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(
            1, stream, channel, config,
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::Error::Consensus(crate::ConsensusError::Snapshot(
                SnapshotError::InvalidFirstChunk
            ))
        ));
    }

    // Test: First chunk validation fails when sequence is not 0
    #[tokio::test]
    async fn test_push_transfer_fails_on_invalid_first_chunk_sequence() {
        let config = default_snapshot_config();

        // Create a stream with first chunk having wrong sequence
        let invalid_chunk = create_snapshot_chunk(1, 1024); // seq=1 instead of 0
        let stream = stream::iter(vec![Ok(invalid_chunk)]).boxed();

        // Start mock server
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let (channel, _port) = MockNode::simulate_snapshot_mock_server(
            Ok(SnapshotResponse {
                term: 1,
                success: true,
                next_chunk: 1,
            }),
            shutdown_rx,
        )
        .await
        .unwrap();

        let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(
            1, stream, channel, config,
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::Error::Consensus(crate::ConsensusError::Snapshot(
                SnapshotError::InvalidFirstChunk
            ))
        ));
    }

    // Test: Transfer fails when stream is empty
    #[tokio::test]
    async fn test_push_transfer_fails_on_empty_stream() {
        let config = default_snapshot_config();

        // Empty stream
        let stream = stream::iter(vec![]).boxed();

        // Start mock server
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let (channel, _port) = MockNode::simulate_snapshot_mock_server(
            Ok(SnapshotResponse {
                term: 1,
                success: true,
                next_chunk: 1,
            }),
            shutdown_rx,
        )
        .await
        .unwrap();

        let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(
            1, stream, channel, config,
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::Error::Consensus(crate::ConsensusError::Snapshot(
                SnapshotError::EmptySnapshot
            ))
        ));
    }

    // Test: Transfer fails when gRPC call returns error
    #[tokio::test]
    async fn test_push_transfer_fails_on_grpc_error() {
        let config = default_snapshot_config();
        let stream = create_snapshot_stream(3, 1024);

        // Start mock server that returns gRPC error
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let (channel, _port) = MockNode::simulate_snapshot_mock_server(
            Err(Status::internal("Internal server error")),
            shutdown_rx,
        )
        .await
        .unwrap();

        let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(
            1,
            Box::pin(stream),
            channel,
            config,
        )
        .await;

        assert!(result.is_err());
        debug!(?result);
        assert!(matches!(
            result.unwrap_err(),
            Error::System(SystemError::Network(NetworkError::TonicStatusError(_)))
        ));
    }

    // Test: Transfer fails when background task fails to send chunk
    #[tokio::test]
    async fn test_push_transfer_fails_on_background_task_error() {
        let mut config = default_snapshot_config();
        config.push_queue_size = 1; // Small queue to cause backpressure
        config.snapshot_push_backoff_in_ms = 0;
        config.snapshot_push_max_retry = 0;

        // Create a stream that will overflow the queue
        let chunks: Vec<Result<SnapshotChunk>> = (0..10)
            .map(|i| Ok(create_snapshot_chunk(i, 1024 * 1024))) // 1MB chunks
            .collect();
        let stream = stream::iter(chunks).boxed();

        // Start mock server that processes slowly
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let (channel, _port) = MockNode::simulate_snapshot_mock_server(
            Ok(SnapshotResponse {
                term: 1,
                success: true,
                next_chunk: 1,
            }),
            shutdown_rx,
        )
        .await
        .unwrap();

        let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(
            1, stream, channel, config,
        )
        .await;

        debug!(?result);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::Error::Consensus(crate::ConsensusError::Snapshot(
                SnapshotError::Backpressure | SnapshotError::ReceiverDisconnected
            ))
        ));
    }

    // Test: Transfer fails when gRPC response indicates failure
    #[tokio::test]
    async fn test_push_transfer_fails_on_remote_rejection() {
        let mut config = default_snapshot_config();
        config.push_timeout_in_ms = 50;
        let stream = create_snapshot_stream(3, 1024);

        // Start mock server that returns failure response
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let (channel, _port) = MockNode::simulate_snapshot_mock_server(
            Ok(SnapshotResponse {
                term: 1,
                success: false, // rejection
                next_chunk: 1,
            }),
            shutdown_rx,
        )
        .await
        .unwrap();

        let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(
            1,
            Box::pin(stream),
            channel,
            config,
        )
        .await;

        debug!(?result);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::Error::Consensus(crate::ConsensusError::Snapshot(
                SnapshotError::RemoteRejection
            ))
        ));
    }
}

#[cfg(test)]
mod run_pull_transfer_test {
    use std::sync::Arc;

    use d_engine_proto::server::storage::SnapshotAck;
    use d_engine_proto::server::storage::snapshot_ack::ChunkStatus;
    use futures::stream;
    use tokio::sync::mpsc;

    use super::*;
    use crate::MockTypeConfig;
    use crate::SnapshotConfig;
    use crate::SnapshotError;
    use crate::create_test_chunk;

    /// Config tuned for pull tests: no rate limiting, fast retry, generous timeout.
    fn pull_config() -> SnapshotConfig {
        SnapshotConfig {
            max_bandwidth_mbps: 0,
            transfer_timeout_in_sec: 10,
            retry_interval_in_ms: 20,
            max_retries: 3,
            cache_size: 16,
            ..Default::default()
        }
    }

    /// Build a valid SnapshotChunk with the given seq / total.
    /// Seq 0 always carries metadata (required by the protocol).
    fn make_chunk(
        seq: u32,
        total: u32,
    ) -> SnapshotChunk {
        let data = vec![seq as u8; 64];
        let mut chunk = create_test_chunk(seq, &data, 1, 1, total);
        // Only the first chunk should carry metadata; clear it for subsequent chunks.
        if seq != 0 {
            chunk.metadata = None;
        }
        chunk
    }

    fn make_ack(
        seq: u32,
        status: ChunkStatus,
    ) -> SnapshotAck {
        SnapshotAck {
            seq,
            status: status as i32,
            next_requested: seq + 1,
        }
    }

    // Test: All chunks sent and ACKed in order — transfer completes successfully.
    //
    // The leader sends 2 chunks; the follower ACKs each as Accepted.
    // Expected: run_pull_transfer returns Ok(()).
    // Expected: chunks arrive at the receiver in seq order (0, 1).
    #[tokio::test]
    async fn test_pull_transfer_happy_path() {
        let config = pull_config();
        let (ack_tx, ack_rx) = mpsc::channel::<SnapshotAck>(32);
        let (chunk_tx, mut chunk_rx) = mpsc::channel::<Arc<SnapshotChunk>>(32);

        let data_stream = stream::iter(vec![Ok(make_chunk(0, 2)), Ok(make_chunk(1, 2))]).boxed();

        let transfer = tokio::spawn(async move {
            BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
                ack_rx,
                chunk_tx,
                data_stream,
                config,
            )
            .await
        });

        // Simulate follower: receive each chunk and ACK it as Accepted.
        for expected_seq in 0..2u32 {
            let chunk = chunk_rx.recv().await.expect("should receive chunk");
            assert_eq!(chunk.seq, expected_seq, "chunks must arrive in seq order");
            ack_tx
                .send(make_ack(expected_seq, ChunkStatus::Accepted))
                .await
                .expect("ack channel should be open");
        }

        let result = transfer.await.expect("task should not panic");
        assert!(result.is_ok(), "expected Ok(()), got: {result:?}");
    }

    // Test: Checksum mismatch causes the same chunk to be retransmitted.
    //       Transfer succeeds once the follower ACKs the retried chunk as Accepted.
    //
    // Setup: 1-chunk stream. Follower sends ChecksumMismatch on first receipt,
    //        then Accepted on the retry.
    // Expected: chunk_rx receives the same seq twice.
    // Expected: run_pull_transfer returns Ok(()).
    #[tokio::test]
    async fn test_pull_transfer_retry_on_checksum_mismatch() {
        let mut config = pull_config();
        config.max_retries = 1;
        config.retry_interval_in_ms = 10;

        let (ack_tx, ack_rx) = mpsc::channel::<SnapshotAck>(32);
        let (chunk_tx, mut chunk_rx) = mpsc::channel::<Arc<SnapshotChunk>>(32);

        let expected_seq = 0;
        let data_stream = stream::iter(vec![Ok(make_chunk(expected_seq, 1))]).boxed();

        let transfer = tokio::spawn(async move {
            BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
                ack_rx,
                chunk_tx,
                data_stream,
                config,
            )
            .await
        });

        // First delivery: follower rejects with ChecksumMismatch.
        let first = chunk_rx.recv().await.expect("should receive initial chunk");
        assert_eq!(first.seq, 0);
        ack_tx
            .send(make_ack(0, ChunkStatus::ChecksumMismatch))
            .await
            .expect("ack channel should be open");

        // After retry_interval_in_ms the same chunk must be retransmitted.
        // Follower ACKs as Accepted this time.
        let retried = chunk_rx.recv().await.expect("should receive retried chunk");
        assert_eq!(retried.seq, 0, "retried chunk must carry the same seq");
        ack_tx
            .send(make_ack(0, ChunkStatus::Accepted))
            .await
            .expect("ack channel should be open");

        let result = transfer.await.expect("task should not panic");
        assert!(result.is_ok(), "expected Ok(()), got: {result:?}");
    }

    // Test: Exceeding max_retries on a single chunk causes the transfer to fail.
    //
    // Setup: 1-chunk stream; follower always replies ChecksumMismatch.
    //        max_retries = 1, so the second mismatch pushes the count over the limit.
    // Expected: run_pull_transfer returns Err(TransferFailed).
    #[tokio::test]
    async fn test_pull_transfer_fails_on_max_retries_exceeded() {
        let mut config = pull_config();
        config.max_retries = 1;
        config.retry_interval_in_ms = 10;

        let (ack_tx, ack_rx) = mpsc::channel::<SnapshotAck>(32);
        let (chunk_tx, mut chunk_rx) = mpsc::channel::<Arc<SnapshotChunk>>(32);

        let data_stream = stream::iter(vec![Ok(make_chunk(0, 1))]).boxed();

        let transfer = tokio::spawn(async move {
            BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
                ack_rx,
                chunk_tx,
                data_stream,
                config,
            )
            .await
        });

        // Keep sending ChecksumMismatch until the transfer gives up.
        // We drain chunk_rx in a loop so the transfer is never blocked waiting to send.
        tokio::spawn(async move {
            while let Some(chunk) = chunk_rx.recv().await {
                let _ = ack_tx.send(make_ack(chunk.seq, ChunkStatus::ChecksumMismatch)).await;
            }
        });

        let result = transfer.await.expect("task should not panic");
        assert!(
            matches!(
                result,
                Err(crate::Error::Consensus(crate::ConsensusError::Snapshot(
                    SnapshotError::TransferFailed
                )))
            ),
            "expected TransferFailed, got: {result:?}"
        );
    }

    // Test: Data stream ending before all declared chunks are delivered returns an error.
    //
    // Setup: data_stream declares total_chunks=2 but only yields seq=0 then closes.
    //        The transfer sends seq=0, then tries to read seq=1 — stream returns None.
    //        The transfer fails immediately; no ACK is needed.
    // Expected: run_pull_transfer returns Err(IncompleteSnapshot).
    #[tokio::test]
    async fn test_pull_transfer_fails_on_incomplete_data_stream() {
        let config = pull_config();
        let (_ack_tx, ack_rx) = mpsc::channel::<SnapshotAck>(32);
        // Buffer large enough so the send of seq=0 does not block before the error fires.
        let (chunk_tx, _chunk_rx) = mpsc::channel::<Arc<SnapshotChunk>>(32);

        // Declare total_chunks=2 but only provide seq=0.
        let data_stream = stream::iter(vec![Ok(make_chunk(0, 2))]).boxed();

        let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
            ack_rx,
            chunk_tx,
            data_stream,
            config,
        )
        .await;

        assert!(
            matches!(
                result,
                Err(crate::Error::Consensus(crate::ConsensusError::Snapshot(
                    SnapshotError::IncompleteSnapshot
                )))
            ),
            "expected IncompleteSnapshot, got: {result:?}"
        );
    }

    // Test: First chunk missing metadata causes an immediate protocol error.
    //
    // Expected: run_pull_transfer returns Err(MissingMetadata) without reading
    //           any further chunks.
    #[tokio::test]
    async fn test_pull_transfer_fails_on_missing_metadata() {
        let config = pull_config();
        let (_ack_tx, ack_rx) = mpsc::channel::<SnapshotAck>(32);
        let (chunk_tx, _chunk_rx) = mpsc::channel::<Arc<SnapshotChunk>>(32);

        let mut bad_chunk = make_chunk(0, 1);
        bad_chunk.metadata = None; // violates protocol requirement

        let data_stream = stream::iter(vec![Ok(bad_chunk)]).boxed();

        let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
            ack_rx,
            chunk_tx,
            data_stream,
            config,
        )
        .await;

        assert!(
            matches!(
                result,
                Err(crate::Error::Consensus(crate::ConsensusError::Snapshot(
                    SnapshotError::MissingMetadata
                )))
            ),
            "expected MissingMetadata, got: {result:?}"
        );
    }

    // Test: ACK channel closes before all chunks are acknowledged — transfer must fail.
    //
    // This test guards the fix in the `None` arm of `ack_rx.recv()`.
    // Before the fix, `None => break` exited the loop and returned Ok(()) even
    // with unacknowledged chunks still in pending_acks.
    //
    // Setup: 2-chunk stream. Follower ACKs seq=0 (Accepted), receives seq=1,
    //        then drops the ACK sender without ACKing seq=1 (simulates disconnect).
    // Expected: run_pull_transfer returns Err(TransferFailed).
    // Regression: revert `None` arm to just `break` — this test must then FAIL
    //             because the transfer would incorrectly return Ok(()).
    #[tokio::test]
    async fn test_pull_transfer_fails_on_early_ack_channel_closure() {
        let config = pull_config();
        let (ack_tx, ack_rx) = mpsc::channel::<SnapshotAck>(32);
        let (chunk_tx, mut chunk_rx) = mpsc::channel::<Arc<SnapshotChunk>>(32);

        let data_stream = stream::iter(vec![Ok(make_chunk(0, 2)), Ok(make_chunk(1, 2))]).boxed();

        let transfer = tokio::spawn(async move {
            BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
                ack_rx,
                chunk_tx,
                data_stream,
                config,
            )
            .await
        });

        // Receive seq=0 and ACK it — pending_acks becomes {1} after seq=1 is sent.
        let chunk0 = chunk_rx.recv().await.expect("should receive chunk 0");
        assert_eq!(chunk0.seq, 0);
        ack_tx
            .send(make_ack(0, ChunkStatus::Accepted))
            .await
            .expect("ack channel should be open");

        // Receive seq=1 but do NOT ACK it — then close the ACK channel.
        // At this point pending_acks = {1}, so early closure must be an error.
        let chunk1 = chunk_rx.recv().await.expect("should receive chunk 1");
        assert_eq!(chunk1.seq, 1);
        drop(ack_tx);

        let result = transfer.await.expect("task should not panic");
        assert!(
            matches!(
                result,
                Err(crate::Error::Consensus(crate::ConsensusError::Snapshot(
                    SnapshotError::TransferFailed
                )))
            ),
            "expected TransferFailed when ack channel closes with pending chunks, got: {result:?}"
        );
    }

    // Test: Chunks arriving out of sequence cause an immediate protocol error.
    //
    // Setup: data_stream yields seq=0 then seq=2 (skipping seq=1).
    // Expected: run_pull_transfer returns Err(OutOfOrderChunk).
    #[tokio::test]
    async fn test_pull_transfer_fails_on_out_of_order_chunk() {
        let config = pull_config();
        let (_ack_tx, ack_rx) = mpsc::channel::<SnapshotAck>(32);
        let (chunk_tx, mut chunk_rx) = mpsc::channel::<Arc<SnapshotChunk>>(32);

        let data_stream = stream::iter(vec![Ok(make_chunk(0, 3)), Ok(make_chunk(2, 3))]).boxed();

        let transfer = tokio::spawn(async move {
            BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
                ack_rx,
                chunk_tx,
                data_stream,
                config,
            )
            .await
        });

        // Drain chunk_rx so the transfer is not blocked on sends.
        drop(chunk_rx.recv().await); // consume seq=0

        let result = transfer.await.expect("task should not panic");
        assert!(
            matches!(
                result,
                Err(crate::Error::Consensus(crate::ConsensusError::Snapshot(
                    SnapshotError::OutOfOrderChunk
                )))
            ),
            "expected OutOfOrderChunk, got: {result:?}"
        );
    }
}
