use std::time::Duration;

use bytes::Bytes;
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
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotResponse;

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
    use crate::{MockNode, SystemError};

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

// ... rest of the file remains unchanged ...
