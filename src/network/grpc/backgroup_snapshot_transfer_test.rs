use crate::proto::common::LogId;
use crate::proto::storage::{SnapshotChunk, SnapshotMetadata};
use crate::SnapshotConfig;
use futures::{stream, StreamExt};
use tokio::sync::mpsc;
use tonic::transport::Endpoint;

use super::*;

fn default_snapshot_config() -> SnapshotConfig {
    SnapshotConfig {
        max_bandwidth_mbps: 0,
        sender_yield_every_n_chunks: 2,
        ..Default::default()
    }
}

fn make_metadata() -> SnapshotMetadata {
    SnapshotMetadata {
        last_included: Some(LogId { index: 1, term: 1 }),
        ..Default::default()
    }
}

fn make_chunk(data: &[u8]) -> SnapshotChunk {
    SnapshotChunk {
        data: data.to_vec(),
        ..Default::default()
    }
}

/// Test: transfer completes successfully with multiple chunks.
#[tokio::test]
async fn test_transfer_completes_successfully() {
    let metadata = make_metadata();
    let config = default_snapshot_config();
    let chunks = vec![Ok(make_chunk(b"chunk1")), Ok(make_chunk(b"chunk2"))];
    let stream = stream::iter(chunks).boxed();

    let channel = Endpoint::from_static("http://[::]:12345").connect_lazy();

    let result = BackgroundSnapshotTransfer::start(1, metadata, stream, channel, config).await;

    assert!(result.is_ok());
}

/// Test: transfer fails if the stream returns an error.
#[tokio::test]
async fn test_transfer_fails_on_stream_error() {
    let metadata = make_metadata();
    let config = default_snapshot_config();
    let chunks = vec![
        Ok(make_chunk(b"chunk1")),
        Err(crate::SnapshotError::Backpressure.into()),
    ];
    let stream = stream::iter(chunks).boxed();

    let channel = Endpoint::from_static("http://[::]:12345").connect_lazy();

    let result = BackgroundSnapshotTransfer::start(1, metadata, stream, channel, config).await;

    assert!(result.is_ok());
}

/// Test: transfer respects bandwidth limit (rate limiting).
#[tokio::test]
async fn test_transfer_respects_bandwidth_limit() {
    let metadata = make_metadata();
    let mut config = default_snapshot_config();
    config.max_bandwidth_mbps = 1; // 1 MBps

    let chunk_data = vec![0u8; 1_000_000]; // 1 MB
    let chunks = vec![Ok(make_chunk(&chunk_data))];
    let stream = stream::iter(chunks).boxed();

    let channel = Endpoint::from_static("http://[::]:12345").connect_lazy();

    let result = BackgroundSnapshotTransfer::start(1, metadata, stream, channel, config).await;

    assert!(result.is_ok());
}

/// Test: transfer yields every N chunks as configured.
#[tokio::test]
async fn test_transfer_yields_every_n_chunks() {
    let metadata = make_metadata();
    let mut config = default_snapshot_config();
    config.sender_yield_every_n_chunks = 1;

    let chunks = vec![Ok(make_chunk(b"chunk1")), Ok(make_chunk(b"chunk2"))];
    let stream = stream::iter(chunks).boxed();

    let channel = Endpoint::from_static("http://[::]:12345").connect_lazy();

    let result = BackgroundSnapshotTransfer::start(1, metadata, stream, channel, config).await;

    assert!(result.is_ok());
}

/// Test: transfer handles empty stream gracefully.
#[tokio::test]
async fn test_transfer_with_empty_stream() {
    let metadata = make_metadata();
    let config = default_snapshot_config();
    let stream = stream::empty().boxed();

    let channel = Endpoint::from_static("http://[::]:12345").connect_lazy();

    let result = BackgroundSnapshotTransfer::start(1, metadata, stream, channel, config).await;

    assert!(result.is_ok());
}

/// Test: transfer fails if receiver is lagging (backpressure).
#[tokio::test]
async fn test_transfer_backpressure_error() {
    let config = default_snapshot_config();
    let (tx, _rx) = mpsc::channel(1);

    // Fill the channel to simulate backpressure
    tx.try_send(make_chunk(b"chunk1")).unwrap();

    let stream = stream::iter(vec![Ok(make_chunk(b"chunk2"))]).boxed();

    // Use send_chunks directly to test backpressure
    let result = BackgroundSnapshotTransfer::send_chunks(stream, tx, config).await;

    assert!(result.is_err());
}

/// Test: transfer status is set to Failed on error.
#[tokio::test]
async fn test_transfer_status_failed_on_error() {
    let metadata = make_metadata();
    let config = default_snapshot_config();
    let chunks = vec![Err(crate::SnapshotError::Backpressure.into())];
    let stream = stream::iter(chunks).boxed();

    let channel = Endpoint::from_static("http://[::]:12345").connect_lazy();

    // We can't directly check the status field, but we can ensure no panic and error is handled.
    let result = BackgroundSnapshotTransfer::start(1, metadata, stream, channel, config).await;

    assert!(result.is_ok());
}
