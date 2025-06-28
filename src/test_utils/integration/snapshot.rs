use std::pin::Pin;
use std::task::Poll;

use crate::stream::GrpcStreamDecoder;
use crate::Result;
use bytes::BufMut;
use bytes::BytesMut;
use crc32fast::Hasher;
use futures::stream;
use futures::stream::BoxStream;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use http_body::Frame;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;
use tonic::Status;
use tracing::debug;

use crate::proto::common::LogId;
use crate::proto::storage::SnapshotChunk;
use crate::proto::storage::SnapshotMetadata;
use crate::NetworkError;

/// Helper to create a valid snapshot stream
pub(crate) fn create_snapshot_stream(
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

    let stream = crate_test_snapshot_stream(chunks);
    Box::pin(stream.map(|item| item.map_err(|s| NetworkError::TonicStatusError(Box::new(s)).into())))
}

pub(crate) fn crate_test_snapshot_stream<T>(chunks: Vec<T>) -> tonic::Streaming<T>
where
    T: prost::Message + Default + 'static,
{
    // Convert chunks to encoded byte streams
    let byte_stream = stream::iter(chunks.into_iter().map(|chunk| {
        let mut buf = Vec::new();

        chunk
            .encode(&mut buf)
            .map_err(|e| Status::new(Code::Internal, format!("Encoding failed: {e}")))?;

        // Add Tonic frame header
        let mut frame = BytesMut::new();
        frame.put_u8(0); // No compression
        debug!("buf.len()={}", buf.len());

        frame.put_u32(buf.len() as u32); // Message length
        frame.extend_from_slice(&buf);

        Ok(frame.freeze())
    }));

    let body = StreamBody::new(
        byte_stream
            .map_ok(Frame::data)
            .map_err(|e: Status| Status::new(Code::Internal, format!("Stream error: {e}"))),
    );
    tonic::Streaming::new_request(
        GrpcStreamDecoder::<T>::new(),
        body.boxed_unsync(),
        None,
        Some(1024 * 1024 * 1024),
    )
}

pub(crate) fn crate_test_snapshot_stream_from_receiver<T>(receiver: mpsc::Receiver<T>) -> tonic::Streaming<T>
where
    T: prost::Message + Default + 'static,
{
    let byte_stream = ReceiverStream::new(receiver).map(|item| {
        let mut buf = Vec::new();
        item.encode(&mut buf)
            .map_err(|e| Status::new(Code::Internal, format!("Encoding failed: {e}")))?;

        let mut frame = BytesMut::new();
        frame.put_u8(0);
        frame.put_u32(buf.len() as u32);
        frame.extend_from_slice(&buf);
        Ok(frame.freeze())
    });

    let body = StreamBody::new(
        byte_stream
            .map_ok(Frame::data)
            .map_err(|e: Status| Status::new(Code::Internal, format!("Stream error: {e}"))),
    );

    tonic::Streaming::new_request(
        GrpcStreamDecoder::<T>::new(),
        body.boxed_unsync(),
        None,
        Some(1024 * 1024 * 1024),
    )
}

/// Helper to create valid test chunk
pub(crate) fn create_test_chunk(
    seq: u32,
    data: &[u8],
    leader_term: u64,
    leader_id: u32,
    total_chunks: u32,
) -> SnapshotChunk {
    SnapshotChunk {
        leader_term,
        leader_id,
        seq,
        total_chunks,
        chunk_checksum: compute_checksum(data),
        metadata: Some(SnapshotMetadata {
            last_included: Some(LogId {
                index: 100,
                term: leader_term,
            }),
            checksum: vec![],
        }),
        data: data.to_vec(),
    }
}

/// Helper to compute CRC32 checksum for test data
fn compute_checksum(data: &[u8]) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize().to_be_bytes().to_vec()
}

// Helper struct to convert ReceiverStream to tonic::Streaming
pub struct MockStreaming<T>(ReceiverStream<T>);

impl<T> MockStreaming<T> {
    pub fn new(stream: ReceiverStream<T>) -> Self {
        Self(stream)
    }
}

#[tonic::async_trait]
impl<T: Send + 'static> Stream for MockStreaming<T> {
    type Item = std::result::Result<T, tonic::Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.0).poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
