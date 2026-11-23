use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use crc32fast::Hasher;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use http_body::Frame;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;
use tonic::Status;
use tracing::debug;

use super::stream::GrpcStreamDecoder;

pub fn create_test_snapshot_stream<T>(chunks: Vec<T>) -> tonic::Streaming<T>
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

#[allow(unused)]
pub(crate) fn create_test_snapshot_stream_from_receiver<T>(
    receiver: mpsc::Receiver<T>
) -> tonic::Streaming<T>
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
pub fn create_test_chunk(
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
        chunk_checksum: Bytes::from(compute_checksum(data)),
        metadata: Some(SnapshotMetadata {
            last_included: Some(LogId {
                index: 100,
                term: leader_term,
            }),
            checksum: Bytes::new(),
        }),
        data: Bytes::from(data.to_vec()),
    }
}

/// Helper to compute CRC32 checksum for test data
fn compute_checksum(data: &[u8]) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize().to_be_bytes().to_vec()
}
