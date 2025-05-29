use bytes::BufMut;
use bytes::BytesMut;
use crc32fast::Hasher;
use futures::stream;
use futures::TryStreamExt;
use http_body::Frame;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use prost::Message;
use tonic::Code;
use tonic::Status;
use tracing::debug;

use crate::proto::common::LogId;
use crate::proto::storage::SnapshotChunk;
use crate::proto::storage::SnapshotMetadata;

pub(crate) fn crate_test_snapshot_stream(chunks: Vec<SnapshotChunk>) -> tonic::Streaming<SnapshotChunk> {
    // Convert chunks to encoded byte streams
    let byte_stream = stream::iter(chunks.into_iter().map(|chunk| {
        let mut buf = Vec::new();

        chunk
            .encode(&mut buf)
            .map_err(|e| Status::new(Code::Internal, format!("Encoding failed: {}", e)))?;

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
            .map_err(|e: Status| Status::new(Code::Internal, format!("Stream error: {}", e))),
    );
    tonic::Streaming::new_request(
        SnapshotChunkDecoder,
        body.boxed_unsync(),
        None,
        Some(1024 * 1024 * 1024),
    )
}

/// Helper to create valid test chunk
pub(crate) fn create_test_chunk(
    seq: u32,
    data: &[u8],
    term: u64,
    leader_id: u64,
    total: u32,
) -> SnapshotChunk {
    SnapshotChunk {
        term,
        leader_id,
        seq,
        total,
        checksum: compute_checksum(data),
        metadata: Some(SnapshotMetadata {
            last_included: Some(LogId { index: 100, term }),
            checksum: vec![],
        }),
        data: data.to_vec(),
    }
}

// Create a custom Decoder implementation
pub(crate) struct SnapshotChunkDecoder;
impl tonic::codec::Decoder for SnapshotChunkDecoder {
    type Item = SnapshotChunk;
    type Error = Status;
    fn decode(
        &mut self,
        buf: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Self::Item>, Self::Error> {
        debug!(?buf, "SnapshotChunkDecoder");

        match SnapshotChunk::decode(buf) {
            Ok(chunk) => Ok(Some(chunk)),
            Err(e) => Err(Status::new(Code::Internal, format!("Decode error: {}", e))),
        }
    }
    fn buffer_settings(&self) -> tonic::codec::BufferSettings {
        tonic::codec::BufferSettings::new(4 * 1024 * 1024, 4 * 1024 * 1025)
    }
}

/// Helper to compute CRC32 checksum for test data
fn compute_checksum(data: &[u8]) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize().to_be_bytes().to_vec()
}
