use std::path::Path;

use crate::stream::GrpcStreamDecoder;
use crate::Result;
use bytes::BufMut;
use bytes::BytesMut;
use crc32fast::Hasher;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use futures::TryStreamExt;
use http_body::Frame;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
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

#[allow(unused)]
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

/// Creates a fake compressed snapshot file for testing
#[allow(unused)]
pub async fn create_fake_compressed_snapshot(
    path: &Path,
    content: &[u8],
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    use async_compression::tokio::write::GzipEncoder;
    use tokio::io::AsyncWriteExt;

    let file = File::create(path).await?;
    let mut encoder = GzipEncoder::new(file);
    encoder.write_all(content).await?;
    encoder.shutdown().await?;
    Ok(())
}

/// Creates a fake compressed snapshot with directory structure
#[allow(unused)]
pub async fn create_fake_dir_compressed_snapshot(
    path: &Path,
    files: &[(&str, &[u8])],
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    use async_compression::tokio::write::GzipEncoder;
    use tokio_tar::Builder;

    let file = File::create(path).await?;
    let gzip_encoder = GzipEncoder::new(file);
    let mut tar_builder = Builder::new(gzip_encoder);

    let temp_dir = tempfile::tempdir()?;
    for (file_name, content) in files {
        let file_path = temp_dir.path().join(file_name);
        tokio::fs::write(&file_path, content).await?;
        tar_builder.append_path(&file_path).await?;
    }

    tar_builder.finish().await?;
    let mut gzip_encoder = tar_builder.into_inner().await?;
    gzip_encoder.shutdown().await?;
    Ok(())
}
