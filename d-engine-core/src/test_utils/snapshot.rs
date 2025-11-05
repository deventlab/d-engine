use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use crc32fast::Hasher;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use http_body::Frame;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;
use tonic::Status;
use tracing::debug;

use crate::stream::GrpcStreamDecoder;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;

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

pub async fn create_test_compressed_snapshot() -> (Vec<u8>, SnapshotMetadata) {
    // Create a temporary directory for our test data
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path();

    // Create metadata
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 5, term: 1 }),
        checksum: Bytes::from(vec![1; 32]),
    };

    // Create test data file
    let data_file = temp_path.join("test_data.bin");
    tokio::fs::write(&data_file, b"test snapshot content").await.unwrap();

    // Create metadata file
    let metadata_bytes = bincode::serialize(&metadata).unwrap();
    tokio::fs::write(temp_path.join("metadata.bin"), &metadata_bytes).await.unwrap();

    // Create compressed file
    let compressed_path = temp_path.join("snapshot.tar.gz");
    let file = tokio::fs::File::create(&compressed_path).await.unwrap();
    let gzip_encoder = async_compression::tokio::write::GzipEncoder::new(file);
    let mut tar_builder = tokio_tar::Builder::new(gzip_encoder);

    // Add files to tar
    tar_builder.append_path_with_name(&data_file, "test_data.bin").await.unwrap();
    tar_builder
        .append_path_with_name(temp_path.join("metadata.bin"), "metadata.bin")
        .await
        .unwrap();

    // Finish compression
    tar_builder.finish().await.unwrap();
    let mut gzip_encoder = tar_builder.into_inner().await.unwrap();
    gzip_encoder.shutdown().await.unwrap();

    // Read compressed data back
    let compressed_data = tokio::fs::read(&compressed_path).await.unwrap();

    (compressed_data, metadata)
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
