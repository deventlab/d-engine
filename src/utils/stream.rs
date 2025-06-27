use std::marker::PhantomData;

use bytes::{BufMut, BytesMut};
use futures::TryStreamExt;
use http_body::Frame;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Code, Status, Streaming}; // Adjust path as needed

/// Generic gRPC stream decoder for any protobuf message
///
/// Implements Tonic's Decoder trait to handle:
/// - Protobuf deserialization
/// - Error conversion
/// - Buffer management
pub(crate) struct GrpcStreamDecoder<T> {
    _marker: PhantomData<T>,
}

impl<T> GrpcStreamDecoder<T> {
    pub(crate) fn new() -> Self {
        GrpcStreamDecoder { _marker: PhantomData }
    }
}

impl<T> tonic::codec::Decoder for GrpcStreamDecoder<T>
where
    T: prost::Message + Default + 'static,
{
    type Item = T;
    type Error = Status;
    fn decode(
        &mut self,
        buf: &mut tonic::codec::DecodeBuf<'_>,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        match T::decode(buf) {
            Ok(chunk) => Ok(Some(chunk)),
            Err(e) => Err(Status::new(Code::Internal, format!("Decode error: {e}"))),
        }
    }
    fn buffer_settings(&self) -> tonic::codec::BufferSettings {
        tonic::codec::BufferSettings::new(4 * 1024 * 1024, 4 * 1024 * 1025)
    }
}

/// Converts a receiver channel into a properly encoded tonic::Streaming<SnapshotChunk>
///
/// This handles:
/// 1. Proper gRPC frame encoding
/// 2. Error conversion
/// 3. Backpressure through bounded channel
/// 4. Efficient memory usage
pub(crate) fn create_production_snapshot_stream<T>(
    rx: mpsc::Receiver<Result<T, Status>>,
    max_message_size: usize,
) -> Streaming<T>
where
    T: Message + Default + 'static,
{
    // Create byte stream with proper gRPC framing
    let byte_stream = ReceiverStream::new(rx).map(|res| {
        match res {
            Ok(chunk) => {
                // Encode the T to bytes
                let mut buf = Vec::new();
                chunk
                    .encode(&mut buf)
                    .map_err(|e| Status::new(Code::Internal, format!("Snapshot encoding failed: {}", e)))?;

                // Create gRPC frame with header
                let mut frame = BytesMut::with_capacity(5 + buf.len());
                frame.put_u8(0); // No compression
                frame.put_u32(buf.len() as u32); // Message length
                frame.extend_from_slice(&buf);

                Ok(frame.freeze())
            }
            Err(e) => Err(e),
        }
    });

    // Create stream body with proper boxing
    let body = StreamBody::new(byte_stream.map_ok(Frame::data).map_err(|e: Status| e));

    // Create streaming with appropriate codec
    Streaming::new_request(
        GrpcStreamDecoder::<T> { _marker: PhantomData },
        body.boxed_unsync(),
        None,
        Some(max_message_size),
        // Some(1024 * 1024 * 1024), // 1GB max message size
    )
}
