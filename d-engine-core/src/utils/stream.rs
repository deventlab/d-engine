use std::marker::PhantomData;

use bytes::BufMut;
use tonic::Code;
use tonic::Status;

/// Generic gRPC stream decoder for any protobuf message
///
/// Implements Tonic's Decoder trait to handle:
/// - Protobuf deserialization
/// - Error conversion
/// - Buffer management
pub struct GrpcStreamDecoder<T> {
    _marker: PhantomData<T>,
}

// Helper functions for varint encoding (prost uses these internally, but they're not public)
pub fn encoded_len_varint(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

pub fn encode_varint(
    mut value: u64,
    buf: &mut impl BufMut,
) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

impl<T> Default for GrpcStreamDecoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GrpcStreamDecoder<T> {
    pub fn new() -> Self {
        GrpcStreamDecoder {
            _marker: PhantomData,
        }
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
