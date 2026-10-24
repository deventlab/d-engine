//! Adapts d-engine's prost `Entry` to raft-engine's `protobuf::Message` bound.
//!
//! raft-engine serializes log entries through `write_to_vec` and parses them back
//! through `parse_from_bytes`. Delegating both to prost stores d-engine's own
//! `Entry` bytes on disk, with no wrapper message and no second encoding.

use std::any::Any;
use std::fmt;
use std::io::{self, Read};
use std::sync::OnceLock;

use d_engine::common::Entry;
use prost::Message as _;
use protobuf::reflect::MessageDescriptor;
use protobuf::{
    Clear, CodedInputStream, CodedOutputStream, Message, ProtobufError, ProtobufResult,
    UnknownFields,
};

#[derive(Clone, PartialEq, Default)]
pub struct EntryRecord {
    pub entry: Entry,
    unknown: UnknownFields,
}

impl From<Entry> for EntryRecord {
    fn from(entry: Entry) -> Self {
        Self {
            entry,
            unknown: UnknownFields::default(),
        }
    }
}

impl fmt::Debug for EntryRecord {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        self.entry.fmt(f)
    }
}

impl Clear for EntryRecord {
    fn clear(&mut self) {
        self.entry = Entry::default();
    }
}

impl Message for EntryRecord {
    // Reflection is never used on the raft-engine read/write path.
    fn descriptor(&self) -> &'static MessageDescriptor {
        unreachable!("reflection is not used on the log path")
    }

    fn descriptor_static() -> &'static MessageDescriptor {
        unreachable!("reflection is not used on the log path")
    }

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(
        &mut self,
        is: &mut CodedInputStream,
    ) -> ProtobufResult<()> {
        let mut buf = Vec::new();
        is.read_to_end(&mut buf)?;
        self.entry = Entry::decode(buf.as_slice())
            .map_err(|e| ProtobufError::IoError(io::Error::new(io::ErrorKind::InvalidData, e)))?;
        Ok(())
    }

    fn write_to_with_cached_sizes(
        &self,
        os: &mut CodedOutputStream,
    ) -> ProtobufResult<()> {
        os.write_raw_bytes(&self.entry.encode_to_vec())
    }

    fn compute_size(&self) -> u32 {
        self.entry.encoded_len() as u32
    }

    fn get_cached_size(&self) -> u32 {
        self.entry.encoded_len() as u32
    }

    fn get_unknown_fields(&self) -> &UnknownFields {
        &self.unknown
    }

    fn mut_unknown_fields(&mut self) -> &mut UnknownFields {
        &mut self.unknown
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn new() -> Self {
        Self::default()
    }

    fn default_instance() -> &'static Self {
        static DEFAULT: OnceLock<EntryRecord> = OnceLock::new();
        DEFAULT.get_or_init(EntryRecord::default)
    }
}
