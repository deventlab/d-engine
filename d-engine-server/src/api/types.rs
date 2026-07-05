//! Server-layer native types for client write requests.
//!
//! [`WriteOperation`] is the working type used by [`EmbeddedClient`] and
//! [`StandaloneServer`] before encoding. The server converts it to proto
//! `WriteCommand` bytes, which core receives as opaque [`bytes::Bytes`] —
//! core is unaware of KV semantics.
//!
//! [`BatchOp`] is defined in `d-engine-core::command` alongside
//! `Command::Batch` and re-used here to keep the two in sync.
//!
//! # Type flow (write path)
//!
//! ```text
//! WriteOperation  →  WriteCommand (proto)  →  Bytes  →  core (opaque)
//! ```

use bytes::Bytes;
use d_engine_core::BatchOp;
use d_engine_proto::client::{
    WriteCommand,
    write_command::{
        Batch, BatchOp as ProtoBatchOp, CompareAndSwap, Delete, Insert, Operation, batch_op,
    },
};

/// Decoded write operation — the unit submitted by a client.
///
/// Mirrors proto `WriteCommand` in shape but carries no prost annotations.
/// Core owns the serialization to Raft log bytes (`WriteOperation → proto::WriteCommand → bytes`);
/// transport adapters work with this native type only.

#[derive(Debug, Clone, PartialEq)]
pub enum WriteOperation {
    Insert {
        key: Bytes,
        value: Bytes,
        /// `None` = no expiration. Proto encodes this as `ttl_secs = 0`.
        ttl_secs: Option<u64>,
    },
    Delete {
        key: Bytes,
    },
    CompareAndSwap {
        key: Bytes,
        /// `None` means the key must not exist for the swap to succeed.
        expected: Option<Bytes>,
        new_value: Bytes,
    },

    Batch {
        ops: Vec<BatchOp>,
    },
}

/// Serializes a native write operation to proto wire format for Raft log storage.
///
/// Symmetric counterpart to `TryFrom<WriteCommand> for Command` (decode direction).
/// Single coupling point on the write path — when a new `WriteOperation` variant is
/// added, the non-exhaustive `match` forces an update here at compile time.
///
/// Infallible: every `WriteOperation` variant has a direct proto equivalent;
/// the Rust type system guarantees no invalid state can reach this conversion.
// encode: native → proto (infallible)
impl From<WriteOperation> for WriteCommand {
    fn from(op: WriteOperation) -> Self {
        match op {
            WriteOperation::Insert {
                key,
                value,
                ttl_secs,
            } => WriteCommand {
                operation: Some(Operation::Insert(Insert {
                    key,
                    value,
                    ttl_secs: ttl_secs.unwrap_or(0),
                })),
            },
            WriteOperation::Delete { key } => WriteCommand {
                operation: Some(Operation::Delete(Delete { key })),
            },
            WriteOperation::CompareAndSwap {
                key,
                expected,
                new_value,
            } => WriteCommand {
                operation: Some(Operation::CompareAndSwap(CompareAndSwap {
                    key,
                    expected_value: expected,
                    new_value,
                })),
            },
            WriteOperation::Batch { ops } => {
                let proto_ops = ops
                    .into_iter()
                    .map(|op| match op {
                        BatchOp::Insert { key, value } => ProtoBatchOp {
                            op: Some(batch_op::Op::Insert(Insert {
                                key,
                                value,
                                ttl_secs: 0,
                            })),
                        },
                        BatchOp::Delete { key } => ProtoBatchOp {
                            op: Some(batch_op::Op::Delete(Delete { key })),
                        },
                    })
                    .collect();
                WriteCommand {
                    operation: Some(Operation::Batch(Batch { ops: proto_ops })),
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "types_test.rs"]
mod tests;
