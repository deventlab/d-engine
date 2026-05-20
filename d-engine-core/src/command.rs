/// # Why this module exists
///
/// `EntryPayload.command` is stored as raw `bytes` in the Raft log — intentionally.
/// The replication path (Leader → Followers → WAL) must be zero-copy: Followers store
/// and forward bytes without ever decoding them.  Only the *apply* step needs to know
/// what the command means.
///
/// Before this module, every `StateMachine` implementor called
/// `WriteCommand::decode(&data[..])` themselves, duplicating the same proto decode
/// logic and forcing a `d-engine-proto` dependency on anyone writing a custom SM.
///
/// This module moves the decode to one place — `DefaultStateMachineHandler` — and
/// exposes a clean Rust-native type (`Command`) at the `StateMachine::apply_chunk`
/// boundary.  `WriteCommand` (proto, wire format) and `Command` (Rust enum, execution
/// interface) are intentionally two separate types — anti-corruption layer — so that
/// proto schema changes never break the SM trait.
use bytes::Bytes;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::common::Entry;
use d_engine_proto::common::entry_payload::Payload;
use prost::Message;

use crate::Error;
use crate::StorageError;

/// Decoded KV operation — the unit of work delivered to a `StateMachine`.
///
/// Replaces raw proto `Entry` at the `StateMachine::apply_chunk` boundary.
/// The framework decodes wire bytes exactly once (in `DefaultStateMachineHandler`)
/// before dispatching; implementors never touch prost or proto types.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// Raft-internal no-op written by a newly elected leader.
    /// SM must return `ApplyResult::success(entry.index)` and otherwise ignore it.
    /// Cannot be filtered before reaching SM: omitting it would create gaps in
    /// `last_applied`, breaking the index-tracking invariant in `StateMachineWorker`.
    Noop,

    Insert {
        key: Bytes,
        value: Bytes,
        /// `None` = no expiration.  Proto encodes this as `ttl_secs = 0`; we convert
        /// to `Option` here so SM implementors use idiomatic Rust instead of magic zeros.
        ttl_secs: Option<u64>,
    },

    Delete {
        key: Bytes,
    },

    CompareAndSwap {
        key: Bytes,
        /// `None` means the key must not exist for the swap to succeed.
        expected: Option<Bytes>,
        value: Bytes,
    },
}

/// A decoded Raft log entry ready for state machine application.
///
/// Pairs `index` / `term` with the decoded `Command` so that state machines can
/// produce `ApplyResult { index, .. }` without re-reading the original `Entry`.
#[derive(Debug, Clone, PartialEq)]
pub struct ApplyEntry {
    pub index: u64,
    pub term: u64,
    pub command: Command,
}

/// Single coupling point between the proto schema and the SM trait.
///
/// When a new operation variant is added to `WriteCommand`, the compiler forces
/// an update here (non-exhaustive `match`), preventing silent omissions downstream.
impl TryFrom<WriteCommand> for Command {
    type Error = Error;

    fn try_from(wc: WriteCommand) -> Result<Self, Error> {
        match wc.operation {
            Some(Operation::Insert(i)) => Ok(Command::Insert {
                key: i.key,
                value: i.value,
                ttl_secs: if i.ttl_secs == 0 {
                    None
                } else {
                    Some(i.ttl_secs)
                },
            }),
            Some(Operation::Delete(d)) => Ok(Command::Delete { key: d.key }),
            Some(Operation::CompareAndSwap(c)) => Ok(Command::CompareAndSwap {
                key: c.key,
                expected: c.expected_value,
                value: c.new_value,
            }),
            None => {
                Err(StorageError::StateMachineError("WriteCommand has no operation".into()).into())
            }
        }
    }
}

/// Decode a batch of raw Raft `Entry` values into `ApplyEntry` values.
///
/// - `Config` entries become `Command::Noop` — membership is handled by the Raft layer, but
///   the index must still reach SM so `last_applied` stays contiguous; dropping them would
///   leave `last_applied` stuck below the config index, breaking ReadIndex drain.
/// - `Noop` entries become `Command::Noop`.
/// - `Command` bytes are decoded via `TryFrom<WriteCommand>`.
/// - Any decode failure returns `Err` immediately.
pub fn decode_entries(entries: Vec<Entry>) -> Result<Vec<ApplyEntry>, Error> {
    let mut result = Vec::with_capacity(entries.len());

    for entry in entries {
        let index = entry.index;
        let term = entry.term;

        let payload = entry.payload.and_then(|p| p.payload).ok_or_else(|| {
            StorageError::StateMachineError(format!("Entry at index {index} has no payload"))
        })?;

        match payload {
            Payload::Noop(_) => {
                result.push(ApplyEntry {
                    index,
                    term,
                    command: Command::Noop,
                });
            }
            Payload::Config(_) => {
                // Membership changes are handled by the membership layer.
                // Convert to Noop so sm.last_applied advances through this index —
                // dropping the entry would leave last_applied stuck, breaking ReadIndex.
                result.push(ApplyEntry {
                    index,
                    term,
                    command: Command::Noop,
                });
            }
            Payload::Command(data) => {
                let wc = WriteCommand::decode(&data[..]).map_err(|e| {
                    StorageError::SerializationError(format!(
                        "Failed to decode WriteCommand at index {index}: {e}"
                    ))
                })?;
                result.push(ApplyEntry {
                    index,
                    term,
                    command: Command::try_from(wc)?,
                });
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use d_engine_proto::common::EntryPayload;
    use d_engine_proto::common::MembershipChange;
    use d_engine_proto::common::Noop;
    use d_engine_proto::common::entry_payload::Payload;

    use super::*;

    fn noop_entry(index: u64) -> Entry {
        Entry {
            index,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Noop(Noop {})),
            }),
        }
    }

    fn config_entry(index: u64) -> Entry {
        Entry {
            index,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Config(MembershipChange { change: None })),
            }),
        }
    }

    fn insert_entry(
        index: u64,
        key: &str,
        value: &str,
    ) -> Entry {
        use d_engine_proto::client::WriteCommand;
        use d_engine_proto::client::write_command::{Insert, Operation};
        use prost::Message;

        let wc = WriteCommand {
            operation: Some(Operation::Insert(Insert {
                key: Bytes::from(key.to_owned()),
                value: Bytes::from(value.to_owned()),
                ttl_secs: 0,
            })),
        };
        let mut buf = Vec::new();
        wc.encode(&mut buf).unwrap();
        Entry {
            index,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Command(Bytes::from(buf))),
            }),
        }
    }

    /// Config entries must produce Command::Noop, not be dropped.
    /// Dropping them leaves sm.last_applied stuck, breaking ReadIndex drain.
    #[test]
    fn config_entry_becomes_noop() {
        let entries = vec![config_entry(5)];
        let result = decode_entries(entries).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].index, 5);
        assert_eq!(result[0].command, Command::Noop);
    }

    /// last_applied continuity: a mixed batch of Insert→Config→Insert must yield
    /// three ApplyEntry values with no index gaps.
    #[test]
    fn config_entry_preserves_index_continuity() {
        let entries = vec![
            insert_entry(10, "k1", "v1"),
            config_entry(11), // membership change
            insert_entry(12, "k2", "v2"),
        ];
        let result = decode_entries(entries).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].index, 10);
        assert_eq!(result[1].index, 11);
        assert!(matches!(result[1].command, Command::Noop));
        assert_eq!(result[2].index, 12);
    }

    /// A chunk that is entirely Config entries must still produce one Noop per entry,
    /// so the SM can advance last_applied to the highest config index.
    #[test]
    fn all_config_chunk_produces_noops() {
        let entries = vec![config_entry(3), config_entry(4), config_entry(5)];
        let result = decode_entries(entries).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.iter().all(|e| e.command == Command::Noop));
        assert_eq!(result.last().unwrap().index, 5);
    }

    #[test]
    fn noop_entry_produces_noop() {
        let result = decode_entries(vec![noop_entry(1)]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].command, Command::Noop);
    }
}
