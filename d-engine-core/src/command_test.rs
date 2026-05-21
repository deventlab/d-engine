use bytes::Bytes;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::{CompareAndSwap, Delete, Insert, Operation};
use d_engine_proto::common::AddNode;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::membership_change::Change;
use prost::Message;

use crate::command::{ApplyEntry, Command, decode_entries};

// ── helpers ────────────────────────────────────────────────────────────────

fn noop_entry(
    index: u64,
    term: u64,
) -> Entry {
    Entry {
        index,
        term,
        payload: Some(EntryPayload::noop()),
    }
}

fn config_entry(
    index: u64,
    term: u64,
) -> Entry {
    Entry {
        index,
        term,
        payload: Some(EntryPayload::config(Change::AddNode(AddNode {
            node_id: 1,
            address: "127.0.0.1:4000".to_string(),
            status: 0,
        }))),
    }
}

fn encode_write_cmd(op: Operation) -> Bytes {
    let cmd = WriteCommand {
        operation: Some(op),
    };
    let mut buf = Vec::new();
    cmd.encode(&mut buf).unwrap();
    Bytes::from(buf)
}

fn insert_entry(
    index: u64,
    term: u64,
    key: &[u8],
    value: &[u8],
    ttl_secs: u64,
) -> Entry {
    Entry {
        index,
        term,
        payload: Some(EntryPayload::command(encode_write_cmd(Operation::Insert(
            Insert {
                key: Bytes::copy_from_slice(key),
                value: Bytes::copy_from_slice(value),
                ttl_secs,
            },
        )))),
    }
}

fn delete_entry(
    index: u64,
    term: u64,
    key: &[u8],
) -> Entry {
    Entry {
        index,
        term,
        payload: Some(EntryPayload::command(encode_write_cmd(Operation::Delete(
            Delete {
                key: Bytes::copy_from_slice(key),
            },
        )))),
    }
}

fn cas_entry(
    index: u64,
    term: u64,
    key: &[u8],
    expected: Option<&[u8]>,
    new_value: &[u8],
) -> Entry {
    Entry {
        index,
        term,
        payload: Some(EntryPayload::command(encode_write_cmd(
            Operation::CompareAndSwap(CompareAndSwap {
                key: Bytes::copy_from_slice(key),
                expected_value: expected.map(Bytes::copy_from_slice),
                new_value: Bytes::copy_from_slice(new_value),
            }),
        ))),
    }
}

// ── decode: single-variant tests ───────────────────────────────────────────

#[test]
fn test_decode_empty_batch_returns_empty_vec() {
    let result = decode_entries(vec![]).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_decode_noop_entry_produces_noop_command() {
    let entries = vec![noop_entry(1, 1)];
    let result = decode_entries(entries).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0],
        ApplyEntry {
            index: 1,
            term: 1,
            command: Command::Noop
        }
    );
}

#[test]
fn test_decode_config_entry_becomes_noop() {
    // Config entries must become Command::Noop, not be dropped.
    // Dropping them leaves sm.last_applied stuck, breaking ReadIndex drain.
    let entries = vec![config_entry(1, 1)];
    let result = decode_entries(entries).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].index, 1);
    assert!(matches!(result[0].command, Command::Noop));
}

#[test]
fn test_decode_insert_without_ttl() {
    let entries = vec![insert_entry(2, 1, b"k", b"v", 0)];
    let result = decode_entries(entries).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].command,
        Command::Insert {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            ttl_secs: None,
        }
    );
}

#[test]
fn test_decode_insert_with_ttl_converts_to_some() {
    let entries = vec![insert_entry(3, 1, b"key", b"val", 60)];
    let result = decode_entries(entries).unwrap();
    assert_eq!(
        result[0].command,
        Command::Insert {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"val"),
            ttl_secs: Some(60),
        }
    );
}

#[test]
fn test_decode_delete_entry() {
    let entries = vec![delete_entry(4, 2, b"gone")];
    let result = decode_entries(entries).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].command,
        Command::Delete {
            key: Bytes::from_static(b"gone")
        }
    );
}

#[test]
fn test_decode_cas_with_expected_value() {
    let entries = vec![cas_entry(5, 1, b"k", Some(b"old"), b"new")];
    let result = decode_entries(entries).unwrap();
    assert_eq!(
        result[0].command,
        Command::CompareAndSwap {
            key: Bytes::from_static(b"k"),
            expected: Some(Bytes::from_static(b"old")),
            value: Bytes::from_static(b"new"),
        }
    );
}

#[test]
fn test_decode_cas_with_no_expected_value_means_key_must_not_exist() {
    let entries = vec![cas_entry(6, 1, b"k", None, b"new")];
    let result = decode_entries(entries).unwrap();
    assert_eq!(
        result[0].command,
        Command::CompareAndSwap {
            key: Bytes::from_static(b"k"),
            expected: None,
            value: Bytes::from_static(b"new"),
        }
    );
}

// ── decode: index and term preservation ────────────────────────────────────

#[test]
fn test_decode_preserves_index_and_term() {
    let entries = vec![
        insert_entry(10, 3, b"a", b"1", 0),
        delete_entry(11, 3, b"b"),
    ];
    let result = decode_entries(entries).unwrap();
    assert_eq!(result[0].index, 10);
    assert_eq!(result[0].term, 3);
    assert_eq!(result[1].index, 11);
    assert_eq!(result[1].term, 3);
}

// ── decode: mixed batch ─────────────────────────────────────────────────────

#[test]
fn test_decode_mixed_batch_config_becomes_noop_keeps_order() {
    // index: 1=Noop, 2=Config(→Noop), 3=Insert, 4=Delete — all 4 entries preserved
    let entries = vec![
        noop_entry(1, 1),
        config_entry(2, 1),
        insert_entry(3, 1, b"x", b"y", 0),
        delete_entry(4, 1, b"x"),
    ];
    let result = decode_entries(entries).unwrap();
    assert_eq!(result.len(), 4); // Config becomes Noop, index continuity preserved
    assert_eq!(result[0].index, 1);
    assert!(matches!(result[0].command, Command::Noop));
    assert_eq!(result[1].index, 2);
    assert!(matches!(result[1].command, Command::Noop)); // config → noop
    assert_eq!(result[2].index, 3);
    assert!(matches!(result[2].command, Command::Insert { .. }));
    assert_eq!(result[3].index, 4);
    assert!(matches!(result[3].command, Command::Delete { .. }));
}

// ── decode: error cases ─────────────────────────────────────────────────────

#[test]
fn test_decode_invalid_command_bytes_returns_error() {
    let entry = Entry {
        index: 1,
        term: 1,
        payload: Some(EntryPayload::command(Bytes::from_static(
            b"\xff\xff\xff\xff",
        ))),
    };
    let result = decode_entries(vec![entry]);
    assert!(result.is_err(), "Corrupt command bytes must return Err");
}

#[test]
fn test_decode_write_command_with_no_operation_returns_error() {
    // WriteCommand with operation = None
    let empty_cmd = WriteCommand { operation: None };
    let mut buf = Vec::new();
    empty_cmd.encode(&mut buf).unwrap();
    let entry = Entry {
        index: 2,
        term: 1,
        payload: Some(EntryPayload::command(Bytes::from(buf))),
    };
    let result = decode_entries(vec![entry]);
    assert!(
        result.is_err(),
        "WriteCommand without operation must return Err"
    );
}
