use crate::common::{
    AddNode, EntryPayload, RemoveNode, entry_payload::Payload, membership_change::Change,
};
use bytes::Bytes;

#[test]
fn test_entry_payload_command_creation() {
    let command_data = Bytes::from("test_command");
    let payload = EntryPayload::command(command_data.clone());

    assert!(payload.is_command());
    assert!(!payload.is_noop());
    assert!(!payload.is_config());

    match payload.payload {
        Some(Payload::Command(cmd)) => {
            assert_eq!(cmd, command_data);
        }
        _ => panic!("Expected Command payload"),
    }
}

#[test]
fn test_entry_payload_noop_creation() {
    let payload = EntryPayload::noop();

    assert!(payload.is_noop());
    assert!(!payload.is_command());
    assert!(!payload.is_config());

    match payload.payload {
        Some(Payload::Noop(_)) => {
            // Success
        }
        _ => panic!("Expected Noop payload"),
    }
}

#[test]
fn test_entry_payload_config_with_add_node() {
    let add_node = AddNode {
        node_id: 1,
        address: "127.0.0.1:5000".to_string(),
    };
    let change = Change::AddNode(add_node.clone());
    let payload = EntryPayload::config(change);

    assert!(payload.is_config());
    assert!(!payload.is_command());
    assert!(!payload.is_noop());

    match payload.payload {
        Some(Payload::Config(config)) => match config.change {
            Some(Change::AddNode(node)) => {
                assert_eq!(node.node_id, 1);
                assert_eq!(node.address, "127.0.0.1:5000");
            }
            _ => panic!("Expected AddNode change"),
        },
        _ => panic!("Expected Config payload"),
    }
}

#[test]
fn test_entry_payload_config_with_remove_node() {
    let remove_node = RemoveNode { node_id: 2 };
    let change = Change::RemoveNode(remove_node);
    let payload = EntryPayload::config(change);

    assert!(payload.is_config());
    assert!(!payload.is_command());
    assert!(!payload.is_noop());

    match payload.payload {
        Some(Payload::Config(config)) => match config.change {
            Some(Change::RemoveNode(node)) => {
                assert_eq!(node.node_id, 2);
            }
            _ => panic!("Expected RemoveNode change"),
        },
        _ => panic!("Expected Config payload"),
    }
}

#[test]
fn test_entry_payload_empty_payload() {
    let payload = EntryPayload { payload: None };

    assert!(!payload.is_command());
    assert!(!payload.is_noop());
    assert!(!payload.is_config());
}

#[test]
fn test_entry_payload_command_with_empty_bytes() {
    let command_data = Bytes::new();
    let payload = EntryPayload::command(command_data);

    assert!(payload.is_command());
    assert!(!payload.is_noop());
    assert!(!payload.is_config());
}

#[test]
fn test_entry_payload_command_with_large_data() {
    let large_data = Bytes::from(vec![0u8; 10_000]);
    let payload = EntryPayload::command(large_data.clone());

    assert!(payload.is_command());
    match payload.payload {
        Some(Payload::Command(cmd)) => {
            assert_eq!(cmd.len(), 10_000);
        }
        _ => panic!("Expected Command payload"),
    }
}

#[test]
fn test_entry_payload_mutually_exclusive() {
    let cmd = EntryPayload::command(Bytes::from("data"));
    assert!(cmd.is_command());
    assert!(!cmd.is_noop());
    assert!(!cmd.is_config());

    let noop = EntryPayload::noop();
    assert!(!noop.is_command());
    assert!(noop.is_noop());
    assert!(!noop.is_config());

    let remove_node = RemoveNode { node_id: 1 };
    let change = Change::RemoveNode(remove_node);
    let cfg = EntryPayload::config(change);
    assert!(!cfg.is_command());
    assert!(!cfg.is_noop());
    assert!(cfg.is_config());
}

#[test]
fn test_entry_payload_command_string() {
    let command = "set key value";
    let payload = EntryPayload::command(Bytes::from(command));

    assert!(payload.is_command());
    match payload.payload {
        Some(Payload::Command(cmd)) => {
            assert_eq!(cmd.as_ref(), command.as_bytes());
        }
        _ => panic!("Expected Command payload"),
    }
}

#[test]
fn test_entry_payload_command_binary() {
    let binary_data = vec![0xFF, 0xEE, 0xDD, 0xCC, 0xBB];
    let payload = EntryPayload::command(Bytes::copy_from_slice(&binary_data));

    assert!(payload.is_command());
    match payload.payload {
        Some(Payload::Command(cmd)) => {
            assert_eq!(cmd.as_ref(), binary_data.as_slice());
        }
        _ => panic!("Expected Command payload"),
    }
}
