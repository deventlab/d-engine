use crate::test_utils::EntryBuilder;
use d_engine_proto::common::{AddNode, RemoveNode, membership_change::Change};

#[test]
fn test_entry_builder_creation() {
    let builder = EntryBuilder::new(1, 1);
    let (_, entry) = builder.noop();

    assert_eq!(entry.index, 1);
    assert_eq!(entry.term, 1);
}

#[test]
fn test_entry_builder_command_single() {
    let builder = EntryBuilder::new(1, 1);
    let (_, entry) = builder.command(b"test_command");

    assert_eq!(entry.index, 1);
    assert_eq!(entry.term, 1);
    assert!(entry.payload.is_some());
}

#[test]
fn test_entry_builder_command_chain() {
    let builder = EntryBuilder::new(1, 1);
    let (builder, entry1) = builder.command(b"cmd1");
    let (_, entry2) = builder.command(b"cmd2");

    assert_eq!(entry1.index, 1);
    assert_eq!(entry2.index, 2);
}

#[test]
fn test_entry_builder_noop_single() {
    let builder = EntryBuilder::new(5, 3);
    let (_, entry) = builder.noop();

    assert_eq!(entry.index, 5);
    assert_eq!(entry.term, 3);
    assert!(entry.payload.is_some());
}

#[test]
fn test_entry_builder_config_add_node() {
    let builder = EntryBuilder::new(1, 1);
    let change = Change::AddNode(AddNode {
        node_id: 2,
        address: "127.0.0.1:8080".to_string(),
    });
    let (_, entry) = builder.config(change);

    assert_eq!(entry.index, 1);
    assert_eq!(entry.term, 1);
    assert!(entry.payload.is_some());
}

#[test]
fn test_entry_builder_config_remove_node() {
    let builder = EntryBuilder::new(10, 2);
    let change = Change::RemoveNode(RemoveNode { node_id: 3 });
    let (_, entry) = builder.config(change);

    assert_eq!(entry.index, 10);
    assert_eq!(entry.term, 2);
    assert!(entry.payload.is_some());
}

#[test]
fn test_entry_builder_mixed_sequence() {
    let builder = EntryBuilder::new(1, 1);
    let (builder, cmd_entry) = builder.command(b"test");
    let (builder, noop_entry) = builder.noop();
    let change = Change::AddNode(AddNode {
        node_id: 5,
        address: "192.168.1.1:9000".to_string(),
    });
    let (_, config_entry) = builder.config(change);

    assert_eq!(cmd_entry.index, 1);
    assert_eq!(noop_entry.index, 2);
    assert_eq!(config_entry.index, 3);

    assert_eq!(cmd_entry.term, 1);
    assert_eq!(noop_entry.term, 1);
    assert_eq!(config_entry.term, 1);
}

#[test]
fn test_entry_builder_index_increments() {
    let builder = EntryBuilder::new(100, 5);
    let (builder, entry1) = builder.noop();
    let (builder, entry2) = builder.noop();
    let (builder, entry3) = builder.noop();
    let (_, entry4) = builder.noop();

    assert_eq!(entry1.index, 100);
    assert_eq!(entry2.index, 101);
    assert_eq!(entry3.index, 102);
    assert_eq!(entry4.index, 103);
}

#[test]
fn test_entry_builder_term_constant() {
    let builder = EntryBuilder::new(1, 5);
    let (builder, entry1) = builder.noop();
    let (builder, entry2) = builder.command(b"data");
    let (_, entry3) = builder.noop();

    assert_eq!(entry1.term, 5);
    assert_eq!(entry2.term, 5);
    assert_eq!(entry3.term, 5);
}

#[test]
fn test_entry_builder_command_empty_data() {
    let builder = EntryBuilder::new(1, 1);
    let (_, entry) = builder.command(b"");

    assert!(entry.payload.is_some());
}

#[test]
fn test_entry_builder_command_large_data() {
    let large_data = vec![0u8; 10000];
    let builder = EntryBuilder::new(1, 1);
    let (_, entry) = builder.command(&large_data);

    assert!(entry.payload.is_some());
}

#[test]
fn test_entry_builder_high_index() {
    let builder = EntryBuilder::new(u64::MAX - 2, 1);
    let (builder, entry1) = builder.noop();
    let (_, entry2) = builder.noop();

    assert_eq!(entry1.index, u64::MAX - 2);
    assert_eq!(entry2.index, u64::MAX - 1);
}

#[test]
fn test_entry_builder_high_term() {
    let builder = EntryBuilder::new(1, u64::MAX);
    let (_, entry) = builder.noop();

    assert_eq!(entry.term, u64::MAX);
}

#[test]
fn test_entry_builder_zero_index() {
    let builder = EntryBuilder::new(0, 1);
    let (_, entry) = builder.noop();

    assert_eq!(entry.index, 0);
}

#[test]
fn test_entry_builder_zero_term() {
    let builder = EntryBuilder::new(1, 0);
    let (_, entry) = builder.noop();

    assert_eq!(entry.term, 0);
}

#[test]
fn test_entry_builder_command_binary_data() {
    let binary_data = vec![0xFF, 0xFE, 0xFD, 0xFC];
    let builder = EntryBuilder::new(1, 1);
    let (_, entry) = builder.command(&binary_data);

    assert!(entry.payload.is_some());
}

#[test]
fn test_entry_builder_multiple_commands() {
    let builder = EntryBuilder::new(1, 1);
    let (builder, e1) = builder.command(b"cmd1");
    let (builder, e2) = builder.command(b"cmd2");
    let (builder, e3) = builder.command(b"cmd3");
    let (_, e4) = builder.command(b"cmd4");

    assert_eq!(
        vec![e1.index, e2.index, e3.index, e4.index],
        vec![1, 2, 3, 4]
    );
}

#[test]
fn test_entry_builder_payload_types() {
    let builder = EntryBuilder::new(1, 1);
    let (builder, cmd_entry) = builder.command(b"data");
    let (builder, noop_entry) = builder.noop();
    let change = Change::AddNode(AddNode {
        node_id: 1,
        address: "127.0.0.1:5000".to_string(),
    });
    let (_, config_entry) = builder.config(change);

    assert!(cmd_entry.payload.is_some());
    assert!(noop_entry.payload.is_some());
    assert!(config_entry.payload.is_some());
}

#[test]
fn test_entry_builder_state_after_sequence() {
    let builder = EntryBuilder::new(10, 5);
    let (builder, _) = builder.noop();
    let (builder, _) = builder.command(b"test");
    let (_, last_entry) = builder.noop();

    assert_eq!(last_entry.index, 12);
    assert_eq!(last_entry.term, 5);
}
