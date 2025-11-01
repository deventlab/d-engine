use crate::test_utils::{
    create_config_entries, create_mixed_entries, generate_delete_commands,
    generate_insert_commands, node_config, snapshot_config,
};
use std::path::PathBuf;

#[test]
fn test_create_mixed_entries_structure() {
    let entries = create_mixed_entries();

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].index, 1);
    assert_eq!(entries[0].term, 1);
    assert_eq!(entries[1].index, 2);
    assert_eq!(entries[1].term, 1);
}

#[test]
fn test_create_mixed_entries_config_entry() {
    let entries = create_mixed_entries();
    let first_entry = &entries[0];

    assert!(first_entry.payload.is_some());
}

#[test]
fn test_create_mixed_entries_app_entry() {
    let entries = create_mixed_entries();
    let second_entry = &entries[1];

    assert!(second_entry.payload.is_some());
    assert_eq!(second_entry.index, 2);
}

#[test]
fn test_create_config_entries_count() {
    let entries = create_config_entries();

    assert_eq!(entries.len(), 1);
}

#[test]
fn test_create_config_entries_values() {
    let entries = create_config_entries();
    let entry = &entries[0];

    assert_eq!(entry.index, 1);
    assert_eq!(entry.term, 1);
    assert!(entry.payload.is_some());
}

#[test]
fn test_generate_insert_commands_single() {
    let commands = generate_insert_commands(vec![42]);

    assert!(!commands.is_empty());
}

#[test]
fn test_generate_insert_commands_multiple() {
    let ids = vec![1, 2, 3, 4, 5];
    let commands = generate_insert_commands(ids);

    assert!(!commands.is_empty());
}

#[test]
fn test_generate_insert_commands_empty() {
    let commands = generate_insert_commands(vec![]);

    assert!(commands.is_empty());
}

#[test]
fn test_generate_insert_commands_large_id() {
    let commands = generate_insert_commands(vec![u64::MAX]);

    assert!(!commands.is_empty());
}

#[test]
fn test_generate_insert_commands_zero_id() {
    let commands = generate_insert_commands(vec![0]);

    assert!(!commands.is_empty());
}

#[test]
fn test_generate_delete_commands_range() {
    let commands = generate_delete_commands(1..=5);

    assert!(!commands.is_empty());
}

#[test]
fn test_generate_delete_commands_single_element() {
    let commands = generate_delete_commands(1..=1);

    assert!(!commands.is_empty());
}

#[test]
fn test_generate_delete_commands_large_range() {
    let commands = generate_delete_commands(1..=100);

    assert!(!commands.is_empty());
}

#[test]
fn test_snapshot_config_defaults() {
    let dir = PathBuf::from("/tmp/snapshots");
    let config = snapshot_config(dir.clone());

    assert_eq!(config.max_log_entries_before_snapshot, 1);
    assert_eq!(config.cleanup_retain_count, 2);
    assert_eq!(config.chunk_size, 1024);
    assert!(config.enable);
    assert_eq!(config.snapshots_dir, dir);
}

#[test]
fn test_snapshot_config_paths() {
    let dir = PathBuf::from("/var/raft/snapshots");
    let config = snapshot_config(dir.clone());

    assert_eq!(config.snapshots_dir, dir);
    assert_eq!(config.snapshots_dir_prefix, "snapshot-");
}

#[test]
fn test_snapshot_config_timeout_values() {
    let dir = PathBuf::from("/tmp");
    let config = snapshot_config(dir);

    assert_eq!(config.transfer_timeout_in_sec, 1);
    assert_eq!(config.retry_interval_in_ms, 1);
    assert_eq!(config.push_timeout_in_ms, 100);
}

#[test]
fn test_snapshot_config_bandwidth() {
    let dir = PathBuf::from("/tmp");
    let config = snapshot_config(dir);

    assert_eq!(config.max_bandwidth_mbps, 1);
    assert_eq!(config.max_retries, 1);
}

#[test]
fn test_snapshot_config_queue_settings() {
    let dir = PathBuf::from("/tmp");
    let config = snapshot_config(dir);

    assert_eq!(config.push_queue_size, 1);
    assert_eq!(config.cache_size, 1);
}

#[test]
fn test_snapshot_config_yield_settings() {
    let dir = PathBuf::from("/tmp");
    let config = snapshot_config(dir);

    assert_eq!(config.sender_yield_every_n_chunks, 1);
    assert_eq!(config.receiver_yield_every_n_chunks, 1);
}

#[test]
fn test_node_config_initialization() {
    let config = node_config("/tmp/raft");

    assert_eq!(
        config.cluster.db_root_dir,
        std::path::PathBuf::from("/tmp/raft")
    );
}

#[test]
fn test_node_config_different_paths() {
    let path1 = "/data/raft1";
    let path2 = "/data/raft2";

    let config1 = node_config(path1);
    let config2 = node_config(path2);

    assert_eq!(config1.cluster.db_root_dir, std::path::PathBuf::from(path1));
    assert_eq!(config2.cluster.db_root_dir, std::path::PathBuf::from(path2));
    assert_ne!(config1.cluster.db_root_dir, config2.cluster.db_root_dir);
}

#[test]
fn test_node_config_absolute_path() {
    let path = "/absolute/path/to/db";
    let config = node_config(path);

    assert!(config.cluster.db_root_dir.is_absolute());
}

#[test]
fn test_node_config_relative_path() {
    let path = "./relative/db";
    let config = node_config(path);

    assert_eq!(config.cluster.db_root_dir, std::path::PathBuf::from(path));
}

#[test]
fn test_generate_insert_commands_consistency() {
    let ids = vec![10, 20, 30];
    let cmd1 = generate_insert_commands(ids.clone());
    let cmd2 = generate_insert_commands(ids);

    assert_eq!(cmd1.len(), cmd2.len());
}

#[test]
fn test_generate_delete_commands_consistency() {
    let cmd1 = generate_delete_commands(5..=10);
    let cmd2 = generate_delete_commands(5..=10);

    assert_eq!(cmd1.len(), cmd2.len());
}

#[test]
fn test_snapshot_config_immutable_after_creation() {
    let dir = PathBuf::from("/tmp");
    let config = snapshot_config(dir.clone());

    let max_entries = config.max_log_entries_before_snapshot;
    let chunk_sz = config.chunk_size;

    assert_eq!(max_entries, 1);
    assert_eq!(chunk_sz, 1024);
}

#[test]
fn test_create_mixed_entries_ordering() {
    let entries = create_mixed_entries();

    assert!(entries[0].index < entries[1].index);
    assert_eq!(entries[0].term, entries[1].term);
}
