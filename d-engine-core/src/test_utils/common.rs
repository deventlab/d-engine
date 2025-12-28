use crate::SnapshotConfig;
use crate::convert::safe_kv_bytes;
use bytes::Bytes;
use bytes::BytesMut;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::common::AddNode;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::membership_change::Change;
use prost::Message;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::time::Duration;

pub fn create_mixed_entries() -> Vec<Entry> {
    let config_entry = Entry {
        index: 1,
        term: 1,
        payload: Some(EntryPayload::config(Change::AddNode(AddNode {
            node_id: 7,
            address: "127.0.0.1:8080".into(),
            status: d_engine_proto::common::NodeStatus::Promotable as i32,
        }))),
    };

    let app_entry = Entry {
        index: 2,
        term: 1,
        payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
    };

    vec![config_entry, app_entry]
}

pub fn create_config_entries() -> Vec<Entry> {
    let entry = Entry {
        index: 1,
        term: 1,
        payload: Some(EntryPayload::config(Change::AddNode(AddNode {
            node_id: 8,
            address: "127.0.0.1:8080".into(),
            status: d_engine_proto::common::NodeStatus::Promotable as i32,
        }))),
    };
    vec![entry]
}

pub fn generate_insert_commands(ids: Vec<u64>) -> Bytes {
    let mut buffer = BytesMut::new();

    for id in ids {
        let cmd = WriteCommand::insert(safe_kv_bytes(id), safe_kv_bytes(id));
        cmd.encode(&mut buffer).expect("Failed to encode insert command");
    }

    buffer.freeze()
}

pub fn generate_delete_commands(range: RangeInclusive<u64>) -> Bytes {
    let mut buffer = BytesMut::new();

    for id in range {
        let cmd = WriteCommand::delete(safe_kv_bytes(id));
        cmd.encode(&mut buffer).expect("Failed to encode delete command");
    }

    buffer.freeze()
}

pub fn snapshot_config(snapshots_dir: PathBuf) -> SnapshotConfig {
    SnapshotConfig {
        max_log_entries_before_snapshot: 1,
        snapshot_cool_down_since_last_check: Duration::from_secs(0),
        cleanup_retain_count: 2,
        snapshots_dir,
        chunk_size: 1024,
        retained_log_entries: 1,
        sender_yield_every_n_chunks: 1,
        receiver_yield_every_n_chunks: 1,
        max_bandwidth_mbps: 1,
        push_queue_size: 1,
        cache_size: 1,
        max_retries: 1,
        transfer_timeout_in_sec: 1,
        retry_interval_in_ms: 1,
        snapshot_push_backoff_in_ms: 1,
        snapshot_push_max_retry: 1,
        push_timeout_in_ms: 100,
        enable: true,
        snapshots_dir_prefix: "snapshot-".to_string(),
    }
}
