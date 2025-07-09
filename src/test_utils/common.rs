use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use prost::Message;
use crate::alias::ROF;
use crate::convert::safe_kv;
use crate::proto::client::WriteCommand;
use crate::proto::common::membership_change::Change;
use crate::proto::common::AddNode;
use crate::proto::common::Entry;
use crate::proto::common::EntryPayload;
use crate::RaftLog;
use crate::RaftTypeConfig;
use crate::SnapshotConfig;

pub fn create_mixed_entries() -> Vec<Entry>{
    let config_entry = Entry {
        index: 1,
        term: 1,
        payload: Some(EntryPayload::config(Change::AddNode(AddNode{
            node_id: 7,
            address: "127.0.0.1:8080".into(),
        }))),
    };

    let app_entry = Entry {
        index: 2,
        term: 1,
        payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
    };

    vec![config_entry, app_entry]
}

pub fn create_config_entries() ->  Vec<Entry> {
    let entry = Entry {
        index: 1,
        term: 1,
        payload: Some(EntryPayload::config(Change::AddNode(AddNode{
            node_id: 8,
            address: "127.0.0.1:8080".into(),
        }))),
    };
    vec![entry]
}


pub(crate) fn generate_insert_commands(ids: Vec<u64>) -> Vec<u8> {
    let mut buffer = Vec::new();

    let mut commands = Vec::new();
    for id in ids {
        commands.push(WriteCommand::insert(safe_kv(id), safe_kv(id)));
    }

    for c in commands {
        buffer.append(&mut c.encode_to_vec());
    }

    buffer
}

pub(crate) fn generate_delete_commands(range: RangeInclusive<u64>) -> Vec<u8> {
    let mut buffer = Vec::new();

    let mut commands = Vec::new();
    for id in range {
        commands.push(WriteCommand::delete(safe_kv(id)));
    }

    for c in commands {
        buffer.append(&mut c.encode_to_vec());
    }

    buffer
}
///Dependes on external id to specify the local log entry index.
/// If duplicated ids are specified, then the only one entry will be inserted.
pub(crate) fn simulate_insert_command(
    raft_log: &Arc<ROF<RaftTypeConfig>>,
    ids: Vec<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in ids {
        let log = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![id]))),
        };
        entries.push(log);
    }
    if let Err(e) = raft_log.insert_batch(entries) {
        panic!("error: {e:?}");
    }
}

pub(crate) fn simulate_delete_command(
    raft_log: &Arc<ROF<RaftTypeConfig>>,
    id_range: RangeInclusive<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in id_range {
        let log = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            payload: Some(EntryPayload::command(generate_delete_commands(id..=id))),
        };
        entries.push(log);
    }
    if let Err(e) = raft_log.insert_batch(entries) {
        panic!("error: {e:?}");
    }
}

static LOGGER_INIT: once_cell::sync::Lazy<()> = once_cell::sync::Lazy::new(|| {
    env_logger::init();
});

pub fn enable_logger() {
    *LOGGER_INIT;
    println!("setup logger for unit test.");
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
    }
}
