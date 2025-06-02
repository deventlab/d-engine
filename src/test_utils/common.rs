use crate::alias::ROF;
use crate::convert::safe_kv;
use crate::proto::client::WriteCommand;
use crate::proto::common::Entry;
use crate::proto::common::EntryPayload;
use crate::RaftLog;
use crate::RaftTypeConfig;
use prost::Message;
use std::ops::RangeInclusive;
use std::sync::Arc;

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
