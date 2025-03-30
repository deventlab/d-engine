use crate::{
    alias::{ROF, SMOF},
    grpc::rpc_service::Entry,
    util::kv,
    RaftLog, RaftTypeConfig, StateMachine,
};
use sled::Batch;
use std::{ops::RangeInclusive, sync::Arc};

pub(crate) fn generate_insert_commands(ids: Vec<u64>) -> Vec<u8> {
    use prost::Message;

    use crate::grpc::rpc_service::ClientCommand;

    let mut buffer = Vec::new();

    let mut commands = Vec::new();
    for id in ids {
        commands.push(ClientCommand::insert(kv(id), kv(id)));
    }

    for c in commands {
        buffer.append(&mut c.encode_to_vec());
    }

    buffer
}

pub(crate) fn generate_delete_commands(range: RangeInclusive<u64>) -> Vec<u8> {
    use prost::Message;

    use crate::grpc::rpc_service::ClientCommand;

    let mut buffer = Vec::new();

    let mut commands = Vec::new();
    for id in range {
        commands.push(ClientCommand::delete(kv(id)));
    }

    for c in commands {
        buffer.append(&mut c.encode_to_vec());
    }

    buffer
}
///Dependes on external id to specify the local log entry index.
/// If duplicated ids are specified, then the only one entry will be inserted.
///
pub(crate) fn simulate_insert_proposal(
    raft_log: &Arc<ROF<RaftTypeConfig>>,
    ids: Vec<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in ids {
        let log = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            command: generate_insert_commands(vec![id]),
        };
        entries.push(log);
    }
    if let Err(e) = raft_log.insert_batch(entries) {
        eprint!("error: {:?}", e);
        assert!(false);
    }
}

pub(crate) fn simulate_delete_proposal(
    raft_log: &Arc<ROF<RaftTypeConfig>>,
    id_range: RangeInclusive<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in id_range {
        let log = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            command: generate_delete_commands(id..=id),
        };
        entries.push(log);
    }
    if let Err(e) = raft_log.insert_batch(entries) {
        eprint!("error: {:?}", e);
        assert!(false);
    }
}

///Dependes on external id to specify the local log entry index.
/// If duplicated ids are specified, then the only one entry will be inserted.
///
pub(crate) fn simulate_state_machine_insert_commands(
    state_machine: Arc<SMOF<RaftTypeConfig>>,
    id_range: RangeInclusive<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in id_range {
        let log = Entry {
            index: id,
            term,
            command: generate_delete_commands(id..=id),
        };
        entries.push(log);
    }
    state_machine.apply_chunk(entries).expect("should succeed");
}

pub(crate) fn prepare_locallog_entry_with_specify_ids_and_term(
    raft_log: &Arc<ROF<RaftTypeConfig>>,
    ids: Vec<u64>,
    term: u64,
) {
    use crate::{grpc::rpc_service::Entry, RaftLog};
    let mut entries = Vec::new();
    for index in ids {
        let log = Entry {
            index,
            term,
            command: generate_insert_commands(vec![index]),
        };
        entries.push(log);
    }
    if let Err(e) = raft_log.insert_batch(entries) {
        eprint!("error: {:?}", e);
        assert!(false);
    }
}

static LOGGER_INIT: once_cell::sync::Lazy<()> = once_cell::sync::Lazy::new(|| {
    env_logger::init();
});

pub fn enable_logger() {
    *LOGGER_INIT;
    println!("setup logger for unit test.");
}
