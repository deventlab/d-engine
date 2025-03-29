//! Integration test module for simulating RaftContext components with actual underlying operations.
//!
//! This module provides real-world implementations of storage, network and handler components
//! rather than using mocking frameworks. Designed for testing Raft consensus algorithm internals
//! with the following characteristics:
//!
//! - Uses **real storage backends** (SledRaftLog, SledStateStorage) with test isolation
//!   through temporary databases
//! - Implements **actual network transport** (GrpcTransport) for RPC communication
//! - Contains complete handler implementations (ElectionHandler, ReplicationHandler)
//! - Maintains real cluster membership state
//!
//! The [`TestContext`] struct encapsulates a complete testing environment containing:
//! - Storage components (raft log, state machine, snapshots)
//! - Network transport layer
//! - Cluster membership configuration
//! - Core Raft handlers
//!
//! The [`setup_raft_context`] function initializes a test environment with:
//! - Fresh database instances via [`reset_dbs`]
//! - Default or custom peer configurations
//! - Real gRPC transport binding
//! - Complete handler chains
//!
//! This differs from unit test mocks in that:
//! - All I/O operations use actual storage implementations
//! - Network communication uses real transport layers
//! - State changes persist across operations
//! - Full Raft state transitions are exercised
//!
//! Typical usage scenarios:
//! - Integration testing of Raft protocol implementation
//! - End-to-end simulation of node behavior
//! - Cluster formation and interaction tests
//! - Failure scenario testing with real component interactions

use crate::grpc::rpc_service::Entry;
use crate::utils::util::kv;
use crate::{
    alias::{MOF, ROF, SMOF, SSOF, TROF},
    grpc::{grpc_transport::GrpcTransport, rpc_service::NodeMeta},
    test_utils::{enable_logger, MockTypeConfig},
    DefaultStateMachineHandler, ElectionHandler, RaftContext, RaftMembership, RaftRole,
    RaftStateMachine, RaftTypeConfig, ReplicationHandler, Settings, SledRaftLog, SledStateStorage,
    StateMachine, TypeConfig,
};
use crate::{init_sled_storages, RaftLog, StateStorage};
use log::error;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use super::generate_insert_commands;

#[allow(dead_code)]
pub struct TestContext<T>
where
    T: TypeConfig,
{
    pub id: u32,

    // Storages
    pub raft_log: Arc<ROF<T>>,
    pub state_machine: Arc<SMOF<T>>,
    pub state_storage: Box<SSOF<T>>,

    // Network
    pub transport: Arc<TROF<T>>,

    // Cluster Membership
    pub membership: Arc<MOF<T>>,

    // Handlers
    pub election_handler: ElectionHandler<T>,
    pub replication_handler: ReplicationHandler<T>,
    pub state_machine_handler: DefaultStateMachineHandler<T>,

    pub settings: Settings,
    pub arc_settings: Arc<Settings>,
}

// If param `restart` is true, we will not reset dbs
pub fn setup_context(
    db_path: &str,
    peers_meta_option: Option<Vec<NodeMeta>>,
    restart: bool,
) -> RaftContext<RaftTypeConfig> {
    let components = setup_raft_components(db_path, peers_meta_option, restart);
    RaftContext::<RaftTypeConfig> {
        node_id: components.id,
        raft_log: components.raft_log,
        state_machine: components.state_machine,
        state_storage: components.state_storage,
        transport: components.transport,
        election_handler: components.election_handler,
        replication_handler: components.replication_handler,
        state_machine_handler: Arc::new(components.state_machine_handler),
        membership: components.membership,
        settings: components.arc_settings,
    }
}
// If param `restart` is true, we will not reset dbs
pub fn setup_raft_components(
    db_path: &str,
    peers_meta_option: Option<Vec<NodeMeta>>,
    restart: bool,
) -> TestContext<RaftTypeConfig> {
    println!("Test setup_raft_components ...");
    enable_logger();
    //start from fresh
    let (raft_log_db, state_machine_db, state_storage_db, _snapshot_storage_db) = if restart {
        reuse_dbs(db_path)
    } else {
        reset_dbs(db_path)
    };
    let id = 1;
    let raft_log_db = Arc::new(raft_log_db);
    let state_machine_db = Arc::new(state_machine_db);
    let state_storage_db = Arc::new(state_storage_db);

    let sled_raft_log = SledRaftLog::new(raft_log_db, None);
    let sled_state_machine = RaftStateMachine::new(id, state_machine_db.clone());
    let last_applied_index_option = sled_state_machine.last_entry_index();
    let sled_state_storage = SledStateStorage::new(state_storage_db);

    let grpc_transport = GrpcTransport { my_id: id };

    let address = "127.0.0.1:8080".to_string();

    let peers_meta;
    if peers_meta_option.is_none() {
        let follower_role = RaftRole::<MockTypeConfig>::follower_role_i32();
        peers_meta = vec![
            NodeMeta {
                id: 2,
                ip: "127.0.0.1".to_string(),
                port: 8080,
                role: follower_role,
            },
            NodeMeta {
                id: 3,
                ip: "127.0.0.1".to_string(),
                port: 8080,
                role: follower_role,
            },
        ];
    } else {
        peers_meta = peers_meta_option.expect("should succeed");
    }
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Each unit test db path will be different
    let mut settings = Settings::load(None).expect("Should succeed to init Settings.");
    settings.cluster.db_root_dir = PathBuf::from(db_path);
    settings.cluster.initial_cluster = peers_meta.clone();

    let (event_tx, event_rx) = mpsc::channel(1024);

    let state_machine = Arc::new(sled_state_machine);
    let state_machine_handler = DefaultStateMachineHandler::new(
        last_applied_index_option,
        settings.raft.commit_handler.max_entries_per_chunk,
        state_machine.clone(),
    );

    let settings_clone = settings.clone();
    let arc_settings = Arc::new(settings);

    TestContext::<RaftTypeConfig> {
        id,
        raft_log: Arc::new(sled_raft_log),
        state_machine,
        state_storage: Box::new(sled_state_storage),
        transport: Arc::new(grpc_transport),
        membership: Arc::new(RaftMembership::new(
            id,
            arc_settings.cluster.initial_cluster.clone(),
        )),
        election_handler: ElectionHandler::new(id, event_tx),
        replication_handler: ReplicationHandler::new(id),
        settings: settings_clone,
        arc_settings,
        state_machine_handler,
    }
}

///Returns
/// raft_log_db,
/// state_machine_db,
/// cluster_metadata_tree,
/// node_state_metadata_tree,
/// node_snapshot_metadata_tree,
///
pub fn reset_dbs(db_path: &str) -> (sled::Db, sled::Db, sled::Db, sled::Db) {
    println!("[Test] reset_dbs ...");
    let _ = std::fs::remove_dir_all(db_path);
    init_sled_storages(db_path.to_string()).expect("init storage failed.")
}
///Returns
/// raft_log_db,
/// state_machine_db,
/// cluster_metadata_tree,
/// node_state_metadata_tree,
/// node_snapshot_metadata_tree,
///
pub fn reuse_dbs(db_path: &str) -> (sled::Db, sled::Db, sled::Db, sled::Db) {
    println!("[Test] reuse_dbs ...");
    init_sled_storages(db_path.to_string()).expect("init storage failed.")
}

pub(crate) fn insert_raft_log(raft_log: &ROF<RaftTypeConfig>, ids: Vec<u64>, term: u64) {
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
        error!("error: {:?}", e);
        assert!(false);
    }
}
pub(crate) fn insert_state_storage(state_storage: &SSOF<RaftTypeConfig>, ids: Vec<u64>) {
    for i in ids {
        if let Err(e) = state_storage.insert(kv(i), kv(i)) {
            error!("error: {:?}", e);
            assert!(false);
        }
    }
}

pub(crate) fn insert_state_machine(state_machine: &SMOF<RaftTypeConfig>, ids: Vec<u64>, term: u64) {
    let mut entries = Vec::new();
    for id in ids {
        let log = Entry {
            index: id,
            term,
            command: generate_insert_commands(vec![id]),
        };
        entries.push(log);
    }
    if let Err(e) = state_machine.apply_chunk(entries) {
        error!("error: {:?}", e);
        assert!(false);
    }
}

pub(crate) fn settings(db_path: &str) -> Settings {
    let mut s = Settings::load(None).expect("Settings should be inited successfully.");
    s.cluster.db_root_dir = PathBuf::from(db_path);
    s
}
