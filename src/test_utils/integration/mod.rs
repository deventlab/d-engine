//! Integration test module for simulating RaftContext components with actual
//! underlying operations.
//!
//! This module provides real-world implementations of storage, network and
//! handler components rather than using mocking frameworks. Designed for
//! testing Raft consensus algorithm internals with the following
//! characteristics:
//!
//! - Uses **real storage backends** (SledRaftLog, SledStateStorage) with test isolation through
//!   temporary databases
//! - Implements **actual network transport** (GrpcTransport) for RPC communication
//! - Contains complete handler implementations (ElectionHandler, ReplicationHandler)
//! - Maintains real cluster membership state
//!
//! The [`TestContext`] struct encapsulates a complete testing environment
//! containing:
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

mod snapshot;
use std::path::PathBuf;
use std::sync::Arc;

#[allow(unused)]
pub(crate) use snapshot::*;
use tokio::sync::watch;

use super::generate_insert_commands;
use crate::alias::MOF;
use crate::alias::ROF;
use crate::alias::SMOF;
use crate::alias::TROF;
use crate::grpc::grpc_transport::GrpcTransport;
use crate::init_sled_storages;
use crate::proto::cluster::NodeMeta;
use crate::proto::common::Entry;
use crate::proto::common::EntryPayload;
use crate::proto::common::NodeStatus;
use crate::test_utils::enable_logger;
use crate::test_utils::MockTypeConfig;
use crate::BufferedRaftLog;
use crate::DefaultStateMachineHandler;
use crate::ElectionHandler;
use crate::FlushPolicy;
use crate::LogSizePolicy;
use crate::PersistenceConfig;
use crate::PersistenceStrategy;
use crate::RaftLog;
use crate::RaftMembership;
use crate::RaftNodeConfig;
use crate::RaftRole;
use crate::RaftTypeConfig;
use crate::ReplicationHandler;
use crate::SledStateMachine;
use crate::SledStorageEngine;
use crate::StateMachine;
use crate::TypeConfig;

#[allow(dead_code)]
pub struct TestContext<T>
where T: TypeConfig
{
    pub id: u32,

    // Storages
    pub raft_log: Arc<ROF<T>>,
    pub state_machine: Arc<SMOF<T>>,

    // Network
    pub transport: Arc<TROF<T>>,

    // Cluster Membership
    pub membership: Arc<MOF<T>>,

    // Handlers
    pub election_handler: ElectionHandler<T>,
    pub replication_handler: ReplicationHandler<T>,
    pub state_machine_handler: DefaultStateMachineHandler<T>,

    pub node_config: RaftNodeConfig,
    pub arc_node_config: Arc<RaftNodeConfig>,
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
    let (raft_log_db, state_machine_db, _state_storage_db, _snapshot_storage_db) = if restart {
        reuse_dbs(db_path)
    } else {
        reset_dbs(db_path)
    };
    let id = 1;
    // let raft_log_db = Arc::new(raft_log_db);
    let state_machine_db = Arc::new(state_machine_db);

    let storage_engine = Arc::new(
        SledStorageEngine::new(id, raft_log_db).expect("Init storage engine successfully."),
    );

    let (buffered_raft_log, receiver) = BufferedRaftLog::new(
        id,
        PersistenceConfig {
            strategy: PersistenceStrategy::DiskFirst,
            flush_policy: FlushPolicy::Immediate,
            max_buffered_entries: 10000,
        },
        storage_engine.clone(),
    );
    let buffered_raft_log = buffered_raft_log.start(receiver);
    let sled_state_machine = SledStateMachine::new(id, state_machine_db.clone()).expect("success");
    let last_applied_pair = sled_state_machine.last_applied();

    let grpc_transport = GrpcTransport::new(id);

    let peers_meta = if let Some(meta) = peers_meta_option {
        meta
    } else {
        let follower_role = RaftRole::<MockTypeConfig>::follower_role_i32();
        vec![
            NodeMeta {
                id: 2,
                address: "127.0.0.1:8080".to_string(),
                role: follower_role,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                address: "127.0.0.1:8080".to_string(),
                role: follower_role,
                status: NodeStatus::Active.into(),
            },
        ]
    };

    let (_graceful_tx, _graceful_rx) = watch::channel(());

    // Each unit test db path will be different
    let mut node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");
    node_config.cluster.db_root_dir = PathBuf::from(db_path);
    node_config.cluster.initial_cluster = peers_meta.clone();

    let snapshot_policy = LogSizePolicy::new(
        node_config.raft.snapshot.max_log_entries_before_snapshot,
        node_config.raft.snapshot.snapshot_cool_down_since_last_check,
    );

    let state_machine = Arc::new(sled_state_machine);
    let state_machine_handler = DefaultStateMachineHandler::new(
        id,
        last_applied_pair.index,
        node_config.raft.commit_handler.max_entries_per_chunk,
        state_machine.clone(),
        node_config.raft.snapshot.clone(),
        snapshot_policy,
    );

    let node_config_clone = node_config.clone();
    let arc_node_config = Arc::new(node_config);

    TestContext::<RaftTypeConfig> {
        id,
        raft_log: buffered_raft_log,
        state_machine,
        transport: Arc::new(grpc_transport),
        membership: Arc::new(RaftMembership::new(
            id,
            arc_node_config.cluster.initial_cluster.clone(),
            node_config_clone.clone(),
        )),
        election_handler: ElectionHandler::new(id),
        replication_handler: ReplicationHandler::new(id),
        node_config: node_config_clone,
        arc_node_config,
        state_machine_handler,
    }
}

///Returns
/// raft_log_db,
/// state_machine_db,
/// cluster_metadata_tree,
/// node_state_metadata_tree,
/// node_snapshot_metadata_tree,
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
pub fn reuse_dbs(db_path: &str) -> (sled::Db, sled::Db, sled::Db, sled::Db) {
    println!("[Test] reuse_dbs ...");
    init_sled_storages(db_path.to_string()).expect("init storage failed.")
}

pub(crate) async fn insert_raft_log(
    raft_log: &ROF<RaftTypeConfig>,
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
    if let Err(e) = raft_log.insert_batch(entries).await {
        panic!("error:{e:?}");
    }
}

pub(crate) fn insert_state_machine(
    state_machine: &SMOF<RaftTypeConfig>,
    ids: Vec<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in ids {
        let log = Entry {
            index: id,
            term,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![id]))),
        };
        entries.push(log);
    }
    if let Err(e) = state_machine.apply_chunk(entries) {
        panic!("error: {e:?}");
    }
}

pub(crate) fn node_config(db_path: &str) -> RaftNodeConfig {
    let mut s = RaftNodeConfig::new().expect("RaftNodeConfig should be inited successfully.");
    s.cluster.db_root_dir = PathBuf::from(db_path);
    s
}
