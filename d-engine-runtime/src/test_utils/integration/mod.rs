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

use crate::BufferedRaftLog;
use crate::FileStateMachine;
use crate::FileStorageEngine;
use crate::RaftMembership;
use crate::RaftTypeConfig;
use crate::grpc::grpc_transport::GrpcTransport;
use crate::test_utils::mock_state_machine;
use d_engine_core::DefaultStateMachineHandler;
use d_engine_core::ElectionHandler;
use d_engine_core::FlushPolicy;
use d_engine_core::LogSizePolicy;
use d_engine_core::MockStateMachine;
use d_engine_core::PersistenceConfig;
use d_engine_core::PersistenceStrategy;
use d_engine_core::RaftLog;
use d_engine_core::RaftNodeConfig;
use d_engine_core::ReplicationHandler;
use d_engine_core::StateMachine;
use d_engine_core::StorageEngine;
use d_engine_core::TypeConfig;
use d_engine_core::alias::MOF;
use d_engine_core::alias::ROF;
use d_engine_core::alias::SMOF;
use d_engine_core::alias::TROF;
use d_engine_core::generate_delete_commands;
use d_engine_core::generate_insert_commands;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::NodeMeta;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;

#[allow(dead_code)]
pub struct TestContext<T>
where
    T: TypeConfig,
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
) -> TestContext<RaftTypeConfig<FileStorageEngine, MockStateMachine>> {
    println!("Test setup_raft_components ...");

    unsafe {
        std::env::remove_var("CONFIG_PATH");
        std::env::remove_var("RAFT__INITIAL_CLUSTER");
    }
    //start from fresh
    let id = 1;

    // let tempdir = tempfile::tempdir().unwrap();
    let path = PathBuf::from(db_path);
    let storage_engine = Arc::new(FileStorageEngine::new(path).unwrap());
    if restart {
        storage_engine.log_store().reset_sync().unwrap();
    }

    let (buffered_raft_log, receiver) = BufferedRaftLog::new(
        id,
        PersistenceConfig {
            strategy: PersistenceStrategy::DiskFirst,
            flush_policy: FlushPolicy::Immediate,
            max_buffered_entries: 10000,
            ..Default::default()
        },
        storage_engine.clone(),
    );
    let buffered_raft_log = buffered_raft_log.start(receiver);
    let mock_state_machine = mock_state_machine();
    let last_applied_pair = mock_state_machine.last_applied();

    let grpc_transport = GrpcTransport::new(id);

    let peers_meta = if let Some(meta) = peers_meta_option {
        meta
    } else {
        let follower_role = Follower.into();
        vec![
            NodeMeta {
                id: 1,
                address: "127.0.0.1:8080".to_string(),
                role: follower_role,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 2,
                address: "127.0.0.1:8081".to_string(),
                role: follower_role,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                address: "127.0.0.1:8082".to_string(),
                role: follower_role,
                status: NodeStatus::Active.into(),
            },
        ]
    };

    let (_graceful_tx, _graceful_rx) = watch::channel(());

    // Each unit test db path will be different
    let mut node_config = RaftNodeConfig::default();
    node_config.cluster.db_root_dir = PathBuf::from(db_path);
    node_config.cluster.initial_cluster = peers_meta.clone();

    let snapshot_policy = LogSizePolicy::new(
        node_config.raft.snapshot.max_log_entries_before_snapshot,
        node_config.raft.snapshot.snapshot_cool_down_since_last_check,
    );

    let state_machine = Arc::new(mock_state_machine);
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

    TestContext::<RaftTypeConfig<FileStorageEngine, MockStateMachine>> {
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

pub async fn insert_raft_log(
    raft_log: &Arc<ROF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>,
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

pub async fn insert_state_machine(
    state_machine: &SMOF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>,
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
    if let Err(e) = state_machine.apply_chunk(entries).await {
        panic!("error: {e:?}");
    }
}

///Dependes on external id to specify the local log entry index.
/// If duplicated ids are specified, then the only one entry will be inserted.
pub async fn simulate_insert_command(
    raft_log: &Arc<ROF<RaftTypeConfig<FileStorageEngine, MockStateMachine>>>,
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
    raft_log.insert_batch(entries).await.unwrap();
    raft_log.flush().await.unwrap();
}

#[allow(dead_code)]
pub async fn simulate_delete_command(
    raft_log: &Arc<ROF<RaftTypeConfig<FileStorageEngine, MockStateMachine>>>,
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
    if let Err(e) = raft_log.insert_batch(entries).await {
        panic!("error: {e:?}");
    }
}
