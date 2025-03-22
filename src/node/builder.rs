use super::{RaftTypeConfig, ServerSettings, Settings};
use crate::{
    alias::{COF, MOF, ROF, SMHOF, SMOF, SSOF, TROF},
    grpc::{self, grpc_transport::GrpcTransport},
    init_sled_storages, metrics, CommitHandler, DefaultCommitHandler, DefaultStateMachineHandler,
    ElectionHandler, Error, Node, Raft, RaftMembership, RaftStateMachine, ReplicationHandler,
    Result, SledRaftLog, SledStateStorage, StateMachine,
};
use log::{debug, error, info};
use std::sync::{atomic::AtomicBool, Arc};
use tokio::sync::{mpsc, watch, Mutex};

pub struct NodeBuilder {
    id: u32,
    raft_log: Option<ROF<RaftTypeConfig>>,
    membership: Option<MOF<RaftTypeConfig>>,
    state_machine: Option<Arc<SMOF<RaftTypeConfig>>>,
    state_storage: Option<SSOF<RaftTypeConfig>>,
    transport: Option<TROF<RaftTypeConfig>>,
    commit_handler: Option<COF<RaftTypeConfig>>,
    state_machine_handler: Option<Arc<SMHOF<RaftTypeConfig>>>,
    settings: Settings,
    shutdown_signal: watch::Receiver<()>,

    node: Option<Arc<Node<RaftTypeConfig>>>,
}

impl NodeBuilder {
    pub fn new(settings: Settings, shutdown_signal: watch::Receiver<()>) -> Self {
        let ServerSettings {
            id, db_root_dir, ..
        } = settings.server_settings.clone();

        let (raft_log_db, state_machine_db, state_storage_db, _snapshot_storage_db) =
            init_sled_storages(format!("{}/{}", db_root_dir.clone(), id))
                .expect("init storage failed.");

        let raft_log_db = Arc::new(raft_log_db);
        let state_machine_db = Arc::new(state_machine_db);
        let state_storage_db = Arc::new(state_storage_db);

        let sled_state_machine = RaftStateMachine::new(id, state_machine_db.clone());
        let last_applied_index = sled_state_machine.last_entry_index();
        let sled_raft_log = SledRaftLog::new(raft_log_db, last_applied_index);
        let sled_state_storage = SledStateStorage::new(state_storage_db);

        let grpc_transport = GrpcTransport { my_id: id };

        let state_machine = Arc::new(sled_state_machine);
        let state_machine_handler = Arc::new(DefaultStateMachineHandler::new(
            last_applied_index,
            settings.commit_handler_settings.max_entries_per_chunk,
            state_machine.clone(),
        ));

        Self {
            id,
            raft_log: Some(sled_raft_log),
            membership: None,
            state_machine: Some(state_machine),
            state_storage: Some(sled_state_storage),
            transport: Some(grpc_transport),
            settings,
            shutdown_signal,
            commit_handler: None,
            state_machine_handler: Some(state_machine_handler),
            node: None,
        }
    }

    pub fn raft_log(mut self, raft_log: ROF<RaftTypeConfig>) -> Self {
        self.raft_log = Some(raft_log);
        self
    }

    pub fn state_machine(mut self, state_machine: SMOF<RaftTypeConfig>) -> Self {
        self.state_machine = Some(Arc::new(state_machine));
        self
    }

    pub fn state_storage(mut self, state_storage: SSOF<RaftTypeConfig>) -> Self {
        self.state_storage = Some(state_storage);
        self
    }

    pub fn transport(mut self, transport: TROF<RaftTypeConfig>) -> Self {
        self.transport = Some(transport);
        self
    }

    pub fn commit_handler(mut self, commit_handler: COF<RaftTypeConfig>) -> Self {
        self.commit_handler = Some(commit_handler);
        self
    }

    pub fn settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    pub fn build(mut self) -> Self {
        let id = self.id;
        let settings = self.settings.clone();

        // Init CommitHandler
        let (new_commit_event_tx, new_commit_event_rx) = mpsc::unbounded_channel::<u64>();

        let state_machine = self.state_machine.take().unwrap();
        let raft_log = self.raft_log.take().unwrap();
        let state_machine_handler = self.state_machine_handler.take().unwrap();
        let (role_tx, role_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel(1024);

        let settings_arc = Arc::new(settings);
        let shutdown_signal = self.shutdown_signal.clone();
        let mut raft_core = Raft::<RaftTypeConfig>::new(
            id,
            raft_log,
            state_machine.clone(),
            self.state_storage.take().unwrap(),
            self.transport.take().unwrap(),
            ElectionHandler::new(id, event_tx.clone()),
            ReplicationHandler::new(id),
            state_machine_handler.clone(),
            Arc::new(RaftMembership::new(
                id,
                settings_arc.server_settings.initial_cluster.clone(),
                event_tx.clone(),
                settings_arc.clone(),
            )),
            settings_arc.clone(),
            role_tx,
            role_rx,
            event_tx,
            event_rx,
            shutdown_signal.clone(),
        );

        // Register commit event listener
        raft_core.register_new_commit_listener(new_commit_event_tx);

        // Start CommitHandler in a single thread
        let mut commit_handler = DefaultCommitHandler::<RaftTypeConfig>::new(
            state_machine_handler,
            raft_core.ctx.raft_log.clone(),
            new_commit_event_rx,
            settings_arc.commit_handler_settings.batch_size_threshold,
            settings_arc
                .commit_handler_settings
                .commit_handle_interval_in_ms,
            shutdown_signal,
        );
        tokio::spawn(async move {
            match commit_handler.run().await {
                Ok(_) => {
                    info!("commit_handler exit program");
                }
                Err(Error::Exit) => {
                    info!("commit_handler exit program");
                    println!("commit_handler exit program");
                }
                Err(e) => {
                    error!("commit_handler exit program with error: {:?}", e);
                    println!("commit_handler exit program");
                }
            }
        });

        let event_tx = raft_core.event_tx.clone();
        let node = Node::<RaftTypeConfig> {
            id,
            raft_core: Arc::new(Mutex::new(raft_core)),
            event_tx: event_tx.clone(),
            ready: AtomicBool::new(false),
            settings: settings_arc,
        };

        self.node = Some(Arc::new(node));
        self
    }

    pub fn start_metrics_server(self, shutdown_signal: watch::Receiver<()>) -> Self {
        println!("start metric server!");
        let port = self.settings.server_settings.prometheus_metrics_port;
        tokio::spawn(async move {
            metrics::start_server(port, shutdown_signal).await;
        });
        self
    }

    pub async fn start_rpc_server(self) -> Self {
        debug!("1. --- start RPC server --- ");
        if let Some(ref node) = self.node {
            let node_clone = node.clone();
            let shutdown = self.shutdown_signal.clone();
            let listen_address = self.settings.server_settings.listen_address.clone();
            let rpc_connection_settings = self.settings.rpc_connection_settings.clone();
            tokio::spawn(async move {
                if let Err(e) = grpc::start_rpc_server(
                    node_clone,
                    listen_address,
                    rpc_connection_settings,
                    shutdown,
                )
                .await
                {
                    eprintln!("RPC server stops. {:?}", e);
                    error!("RPC server stops. {:?}", e);
                }
            });
            self
        } else {
            error!("failed to start RPC server");
            panic!("failed to start RPC server");
        }
    }

    pub fn ready(self) -> Result<Arc<Node<RaftTypeConfig>>> {
        self.node.ok_or_else(|| Error::ServerFailedToStartError)
    }
}
