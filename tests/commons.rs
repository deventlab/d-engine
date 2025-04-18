use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use d_engine::alias::ROF;
use d_engine::alias::SMOF;
use d_engine::alias::SSOF;
use d_engine::config::RaftNodeConfig;
use d_engine::convert::kv;
use d_engine::node::Node;
use d_engine::node::NodeBuilder;
use d_engine::node::RaftTypeConfig;
use d_engine::proto::ClientCommand;
use d_engine::proto::Entry;
use d_engine::proto::VotedFor;
use d_engine::storage::RaftLog;
use d_engine::storage::RaftStateMachine;
use d_engine::storage::SledRaftLog;
use d_engine::storage::SledStateStorage;
use d_engine::storage::StateStorage;
use d_engine::HardState;
use d_engine::Result;
use prost::Message;
use tokio::fs::remove_dir_all;
use tokio::fs::{self};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time;
use tracing::debug;
use tracing::error;

pub const WAIT_FOR_NODE_READY_IN_SEC: u64 = 6;

// we are testing linearizable read from Leader directly, the latency should less than 1ms ideally
pub const LATENCY_IN_MS: u64 = 10;

// to make sure the result is consistent
pub const ITERATIONS: u64 = 10;

#[derive(Debug)]
pub enum ClientCommands {
    PUT,
    READ,
    LREAD,
    DELETE,
}

pub async fn start_cluster(nodes_config_paths: Vec<&str>) -> Result<()> {
    // Start all nodes
    let mut controllers = vec![];
    for config_path in nodes_config_paths {
        let (tx, handle) = start_node(config_path, None, None, None).await?;
        controllers.push((tx, handle));
    }

    // Perform test operations...

    // Shut down all nodes
    for (tx, handle) in controllers {
        tx.send(()).expect("Should succeed to send shutdown");
        handle.await??;
    }

    Ok(())
}
pub async fn start_node(
    config_path: &str,
    state_machine: Option<Arc<SMOF<RaftTypeConfig>>>,
    raft_log: Option<ROF<RaftTypeConfig>>,
    state_storage: Option<SSOF<RaftTypeConfig>>,
) -> Result<(watch::Sender<()>, tokio::task::JoinHandle<Result<()>>)> {
    let (graceful_tx, graceful_rx) = watch::channel(());

    let root_path = get_root_path();
    let config_path = format!("{}", root_path.join(config_path).display());
    let node = build_node(&config_path, graceful_rx, state_machine, raft_log, state_storage).await?;

    let node_clone = node.clone();
    let node_id = node.settings.cluster.node_id;
    let handle = tokio::spawn(async move { run_node(node_id, node_clone).await });

    Ok((graceful_tx, handle))
}

async fn build_node(
    config_path: &str,
    graceful_rx: watch::Receiver<()>,
    state_machine: Option<Arc<SMOF<RaftTypeConfig>>>,
    raft_log: Option<ROF<RaftTypeConfig>>,
    state_storage: Option<SSOF<RaftTypeConfig>>,
) -> Result<Arc<Node<RaftTypeConfig>>> {
    // Load configuration from the specified path
    let config = RaftNodeConfig::default();
    config
        .with_override_config(config_path)
        .expect("Overwrite config successfully.");

    // Prepare raft log entries
    let mut builder = NodeBuilder::new(Some(config_path), graceful_rx);
    if let Some(r) = raft_log {
        // let sled_raft_log = prepare_raft_log(&config_path, last_applied_index);
        // manipulate_log(&sled_raft_log, ids, term);
        builder = builder.raft_log(r);
    }
    if let Some(sm) = state_machine {
        builder = builder.state_machine(sm);
    }
    if let Some(ss) = state_storage {
        builder = builder.state_storage(ss);
    }
    // Build and start the node
    let node = builder
        .build()
        .start_rpc_server()
        .await
        .ready()
        .expect("Should succeed to start node");

    Ok(node)
}

async fn run_node(
    node_id: u32,
    node: Arc<Node<RaftTypeConfig>>,
) -> Result<()> {
    // Run the node until shutdown
    if let Err(e) = node.run().await {
        error!("Node error: {:?}", e);
    }

    debug!("Exiting program: {:?}", node_id);
    drop(node);
    Ok(())
}

pub fn get_root_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn prepare_raft_log(
    db_path: &str,
    last_applied_index: Option<u64>,
) -> SledRaftLog {
    let raft_log_db_path = format!("{}/raft_log", db_path);
    let raft_log_db = sled::Config::default()
        .path(raft_log_db_path)
        .use_compression(true)
        .compression_factor(1)
        .open()
        .unwrap();
    SledRaftLog::new(Arc::new(raft_log_db), last_applied_index)
}
pub fn prepare_state_machine(
    node_id: u32,
    db_path: &str,
) -> RaftStateMachine {
    let state_machine_db_path = format!("{}/state_machine", db_path);
    let state_machine_db = sled::Config::default()
        .path(state_machine_db_path)
        .use_compression(true)
        .compression_factor(1)
        .open()
        .unwrap();
    RaftStateMachine::new(node_id, Arc::new(state_machine_db))
}
pub fn prepare_state_storage(db_path: &str) -> SledStateStorage {
    let state_storage_db_path = format!("{}/state_storage", db_path);
    let state_storage_db = sled::Config::default()
        .path(state_storage_db_path)
        .use_compression(true)
        .compression_factor(1)
        .open()
        .unwrap();
    SledStateStorage::new(Arc::new(state_storage_db))
}

pub fn manipulate_log(
    raft_log: &dyn RaftLog,
    log_ids: Vec<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in log_ids {
        let log = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            command: generate_insert_commands(vec![id]),
        };
        entries.push(log);
    }
    if let Err(e) = raft_log.insert_batch(entries) {
        eprintln!("manipulate_log error: {:?}", e);
        assert!(false);
    }
}
pub fn init_state_storage(
    state_storage: &dyn StateStorage,
    current_term: u64,
    voted_for: Option<VotedFor>,
) {
    if let Err(e) = state_storage.save_hard_state(HardState {
        current_term,
        voted_for,
    }) {
        eprintln!("init_state_storage error: {:?}", e);
        assert!(false);
    }
}

pub fn generate_insert_commands(ids: Vec<u64>) -> Vec<u8> {
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

pub async fn reset(case_name: &str) -> Result<()> {
    let root_path = get_root_path();
    // Define path
    let logs_dir = format!("{}/logs/{}", root_path.display(), case_name);
    let db_dir = format!("{}/db/{}", root_path.display(), case_name);

    // Make sure the parent directory exists
    fs::create_dir_all(&logs_dir).await?;
    fs::create_dir_all(&db_dir).await?;

    // Clean up the log directory (ignore errors that do not exist)
    let _ = remove_dir_all(Path::new(&logs_dir)).await;

    // Clean up the database directory (ignore errors that do not exist)
    let _ = remove_dir_all(Path::new(&db_dir)).await;

    Ok(())
}

pub async fn check_cluster_is_ready(
    peer_addr: &str,
    timeout_secs: u64,
) -> std::result::Result<(), std::io::Error> {
    let timeout_duration = Duration::from_secs(timeout_secs);
    let retry_interval = Duration::from_millis(500);

    let result = time::timeout(timeout_duration, async {
        loop {
            if TcpStream::connect(peer_addr).await.is_ok() {
                println!("Node is ready!");
                return Ok::<(), std::io::Error>(());
            } else {
                error!("Node({:?}) not ready, retrying...", peer_addr);
                time::sleep(retry_interval).await;
            }
        }
    })
    .await;

    match result {
        Ok(_) => Ok(()),
        Err(_) => {
            let err_msg = format!(
                "Node({:?}) did not become ready within {} seconds.",
                peer_addr, timeout_secs
            );
            Err(std::io::Error::new(std::io::ErrorKind::TimedOut, err_msg))
        }
    }
}
