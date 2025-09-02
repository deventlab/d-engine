use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use config::Config;
use d_engine::alias::SMOF;
use d_engine::alias::SOF;
use d_engine::config::BackoffPolicy;
use d_engine::config::ClusterConfig;
use d_engine::config::CommitHandlerConfig;
use d_engine::config::ElectionConfig;
use d_engine::config::FlushPolicy;
use d_engine::config::PersistenceConfig;
use d_engine::config::PersistenceStrategy;
use d_engine::config::RaftConfig;
use d_engine::config::RaftNodeConfig;
use d_engine::config::ReplicationConfig;
use d_engine::config::SnapshotConfig;
use d_engine::convert::safe_kv;
use d_engine::node::Node;
use d_engine::node::NodeBuilder;
use d_engine::node::RaftTypeConfig;
use d_engine::proto::client::WriteCommand;
use d_engine::proto::common::Entry;
use d_engine::proto::common::EntryPayload;
use d_engine::proto::election::VotedFor;
use d_engine::storage::FileStateMachine;
use d_engine::storage::StorageEngine;
use d_engine::ClientApiError;
use d_engine::FileStorageEngine;
use d_engine::HardState;
use d_engine::LogStore;
use d_engine::MetaStore;
use prost::Message;
use tokio::fs::remove_dir_all;
use tokio::fs::{self};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::sync::watch::Sender;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::debug;
use tracing::error;

use crate::client_manager::ClientManager;

pub const WAIT_FOR_NODE_READY_IN_SEC: u64 = 6;

// we are testing linearizable read from Leader directly, the latency should less than 1ms ideally
pub const LATENCY_IN_MS: u64 = 50;

// to make sure the result is consistent
pub const ITERATIONS: u64 = 10;

#[allow(dead_code)]
#[derive(Debug)]
pub enum ClientCommands {
    Put,
    Read,
    Lread,
    Delete,
}

pub struct TestContext {
    pub graceful_txs: Vec<Sender<()>>,
    pub node_handles: Vec<JoinHandle<Result<(), ClientApiError>>>,
}

impl TestContext {
    pub async fn shutdown(self) -> Result<(), ClientApiError> {
        for tx in self.graceful_txs {
            tx.send(()).map_err(|_| {
                ClientApiError::general_client_error("failed to shutdown".to_string())
            })?;
        }

        for handle in self.node_handles {
            handle.await??;
        }

        Ok(())
    }
}

// static LOGGER_INIT: once_cell::sync::Lazy<()> = once_cell::sync::Lazy::new(|| {
//     tracing_subscriber::registry()
//         .with(fmt::layer())
//         .with(EnvFilter::from_default_env())
//         .init();
// });

// pub fn enable_logger() {
//     *LOGGER_INIT;
//     println!("setup logger for unit test.");
// }

pub async fn create_node_config(
    node_id: u64,
    port: u16,
    cluster_ports: &[u16],
    db_root_dir: &str,
    log_dir: &str,
) -> String {
    let initial_cluster_entries = cluster_ports
        .iter()
        .enumerate()
        .map(|(i, &p)| {
            let id = i as u64 + 1;
            format!(
                "{{ id = {id}, name = 'n{id}', address = '127.0.0.1:{p}', role = 1, status = 2 }}"
            )
        })
        .collect::<Vec<_>>()
        .join(",\n            ");

    format!(
        r#"
        node_id = {node_id}
        listen_address = '127.0.0.1:{port}'
        initial_cluster = [
            {initial_cluster_entries}
        ]
        db_root_dir = '{db_root_dir}'
        log_dir = '{log_dir}'
        "#
    )
}

pub fn node_config(cluster_toml: &str) -> RaftNodeConfig {
    let mut config = RaftNodeConfig::default();

    let settings = Config::builder()
        .add_source(config::File::from_str(
            cluster_toml,
            config::FileFormat::Toml,
        ))
        .build()
        .unwrap();

    let cluster: ClusterConfig = settings.try_deserialize().unwrap();

    println!("Parsed cluster: {cluster:#?}",);

    let raft = RaftConfig {
        general_raft_timeout_duration_in_ms: 10000,
        snapshot_rpc_timeout_ms: 300_000,
        replication: ReplicationConfig {
            rpc_append_entries_in_batch_threshold: 1,
            ..Default::default()
        },
        commit_handler: CommitHandlerConfig {
            batch_size_threshold: 1,
            process_interval_ms: 100,
            ..Default::default()
        },
        snapshot: SnapshotConfig {
            max_log_entries_before_snapshot: 1,
            cleanup_retain_count: 2,
            retained_log_entries: 1,
            snapshots_dir: cluster.db_root_dir.join("snapshots").join(cluster.node_id.to_string()),
            ..Default::default()
        },
        persistence: PersistenceConfig {
            strategy: PersistenceStrategy::DiskFirst,
            flush_policy: FlushPolicy::Immediate,
            ..Default::default()
        },
        election: ElectionConfig {
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            ..Default::default()
        },
        ..Default::default()
    };

    let append_policy = BackoffPolicy {
        max_retries: 2,
        timeout_ms: 200,
        ..Default::default()
    };

    config.cluster = cluster;
    config.raft = raft;
    config.retry.append_entries = append_policy;

    config
}

#[allow(dead_code)]
pub async fn start_cluster(
    nodes_config: Vec<RaftNodeConfig>
) -> std::result::Result<(), ClientApiError> {
    // Start all nodes
    let mut controllers = vec![];
    for config in nodes_config {
        let (tx, handle) = start_node(config, None, None).await?;
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
    config: RaftNodeConfig,
    state_machine: Option<Arc<SMOF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>>,
    storage_engine: Option<Arc<SOF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>>,
) -> std::result::Result<
    (
        watch::Sender<()>,
        tokio::task::JoinHandle<std::result::Result<(), ClientApiError>>,
    ),
    ClientApiError,
> {
    let (graceful_tx, graceful_rx) = watch::channel(());

    let node = build_node(config, graceful_rx, state_machine, storage_engine).await?;

    let node_clone = node.clone();
    let node_id = node.node_config.cluster.node_id;
    let handle = tokio::spawn(async move { run_node(node_id, node_clone).await });

    Ok((graceful_tx, handle))
}

async fn build_node(
    config: RaftNodeConfig,
    graceful_rx: watch::Receiver<()>,
    state_machine: Option<Arc<SMOF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>>,
    storage_engine: Option<Arc<SOF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>>,
) -> std::result::Result<
    Arc<Node<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>,
    ClientApiError,
> {
    // Prepare raft log entries
    let mut builder = NodeBuilder::init(config.clone(), graceful_rx);

    // Use provided storage engine or create a new one with temp directory
    let storage_engine = if let Some(s) = storage_engine {
        s
    } else {
        let storage_path = config.cluster.db_root_dir.clone().join("storage_engine");
        Arc::new(
            FileStorageEngine::new(storage_path).expect("Failed to create file storage engine"),
        )
    };

    builder = builder.storage_engine(storage_engine);

    // Use provided state machine or create a new one with temp directory
    let state_machine = if let Some(sm) = state_machine {
        sm
    } else {
        let state_machine_path = config.cluster.db_root_dir.join("state_machine");
        Arc::new(
            FileStateMachine::new(state_machine_path, config.cluster.node_id)
                .await
                .expect("Failed to create file state machine"),
        )
    };

    builder = builder.state_machine(state_machine);

    // Build and start the node
    let node = builder
        .build()
        .start_rpc_server()
        .await
        .ready()
        .expect("Should succeed to start node");

    // Return both the node and the temp directory to keep it alive
    Ok(node)
}

async fn run_node(
    node_id: u32,
    node: Arc<Node<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>,
) -> std::result::Result<(), ClientApiError> {
    println!("Run node: {node_id}",);
    // Run the node until shutdown
    if let Err(e) = node.run().await {
        error!("Node error: {:?}", e);
    }

    debug!("Exiting program: {node_id}");
    drop(node);
    Ok(())
}

pub fn get_root_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn prepare_storage_engine(
    _node_id: u32,
    db_path: &str,
    _last_applied_index: u64,
) -> Arc<FileStorageEngine> {
    Arc::new(FileStorageEngine::new(PathBuf::from(format!("{db_path}/raft_log"))).unwrap())
}

pub async fn prepare_state_machine(
    node_id: u32,
    db_path: &str,
) -> FileStateMachine {
    let state_machine_db_path = format!("{db_path}/state_machine",);
    FileStateMachine::new(PathBuf::from(state_machine_db_path), node_id)
        .await
        .unwrap()
}

pub async fn manipulate_log(
    storage_engine: &Arc<SOF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>,
    log_ids: Vec<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in log_ids {
        println!("pre_allocate_raft_logs_next_index: {id}",);

        let log = Entry {
            index: id,
            term,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![id]))),
        };
        entries.push(log);
    }
    assert!(storage_engine.log_store().persist_entries(entries).await.is_ok());
    assert!(storage_engine.log_store().flush().is_ok());
}

pub fn init_hard_state(
    storage_engine: &Arc<impl StorageEngine>,
    current_term: u64,
    voted_for: Option<VotedFor>,
) {
    assert!(storage_engine
        .meta_store()
        .save_hard_state(&HardState {
            current_term,
            voted_for,
        })
        .is_ok());
}

pub async fn test_put_get(
    client_manager: &mut ClientManager,
    key: u64,
    value: u64,
) -> Result<(), ClientApiError> {
    println!("put {key} {value}");
    assert!(
        client_manager
            .execute_command(ClientCommands::Put, key, Some(value))
            .await
            .is_ok(),
        "Put command failed for key {key} value {value}"
    );
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
    client_manager.verify_read(key, value, ITERATIONS).await;
    Ok(())
}

/// Helper function to create bootstrap URLs
pub fn create_bootstrap_urls(ports: &[u16]) -> Vec<String> {
    ports.iter().map(|port| format!("http://127.0.0.1:{port}")).collect()
}

pub fn generate_insert_commands(ids: Vec<u64>) -> Vec<u8> {
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

pub async fn reset(case_name: &str) -> std::result::Result<(), ClientApiError> {
    let root_path = get_root_path();
    // Define path
    let logs_dir = format!("{}/logs/{case_name}", root_path.display(),);
    let db_dir = format!("{}/db/{case_name}", root_path.display(),);
    let snapshots_dir = format!("{}/snapshots/{case_name}", root_path.display(),);

    debug!(?logs_dir, ?db_dir, ?snapshots_dir, "reset path");

    // Make sure the parent directory exists
    fs::create_dir_all(&logs_dir).await?;
    fs::create_dir_all(&db_dir).await?;
    fs::create_dir_all(&snapshots_dir).await?;

    // Clean up the log directory (ignore errors that do not exist)
    let _ = remove_dir_all(Path::new(&logs_dir)).await;
    let _ = remove_dir_all(Path::new(&db_dir)).await;
    let _ = remove_dir_all(Path::new(&snapshots_dir)).await;

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
            let err_msg =
                format!("Node({peer_addr:?}) did not become ready within {timeout_secs} seconds.");
            Err(std::io::Error::new(std::io::ErrorKind::TimedOut, err_msg))
        }
    }
}

/// Checks whether the given snapshot path exists, is a directory, and contains any files or
/// subdirectories.
///
/// # Arguments
///
/// * `snapshot_path` - A string slice that holds the path to the snapshot directory.
///
/// # Returns
///
/// * `Ok(true)` if the path exists, is a directory, and contains at least one file or subdirectory.
/// * `Ok(false)` if the path does not exist, is not a directory, or is empty.
/// * `Err(ClientApiError)` if an error occurs while reading the directory contents.
pub fn check_path_contents(snapshot_path: &str) -> Result<bool, ClientApiError> {
    let path = Path::new(snapshot_path);

    // Check if path exists first
    if !path.exists() {
        println!("Path '{snapshot_path}' does not exist",);
        return Ok(false);
    }

    // Check if it's a directory
    if !path.is_dir() {
        println!("Path '{snapshot_path}' is not a directory");
        return Ok(false);
    }

    // Read directory contents
    let entries = std::fs::read_dir(path)?;
    let mut has_contents = false;

    for entry in entries {
        let entry = entry?;
        let entry_path = entry.path();

        if entry_path.is_dir() {
            println!("Found subdirectory: {}", entry_path.display());
            has_contents = true;
        } else if entry_path.is_file() {
            println!("Found file: {}", entry_path.display());
            has_contents = true;
        }
    }

    if !has_contents {
        println!("Path '{snapshot_path}' is empty (no files or subdirectories)",);
    }

    Ok(has_contents)
}

pub async fn get_available_ports(count: usize) -> Vec<u16> {
    use std::net::TcpListener;
    let mut ports = Vec::new();

    for _ in 0..count {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        ports.push(port);
        drop(listener);
    }
    ports
}
