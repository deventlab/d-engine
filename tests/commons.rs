use dengine::{
    utils::util::{self, kv, vk},
    ClusterConfig, Error, NodeBuilder, RaftNodeConfig, Result,
};
use log::{debug, error, info};
use std::{path::Path, time::Duration};
use tokio::sync::watch;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

#[derive(Debug)]
pub enum ClientCommands {
    PUT,
    READ,
    LREAD,
    DELETE,
}
pub async fn start_cluster(nodes_config_paths: Vec<&str>) -> Result<()> {
    // let nodes = vec![
    //     "tests/config/case1/n1.toml",
    //     "tests/config/case1/n2.toml",
    //     "tests/config/case1/n3.toml",
    // ];

    // Start all nodes
    let mut controllers = vec![];
    for config_path in nodes_config_paths {
        let (tx, handle) = start_node(config_path).await?;
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
) -> Result<(watch::Sender<()>, tokio::task::JoinHandle<Result<()>>)> {
    let (graceful_tx, graceful_rx) = watch::channel(());

    let config_path = config_path.to_string();
    let handle = tokio::spawn(async move { run_node(&config_path, graceful_rx).await });

    Ok((graceful_tx, handle))
}

async fn run_node(config_path: &str, graceful_rx: watch::Receiver<()>) -> Result<()> {
    // Load configuration from the specified path
    let settings = RaftNodeConfig::load(Some(config_path)).expect("init settings successfully.");

    // Initialize logs
    let _guard = init_observability(&settings.cluster)?;

    // Build and start the node
    let node = NodeBuilder::new(Some(config_path), graceful_rx)
        .build()
        .start_rpc_server()
        .await
        .ready()
        .expect("Should succeed to start node");

    info!("Node started with config: {}", config_path);
    debug!("Node started with config: {}", config_path);

    // Run the node until shutdown
    if let Err(e) = node.run().await {
        error!("Node error: {:?}", e);
    }

    debug!("Exiting program: {:?}", config_path);
    drop(node);
    Ok(())
}
pub fn init_observability(settings: &ClusterConfig) -> Result<WorkerGuard> {
    let log_file = util::open_file_for_append(
        Path::new(&settings.log_dir).join(format!("{}/d.log", settings.node_id)),
    )?;

    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);
    let base_subscriber = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env());
    if let Err(e) = tracing_subscriber::registry()
        .with(base_subscriber)
        .try_init()
    {
        error!("{:?}", e);
    }

    Ok(guard)
}

pub async fn execute_command(
    command: ClientCommands,
    bootstrap_urls: &Vec<String>,
    key: u64,
    value: Option<u64>,
) -> Result<u64> {
    let client = match dengine::ClientBuilder::new(bootstrap_urls.clone())
        .connect_timeout(Duration::from_secs(3))
        .request_timeout(Duration::from_secs(2))
        .enable_compression(true)
        .build()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            error!("execute_command, {:?}", e);
            return Err(e);
        }
    };

    debug!("recevied command = {:?}", &command);
    // Handle subcommands
    match command {
        ClientCommands::PUT => {
            let value = value.unwrap();

            info!("put {}:{}", key, value);

            match client.kv().put(kv(key), kv(value)).await {
                Ok(res) => {
                    debug!("Put Success: {:?}", res);
                    return Ok(key);
                }
                Err(Error::NodeIsNotLeaderError) => {
                    error!("node is not leader");
                    return Err(Error::NodeIsNotLeaderError);
                }
                Err(e) => {
                    error!("Error: {:?}", e);
                    return Err(Error::ClientError(format!("Error: {:?}", e)));
                }
            }
        }
        ClientCommands::DELETE => match client.kv().delete(kv(key)).await {
            Ok(res) => {
                debug!("Delete Success: {:?}", res);
                return Ok(key);
            }
            Err(Error::NodeIsNotLeaderError) => {
                error!("node is not leader");
                return Err(Error::NodeIsNotLeaderError);
            }
            Err(e) => {
                error!("Error: {:?}", e);
                return Err(Error::ClientError(format!("Error: {:?}", e)));
            }
        },
        ClientCommands::READ => match client.kv().get(kv(key), false).await? {
            Some(r) => {
                let v = vk(&r.value);
                debug!("Success: {:?}", v);
                return Ok(v);
            }
            None => {
                error!("No entry found for key: {}", key);
                return Err(Error::ClientError(format!(
                    "No entry found for key: {}",
                    key
                )));
            }
        },
        ClientCommands::LREAD => match client.kv().get(kv(key), true).await? {
            Some(r) => {
                let v = vk(&r.value);
                debug!("Success: {:?}", v);
                return Ok(v);
            }
            None => {
                error!("No result found for key: {}", key);
                return Err(Error::ClientError(format!(
                    "No entry found for key: {}",
                    key
                )));
            }
        },
        _ => {
            error!("Invalid subcommand");
            return Err(Error::ClientError(format!("Invalid subcommand")));
        }
    }
}
