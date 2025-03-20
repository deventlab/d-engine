use dengine::{
    client_config::ClientConfig,
    utils::util::{self, kv, vk},
    ClientApis, DengineClient, Error, NodeBuilder, Result, ServerSettings, Settings,
};
use log::{error, info};
use std::path::Path;
use tokio::sync::watch;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    fmt, layer::SubscriberExt, reload, util::SubscriberInitExt, EnvFilter, Layer, Registry,
};

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
    let settings = Settings::from_file(config_path)?;

    // Initialize logs
    let _guard = init_observability(&settings.server_settings)?;

    // Build and start the node
    let node = NodeBuilder::new(settings, graceful_rx)
        .build()
        .start_rpc_server()
        .await
        .ready()
        .expect("Should succeed to start node");

    info!("Node started with config: {}", config_path);
    println!("Node started with config: {}", config_path);

    // Run the node until shutdown
    if let Err(e) = node.run().await {
        error!("Node error: {:?}", e);
    }

    println!("Exiting program: {:?}", config_path);
    drop(node);
    Ok(())
}
pub fn init_observability2(settings: &ServerSettings) -> Result<WorkerGuard> {
    let log_file = util::open_file_for_append(
        Path::new(&settings.log_dir).join(format!("{}/d.log", settings.id)),
    )?;
    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);
    let log_writer = non_blocking; // 统一写入器

    // 初始日志层
    let log_layer = tracing_subscriber::fmt::layer().with_writer(log_writer.clone()); // 使用 clone 的写入器

    let filter = EnvFilter::from_default_env();
    let filtered_layer = log_layer.with_filter(filter);

    // 使用 reload 功能
    let (subscriber, reload_handle) = reload::Layer::new(filtered_layer);

    tracing_subscriber::registry()
        .with(subscriber)
        .try_init()
        .map_err(|_| Error::GeneralLocalLogIOError)?;

    // 错误处理分支中保持相同写入器类型
    let new_log_layer = tracing_subscriber::fmt::layer()
        .with_writer(log_writer) // 使用相同的写入器
        .with_ansi(true);

    let new_filter = EnvFilter::from_default_env();
    let new_filtered_layer = new_log_layer.with_filter(new_filter);

    reload_handle
        .modify(|layer| {
            *layer = new_filtered_layer;
        })
        .map_err(|e| Error::GeneralServerError(format!("{:?}", e)))?;

    Ok(guard)
}

pub fn init_observability(settings: &ServerSettings) -> Result<WorkerGuard> {
    let log_file = util::open_file_for_append(
        Path::new(&settings.log_dir).join(format!("{}/d.log", settings.id)),
    )?;

    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);
    let base_subscriber = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env());
    if let Err(e) = tracing_subscriber::registry()
        .with(base_subscriber)
        .try_init()
    {
        eprintln!("{:?}", e);
    }

    Ok(guard)
}

pub async fn execute_command(
    command: ClientCommands,
    bootstrap_urls: &Vec<String>,
    key: u64,
    value: Option<u64>,
) -> Result<u64> {
    let cfg = ClientConfig {
        bootstrap_urls: bootstrap_urls.clone(),
        connect_timeout_in_ms: 100,
        request_timeout_in_ms: 200,
        concurrency_limit_per_connection: 8192,
        tcp_keepalive_in_secs: 3600,
        http2_keep_alive_interval_in_secs: 300,
        http2_keep_alive_timeout_in_secs: 20,
        max_frame_size: 12582912,
        initial_connection_window_size: 12582912,
        initial_stream_window_size: 12582912,
        buffer_size: 65536,
    };

    let mut client = match DengineClient::new(cfg).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("DengineClient::new failed: {:?}", e);
            return Err(Error::ClientError(format!(
                "DengineClient::new failed: {:?}",
                e
            )));
        }
    };

    // Handle subcommands
    match command {
        ClientCommands::PUT => {
            let value = value.unwrap();

            println!("put {}:{}", key, value);

            match client.write(kv(key), kv(value)).await {
                Ok(res) => {
                    println!("Put Success: {:?}", res);
                    return Ok(key);
                }
                Err(Error::NodeIsNotLeaderError) => {
                    eprintln!("node is not leader");
                    return Err(Error::NodeIsNotLeaderError);
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    return Err(Error::ClientError(format!("Error: {:?}", e)));
                }
            }
        }
        ClientCommands::DELETE => match client.delete(kv(key)).await {
            Ok(res) => {
                println!("Delete Success: {:?}", res);
                return Ok(key);
            }
            Err(Error::NodeIsNotLeaderError) => {
                eprintln!("node is not leader");
                return Err(Error::NodeIsNotLeaderError);
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                return Err(Error::ClientError(format!("Error: {:?}", e)));
            }
        },
        ClientCommands::READ => match client.read(kv(key)).await {
            Ok(client_results) => {
                if client_results.len() > 0 {
                    let v = vk(&client_results[0].value);
                    println!("Success: {:?}", v);
                    return Ok(v);
                } else {
                    println!("entry(k={}) not exist.", key);
                    return Err(Error::ClientError(format!("entry(k={}) not exist.", key)));
                }
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                return Err(Error::ClientError(format!("Error: {:?}", e)));
            }
        },
        ClientCommands::LREAD => match client.lread(kv(key)).await {
            Ok(client_results) => {
                if client_results.len() > 0 {
                    let v = vk(&client_results[0].value);
                    println!("Success: {:?}", v);
                    return Ok(v);
                } else {
                    println!("entry(k={}) not exist.", key);
                    return Err(Error::ClientError(format!("entry(k={}) not exist.", key)));
                }
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                return Err(Error::ClientError(format!("Error: {:?}", e)));
            }
        },
        _ => {
            eprintln!("Invalid subcommand");
            return Err(Error::ClientError(format!("Invalid subcommand")));
        }
    }
}
