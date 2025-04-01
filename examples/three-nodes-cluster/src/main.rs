use core::panic;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use dengine::file_io::open_file_for_append;
use dengine::Error;
use dengine::NodeBuilder;
use dengine::RaftNodeConfig;
use dengine::Result;
use log::error;
use log::info;
use tokio::signal::unix::signal;
use tokio::signal::unix::SignalKind;
use tokio::sync::watch;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let settings = RaftNodeConfig::new()?;

    // Initializing Logs
    let _guard = init_observability(settings.cluster.node_id, &settings.cluster.log_dir)?;

    // Initializing Shutdown Signal
    let (graceful_tx, graceful_rx) = watch::channel(());

    // Start the server (wait for its initialization to complete)
    let server_handler = tokio::spawn(start_dengine_server(graceful_rx.clone()));

    // Wait for the server to initialize (adjust the waiting time according to the actual logic)
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Start the client
    let client_handler = tokio::spawn(simulate_client());

    // Monitor shutdown signals
    let shutdown_handler = tokio::spawn(graceful_shutdown(graceful_tx));

    // Wait for all tasks to complete (or error)
    let (_server_result, _client_result, _shutdown_result) =
        tokio::join!(server_handler, client_handler, shutdown_handler);

    Ok(())
}

async fn simulate_client() -> Result<()> {
    // Initialization (automatically discover clusters)
    let client = dengine::ClientBuilder::new(vec![
        "http://node1:9081".into(),
        "http://node2:9082".into(),
        "http://node2:9083".into(),
    ])
    .connect_timeout(Duration::from_secs(3))
    .request_timeout(Duration::from_secs(1))
    .enable_compression(true)
    .build()
    .await?;

    // Key-value operations
    client.kv().put("user:1001", "Alice").await?;
    let value = client.kv().get("user:1001", true).await?;
    info!("User data: {:?}", value);

    // Cluster management
    let members = client.cluster().list_members().await?;
    info!("Cluster members: {:?}", members);

    Ok(())
}
async fn start_dengine_server(graceful_rx: watch::Receiver<()>) -> Result<()> {
    // Build Node
    let node = NodeBuilder::new(None, graceful_rx.clone())
        .build()
        .start_metrics_server(graceful_rx.clone()) //default: prometheus metrics server starts
        .start_rpc_server()
        .await
        .ready()
        .expect("start node failed.");

    // Start Node
    if let Err(e) = node.run().await {
        error!("node stops: {:?}", e);
    }

    println!("Exiting program.");
    Ok(())
}

async fn graceful_shutdown(graceful_tx: watch::Sender<()>) -> Result<()> {
    info!("Monitoring shutdown signal ...");
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = sigint.recv() => {
            info!("SIGINT detected.");
        },
        _ = sigterm.recv() => {
            info!("SIGTERM detected.");
        },
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C detected.");
        },
    }

    graceful_tx.send(()).map_err(|e| {
        error!("Failed to send shutdown signal: {}", e);
        Error::SignalSenderClosed(format!("Failed to send shutdown signal: {}", e))
    })?;

    info!("Shutdown completed");
    Ok(())
}

pub fn init_observability(
    node_id: u32,
    log_dir: &PathBuf,
) -> Result<WorkerGuard> {
    let log_file = open_file_for_append(Path::new(log_dir).join(format!("{}/d.log", node_id)))?;

    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);
    let base_subscriber = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env());
    tracing_subscriber::registry().with(base_subscriber).init();

    Ok(guard)
}
