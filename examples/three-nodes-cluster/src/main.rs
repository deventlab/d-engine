use core::panic;
use std::env;
use std::error::Error;
use std::path::Path;
use std::time::Duration;

use dengine::file_io::open_file_for_append;
use dengine::NodeBuilder;
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
async fn main() {
    // Get the log directory path from the environment variable
    let log_dir = env::var("LOG_DIR")
        .map_err(|_| "LOG_DIR environment variable not set")
        .expect("Set log dir successfully.");

    // Initialize the log system
    let _guard = init_observability(log_dir);

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
}

async fn simulate_client() {
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
    .await
    .unwrap();

    // Key-value operations
    client.kv().put("user:1001", "Alice").await.unwrap();
    let value = client.kv().get("user:1001", true).await.unwrap();
    info!("User data: {:?}", value);

    // Cluster management
    let members = client
        .cluster()
        .list_members()
        .await
        .expect("List cluster members successfully.");
    info!("Cluster members: {:?}", members);
}
async fn start_dengine_server(graceful_rx: watch::Receiver<()>) {
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
    } else {
        info!("node stops.");
    }

    println!("Exiting program.");
}

async fn graceful_shutdown(graceful_tx: watch::Sender<()>) {
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

    graceful_tx.send(()).unwrap();

    info!("Shutdown completed");
}

pub fn init_observability(log_dir: String) -> Result<WorkerGuard, Box<dyn Error + Send>> {
    let log_file = open_file_for_append(Path::new(&log_dir).join(format!("d.log"))).unwrap();

    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);
    let base_subscriber = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env());
    tracing_subscriber::registry().with(base_subscriber).init();
    Ok(guard)
}
