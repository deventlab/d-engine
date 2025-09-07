use core::panic;
use d_engine::file_io::open_file_for_append;
use d_engine::node::NodeBuilder;
// use d_engine::FileStateMachine;
// use d_engine::FileStorageEngine;
use d_engine::RocksDBStateMachine;
use d_engine::RocksDBStorageEngine;
use std::env;
use std::error::Error;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::signal;
use tokio::signal::unix::SignalKind;
use tokio::sync::watch;
use tracing::error;
use tracing::info;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let db_path: PathBuf = env::var("DB_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/db"));

    let log_dir = env::var("LOG_DIR")
        .map_err(|_| "LOG_DIR environment variable not set")
        .expect("Set log dir successfully.");

    let metrics_port: u16 = env::var("METRICS_PORT")
        .map(|v| v.parse::<u16>().expect("METRICS_PORT must be a valid port"))
        .unwrap_or(9000); // default 9000 if not set

    // Initialize the log system
    let _guard = init_observability(log_dir);

    // Initializing Shutdown Signal
    let (graceful_tx, graceful_rx) = watch::channel(());

    // Start the server (wait for its initialization to complete)
    let server_handler = tokio::spawn(start_dengine_server(db_path, graceful_rx.clone()));

    // Wait for the server to initialize (adjust the waiting time according to the actual logic)
    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Start Prometheus metrics server ---
    let metrics_handle = tokio::spawn(start_metrics_server(metrics_port));

    // Monitor shutdown signals
    let shutdown_handler = tokio::spawn(graceful_shutdown(graceful_tx));

    // Wait for all tasks to complete (or error)
    let (_server_result, _metrics_result, _shutdown_result) =
        tokio::join!(server_handler, metrics_handle, shutdown_handler);
}

async fn start_dengine_server(
    db_path: PathBuf,
    graceful_rx: watch::Receiver<()>,
) {
    // Option 1: RAW FILE
    // let storage_engine = Arc::new(FileStorageEngine::new(db_path.join("storage_engine")).unwrap());
    // let state_machine =
    //     Arc::new(FileStateMachine::new(db_path.join("state_machine")).await.unwrap());

    // Option 2: ROCKSDB
    let storage_engine = Arc::new(RocksDBStorageEngine::new(db_path.join("storage")).unwrap());
    let state_machine = Arc::new(RocksDBStateMachine::new(db_path.join("state_machine")).unwrap());

    // Build Node
    let node = NodeBuilder::new(None, graceful_rx.clone())
        .storage_engine(storage_engine)
        .state_machine(state_machine)
        .build()
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
        }
    }

    graceful_tx.send(()).unwrap();

    info!("Shutdown completed");
}

pub fn init_observability(log_dir: String) -> Result<WorkerGuard, Box<dyn Error + Send>> {
    let log_file = open_file_for_append(Path::new(&log_dir).join("d.log")).unwrap();

    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);
    let base_subscriber = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env());
    tracing_subscriber::registry().with(base_subscriber).init();
    Ok(guard)
}

async fn start_metrics_server(port: u16) {
    // Start Prometheus exporter with dynamic port
    println!("Metrics server will start at http://0.0.0.0:{port}/metrics",);
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        .install()
        .expect("failed to start Prometheus metrics exporter");
}
