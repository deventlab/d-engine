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
use tokio_metrics::RuntimeMonitor;
use tracing::error;
use tracing::info;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;

#[tokio::main(flavor = "current_thread")]
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

    if env::var("TOKIO_CONSOLE").is_ok() {
        let tokio_console_port: u16 = env::var("TOKIO_CONSOLE_PORT")
            .map(|v| v.parse::<u16>().expect("TOKIO_CONSOLE_PORT must be a valid port"))
            .unwrap_or(6669);

        println!("Tokio Console port: {tokio_console_port}");

        console_subscriber::Builder::default()
            .server_addr(([127, 0, 0, 1], tokio_console_port))
            .init();

        // Your application code here
        println!("Application started with Tokio Console monitoring");
    } else {
        // Initialize the log system
        let _guard = init_observability(log_dir);
    }

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
    if env::var("TOKIO_CONSOLE").is_ok() {
        // Initialize Tokio metrics monitoring
        let handle = tokio::runtime::Handle::current();
        let runtime_monitor = RuntimeMonitor::new(&handle);
        // Start the Tokio metrics collection task
        let tokio_metrics_handle = tokio::spawn(collect_tokio_metrics(runtime_monitor));
        let (_server_result, _metrics_result, _shutdown_result, _tokio_metrics_result) = tokio::join!(
            server_handler,
            metrics_handle,
            shutdown_handler,
            tokio_metrics_handle
        );
    } else {
        let (_server_result, _metrics_result, _shutdown_result) =
            tokio::join!(server_handler, metrics_handle, shutdown_handler);
    }
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

// Tokio metrics collection function
async fn collect_tokio_metrics(monitor: RuntimeMonitor) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut intervals = monitor.intervals();
    loop {
        interval.tick().await;
        if let Some(metrics) = intervals.next() {
            metrics::gauge!("tokio.runtime.workers_count").set(metrics.workers_count as f64);
            metrics::counter!("tokio.runtime.park_total").absolute(metrics.total_park_count);
            metrics::gauge!("tokio.runtime.park_max").set(metrics.max_park_count as f64);
            metrics::gauge!("tokio.runtime.park_min").set(metrics.min_park_count as f64);
            metrics::histogram!("tokio.runtime.busy_duration_ns")
                .record(metrics.total_busy_duration.as_nanos() as f64);
        }
    }
}
