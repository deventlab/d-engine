use core::panic;
use dengine::utils::util;
use dengine::{Error, Result};
use dengine::{NodeBuilder, RaftNodeConfig};
use log::{error, info};
use std::path::{Path, PathBuf};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let settings = RaftNodeConfig::load(None)?;

    // Initializing Logs
    let _guard = init_observability(settings.cluster.node_id, &settings.cluster.log_dir)?;

    // Initializing Shutdown Signal
    let (graceful_tx, graceful_rx) = watch::channel(());

    // Build Node
    let node = NodeBuilder::init(settings, graceful_rx.clone())
        .build()
        .start_metrics_server(graceful_rx.clone()) //default: prometheus metrics server starts
        .start_rpc_server()
        .await
        .ready()
        .expect("start node failed.");

    info!("Application started. Waiting for CTRL+C signal...");
    // Listen on Shutdown Signal
    tokio::spawn(async {
        if let Err(e) = graceful_shutdown(graceful_tx).await {
            error!("Failed to shutdown: {:?}", e);
        }
    });

    // Start Node
    if let Err(e) = node.run().await {
        error!("node stops: {:?}", e);
    }

    println!("Exiting program.");
    Ok(())
}

async fn graceful_shutdown(graceful_tx: watch::Sender<()>) -> Result<()> {
    info!("Shutdown server..");
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

pub fn init_observability(node_id: u32, log_dir: &PathBuf) -> Result<WorkerGuard> {
    let log_file =
        util::open_file_for_append(Path::new(log_dir).join(format!("{}/d.log", node_id)))?;

    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);
    let base_subscriber = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env());
    tracing_subscriber::registry().with(base_subscriber).init();

    Ok(guard)
}
