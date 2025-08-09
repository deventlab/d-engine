use d_engine::{NodeBuilder, RaftNodeConfig, StorageEngine};
use rocksdb::DB;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info, info_span, Instrument};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Parse configuration
    let node_id = env::var("NODE_ID")
        .expect("NODE_ID required")
        .parse::<u32>()
        .expect("Invalid NODE_ID");

    let db_path = env::var("DB_PATH").unwrap_or_else(|_| format!("/data/node-{}", node_id));

    let listen_addr =
        env::var("LISTEN_ADDR").unwrap_or_else(|_| format!("127.0.0.1:{}", 8000 + node_id));

    // 2. Initialize observability
    let _guard = init_observability(&format!("logs/node-{}.log", node_id))?;

    // 3. Create RocksDB storage engine
    let rocksdb_engine = Arc::new(d_engine::storage::rocksdb_engine::RocksDBEngine::new(
        &db_path,
    )?);

    // 4. Configure node
    let mut config = RaftNodeConfig::default();
    config.cluster.node_id = node_id;
    config.cluster.listen_address = listen_addr.parse()?;

    // 5. Initialize shutdown channel
    let (graceful_tx, graceful_rx) = watch::channel(());

    // 6. Build node with RocksDB storage
    let node = NodeBuilder::from_cluster_config(config, graceful_rx.clone())
        .storage_engine(rocksdb_engine)
        .build()
        .start_rpc_server()
        .await
        .ready()
        .expect("Node initialization failed");

    // 7. Start node in background
    tokio::spawn(
        async move {
            if let Err(e) = node.run().await {
                error!("Node stopped with error: {}", e);
            }
        }
        .instrument(info_span!("node_runtime", node_id = node_id)),
    );

    // 8. Handle graceful shutdown
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C received, shutting down");
        }
        _ = graceful_shutdown_signal() => {
            info!("Shutdown signal received");
        }
    }

    graceful_tx.send(()).ok();
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("Node {} shutdown complete", node_id);
    Ok(())
}

async fn graceful_shutdown_signal() {
    let mut stream =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    stream.recv().await;
}
