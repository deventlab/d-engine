//! Standalone mode for d-engine - independent server deployment

use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::error;

use crate::node::NodeBuilder;
use crate::{Result, StateMachine, StorageEngine};

#[cfg(feature = "rocksdb")]
use crate::{RocksDBStateMachine, RocksDBStorageEngine};

/// Standalone d-engine server for independent deployment
pub struct StandaloneServer {
    node_handle: Option<JoinHandle<Result<()>>>,
}

impl StandaloneServer {
    /// Start standalone server with RocksDB storage.
    ///
    /// # Arguments
    /// * `data_dir` - Base directory for all data
    /// * `config_path` - Optional path to config file
    /// * `shutdown_rx` - Shutdown signal receiver
    #[cfg(feature = "rocksdb")]
    pub async fn start<P: AsRef<std::path::Path>>(
        data_dir: P,
        config_path: Option<&str>,
        shutdown_rx: watch::Receiver<()>,
    ) -> Result<Self> {
        let base_dir = data_dir.as_ref();
        tokio::fs::create_dir_all(base_dir)
            .await
            .map_err(|e| crate::Error::Fatal(format!("Failed to create data directory: {e}")))?;

        let storage_path = base_dir.join("storage");
        let sm_path = base_dir.join("state_machine");

        tracing::info!("Starting standalone server with RocksDB at {:?}", base_dir);

        let config = if let Some(path) = config_path {
            d_engine_core::RaftNodeConfig::new()?.with_override_config(path)?
        } else {
            d_engine_core::RaftNodeConfig::new()?
        };

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let mut sm = RocksDBStateMachine::new(sm_path)?;

        let lease_cfg = &config.raft.state_machine.lease;
        let lease = Arc::new(crate::storage::DefaultLease::new(lease_cfg.clone()));
        sm.set_lease(lease);

        let sm = Arc::new(sm);

        Self::start_custom(config, storage, sm, shutdown_rx).await
    }

    /// Start server with custom storage and state machine.
    ///
    /// For advanced users who want to provide custom implementations.
    ///
    /// # Arguments
    /// * `config` - Node configuration
    /// * `storage_engine` - Custom storage engine implementation
    /// * `state_machine` - Custom state machine implementation (already initialized)
    /// * `shutdown_rx` - Shutdown signal receiver
    ///
    /// # Example
    /// ```ignore
    /// let config = RaftNodeConfig::new()?;
    /// let storage = Arc::new(MyCustomStorage::new()?);
    /// let sm = Arc::new(MyCustomStateMachine::new()?);
    /// let server = StandaloneServer::start_custom(
    ///     config,
    ///     storage,
    ///     sm,
    ///     shutdown_rx
    /// ).await?;
    /// ```
    pub async fn start_custom<SE, SM>(
        config: d_engine_core::RaftNodeConfig,
        storage_engine: Arc<SE>,
        state_machine: Arc<SM>,
        shutdown_rx: watch::Receiver<()>,
    ) -> Result<Self>
    where
        SE: StorageEngine + std::fmt::Debug + 'static,
        SM: StateMachine + std::fmt::Debug + 'static,
    {
        let node = NodeBuilder::init(config, shutdown_rx)
            .storage_engine(storage_engine)
            .state_machine(state_machine)
            .start()
            .await?;

        let node_handle = tokio::spawn(async move {
            if let Err(e) = node.run().await {
                error!("Node run error: {:?}", e);
                Err(e)
            } else {
                Ok(())
            }
        });

        Ok(Self {
            node_handle: Some(node_handle),
        })
    }

    /// Run the server until shutdown signal received.
    ///
    /// Blocks until the node task completes or errors.
    pub async fn run(mut self) -> Result<()> {
        if let Some(handle) = self.node_handle.take() {
            match handle.await {
                Ok(result) => result,
                Err(e) => Err(crate::Error::Fatal(format!("Node task panicked: {e}"))),
            }
        } else {
            Ok(())
        }
    }
}
