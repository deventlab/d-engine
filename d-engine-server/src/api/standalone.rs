//! Standalone mode for d-engine - independent server deployment

use std::sync::Arc;

use tokio::sync::watch;

use crate::Result;
#[cfg(feature = "rocksdb")]
use crate::RocksDBStateMachine;
#[cfg(feature = "rocksdb")]
use crate::RocksDBStorageEngine;
use crate::StateMachine;
use crate::StorageEngine;
use crate::node::NodeBuilder;

/// Standalone d-engine server for independent deployment
pub struct StandaloneServer;

impl StandaloneServer {
    /// Run server with configuration from environment.
    ///
    /// Reads `CONFIG_PATH` environment variable or uses default configuration.
    /// Data directory is determined by config's `cluster.db_root_dir` setting.
    /// Blocks until shutdown signal is received.
    ///
    /// # Arguments
    /// * `shutdown_rx` - Shutdown signal receiver
    ///
    /// # Example
    /// ```ignore
    /// // Set config path via environment variable
    /// std::env::set_var("CONFIG_PATH", "/etc/d-engine/production.toml");
    ///
    /// let (shutdown_tx, shutdown_rx) = watch::channel(());
    /// StandaloneServer::run(shutdown_rx).await?;
    /// ```
    #[cfg(feature = "rocksdb")]
    pub async fn run(shutdown_rx: watch::Receiver<()>) -> Result<()> {
        let config = d_engine_core::RaftNodeConfig::new()?;
        let base_dir = std::path::PathBuf::from(&config.cluster.db_root_dir);

        tokio::fs::create_dir_all(&base_dir)
            .await
            .map_err(|e| crate::Error::Fatal(format!("Failed to create data directory: {e}")))?;

        let storage_path = base_dir.join("storage");
        let sm_path = base_dir.join("state_machine");

        tracing::info!("Starting standalone server with RocksDB at {:?}", base_dir);

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let mut sm = RocksDBStateMachine::new(sm_path)?;

        // Inject lease if enabled
        let lease_cfg = &config.raft.state_machine.lease;
        if lease_cfg.enabled {
            let lease = Arc::new(crate::storage::DefaultLease::new(lease_cfg.clone()));
            sm.set_lease(lease);
        }

        let sm = Arc::new(sm);

        Self::run_custom(storage, sm, shutdown_rx, None).await
    }

    /// Run server with explicit configuration file.
    ///
    /// Reads configuration from specified file path.
    /// Data directory is determined by config's `cluster.db_root_dir` setting.
    /// Blocks until shutdown signal is received.
    ///
    /// # Arguments
    /// * `config_path` - Path to configuration file
    /// * `shutdown_rx` - Shutdown signal receiver
    ///
    /// # Example
    /// ```ignore
    /// let (shutdown_tx, shutdown_rx) = watch::channel(());
    /// StandaloneServer::run_with("config/node1.toml", shutdown_rx).await?;
    /// ```
    #[cfg(feature = "rocksdb")]
    pub async fn run_with(
        config_path: &str,
        shutdown_rx: watch::Receiver<()>,
    ) -> Result<()> {
        let config = d_engine_core::RaftNodeConfig::new()?.with_override_config(config_path)?;
        let base_dir = std::path::PathBuf::from(&config.cluster.db_root_dir);

        tokio::fs::create_dir_all(&base_dir)
            .await
            .map_err(|e| crate::Error::Fatal(format!("Failed to create data directory: {e}")))?;

        let storage_path = base_dir.join("storage");
        let sm_path = base_dir.join("state_machine");

        tracing::info!("Starting standalone server with RocksDB at {:?}", base_dir);

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let mut sm = RocksDBStateMachine::new(sm_path)?;

        // Inject lease if enabled
        let lease_cfg = &config.raft.state_machine.lease;
        if lease_cfg.enabled {
            let lease = Arc::new(crate::storage::DefaultLease::new(lease_cfg.clone()));
            sm.set_lease(lease);
        }

        let sm = Arc::new(sm);

        Self::run_custom(storage, sm, shutdown_rx, Some(config_path)).await
    }

    /// Run server with custom storage engine and state machine.
    ///
    /// Advanced API for users providing custom storage implementations.
    /// Blocks until shutdown signal is received.
    ///
    /// # Arguments
    /// * `config` - Node configuration
    /// * `storage_engine` - Custom storage engine implementation
    /// * `state_machine` - Custom state machine implementation
    /// * `shutdown_rx` - Shutdown signal receiver
    ///
    /// # Example
    /// ```ignore
    /// let storage = Arc::new(MyCustomStorage::new()?);
    /// let sm = Arc::new(MyCustomStateMachine::new()?);
    ///
    /// let (shutdown_tx, shutdown_rx) = watch::channel(());
    /// StandaloneServer::run_custom(storage, sm, shutdown_rx, Some("config.toml")).await?;
    /// ```
    pub async fn run_custom<SE, SM>(
        storage_engine: Arc<SE>,
        state_machine: Arc<SM>,
        shutdown_rx: watch::Receiver<()>,
        config_path: Option<&str>,
    ) -> Result<()>
    where
        SE: StorageEngine + std::fmt::Debug + 'static,
        SM: StateMachine + std::fmt::Debug + 'static,
    {
        let node_config = if let Some(path) = config_path {
            d_engine_core::RaftNodeConfig::default().with_override_config(path)?
        } else {
            d_engine_core::RaftNodeConfig::new()?
        };

        let node = NodeBuilder::init(node_config, shutdown_rx)
            .storage_engine(storage_engine)
            .state_machine(state_machine)
            .start()
            .await?;

        node.run().await
    }
}
