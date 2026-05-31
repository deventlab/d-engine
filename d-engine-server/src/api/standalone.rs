//! Standalone mode for d-engine - independent server deployment

use std::sync::Arc;

use tokio::sync::watch;

use crate::Result;
#[cfg(feature = "rocksdb")]
use crate::RocksDBStateMachine;
#[cfg(feature = "rocksdb")]
use crate::RocksDBStorageEngine;
#[cfg(feature = "rocksdb")]
use crate::RocksDBUnifiedEngine;
use crate::StateMachine;
use crate::StorageEngine;
use crate::node::NodeBuilder;

/// Standalone d-engine engine for independent deployment
pub struct StandaloneEngine;

impl StandaloneEngine {
    /// Run server with an explicit data directory.
    ///
    /// `data_dir` has highest priority and always overrides `cluster.db_root_dir` from
    /// `CONFIG_PATH` or `RAFT__` environment variables. Other configuration (network,
    /// Raft timeouts, cluster topology) is still read from those sources if set.
    ///
    /// The directory is created automatically if it does not exist.
    /// Blocks until shutdown signal is received.
    ///
    /// # Arguments
    /// * `data_dir` - Path to the data directory
    /// * `shutdown_rx` - Shutdown signal receiver
    ///
    /// # Example
    /// ```ignore
    /// let (shutdown_tx, shutdown_rx) = watch::channel(());
    /// StandaloneEngine::run("./data/my-node", shutdown_rx).await?;
    /// ```
    #[cfg(feature = "rocksdb")]
    pub async fn run(
        data_dir: impl AsRef<std::path::Path>,
        shutdown_rx: watch::Receiver<()>,
    ) -> Result<()> {
        let mut config = d_engine_core::RaftNodeConfig::new()?;
        config.cluster.db_root_dir = data_dir.as_ref().to_path_buf();
        let config = config.validate()?;
        let base_dir = config.cluster.db_root_dir.clone();

        tokio::fs::create_dir_all(&base_dir)
            .await
            .map_err(|e| crate::Error::Fatal(format!("Failed to create data directory: {e}")))?;

        let (storage, mut sm) = if config.storage.unified_db {
            let db_path = base_dir.join("db");
            tracing::info!(
                "Starting standalone server with unified RocksDB at {:?}",
                db_path
            );
            RocksDBUnifiedEngine::open(&db_path)?
        } else {
            tracing::info!(
                "Starting standalone server with separate RocksDB instances at {:?}",
                base_dir
            );
            let storage = RocksDBStorageEngine::new(base_dir.join("storage"))?;
            let sm = RocksDBStateMachine::new(base_dir.join("state_machine"))?;
            (storage, sm)
        };

        let lease = Arc::new(crate::storage::TtlLease::new(
            config.raft.state_machine.lease.clone(),
        ));
        sm.set_lease(lease);

        Self::start_node(config, Arc::new(storage), Arc::new(sm), shutdown_rx).await
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
    /// StandaloneEngine::run_with("config/node1.toml", shutdown_rx).await?;
    /// ```
    #[cfg(feature = "rocksdb")]
    pub async fn run_with(
        config_path: &str,
        shutdown_rx: watch::Receiver<()>,
    ) -> Result<()> {
        let config = d_engine_core::RaftNodeConfig::new()?
            .with_override_config(config_path)?
            .validate()?;
        let base_dir = std::path::PathBuf::from(&config.cluster.db_root_dir);

        tokio::fs::create_dir_all(&base_dir)
            .await
            .map_err(|e| crate::Error::Fatal(format!("Failed to create data directory: {e}")))?;

        let (storage, mut sm) = if config.storage.unified_db {
            let db_path = base_dir.join("db");
            tracing::info!(
                "Starting standalone server with unified RocksDB at {:?}",
                db_path
            );
            RocksDBUnifiedEngine::open(&db_path)?
        } else {
            tracing::info!(
                "Starting standalone server with separate RocksDB instances at {:?}",
                base_dir
            );
            let storage = RocksDBStorageEngine::new(base_dir.join("storage"))?;
            let sm = RocksDBStateMachine::new(base_dir.join("state_machine"))?;
            (storage, sm)
        };

        let lease = Arc::new(crate::storage::TtlLease::new(
            config.raft.state_machine.lease.clone(),
        ));
        sm.set_lease(lease);

        Self::start_node(config, Arc::new(storage), Arc::new(sm), shutdown_rx).await
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
    /// StandaloneEngine::run_custom(storage, sm, shutdown_rx, Some("config.toml")).await?;
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
        let config = if let Some(path) = config_path {
            d_engine_core::RaftNodeConfig::default()
                .with_override_config(path)?
                .validate()?
        } else {
            d_engine_core::RaftNodeConfig::new()?.validate()?
        };
        Self::start_node(config, storage_engine, state_machine, shutdown_rx).await
    }

    async fn start_node<SE, SM>(
        config: d_engine_core::RaftNodeConfig,
        storage_engine: Arc<SE>,
        state_machine: Arc<SM>,
        shutdown_rx: watch::Receiver<()>,
    ) -> Result<()>
    where
        SE: StorageEngine + std::fmt::Debug + 'static,
        SM: StateMachine + std::fmt::Debug + 'static,
    {
        let node = NodeBuilder::init(config, shutdown_rx)
            .storage_engine(storage_engine)
            .state_machine(state_machine)
            .start()
            .await?;
        node.run().await
    }
}

#[cfg(test)]
#[path = "standalone_test.rs"]
mod tests;
