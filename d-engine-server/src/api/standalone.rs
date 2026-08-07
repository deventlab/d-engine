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
    /// The directory is created automatically if it does not exist.
    /// Blocks until shutdown signal is received.
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
        let config = d_engine_core::RaftNodeConfig::new()?.validate()?;
        let data_dir = crate::node::DataDir::new(data_dir.as_ref())?;
        // Lock before touching storage — otherwise two racing processes both
        // get partway into opening the same RocksDB directory before either
        // one even checks this lock.
        let data_dir_lock = crate::node::DataDirLock::acquire(data_dir.as_path())?;

        let (storage, mut sm) = if config.storage.unified_db {
            let db_path = data_dir.unified_db_path()?;
            tracing::info!(
                "Starting standalone server with unified RocksDB at {:?}",
                db_path
            );
            RocksDBUnifiedEngine::open(&db_path)?
        } else {
            tracing::info!(
                "Starting standalone server with separate RocksDB instances at {:?}",
                data_dir.as_path()
            );
            let storage = RocksDBStorageEngine::new(data_dir.storage_path()?)?;
            let sm = RocksDBStateMachine::new(data_dir.state_machine_path()?)?;
            (storage, sm)
        };

        let lease = Arc::new(crate::storage::TtlLease::new(
            config.raft.state_machine.lease.clone(),
        ));
        sm.set_lease(lease);

        Self::start_node_with_lock(
            data_dir.as_path().to_path_buf(),
            data_dir_lock,
            Arc::new(storage),
            Arc::new(sm),
            shutdown_rx,
            config,
        )
        .await
    }

    /// Run server with an explicit data directory plus a config file for
    /// everything else (a `[cluster] data_dir` entry in the file, if
    /// present, is ignored). Blocks until shutdown signal is received.
    ///
    /// # Example
    /// ```ignore
    /// let (shutdown_tx, shutdown_rx) = watch::channel(());
    /// StandaloneEngine::run_with("./data/my-node", "config/node1.toml", shutdown_rx).await?;
    /// ```
    #[cfg(feature = "rocksdb")]
    pub async fn run_with(
        data_dir: impl AsRef<std::path::Path>,
        config_path: impl AsRef<std::path::Path>,
        shutdown_rx: watch::Receiver<()>,
    ) -> Result<()> {
        let config = d_engine_core::RaftNodeConfig::new()?
            .with_override_config(config_path)?
            .validate()?;
        let data_dir = crate::node::DataDir::new(data_dir.as_ref())?;
        let data_dir_lock = crate::node::DataDirLock::acquire(data_dir.as_path())?;

        let (storage, mut sm) = if config.storage.unified_db {
            let db_path = data_dir.unified_db_path()?;
            tracing::info!(
                "Starting standalone server with unified RocksDB at {:?}",
                db_path
            );
            RocksDBUnifiedEngine::open(&db_path)?
        } else {
            tracing::info!(
                "Starting standalone server with separate RocksDB instances at {:?}",
                data_dir.as_path()
            );
            let storage = RocksDBStorageEngine::new(data_dir.storage_path()?)?;
            let sm = RocksDBStateMachine::new(data_dir.state_machine_path()?)?;
            (storage, sm)
        };

        let lease = Arc::new(crate::storage::TtlLease::new(
            config.raft.state_machine.lease.clone(),
        ));
        sm.set_lease(lease);

        // start_node_with_lock directly — config already loaded/validated,
        // lock already held.
        Self::start_node_with_lock(
            data_dir.as_path().to_path_buf(),
            data_dir_lock,
            Arc::new(storage),
            Arc::new(sm),
            shutdown_rx,
            config,
        )
        .await
    }

    /// Runs the server with a custom storage engine and state machine.
    ///
    /// Blocks until `shutdown_rx` receives a signal. `data_dir` is required and explicit —
    /// `config_path` is only for non-path settings (node_id, cluster topology, timeouts, etc.);
    /// any `data_dir` entry in the config file is ignored.
    ///
    /// d-engine's data_dir lock only protects d-engine's own files.
    /// `storage_engine`/`state_machine` are constructed by the caller;
    /// their concurrency safety is the caller's responsibility.
    ///
    /// # Example
    /// ```ignore
    /// let storage = Arc::new(MyCustomStorage::new()?);
    /// let sm = Arc::new(MyCustomStateMachine::new()?);
    ///
    /// let (shutdown_tx, shutdown_rx) = watch::channel(());
    /// StandaloneEngine::run_custom("./data/my-node", storage, sm, shutdown_rx, Some("config.toml")).await?;
    /// ```
    pub async fn run_custom<SE, SM>(
        data_dir: impl AsRef<std::path::Path>,
        storage_engine: Arc<SE>,
        state_machine: Arc<SM>,
        shutdown_rx: watch::Receiver<()>,
        config_path: Option<impl AsRef<std::path::Path>>,
    ) -> Result<()>
    where
        SE: StorageEngine + std::fmt::Debug + 'static,
        SM: StateMachine + std::fmt::Debug + 'static,
    {
        let config = if let Some(path) = config_path {
            d_engine_core::RaftNodeConfig::default().with_override_config(path)?
        } else {
            d_engine_core::RaftNodeConfig::new()?
        };
        let config = config.validate()?;
        Self::start_node(data_dir, storage_engine, state_machine, shutdown_rx, config).await
    }

    /// Start standalone server with custom storage, state machine, and programmatic config.
    ///
    /// For library wrappers that build their own config layer and
    /// translate to `RaftNodeConfig` in Rust — no config file required.
    /// Same lock scope as [`run_custom`](Self::run_custom) — see its doc.
    ///
    /// Blocks until `shutdown_rx` receives a signal.
    pub async fn start_node<SE, SM>(
        data_dir: impl AsRef<std::path::Path>,
        storage_engine: Arc<SE>,
        state_machine: Arc<SM>,
        shutdown_rx: watch::Receiver<()>,
        config: d_engine_core::RaftNodeConfig,
    ) -> Result<()>
    where
        SE: StorageEngine + std::fmt::Debug + 'static,
        SM: StateMachine + std::fmt::Debug + 'static,
    {
        Self::start_node_inner(
            data_dir.as_ref().to_path_buf(),
            None,
            storage_engine,
            state_machine,
            shutdown_rx,
            config,
        )
        .await
    }

    /// Same as `start_node`, but for callers (`run`/`run_with`) that already
    /// opened storage and therefore must have acquired the lock *before*
    /// that — see `NodeBuilder::data_dir_lock`'s doc comment.
    #[allow(dead_code)]
    async fn start_node_with_lock<SE, SM>(
        data_dir: std::path::PathBuf,
        data_dir_lock: crate::node::DataDirLock,
        storage_engine: Arc<SE>,
        state_machine: Arc<SM>,
        shutdown_rx: watch::Receiver<()>,
        config: d_engine_core::RaftNodeConfig,
    ) -> Result<()>
    where
        SE: StorageEngine + std::fmt::Debug + 'static,
        SM: StateMachine + std::fmt::Debug + 'static,
    {
        Self::start_node_inner(
            data_dir,
            Some(data_dir_lock),
            storage_engine,
            state_machine,
            shutdown_rx,
            config,
        )
        .await
    }

    async fn start_node_inner<SE, SM>(
        data_dir: std::path::PathBuf,
        data_dir_lock: Option<crate::node::DataDirLock>,
        storage_engine: Arc<SE>,
        state_machine: Arc<SM>,
        shutdown_rx: watch::Receiver<()>,
        config: d_engine_core::RaftNodeConfig,
    ) -> Result<()>
    where
        SE: StorageEngine + std::fmt::Debug + 'static,
        SM: StateMachine + std::fmt::Debug + 'static,
    {
        let mut builder = NodeBuilder::init(data_dir, config.validate()?, shutdown_rx)
            .storage_engine(storage_engine)
            .state_machine(state_machine);
        if let Some(lock) = data_dir_lock {
            builder = builder.data_dir_lock(lock);
        } else {
            tracing::warn!(
                "storage_engine/state_machine were built before d-engine's data_dir \
                 lock — their concurrency safety is the caller's responsibility"
            );
        }
        let node = builder.start().await?;
        node.run().await
    }
}

#[cfg(test)]
#[path = "standalone_test.rs"]
mod tests;
