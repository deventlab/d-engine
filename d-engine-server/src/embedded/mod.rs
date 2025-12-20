//! Embedded mode for d-engine - application-friendly API
//!
//! This module provides [`EmbeddedEngine`], a high-level wrapper around [`Node`]
//! that simplifies lifecycle management for embedded use cases.
//!
//! ## Comparison: Node vs EmbeddedEngine
//!
//! ### Using Node (Low-level API)
//! ```ignore
//! let node = NodeBuilder::new(config).start().await?;
//! let client = node.local_client();
//! tokio::spawn(async move { node.run().await });
//! // Manual lifecycle management required
//! ```
//!
//! ### Using EmbeddedEngine (High-level API)
//! ```ignore
//! let engine = EmbeddedEngine::start(config).await?;
//! engine.wait_ready(Duration::from_secs(5)).await?;
//! let client = engine.client();
//! engine.stop().await?;
//! // Lifecycle managed automatically
//! ```
//!
//! ## When to Use
//!
//! - **EmbeddedEngine**: Application developers who want simplicity
//! - **Node**: Framework developers who need fine-grained control

#[cfg(test)]
mod mod_test;

use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::node::{LocalKvClient, NodeBuilder};
use crate::{Result, StateMachine, StorageEngine};
use bytes::Bytes;
#[cfg(feature = "watch")]
use d_engine_core::watch::{WatchRegistry, WatcherHandle};

#[cfg(feature = "rocksdb")]
use crate::{RocksDBStateMachine, RocksDBStorageEngine};

/// Embedded d-engine with automatic lifecycle management.
///
/// Provides a high-level KV API for embedded usage:
/// - `start()` - Build and spawn node in background
/// - `ready()` - Wait for election success
/// - `client()` - Get local KV client
/// - `stop()` - Graceful shutdown
///
/// # Example
/// ```ignore
/// use d_engine::EmbeddedEngine;
///
/// let engine = EmbeddedEngine::start(config).await?;
/// engine.wait_ready(Duration::from_secs(5)).await?;
///
/// let client = engine.client();
/// client.put(b"key", b"value").await?;
///
/// engine.stop().await?;
/// ```
pub struct EmbeddedEngine {
    node_handle: Option<JoinHandle<Result<()>>>,
    shutdown_tx: watch::Sender<()>,
    kv_client: LocalKvClient,
    leader_elected_rx: watch::Receiver<Option<crate::LeaderInfo>>,
    #[cfg(feature = "watch")]
    watch_registry: Option<Arc<WatchRegistry>>,
}

impl EmbeddedEngine {
    /// Quick-start: create embedded engine with RocksDB defaults.
    ///
    /// This is the simplest way to start d-engine for development and testing.
    /// Automatically creates necessary directories and uses RocksDB storage.
    ///
    /// # Arguments
    /// - `data_dir`: Base directory for all data (defaults to "/tmp/d-engine" if empty)
    /// - `config_path`: Optional path to config file (e.g. "d-engine.toml")
    ///
    /// # Example
    /// ```ignore
    /// // Use default /tmp location and default config
    /// let engine = EmbeddedEngine::with_rocksdb("", None).await?;
    ///
    /// // Or specify custom directory and config
    /// let engine = EmbeddedEngine::with_rocksdb("./my-data", Some("config.toml")).await?;
    /// ```
    #[cfg(feature = "rocksdb")]
    pub async fn with_rocksdb<P: AsRef<std::path::Path>>(
        data_dir: P,
        config_path: Option<&str>,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();

        // Use /tmp/d-engine if empty path provided
        let base_dir = if data_dir.as_os_str().is_empty() {
            std::path::PathBuf::from("/tmp/d-engine")
        } else {
            data_dir.to_path_buf()
        };

        // Auto-create all necessary directories
        tokio::fs::create_dir_all(&base_dir)
            .await
            .map_err(|e| crate::Error::Fatal(format!("Failed to create data directory: {e}")))?;

        let storage_path = base_dir.join("storage");
        let sm_path = base_dir.join("state_machine");

        // Create storage and state machine with RocksDB
        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

        info!("Starting embedded engine with RocksDB at {:?}", base_dir);

        Self::start(config_path, storage, state_machine).await
    }

    /// Start the embedded engine with custom storage.
    ///
    /// For advanced users who want to provide custom storage implementations
    /// or use non-default storage engines.
    ///
    /// # Arguments
    /// - `config_path`: Optional path to config file
    /// - `storage_engine`: Custom storage engine implementation
    /// - `state_machine`: Custom state machine implementation
    ///
    /// # Example
    /// ```ignore
    /// let storage = Arc::new(MyCustomStorage::new()?);
    /// let sm = Arc::new(MyCustomStateMachine::new()?);
    /// let engine = EmbeddedEngine::start(None, storage, sm).await?;
    /// ```
    pub async fn start<SE, SM>(
        config_path: Option<&str>,
        storage_engine: Arc<SE>,
        state_machine: Arc<SM>,
    ) -> Result<Self>
    where
        SE: StorageEngine + std::fmt::Debug + 'static,
        SM: StateMachine + std::fmt::Debug + 'static,
    {
        info!("Starting embedded d-engine");

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(());

        // Load config or use default
        let node_config = if let Some(path) = config_path {
            d_engine_core::RaftNodeConfig::new()?.with_override_config(path)?
        } else {
            d_engine_core::RaftNodeConfig::new()?
        };

        // Build node and start RPC server (required for cluster communication)
        let node = NodeBuilder::init(node_config, shutdown_rx)
            .storage_engine(storage_engine)
            .state_machine(state_machine)
            .start()
            .await?;

        // Get leader change notifier before moving node
        let leader_elected_rx = node.leader_change_notifier();

        // Create local KV client before spawning
        let kv_client = node.local_client();

        // Capture watch registry (if enabled)
        #[cfg(feature = "watch")]
        let watch_registry = node.watch_registry.clone();

        // Spawn node.run() in background
        let node_handle = tokio::spawn(async move {
            if let Err(e) = node.run().await {
                error!("Node run error: {:?}", e);
                Err(e)
            } else {
                Ok(())
            }
        });

        info!("Embedded d-engine started (background task spawned)");

        Ok(Self {
            node_handle: Some(node_handle),
            shutdown_tx,
            kv_client,
            leader_elected_rx,
            #[cfg(feature = "watch")]
            watch_registry,
        })
    }

    /// Wait for leader election to complete.
    ///
    /// Blocks until a leader has been elected in the cluster.
    /// Event-driven notification (no polling), <1ms latency.
    ///
    /// # Timeout
    /// - Single-node: Returns immediately (<100ms)
    /// - Multi-node: May take seconds depending on network
    ///
    /// # Example
    /// ```ignore
    /// let engine = EmbeddedEngine::start(config).await?;
    /// let leader = engine.wait_ready(Duration::from_secs(10)).await?;
    /// println!("Leader elected: {} (term {})", leader.leader_id, leader.term);
    /// ```
    pub async fn wait_ready(
        &self,
        timeout: std::time::Duration,
    ) -> Result<crate::LeaderInfo> {
        let mut rx = self.leader_elected_rx.clone();

        tokio::time::timeout(timeout, async {
            // Check current value first (leader may already be elected)
            if let Some(info) = rx.borrow().as_ref() {
                info!(
                    "Leader already elected: {} (term {})",
                    info.leader_id, info.term
                );
                return Ok(*info);
            }

            loop {
                // Wait for leader election event (event-driven, no polling)
                let _ = rx.changed().await;

                // Check if a leader is elected
                if let Some(info) = rx.borrow().as_ref() {
                    info!("Leader elected: {} (term {})", info.leader_id, info.term);
                    return Ok(*info);
                }
            }
        })
        .await
        .map_err(|_| crate::Error::Fatal("Leader election timeout".to_string()))?
    }

    /// Subscribe to leader change notifications.
    ///
    /// Returns a receiver that will be notified whenever:
    /// - First leader is elected
    /// - Leader changes (re-election)
    /// - No leader exists (during election)
    ///
    /// # Performance
    /// Event-driven notification (no polling), <1ms latency
    ///
    /// # Example
    /// ```ignore
    /// let mut leader_rx = engine.leader_change_notifier();
    /// tokio::spawn(async move {
    ///     while leader_rx.changed().await.is_ok() {
    ///         match leader_rx.borrow().as_ref() {
    ///             Some(info) => println!("Leader: {} (term {})", info.leader_id, info.term),
    ///             None => println!("No leader"),
    ///         }
    ///     }
    /// });
    /// ```
    pub fn leader_change_notifier(&self) -> watch::Receiver<Option<crate::LeaderInfo>> {
        self.leader_elected_rx.clone()
    }

    /// Get a reference to the local KV client.
    ///
    /// The client is available immediately after `start()`,
    /// but requests will only succeed after `wait_ready()` completes.
    ///
    /// # Example
    /// ```ignore
    /// let engine = EmbeddedEngine::start(config).await?;
    /// engine.wait_ready(Duration::from_secs(5)).await?;
    /// let client = engine.client();
    /// client.put(b"key", b"value").await?;
    /// ```
    pub fn client(&self) -> &LocalKvClient {
        &self.kv_client
    }

    /// Register a watcher for a specific key.
    ///
    /// Returns a handle that receives watch events via an mpsc channel.
    /// The watcher is automatically unregistered when the handle is dropped.
    ///
    /// # Arguments
    /// * `key` - The exact key to watch
    ///
    /// # Returns
    /// * `Result<WatcherHandle>` - Handle for receiving events
    ///
    /// # Example
    /// ```ignore
    /// let engine = EmbeddedEngine::start(config).await?;
    /// let mut handle = engine.watch(b"mykey")?;
    /// while let Some(event) = handle.receiver_mut().recv().await {
    ///     println!("Key changed: {:?}", event);
    /// }
    /// ```
    #[cfg(feature = "watch")]
    pub fn watch(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<WatcherHandle> {
        let registry = self.watch_registry.as_ref().ok_or_else(|| {
            crate::Error::Fatal(
                "Watch feature disabled (WatchRegistry not initialized)".to_string(),
            )
        })?;

        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        Ok(registry.register(key_bytes))
    }

    /// Gracefully stop the embedded engine.
    ///
    /// This method:
    /// 1. Sends shutdown signal to node
    /// 2. Waits for node.run() to complete
    /// 3. Propagates any errors from node execution
    ///
    /// # Errors
    /// Returns error if node encountered issues during shutdown.
    ///
    /// # Example
    /// ```ignore
    /// engine.stop().await?;
    /// ```
    pub async fn stop(mut self) -> Result<()> {
        info!("Stopping embedded d-engine");

        // Send shutdown signal
        let _ = self.shutdown_tx.send(());

        // Wait for node task to complete
        if let Some(handle) = self.node_handle.take() {
            match handle.await {
                Ok(result) => {
                    info!("Embedded d-engine stopped");
                    result
                }
                Err(e) => {
                    error!("Node task panicked: {:?}", e);
                    Err(crate::Error::Fatal(format!("Node task panicked: {e}")))
                }
            }
        } else {
            Ok(())
        }
    }

    /// Returns the node ID for testing purposes
    ///
    /// This is useful in integration tests that need to identify which node
    /// they're interacting with, especially in multi-node scenarios like
    /// failover testing.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn node_id(&self) -> u32 {
        self.kv_client.node_id()
    }
}

impl Drop for EmbeddedEngine {
    fn drop(&mut self) {
        // Warn if stop() was not called
        if let Some(handle) = &self.node_handle {
            if !handle.is_finished() {
                error!("EmbeddedEngine dropped without calling stop() - background task may leak");
            }
        }
    }
}
