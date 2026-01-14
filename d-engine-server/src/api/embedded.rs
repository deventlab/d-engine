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
//! let engine = EmbeddedEngine::start().await?;
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
//!
//! # Application Responsibilities in Multi-Node Deployments
//!
//! EmbeddedEngine provides **cluster state APIs** but does NOT handle request routing.
//! Applications are responsible for handling follower write requests.
//!
//! ## What EmbeddedEngine Provides
//!
//! - ✅ `is_leader()` - Check if current node is leader
//! - ✅ `leader_info()` - Get leader ID and term
//! - ✅ `LocalKvClient` returns `NotLeader` error on follower writes
//! - ✅ Zero-overhead in-process communication (<0.1ms)
//!
//! ## What Applications Must Handle
//!
//! - ❌ Request routing (follower → leader forwarding)
//! - ❌ Load balancing across nodes
//! - ❌ Health check endpoints for load balancers
//!
//! ## Integration Patterns
//!
//! ### Pattern 1: Load Balancer Health Checks (Recommended for HA)
//!
//! **Application provides HTTP health check endpoints:**
//! ```ignore
//! use axum::{Router, routing::get, http::StatusCode};
//!
//! // Health check endpoint for load balancer (e.g. HAProxy)
//! async fn health_primary(engine: Arc<EmbeddedEngine>) -> StatusCode {
//!     if engine.is_leader() {
//!         StatusCode::OK  // 200 - Load balancer routes writes here
//!     } else {
//!         StatusCode::SERVICE_UNAVAILABLE  // 503 - Load balancer skips this node
//!     }
//! }
//!
//! async fn health_replica(engine: Arc<EmbeddedEngine>) -> StatusCode {
//!     if !engine.is_leader() {
//!         StatusCode::OK  // 200 - Load balancer routes reads here
//!     } else {
//!         StatusCode::SERVICE_UNAVAILABLE  // 503
//!     }
//! }
//!
//! let app = Router::new()
//!     .route("/primary", get(health_primary))  // For write traffic
//!     .route("/replica", get(health_replica)); // For read traffic
//!
//! axum::Server::bind(&"0.0.0.0:8008".parse()?)
//!     .serve(app.into_make_service())
//!     .await?;
//! ```
//!
//! ### Pattern 2: Pre-check Before Write (Simple)
//!
//! **Application checks leadership before handling writes:**
//! ```ignore
//! async fn handle_write_request(engine: &EmbeddedEngine, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
//!     if !engine.is_leader() {
//!         // Return HTTP 503 to client
//!         return Err("Not leader, please retry on another node");
//!     }
//!
//!     // Safe to write (this node is leader)
//!     engine.client().put(key, value).await?;
//!     Ok(())
//! }
//! ```
//!
//! ### Pattern 3: HTTP 307 Redirect (Alternative)
//!
//! **Application returns redirect to leader:**
//! ```ignore
//! async fn handle_write_request(engine: &EmbeddedEngine) -> Response {
//!     match engine.client().put(key, value).await {
//!         Ok(_) => Response::ok(),
//!         Err(LocalClientError::NotLeader { leader_address: Some(addr), .. }) => {
//!             // Redirect client to leader
//!             Response::redirect_307(addr)
//!         }
//!         Err(e) => Response::error(e),
//!     }
//! }
//! ```
//!
//! ## Design Philosophy
//!
//! **Why doesn't EmbeddedEngine auto-forward writes?**
//!
//! 1. **Preserves zero-overhead guarantee** - Auto-forwarding requires gRPC client (adds network/serialization)
//! 2. **Maintains simplicity** - No hidden network calls in "embedded" mode
//! 3. **Flexible deployment** - Applications choose load balancer health checks, HTTP redirect, or other strategies
//! 4. **Performance transparency** - Developers know exactly when network calls occur
//!
//! For auto-forwarding with gRPC overhead, use standalone mode with `GrpcKvClient`.

use std::sync::Arc;

#[cfg(feature = "watch")]
use bytes::Bytes;
#[cfg(feature = "watch")]
use d_engine_core::watch::WatchRegistry;
#[cfg(feature = "watch")]
use d_engine_core::watch::WatcherHandle;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::error;
use tracing::info;

use crate::Result;
#[cfg(feature = "rocksdb")]
use crate::RocksDBStateMachine;
#[cfg(feature = "rocksdb")]
use crate::RocksDBStorageEngine;
use crate::StateMachine;
use crate::StorageEngine;
use crate::node::LocalKvClient;
use crate::node::NodeBuilder;

/// Embedded d-engine with automatic lifecycle management.
///
/// Provides high-level KV API for embedded usage:
/// - `start()` / `start_with()` - Initialize and spawn node
/// - `wait_ready()` - Wait for leader election
/// - `client()` - Get local KV client
/// - `stop()` - Graceful shutdown
///
/// # Example
/// ```ignore
/// use d_engine::EmbeddedEngine;
/// use std::time::Duration;
///
/// let engine = EmbeddedEngine::start().await?;
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
    /// Start engine with configuration from environment.
    ///
    /// Reads `CONFIG_PATH` environment variable or uses default configuration.
    /// Data directory is determined by config's `cluster.db_root_dir` setting.
    ///
    /// # Example
    /// ```ignore
    /// // Set config path via environment variable
    /// std::env::set_var("CONFIG_PATH", "/etc/d-engine/production.toml");
    ///
    /// let engine = EmbeddedEngine::start().await?;
    /// engine.wait_ready(Duration::from_secs(5)).await?;
    /// ```
    #[cfg(feature = "rocksdb")]
    pub async fn start() -> Result<Self> {
        let config = d_engine_core::RaftNodeConfig::new()?.validate()?;
        let base_dir = &config.cluster.db_root_dir;
        tokio::fs::create_dir_all(base_dir)
            .await
            .map_err(|e| crate::Error::Fatal(format!("Failed to create data directory: {e}")))?;

        let storage_path = base_dir.join("storage");
        let sm_path = base_dir.join("state_machine");

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let mut sm = RocksDBStateMachine::new(sm_path)?;

        // Inject lease if enabled
        let lease_cfg = &config.raft.state_machine.lease;
        if lease_cfg.enabled {
            let lease = Arc::new(crate::storage::DefaultLease::new(lease_cfg.clone()));
            sm.set_lease(lease);
        }

        let sm = Arc::new(sm);

        info!("Starting embedded engine with RocksDB at {:?}", base_dir);

        Self::start_custom(storage, sm, None).await
    }

    /// Start engine with explicit configuration file.
    ///
    /// Reads configuration from specified file path.
    /// Data directory is determined by config's `cluster.db_root_dir` setting.
    ///
    /// # Arguments
    /// - `config_path`: Path to configuration file (e.g. "d-engine.toml")
    ///
    /// # Example
    /// ```ignore
    /// let engine = EmbeddedEngine::start_with("config/node1.toml").await?;
    /// engine.wait_ready(Duration::from_secs(5)).await?;
    /// ```
    #[cfg(feature = "rocksdb")]
    pub async fn start_with(config_path: &str) -> Result<Self> {
        let config = d_engine_core::RaftNodeConfig::new()?
            .with_override_config(config_path)?
            .validate()?;
        let base_dir = std::path::PathBuf::from(&config.cluster.db_root_dir);

        tokio::fs::create_dir_all(&base_dir)
            .await
            .map_err(|e| crate::Error::Fatal(format!("Failed to create data directory: {e}")))?;

        let storage_path = base_dir.join("storage");
        let sm_path = base_dir.join("state_machine");

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let mut sm = RocksDBStateMachine::new(sm_path)?;

        // Inject lease if enabled
        let lease_cfg = &config.raft.state_machine.lease;
        if lease_cfg.enabled {
            let lease = Arc::new(crate::storage::DefaultLease::new(lease_cfg.clone()));
            sm.set_lease(lease);
        }

        let sm = Arc::new(sm);

        info!("Starting embedded engine with RocksDB at {:?}", base_dir);

        Self::start_custom(storage, sm, Some(config_path)).await
    }

    /// Start engine with custom storage and state machine.
    ///
    /// Advanced API for users providing custom storage implementations.
    ///
    /// # Arguments
    /// - `config_path`: Optional path to configuration file
    /// - `storage_engine`: Custom storage engine implementation
    /// - `state_machine`: Custom state machine implementation
    ///
    /// # Example
    /// ```ignore
    /// let storage = Arc::new(MyCustomStorage::new()?);
    /// let sm = Arc::new(MyCustomStateMachine::new()?);
    /// let engine = EmbeddedEngine::start_custom(storage, sm, None).await?;
    /// ```
    pub async fn start_custom<SE, SM>(
        storage_engine: Arc<SE>,
        state_machine: Arc<SM>,
        config_path: Option<&str>,
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
            d_engine_core::RaftNodeConfig::default()
                .with_override_config(path)?
                .validate()?
        } else {
            d_engine_core::RaftNodeConfig::new()?.validate()?
        };

        // Build node and start RPC server
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

        info!("Embedded d-engine started successfully");

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
    /// # Timeout Guidelines
    ///
    /// **Single-node mode** (most common for development):
    /// - Typical: <100ms (near-instant election)
    /// - Recommended: `Duration::from_secs(3)`
    ///
    /// **Multi-node HA cluster** (production):
    /// - Typical: 1-3s (depends on network latency and `general_raft_timeout_duration_in_ms`)
    /// - Recommended: `Duration::from_secs(10)`
    ///
    /// **Special cases**:
    /// - Health checks: `Duration::from_secs(3)` (fail fast if cluster unhealthy)
    /// - Startup scripts: `Duration::from_secs(30)` (allow time for cluster stabilization)
    /// - Development/testing: `Duration::from_secs(5)` (balance between speed and reliability)
    ///
    /// # Returns
    /// - `Ok(LeaderInfo)` - Leader elected successfully
    /// - `Err(...)` - Timeout or cluster unavailable
    ///
    /// # Example
    /// ```ignore
    /// // Single-node development
    /// let engine = EmbeddedEngine::start().await?;
    /// let leader = engine.wait_ready(Duration::from_secs(3)).await?;
    ///
    /// // Multi-node production
    /// let engine = EmbeddedEngine::start_with("cluster.toml").await?;
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

    /// Returns true if the current node is the Raft leader.
    ///
    /// # Use Cases
    /// - Load balancer health checks (e.g. HAProxy `/primary` endpoint returning HTTP 200/503)
    /// - Prevent write requests to followers before they fail
    /// - Application-level request routing decisions
    ///
    /// # Performance
    /// Zero-cost operation (reads cached leader state from watch channel)
    ///
    /// # Example
    /// ```ignore
    /// // Load balancer health check endpoint
    /// #[get("/primary")]
    /// async fn health_primary(engine: &EmbeddedEngine) -> StatusCode {
    ///     if engine.is_leader() {
    ///         StatusCode::OK  // Routes writes here
    ///     } else {
    ///         StatusCode::SERVICE_UNAVAILABLE
    ///     }
    /// }
    ///
    /// // Application request handler
    /// if engine.is_leader() {
    ///     client.put(key, value).await?;
    /// } else {
    ///     return Err("Not leader, write rejected");
    /// }
    /// ```
    pub fn is_leader(&self) -> bool {
        self.leader_elected_rx
            .borrow()
            .as_ref()
            .map(|info| info.leader_id == self.kv_client.node_id())
            .unwrap_or(false)
    }

    /// Returns current leader information if available.
    ///
    /// # Returns
    /// - `Some(LeaderInfo)` if a leader is elected (includes leader_id and term)
    /// - `None` if no leader exists (during election or network partition)
    ///
    /// # Use Cases
    /// - Monitoring dashboards showing cluster state
    /// - Debugging leader election issues
    /// - Logging cluster topology changes
    ///
    /// # Example
    /// ```ignore
    /// if let Some(info) = engine.leader_info() {
    ///     println!("Leader: {} (term {})", info.leader_id, info.term);
    /// } else {
    ///     println!("No leader elected, cluster unavailable");
    /// }
    /// ```
    pub fn leader_info(&self) -> Option<crate::LeaderInfo> {
        *self.leader_elected_rx.borrow()
    }

    /// Get a reference to the local KV client.
    ///
    /// The client is available immediately after `start()`,
    /// but requests will only succeed after `wait_ready()` completes.
    ///
    /// # Example
    /// ```ignore
    /// let engine = EmbeddedEngine::start().await?;
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
    /// let engine = EmbeddedEngine::start().await?;
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
                    info!("Embedded d-engine stopped successfully");
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

    /// Returns the node ID for testing purposes.
    ///
    /// Useful in integration tests that need to identify which node
    /// they're interacting with, especially in multi-node scenarios.
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
