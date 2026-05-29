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
//! // Node does not provide client directly - use EmbeddedEngine instead
//! tokio::spawn(async move { node.run().await });
//! // Manual lifecycle management required
//! ```
//!
//! ### Using EmbeddedEngine (High-level API)
//! ```ignore
//! let engine = EmbeddedEngine::start("./data/my-app").await?;
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
//! - ✅ `EmbeddedClient` returns `NotLeader` error on follower writes
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
//! For auto-forwarding with gRPC overhead, use standalone mode with `GrpcClient`.

use super::embedded_client::EmbeddedClient;
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
use crate::node::RaftTypeConfig;
use d_engine_core::TypeConfig;
use d_engine_core::read_actor::run_read_actor;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::error;
use tracing::info;

struct Inner<T: TypeConfig> {
    node_handle: Mutex<Option<JoinHandle<Result<()>>>>,
    shutdown_tx: watch::Sender<()>,
    client: Arc<EmbeddedClient<T>>,
    leader_elected_rx: watch::Receiver<Option<crate::LeaderInfo>>,
    membership_rx: watch::Receiver<crate::membership::MembershipSnapshot>,
    is_stopped: Mutex<bool>,
    node_id: u32,
    /// ReadActor task handle. Aborted during stop() before the Raft node shuts down,
    /// ensuring Arc<SM> (and the RocksDB LOCK) is released before stop() returns.
    read_actor_handle: Mutex<Option<JoinHandle<()>>>,
}

/// Embedded d-engine with automatic lifecycle management.
///
/// **Thread-safe**: Clone and share across threads freely.
/// All methods use `&self` - safe to call from multiple contexts.
///
/// Provides high-level KV API for embedded usage:
/// - `start()` / `start_with()` - Initialize and spawn node
/// - `wait_ready()` - Wait for leader election
/// - `client()` - Get embedded client
/// - `stop()` - Graceful shutdown
///
/// # Example
/// ```ignore
/// use d_engine::EmbeddedEngine;
/// use std::time::Duration;
///
/// let engine = EmbeddedEngine::start("./data/my-app").await?;
/// engine.wait_ready(Duration::from_secs(5)).await?;
///
/// let client = engine.client();
/// client.put(b"key", b"value").await?;
///
/// engine.stop().await?;
/// ```
pub struct EmbeddedEngine<T: TypeConfig> {
    inner: Arc<Inner<T>>,
}

impl<T: TypeConfig> Clone for EmbeddedEngine<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// ─── RocksDB default start ───────────────────────────────────────────────────

#[cfg(feature = "rocksdb")]
impl EmbeddedEngine<RaftTypeConfig<RocksDBStorageEngine, RocksDBStateMachine>> {
    /// Start engine with an explicit data directory.
    ///
    /// `data_dir` has highest priority and always overrides `cluster.db_root_dir` from
    /// `CONFIG_PATH` or `RAFT__` environment variables. Other configuration (network,
    /// Raft timeouts, cluster topology) is still read from those sources if set.
    ///
    /// The directory is created automatically if it does not exist.
    /// If it already contains data the engine opens it in place (idempotent).
    ///
    /// # Example
    /// ```ignore
    /// // Minimal — just supply a path
    /// let engine = EmbeddedEngine::start("./data/my-app").await?;
    /// engine.wait_ready(Duration::from_secs(5)).await?;
    ///
    /// // Works with any AsRef<Path>
    /// let engine = EmbeddedEngine::start(std::path::Path::new("/var/lib/my-app")).await?;
    /// ```
    pub async fn start(data_dir: impl AsRef<std::path::Path>) -> Result<Self> {
        let mut config = d_engine_core::RaftNodeConfig::new()?;
        config.cluster.db_root_dir = data_dir.as_ref().to_path_buf();
        let config = config.validate()?;

        let base_dir = config.cluster.db_root_dir.clone();
        tokio::fs::create_dir_all(&base_dir)
            .await
            .map_err(|e| crate::Error::Fatal(format!("Failed to create data directory: {e}")))?;

        let (storage, mut sm) = if config.storage.unified_db {
            let db_path = base_dir.join("db");
            info!(
                "Starting embedded engine with unified RocksDB at {:?}",
                db_path
            );
            RocksDBUnifiedEngine::open(&db_path)?
        } else {
            info!(
                "Starting embedded engine with separate RocksDB instances at {:?}",
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

        Self::start_node(config, Arc::new(storage), Arc::new(sm)).await
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
    pub async fn start_with(config_path: &str) -> Result<Self> {
        let config = d_engine_core::RaftNodeConfig::new()?
            .with_override_config(config_path)?
            .validate()?;
        let base_dir = std::path::PathBuf::from(&config.cluster.db_root_dir);

        tokio::fs::create_dir_all(&base_dir)
            .await
            .map_err(|e| crate::Error::Fatal(format!("Failed to create data directory: {e}")))?;

        let (storage, mut sm) = if config.storage.unified_db {
            let db_path = base_dir.join("db");
            info!(
                "Starting embedded engine with unified RocksDB at {:?}",
                db_path
            );
            RocksDBUnifiedEngine::open(&db_path)?
        } else {
            info!(
                "Starting embedded engine with separate RocksDB instances at {:?}",
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

        Self::start_custom(Arc::new(storage), Arc::new(sm), Some(config_path)).await
    }
}

// ─── Custom SE/SM: start_custom + start_node ─────────────────────────────────

impl<SE, SM> EmbeddedEngine<RaftTypeConfig<SE, SM>>
where
    SE: StorageEngine + Debug + 'static,
    SM: StateMachine + Debug + 'static,
{
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
    pub async fn start_custom(
        storage_engine: Arc<SE>,
        state_machine: Arc<SM>,
        config_path: Option<&str>,
    ) -> Result<Self> {
        let node_config = if let Some(path) = config_path {
            d_engine_core::RaftNodeConfig::default()
                .with_override_config(path)?
                .validate()?
        } else {
            d_engine_core::RaftNodeConfig::new()?.validate()?
        };

        Self::start_node(node_config, storage_engine, state_machine).await
    }

    /// Build and launch the node from a validated config and pre-built storage.
    async fn start_node(
        node_config: d_engine_core::RaftNodeConfig,
        storage_engine: Arc<SE>,
        state_machine: Arc<SM>,
    ) -> Result<Self> {
        info!("Starting embedded d-engine");
        d_engine_core::init_clock();

        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let node = NodeBuilder::init(node_config, shutdown_rx)
            .storage_engine(storage_engine)
            .state_machine(state_machine.clone())
            .start()
            .await?;

        let leader_elected_rx = node.leader_change_notifier();
        let membership_rx = node.membership_change_notifier();
        let read_lease = node.read_lease();

        // Start ReadActor: sole owner of Arc<SM> on the read path.
        let (read_tx, read_rx) = mpsc::channel(node.node_config.raft.read_actor.channel_capacity);
        let max_drain = node.node_config.raft.read_actor.max_drain;
        let read_actor_handle = tokio::spawn(run_read_actor(
            read_rx,
            read_lease,
            state_machine,
            max_drain,
        ));

        let client = {
            let base = EmbeddedClient::new_internal(
                node.event_tx.clone(),
                node.cmd_tx.clone(),
                node.node_id,
                Duration::from_millis(node.node_config.raft.general_raft_timeout_duration_in_ms),
            )
            .with_read_actor(read_tx);

            #[cfg(feature = "watch")]
            let base = {
                let mut c = base;
                if let Some(registry) = &node.watch_registry {
                    c = c.with_watch_registry(registry.clone());
                }
                c
            };

            Arc::new(base)
        };

        let node_id = node.node_id();

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
            inner: Arc::new(Inner {
                node_handle: Mutex::new(Some(node_handle)),
                shutdown_tx,
                client,
                leader_elected_rx,
                membership_rx,
                is_stopped: Mutex::new(false),
                node_id,
                read_actor_handle: Mutex::new(Some(read_actor_handle)),
            }),
        })
    }
}

// ─── General API (all TypeConfig) ────────────────────────────────────────────

impl<T: TypeConfig> EmbeddedEngine<T> {
    /// Wait until the cluster is ready to serve requests.
    ///
    /// Blocks until a leader has been elected **and** its no-op entry is committed
    /// by a majority of nodes (Raft §8). Only at this point is the leader guaranteed
    /// to be aware of all previously committed entries and safe to serve reads and writes.
    ///
    /// Event-driven notification (no polling), <1ms latency after noop commit.
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
    /// let engine = EmbeddedEngine::start("./data/my-app").await?;
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
        let mut rx = self.inner.leader_elected_rx.clone();

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
        self.inner.leader_elected_rx.clone()
    }

    /// Subscribe to committed membership change notifications.
    ///
    /// Returns a `watch::Receiver` that fires whenever a `ConfChange` entry
    /// (node join, removal, or learner promotion) is committed by a majority.
    ///
    /// All nodes — leader, follower, and learner — fire the notification because
    /// every node walks the same `CommitHandler::apply_config_change()` path.
    ///
    /// The first `borrow()` returns the current membership state immediately
    /// without waiting for a change.
    ///
    /// ## Distinguishing this from other notifiers
    ///
    /// - [`Self::leader_change_notifier`]: fires on leader election changes, **not** membership changes
    /// - [`Self::wait_ready`]: resolves when a leader is elected and the cluster is ready, **not** membership changes
    ///
    /// ## Usage
    /// ```ignore
    /// let mut rx = engine.watch_membership();
    /// while rx.changed().await.is_ok() {
    ///     let snapshot = rx.borrow_and_update().clone();
    ///     // snapshot.members — current voters
    ///     // snapshot.learners — current non-voting learners
    ///     // snapshot.committed_index — idempotency key
    ///     scheduler.on_membership_changed(snapshot).await;
    /// }
    /// ```
    pub fn watch_membership(&self) -> watch::Receiver<crate::membership::MembershipSnapshot> {
        self.inner.membership_rx.clone()
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
        self.inner
            .leader_elected_rx
            .borrow()
            .as_ref()
            .map(|info| info.leader_id == self.inner.node_id)
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
        *self.inner.leader_elected_rx.borrow()
    }

    /// Get a reference to the local KV client.
    ///
    /// The client is available immediately after `start()`,
    /// but requests will only succeed after `wait_ready()` completes.
    ///
    /// # Example
    /// ```ignore
    /// let engine = EmbeddedEngine::start("./data/my-app").await?;
    /// engine.wait_ready(Duration::from_secs(5)).await?;
    /// let client = engine.client();
    /// client.put(b"key", b"value").await?;
    /// ```
    pub fn client(&self) -> Arc<EmbeddedClient<T>> {
        Arc::clone(&self.inner.client)
    }

    /// Stop the embedded d-engine gracefully (idempotent).
    ///
    /// This method:
    /// 1. Sends shutdown signal to node
    /// 2. Waits for node.run() to complete
    /// 3. Propagates any errors from node execution
    ///
    /// Safe to call multiple times - subsequent calls are no-ops.
    ///
    /// # Errors
    /// Returns error if node encountered issues during shutdown.
    ///
    /// # Example
    /// ```ignore
    /// engine.stop().await?;
    /// engine.stop().await?;  // No-op, returns Ok(())
    /// ```
    pub async fn stop(&self) -> Result<()> {
        let mut is_stopped = self.inner.is_stopped.lock().await;
        if *is_stopped {
            return Ok(());
        }

        info!("Stopping embedded d-engine");

        // Abort ReadActor first so Arc<SM> drops and RocksDB LOCK is released
        // before the Raft node exits. Any in-flight reads will get a channel-closed
        // error, which is acceptable during shutdown.
        let mut ra_handle = self.inner.read_actor_handle.lock().await;
        if let Some(handle) = ra_handle.take() {
            handle.abort();
            let _ = handle.await; // expect JoinError::Cancelled
        }
        drop(ra_handle);

        // Send shutdown signal to Raft node
        let _ = self.inner.shutdown_tx.send(());

        // Wait for node task to complete
        let mut handle_guard = self.inner.node_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            match handle.await {
                Ok(result) => {
                    info!("Embedded d-engine stopped successfully");
                    *is_stopped = true;
                    result
                }
                Err(e) => {
                    error!("Node task panicked: {:?}", e);
                    *is_stopped = true;
                    Err(crate::Error::Fatal(format!("Node task panicked: {e}")))
                }
            }
        } else {
            *is_stopped = true;
            Ok(())
        }
    }

    /// Check if the engine has been stopped.
    ///
    /// # Example
    /// ```ignore
    /// if engine.is_stopped() {
    ///     println!("Engine is stopped");
    /// }
    /// ```
    pub async fn is_stopped(&self) -> bool {
        *self.inner.is_stopped.lock().await
    }

    /// Returns the unique identifier for this Raft node.
    pub fn node_id(&self) -> u32 {
        self.inner.node_id
    }
}

impl<T: TypeConfig> Drop for EmbeddedEngine<T> {
    fn drop(&mut self) {
        // Warn if stop() was not called
        if let Ok(handle) = self.inner.node_handle.try_lock()
            && let Some(h) = &*handle
            && !h.is_finished()
        {
            error!("EmbeddedEngine dropped without calling stop() - background task may leak");
        }
    }
}
