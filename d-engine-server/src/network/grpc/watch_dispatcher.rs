//! Watch stream dispatcher for managing all watch gRPC streams
//!
//! The WatchDispatcher is a centralized manager for all active watch streams.
//! It runs as a single long-lived background task (spawned in NodeBuilder::build())
//! and spawns individual tasks for each watch client.
//!
//! # Architecture
//!
//! ```text
//! NodeBuilder::build()
//!   └─> spawn_watch_dispatcher() [1 task]
//!         └─> WatchDispatcher::run()
//!               ├─> spawns task for Client A's watch
//!               ├─> spawns task for Client B's watch
//!               └─> spawns task for Client C's watch
//! ```
//!
//! # Benefits
//!
//! - **Centralized control**: All watch spawns go through dispatcher
//! - **Resource limits**: Can enforce max concurrent watches
//! - **Observability**: Track active watch count, metrics
//! - **Graceful shutdown**: Clean up all watches on shutdown
//!
//! # Resource Usage
//!
//! - Dispatcher itself: ~2KB (1 tokio task)
//! - Per watch client: ~2KB (1 tokio task per client)
//! - Auto cleanup when client disconnects

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tracing::info;

use d_engine_core::watch::{WatchManager, WatcherHandleGuard};

use super::WatchStreamHandler;

/// Request to register a new watch stream
pub struct WatchRegistration {
    /// Key to watch
    pub key: Bytes,

    /// Sender for gRPC responses back to client
    pub response_sender:
        mpsc::Sender<std::result::Result<d_engine_proto::client::WatchResponse, tonic::Status>>,

    /// Guard to keep watcher alive
    pub guard: WatcherHandleGuard,

    /// Event receiver from WatchManager
    pub event_receiver: mpsc::Receiver<d_engine_core::watch::WatchEvent>,

    /// Watcher ID
    pub watcher_id: u64,
}

/// Dispatcher for managing all watch streams
///
/// Runs as a single background task, spawning sub-tasks for each watch client.
pub struct WatchDispatcher {
    /// Watch manager reference
    #[allow(dead_code)]
    watch_manager: Arc<WatchManager>,

    /// Channel to receive new watch registration requests
    registration_rx: mpsc::Receiver<WatchRegistration>,

    /// Shutdown signal
    shutdown: watch::Receiver<()>,

    /// Counter for active watch streams (for observability)
    #[allow(dead_code)]
    active_count: Arc<AtomicU64>,
}

/// Handle for communicating with the dispatcher
#[derive(Clone)]
pub struct WatchDispatcherHandle {
    /// Channel to send registration requests
    registration_tx: mpsc::Sender<WatchRegistration>,

    /// Counter for active watch streams
    active_count: Arc<AtomicU64>,
}

impl WatchDispatcher {
    /// Create a new watch dispatcher
    ///
    /// Returns (dispatcher, handle) pair
    pub fn new(
        watch_manager: Arc<WatchManager>,
        shutdown: watch::Receiver<()>,
    ) -> (Self, WatchDispatcherHandle) {
        let (registration_tx, registration_rx) = mpsc::channel(1024);
        let active_count = Arc::new(AtomicU64::new(0));

        let dispatcher = Self {
            watch_manager,
            registration_rx,
            shutdown,
            active_count: active_count.clone(),
        };

        let handle = WatchDispatcherHandle {
            registration_tx,
            active_count,
        };

        (dispatcher, handle)
    }

    /// Run the dispatcher - processes registration requests and spawns watch handlers
    ///
    /// This is the main loop that runs for the lifetime of the node.
    /// It spawns a new task for each watch client that connects.
    pub async fn run(mut self) {
        info!("Watch dispatcher started");

        loop {
            tokio::select! {
                // New watch registration request
                Some(registration) = self.registration_rx.recv() => {
                    self.handle_registration(registration);
                }

                // Shutdown signal received
                _ = self.shutdown.changed() => {
                    info!("Watch dispatcher shutting down");
                    break;
                }
            }
        }

        info!("Watch dispatcher stopped");
    }

    /// Handle a new watch registration by spawning a task
    fn handle_registration(
        &self,
        registration: WatchRegistration,
    ) {
        let WatchRegistration {
            key,
            response_sender,
            guard,
            event_receiver,
            watcher_id,
        } = registration;

        // Create handler
        let handler = WatchStreamHandler::new(watcher_id, key.clone(), event_receiver);

        // Increment active count
        let prev_count = self.active_count.fetch_add(1, Ordering::Relaxed);

        info!(
            watcher_id = watcher_id,
            key = ?key,
            active_count = prev_count + 1,
            "Spawning watch stream handler"
        );

        // Spawn task for this watch stream
        let active_count = self.active_count.clone();
        tokio::spawn(async move {
            // Keep guard alive
            let _guard = guard;

            // Run handler
            handler.run(response_sender).await;

            // Decrement active count when done
            let prev = active_count.fetch_sub(1, Ordering::Relaxed);

            info!(
                watcher_id = watcher_id,
                active_count = prev - 1,
                "Watch stream handler completed"
            );

            // Guard drops here, triggering cleanup
        });
    }
}

impl WatchDispatcherHandle {
    /// Register a new watch stream
    ///
    /// Called by gRPC service when a new watch request arrives.
    /// Sends the registration request to the dispatcher.
    pub async fn register(
        &self,
        registration: WatchRegistration,
    ) -> Result<(), mpsc::error::SendError<WatchRegistration>> {
        self.registration_tx.send(registration).await
    }

    /// Get the current number of active watch streams
    #[allow(dead_code)]
    pub fn active_count(&self) -> u64 {
        self.active_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use d_engine_core::watch::WatchConfig;

    #[tokio::test]
    async fn test_dispatcher_creation() {
        let config = WatchConfig::default();
        let watch_manager = WatchManager::new(config);
        watch_manager.start();
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let (_dispatcher, handle) = WatchDispatcher::new(Arc::new(watch_manager), shutdown_rx);

        assert_eq!(handle.active_count(), 0);
    }

    #[tokio::test]
    async fn test_registration_channel() {
        let config = WatchConfig::default();
        let watch_manager = WatchManager::new(config);
        watch_manager.start();
        let wm = Arc::new(watch_manager);

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let (dispatcher, handle) = WatchDispatcher::new(wm.clone(), shutdown_rx);

        // Spawn dispatcher
        tokio::spawn(async move {
            dispatcher.run().await;
        });

        // Register a watch
        let key = Bytes::from("test_key");
        let watcher_handle = wm.register(key.clone()).await;
        let (watcher_id, watched_key, event_receiver, guard) = watcher_handle.into_receiver();

        let (tx, _rx) = mpsc::channel(32);

        let registration = WatchRegistration {
            key: watched_key,
            response_sender: tx,
            guard,
            event_receiver,
            watcher_id,
        };

        // Test that we can send registration successfully
        assert!(handle.register(registration).await.is_ok());

        // Cleanup
        drop(shutdown_tx);
    }

    #[tokio::test]
    async fn test_dispatcher_handle_registration() {
        let config = WatchConfig::default();
        let watch_manager = WatchManager::new(config);
        watch_manager.start();
        let wm = Arc::new(watch_manager);

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let (dispatcher, handle) = WatchDispatcher::new(wm.clone(), shutdown_rx);

        // Spawn dispatcher
        tokio::spawn(async move {
            dispatcher.run().await;
        });

        // Register a watch
        let key = Bytes::from("test_key");
        let watcher_handle = wm.register(key.clone()).await;
        let (watcher_id, watched_key, event_receiver, guard) = watcher_handle.into_receiver();

        let (tx, _rx) = mpsc::channel(32);

        let registration = WatchRegistration {
            key: watched_key,
            response_sender: tx,
            guard,
            event_receiver,
            watcher_id,
        };

        // Test that dispatcher accepts registration
        assert!(handle.register(registration).await.is_ok());

        // Give time for handler to spawn
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Cleanup
        drop(shutdown_tx);
    }

    #[tokio::test]
    async fn test_dispatcher_shutdown() {
        let config = WatchConfig::default();
        let watch_manager = WatchManager::new(config);
        watch_manager.start();
        let wm = Arc::new(watch_manager);

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let (dispatcher, _handle) = WatchDispatcher::new(wm, shutdown_rx);

        // Spawn dispatcher
        let dispatcher_handle = tokio::spawn(async move {
            dispatcher.run().await;
        });

        // Trigger shutdown
        drop(shutdown_tx);

        // Dispatcher should exit gracefully
        tokio::time::timeout(tokio::time::Duration::from_millis(200), dispatcher_handle)
            .await
            .expect("Dispatcher should exit on shutdown")
            .expect("Dispatcher task should complete successfully");
    }
}
