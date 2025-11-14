//! Watch mechanism for monitoring key changes
//!
//! This module provides a high-performance, lock-free watch system that allows
//! clients to monitor specific keys for changes. It is designed to replace etcd's
//! Watch functionality with minimal overhead on the write path.
//!
//! # Architecture Overview
//!
//! The watch system uses a three-tier architecture:
//!
//! 1. **Write Path (Hot Path)**: Non-blocking event notification via `try_send`
//! 2. **Dispatcher Thread**: Background thread that distributes events to watchers
//! 3. **Client Streams**: Per-client gRPC streams that deliver events
//!
//! ```text
//! ┌─────────────┐
//! │ StateMachine│
//! │  apply()    │
//! └──────┬──────┘
//!        │ notify_change() [~10ns]
//!        ▼
//! ┌─────────────────┐
//! │  Event Queue    │ (crossbeam-channel, lock-free)
//! │  (bounded 1000) │
//! └──────┬──────────┘
//!        │
//!        ▼
//! ┌─────────────────┐
//! │   Dispatcher    │ (background thread)
//! │   Thread        │
//! └──────┬──────────┘
//!        │ lookup in DashMap (lock-free)
//!        ▼
//! ┌─────────────────┐
//! │ Per-Watcher     │ (tokio mpsc, bounded 10)
//! │ Channels        │
//! └──────┬──────────┘
//!        │
//!        ▼
//! ┌─────────────────┐
//! │ gRPC Stream     │
//! │ to Client       │
//! └─────────────────┘
//! ```
//!
//! # Performance Characteristics
//!
//! - **Write overhead**: < 0.01% with 100+ watchers (target: <10ns per notify)
//! - **Event latency**: Typically < 100μs end-to-end
//! - **Memory per watcher**: ~2.4KB with default buffer size (10)
//! - **Scalability**: Tested with 1000+ concurrent watchers
//!
//! # Usage Example
//!
//! ```ignore
//! use d_engine_core::watch::{WatchManager, WatchConfig, WatchEventType};
//! use bytes::Bytes;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! // Create and start manager
//! let config = WatchConfig::default();
//! let manager = WatchManager::new(config);
//! manager.start();
//!
//! // Register a watcher
//! let key = Bytes::from("mykey");
//! let mut handle = manager.register(key.clone()).await;
//!
//! // Notify of changes (typically called from StateMachine)
//! manager.notify_put(key.clone(), Bytes::from("new_value"));
//!
//! // Receive events
//! while let Some(event) = handle.receiver_mut().unwrap().recv().await {
//!     match event.event_type {
//!         WatchEventType::Put => {
//!             println!("Key updated: {:?} = {:?}", event.key, event.value);
//!         }
//!         WatchEventType::Delete => {
//!             println!("Key deleted: {:?}", event.key);
//!         }
//!     }
//! }
//!
//! manager.stop();
//! # });
//! ```
//!
//! # Configuration
//!
//! The watch system is configurable via [`WatchConfig`]:
//!
//! - `event_queue_size`: Global event queue buffer (default: 1000)
//! - `watcher_buffer_size`: Per-watcher channel buffer (default: 10)
//! - `enable_metrics`: Enable detailed logging and metrics (default: false)
//!
//! ```
//! use d_engine_core::watch::WatchConfig;
//!
//! let config = WatchConfig {
//!     enabled: true,
//!     event_queue_size: 2000,
//!     watcher_buffer_size: 20,
//!     enable_metrics: true,
//! };
//! ```
//!
//! # Error Handling
//!
//! The watch system prioritizes write performance over guaranteed event delivery:
//!
//! - When the global event queue is full, new events are **dropped**
//! - When a per-watcher channel is full, events for that watcher are **dropped**
//! - Clients should use the Read API to re-sync if they detect missing events
//!
//! This design ensures that a slow or stuck watcher never blocks the write path.
//!
//! # Thread Safety
//!
//! All types in this module are thread-safe and can be safely shared across threads:
//!
//! - [`WatchManager`] can be cloned and shared via `Arc`
//! - All operations are lock-free or use fine-grained locking
//! - The dispatcher runs on a dedicated background thread
//!
//! # Implementation Notes
//!
//! - Uses `crossbeam-channel` for the global event queue (lock-free MPMC)
//! - Uses `DashMap` for the watcher registry (lock-free concurrent HashMap)
//! - Uses `tokio::sync::mpsc` for per-watcher channels (async-friendly)
//! - Watchers are automatically cleaned up when dropped (RAII pattern)

mod manager;

#[cfg(test)]
mod manager_test;

pub use manager::{WatchEvent, WatchEventType, WatchManager, WatcherHandle, WatcherHandleGuard};

// Re-export WatchConfig from the unified config system
pub use crate::config::WatchConfig;
