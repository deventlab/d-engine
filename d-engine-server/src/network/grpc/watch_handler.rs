//! Watch stream handler for gRPC streaming
//!
//! This module implements the Handler pattern for managing Watch gRPC streams.
//! It encapsulates the logic for forwarding watch events from the WatchManager
//! to gRPC clients.
//!
//! # Architecture
//!
//! ```text
//! WatchManager (core) → WatcherHandle.receiver → WatchStreamHandler → gRPC Stream
//! ```
//!
//! # Handler Pattern
//!
//! Following d-engine's Builder pattern principles:
//! - Logic encapsulated in Handler structure
//! - `run()` method can be independently tested
//! - Error handling via `Result` return
//! - Lifecycle is explicit and clear

use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::{info, trace};

use d_engine_core::watch::{WatchEvent, WatchEventType};
use d_engine_proto::client::{WatchEventType as ProtoWatchEventType, WatchResponse};

/// Handler for Watch gRPC stream
///
/// Manages the lifecycle of a single watch stream, forwarding events from
/// the WatchManager to the gRPC client.
///
/// # Lifecycle
///
/// 1. Created with watcher metadata and channels
/// 2. `run()` is called (usually via `NodeBuilder::spawn_watch_stream_handler`)
/// 3. Forwards events until client disconnects or watcher is closed
/// 4. Cleanup happens automatically via WatcherHandle Drop
///
/// # Example
///
/// ```ignore
/// let handler = WatchStreamHandler::new(
///     watcher_id,
///     key.clone(),
///     watcher_handle.receiver,
/// );
///
/// // Spawn in background via NodeBuilder method
/// NodeBuilder::<SE, SM>::spawn_watch_stream_handler(handler, response_sender, guard);
/// ```
pub struct WatchStreamHandler {
    /// Unique identifier for this watcher
    watcher_id: u64,

    /// The key being watched
    key: Bytes,

    /// Receiver for watch events from WatchManager
    event_receiver: mpsc::Receiver<WatchEvent>,
}

impl WatchStreamHandler {
    /// Create a new watch stream handler
    ///
    /// # Arguments
    ///
    /// * `watcher_id` - Unique identifier for this watcher
    /// * `key` - The key being watched
    /// * `event_receiver` - Channel receiver for watch events
    pub fn new(
        watcher_id: u64,
        key: Bytes,
        event_receiver: mpsc::Receiver<WatchEvent>,
    ) -> Self {
        Self {
            watcher_id,
            key,
            event_receiver,
        }
    }

    /// Get the watcher ID
    #[allow(dead_code)]
    pub fn id(&self) -> u64 {
        self.watcher_id
    }

    /// Get the key being watched
    #[allow(dead_code)]
    pub fn key(&self) -> &Bytes {
        &self.key
    }

    /// Run the watch stream handler
    ///
    /// This method forwards watch events from the WatchManager to the gRPC stream
    /// until the client disconnects or the watcher is closed.
    ///
    /// # Arguments
    ///
    /// * `response_sender` - Channel sender for gRPC responses
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Stream completed normally (client disconnected or watcher closed)
    ///
    /// # Lifecycle
    ///
    /// 1. Logs stream start
    /// 2. Receives events from WatchManager via `event_receiver`
    /// 3. Converts core events to gRPC responses
    /// 4. Sends responses to client via `response_sender`
    /// 5. Detects client disconnect (send fails)
    /// 6. Logs stream end
    ///
    /// # Error Handling
    ///
    /// - If `response_sender.send()` fails, the client has disconnected
    /// - Returns `Ok(())` on graceful disconnect
    /// - WatcherHandle cleanup happens automatically via Drop
    pub async fn run(
        mut self,
        response_sender: mpsc::Sender<Result<WatchResponse, tonic::Status>>,
    ) {
        let watcher_id = self.watcher_id;
        let key = self.key.clone();

        info!(
            watcher_id = watcher_id,
            key = ?key,
            "Watch stream started"
        );

        // Forward events from WatchManager to gRPC stream
        while let Some(event) = self.event_receiver.recv().await {
            trace!(
                watcher_id = watcher_id,
                key = ?event.key,
                event_type = ?event.event_type,
                "Received watch event"
            );

            // Convert core event to gRPC response
            let response = Self::convert_event_to_response(event);

            // Send to gRPC stream (non-blocking)
            if response_sender.send(Ok(response)).await.is_err() {
                // Client disconnected - this is normal, not an error
                info!(watcher_id = watcher_id, "Watch stream client disconnected");
                return;
            }
        }

        // WatcherHandle was dropped or channel closed
        // This is also a normal completion path
        info!(
            watcher_id = watcher_id,
            "Watch stream ended (watcher closed)"
        );
    }

    /// Convert core WatchEvent to gRPC WatchResponse
    ///
    /// # Arguments
    ///
    /// * `event` - Core watch event from WatchManager
    ///
    /// # Returns
    ///
    /// gRPC WatchResponse ready to send to client
    fn convert_event_to_response(event: WatchEvent) -> WatchResponse {
        let event_type = match event.event_type {
            WatchEventType::Put => ProtoWatchEventType::Put,
            WatchEventType::Delete => ProtoWatchEventType::Delete,
        };

        WatchResponse {
            key: event.key,
            value: event.value,
            event_type: event_type.into(),
            error: 0, // 0 = Success (no error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_watch_stream_handler_creation() {
        let (_tx, rx) = mpsc::channel(10);
        let key = Bytes::from("test_key");
        let handler = WatchStreamHandler::new(1, key.clone(), rx);

        assert_eq!(handler.id(), 1);
        assert_eq!(handler.key(), &key);
    }

    #[tokio::test]
    async fn test_event_conversion() {
        let event = WatchEvent {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
            event_type: WatchEventType::Put,
        };

        let response = WatchStreamHandler::convert_event_to_response(event.clone());

        assert_eq!(response.key, event.key);
        assert_eq!(response.value, event.value);
        assert_eq!(response.event_type, ProtoWatchEventType::Put.into());
        assert_eq!(response.error, 0);
    }

    #[tokio::test]
    async fn test_handler_run_client_disconnect() {
        let (event_tx, event_rx) = mpsc::channel(10);
        let (response_tx, _response_rx) = mpsc::channel(10);

        let handler = WatchStreamHandler::new(1, Bytes::from("key1"), event_rx);

        // Send an event
        let event = WatchEvent {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
            event_type: WatchEventType::Put,
        };
        event_tx.send(event).await.unwrap();

        // Drop response receiver to simulate client disconnect
        drop(_response_rx);

        // Run handler - should complete gracefully
        handler.run(response_tx).await;
    }

    #[tokio::test]
    async fn test_handler_run_watcher_closed() {
        let (event_tx, event_rx) = mpsc::channel(10);
        let (response_tx, _response_rx) = mpsc::channel(10);

        let handler = WatchStreamHandler::new(1, Bytes::from("key1"), event_rx);

        // Drop event sender to close watcher
        drop(event_tx);

        // Run handler - should complete gracefully
        handler.run(response_tx).await;
    }

    #[tokio::test]
    async fn test_handler_forwards_events() {
        let (event_tx, event_rx) = mpsc::channel(10);
        let (response_tx, mut response_rx) = mpsc::channel(10);

        let handler = WatchStreamHandler::new(1, Bytes::from("key1"), event_rx);

        // Send multiple events
        let event1 = WatchEvent {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
            event_type: WatchEventType::Put,
        };
        let event2 = WatchEvent {
            key: Bytes::from("key1"),
            value: Bytes::from("value2"),
            event_type: WatchEventType::Put,
        };

        event_tx.send(event1.clone()).await.unwrap();
        event_tx.send(event2.clone()).await.unwrap();

        // Close event channel to signal end
        drop(event_tx);

        // Run handler in background
        tokio::spawn(async move {
            handler.run(response_tx).await;
        });

        // Verify events are forwarded
        let resp1 = response_rx.recv().await.unwrap().unwrap();
        assert_eq!(resp1.key, event1.key);
        assert_eq!(resp1.value, event1.value);

        let resp2 = response_rx.recv().await.unwrap().unwrap();
        assert_eq!(resp2.key, event2.key);
        assert_eq!(resp2.value, event2.value);
    }
}
