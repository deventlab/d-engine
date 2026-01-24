//! KV client trait - unified interface for key-value operations.
//!
//! Provides a common abstraction for both remote (gRPC) and embedded (local) access
//! to d-engine's key-value store.
//!
//! # Implementations
//!
//! - `GrpcKvClient` (d-engine-client): Remote access via gRPC protocol
//! - `LocalKvClient` (d-engine-server): Zero-overhead embedded access
//!
//! # Design Principles
//!
//! - **Unified Interface**: Same API for remote and embedded modes
//! - **Async-first**: All operations are async for non-blocking I/O
//! - **Type Safety**: Strong typing with clear error handling
//! - **Performance**: Zero-cost abstractions, no runtime overhead
//!
//! # Example
//!
//! ```rust,ignore
//! async fn store_config<C: ClientApi>(client: &C) -> Result<()> {
//!     client.put(b"config:timeout", b"30s").await?;
//!     let value = client.get(b"config:timeout").await?;
//!     Ok(())
//! }
//! ```

use bytes::Bytes;

use crate::client::client_api_error::ClientApiResult;

/// Unified key-value store interface.
///
/// This trait abstracts over different client implementations, allowing applications
/// to write generic code that works with both remote (gRPC) and embedded (local) access.
///
/// # Consistency Guarantees
///
/// - **put()**: Strong consistency, linearizable writes
/// - **get()**: Linearizable reads by default
/// - **delete()**: Strong consistency, linearizable deletes
///
/// # Thread Safety
///
/// All implementations must be `Send + Sync`, safe for concurrent access.
///
/// # Performance Characteristics
///
/// - `GrpcKvClient`: 1-2ms latency (network + serialization)
/// - `LocalKvClient`: <0.1ms latency (direct function call)
#[async_trait::async_trait]
pub trait ClientApi: Send + Sync {
    /// Stores a key-value pair with strong consistency.
    ///
    /// The write is replicated to a quorum of nodes before returning,
    /// ensuring durability and linearizability.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to store (arbitrary bytes)
    /// * `value` - The value to store (arbitrary bytes)
    ///
    /// # Errors
    ///
    /// - [`crate::client::kv_error::KvClientError::ChannelClosed`] if node is shutting down
    /// - [`crate::client::kv_error::KvClientError::Timeout`] if operation exceeds timeout
    /// - [`crate::client::kv_error::KvClientError::ServerError`] for server-side errors (e.g., not leader)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// client.put(b"user:1001", b"Alice").await?;
    /// ```
    async fn put(
        &self,
        key: impl AsRef<[u8]> + Send,
        value: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<()>;

    /// Stores a key-value pair with time-to-live (TTL).
    ///
    /// The key will automatically expire after `ttl_secs` seconds.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to store
    /// * `value` - The value to store
    /// * `ttl_secs` - Time-to-live in seconds
    ///
    /// # Errors
    ///
    /// Same as [`put()`](Self::put)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Session expires after 1 hour
    /// client.put_with_ttl(b"session:abc", b"user_data", 3600).await?;
    /// ```
    async fn put_with_ttl(
        &self,
        key: impl AsRef<[u8]> + Send,
        value: impl AsRef<[u8]> + Send,
        ttl_secs: u64,
    ) -> ClientApiResult<()>;

    /// Retrieves the value associated with a key.
    ///
    /// Uses linearizable reads by default, ensuring the returned value
    /// reflects all previously committed writes.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to retrieve
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` if key exists
    /// * `Ok(None)` if key does not exist or has expired
    /// * `Err(_)` if operation failed
    ///
    /// # Errors
    ///
    /// - [`crate::client::kv_error::KvClientError::ChannelClosed`] if node is shutting down
    /// - [`crate::client::kv_error::KvClientError::Timeout`] if operation exceeds timeout
    /// - [`crate::client::kv_error::KvClientError::ServerError`] for server-side errors
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// match client.get(b"user:1001").await? {
    ///     Some(value) => println!("User: {:?}", value),
    ///     None => println!("User not found"),
    /// }
    /// ```
    async fn get(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<Option<Bytes>>;

    /// Retrieves multiple keys in a single request.
    ///
    /// More efficient than multiple individual `get()` calls when fetching
    /// multiple keys, as it batches the requests.
    ///
    /// # Arguments
    ///
    /// * `keys` - Slice of keys to retrieve
    ///
    /// # Returns
    ///
    /// Vector of results in the same order as input keys.
    /// `None` for keys that don't exist.
    ///
    /// # Errors
    ///
    /// Same as [`get()`](Self::get)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let keys = vec![
    ///     Bytes::from("user:1001"),
    ///     Bytes::from("user:1002"),
    /// ];
    /// let results = client.get_multi(&keys).await?;
    /// ```
    async fn get_multi(
        &self,
        keys: &[Bytes],
    ) -> ClientApiResult<Vec<Option<Bytes>>>;

    /// Deletes a key-value pair with strong consistency.
    ///
    /// The deletion is replicated to a quorum before returning.
    /// Returns successfully even if the key does not exist (idempotent).
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete
    ///
    /// # Errors
    ///
    /// - [`crate::client::kv_error::KvClientError::ChannelClosed`] if node is shutting down
    /// - [`crate::client::kv_error::KvClientError::Timeout`] if operation exceeds timeout
    /// - [`crate::client::kv_error::KvClientError::ServerError`] for server-side errors
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// client.delete(b"temp:session_123").await?;
    /// ```
    async fn delete(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<()>;

    /// Atomically compare and swap a key's value
    ///
    /// Returns:
    /// - Ok(true): CAS succeeded, value was updated
    /// - Ok(false): CAS failed, current value != expected_value
    /// - Err(e): Operation error (timeout, cluster unavailable, etc.)
    async fn compare_and_swap(
        &self,
        key: impl AsRef<[u8]> + Send,
        expected_value: Option<impl AsRef<[u8]> + Send>,
        new_value: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<bool>;

    // ==================== Watch Operations ====================

    /// Watch for changes to a specific key
    ///
    /// Returns a stream of watch events when the key's value changes.
    /// The stream will continue until explicitly closed or a connection error occurs.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to watch
    ///
    /// # Returns
    ///
    /// A streaming response that yields `WatchResponse` events
    ///
    /// # Errors
    ///
    /// Returns error if unable to establish watch connection
    async fn watch(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<tonic::Streaming<d_engine_proto::client::WatchResponse>>;

    // ==================== Cluster Management Operations ====================

    /// Lists all cluster members with metadata
    ///
    /// Returns node information including:
    /// - Node ID
    /// - Address
    /// - Role (Leader/Follower/Learner)
    /// - Status
    ///
    /// # Errors
    ///
    /// Returns error if unable to retrieve cluster metadata
    async fn list_members(&self)
    -> ClientApiResult<Vec<d_engine_proto::server::cluster::NodeMeta>>;

    /// Get the current leader ID
    ///
    /// Returns the leader node ID if known, or None if no leader is currently elected.
    ///
    /// # Errors
    ///
    /// Returns error if unable to determine leader status
    async fn get_leader_id(&self) -> ClientApiResult<Option<u32>>;
}
