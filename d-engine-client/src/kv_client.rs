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
//! async fn store_config<C: KvClient>(client: &C) -> Result<()> {
//!     client.put(b"config:timeout", b"30s").await?;
//!     let value = client.get(b"config:timeout").await?;
//!     Ok(())
//! }
//! ```

use bytes::Bytes;

use crate::kv_error::KvResult;

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
pub trait KvClient: Send + Sync {
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
    /// - [`crate::kv_error::KvClientError::ChannelClosed`] if node is shutting down
    /// - [`crate::kv_error::KvClientError::Timeout`] if operation exceeds timeout
    /// - [`crate::kv_error::KvClientError::ServerError`] for server-side errors (e.g., not leader)
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
    ) -> KvResult<()>;

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
    ) -> KvResult<()>;

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
    /// - [`crate::kv_error::KvClientError::ChannelClosed`] if node is shutting down
    /// - [`crate::kv_error::KvClientError::Timeout`] if operation exceeds timeout
    /// - [`crate::kv_error::KvClientError::ServerError`] for server-side errors
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
    ) -> KvResult<Option<Bytes>>;

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
    ) -> KvResult<Vec<Option<Bytes>>>;

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
    /// - [`crate::kv_error::KvClientError::ChannelClosed`] if node is shutting down
    /// - [`crate::kv_error::KvClientError::Timeout`] if operation exceeds timeout
    /// - [`crate::kv_error::KvClientError::ServerError`] for server-side errors
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// client.delete(b"temp:session_123").await?;
    /// ```
    async fn delete(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> KvResult<()>;
}
