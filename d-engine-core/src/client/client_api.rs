//! KV client trait - unified interface for key-value operations.
//!
//! Provides a common abstraction for both remote (gRPC) and embedded (local) access
//! to d-engine's key-value store.
//!
//! # Implementations
//!
//! - `GrpcClient` (d-engine-client): Remote access via gRPC protocol
//! - `EmbeddedClient` (d-engine-server): Zero-overhead embedded access
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
use d_engine_proto::client::ReadConsistencyPolicy;

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
/// - `GrpcClient`: 1-2ms latency (network + serialization)
/// - `EmbeddedClient`: <0.1ms latency (direct function call)
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
    /// - [`ClientApiError::Network`] if node is shutting down or timeout occurs
    /// - [`ClientApiError::Business`] for server-side errors (e.g., not leader)
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
    /// - [`ClientApiError::Network`] if node is shutting down or timeout occurs
    /// - [`ClientApiError::Business`] for server-side errors
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
    /// - [`ClientApiError::Network`] if node is shutting down or timeout occurs
    /// - [`ClientApiError::Business`] for server-side errors
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

    // ==================== Advanced Read Operations with Consistency Control ====================

    /// Retrieves multiple keys with explicit consistency policy
    ///
    /// Batch retrieval with explicit consistency control.
    /// More efficient than multiple individual calls when fetching multiple keys.
    ///
    /// # Arguments
    ///
    /// * `keys` - Slice of keys to retrieve
    /// * `consistency_policy` - Explicit consistency policy for this batch request
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
    /// use d_engine_proto::client::ReadConsistencyPolicy;
    ///
    /// let keys = vec![
    ///     Bytes::from("key1"),
    ///     Bytes::from("key2"),
    /// ];
    /// let values = client.get_multi_with_policy(
    ///     &keys,
    ///     Some(ReadConsistencyPolicy::EventualConsistency)
    /// ).await?;
    /// ```
    async fn get_multi_with_policy(
        &self,
        keys: &[Bytes],
        consistency_policy: Option<ReadConsistencyPolicy>,
    ) -> ClientApiResult<Vec<Option<Bytes>>>;

    /// Retrieves a key with linearizable (strong) consistency
    ///
    /// Convenience method for [`get_with_policy()`](Self::get_with_policy) with
    /// `LinearizableRead` policy. Guarantees reading the latest committed value.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to retrieve
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` if key exists
    /// * `Ok(None)` if key does not exist or has expired
    ///
    /// # Errors
    ///
    /// Same as [`get()`](Self::get)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Guaranteed to read latest value
    /// let value = client.get_linearizable(b"critical-config").await?;
    /// ```
    async fn get_linearizable(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<Option<Bytes>>;

    /// Retrieves a key with lease-based consistency
    ///
    /// Convenience method for [`get_with_policy()`](Self::get_with_policy) with
    /// `LeaseRead` policy. Optimized linearizable read using Leader lease mechanism.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to retrieve
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` if key exists
    /// * `Ok(None)` if key does not exist or has expired
    ///
    /// # Errors
    ///
    /// Same as [`get()`](Self::get)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Lease-based read (faster than linearizable, still strong consistency)
    /// let value = client.get_lease(b"config").await?;
    /// ```
    async fn get_lease(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<Option<Bytes>>;

    /// Retrieves a key with eventual consistency
    ///
    /// Convenience method for [`get_with_policy()`](Self::get_with_policy) with
    /// `EventualConsistency` policy. Fast but may return stale data if replication is lagging.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to retrieve
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` if key exists (may be stale)
    /// * `Ok(None)` if key does not exist or has expired
    ///
    /// # Errors
    ///
    /// Same as [`get()`](Self::get)
    ///
    /// # Use Cases
    ///
    /// - Read-heavy workloads with acceptable staleness
    /// - Analytics/reporting (staleness acceptable)
    /// - Caching scenarios
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Fast stale read
    /// let cached_value = client.get_eventual(b"user-preference").await?;
    /// ```
    async fn get_eventual(
        &self,
        key: impl AsRef<[u8]> + Send,
    ) -> ClientApiResult<Option<Bytes>>;
}
