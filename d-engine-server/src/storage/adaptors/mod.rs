/// File-based state machine implementation.
///
/// Provides a simple file-backed storage backend for development and testing.
pub mod file;
pub use file::*;

/// RocksDB-based state machine implementation.
///
/// Production-grade storage backend using RocksDB for high-performance persistence.
#[cfg(feature = "rocksdb")]
pub mod rocksdb;

#[cfg(feature = "rocksdb")]
pub use rocksdb::*;
