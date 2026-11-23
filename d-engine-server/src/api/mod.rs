//! Public API layer for d-engine

mod embedded;
mod embedded_client;
mod read_handle;
mod standalone;
pub(crate) mod types;

pub use embedded::EmbeddedEngine;
pub use embedded_client::EmbeddedClient;
pub(crate) use read_handle::ReadHandle;
pub use standalone::StandaloneEngine;

/// Zero-param embedded engine alias using the default RocksDB backend.
#[cfg(feature = "rocksdb")]
pub type DefaultEmbeddedEngine = EmbeddedEngine;

/// Zero-param embedded client alias using the default RocksDB backend.
#[cfg(feature = "rocksdb")]
pub type DefaultEmbeddedClient = EmbeddedClient;
