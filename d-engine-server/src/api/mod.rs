//! Public API layer for d-engine

mod embedded;
mod embedded_client;
mod embedded_read_handle;
mod standalone;
mod standalone_read_handle;

pub use standalone::StandaloneEngine;
pub(crate) use standalone_read_handle::StandaloneReadHandle;

/// Embedded engine generic over any `(SE, SM)` pair.
pub type EmbeddedEngine<SE, SM> = embedded::EmbeddedEngine<crate::node::RaftTypeConfig<SE, SM>>;

/// Embedded client generic over any `(SE, SM)` pair.
pub type EmbeddedClient<SE, SM> =
    embedded_client::EmbeddedClient<crate::node::RaftTypeConfig<SE, SM>>;

/// Zero-param embedded engine alias using the default RocksDB backend.
#[cfg(feature = "rocksdb")]
pub type DefaultEmbeddedEngine =
    EmbeddedEngine<crate::RocksDBStorageEngine, crate::RocksDBStateMachine>;

/// Zero-param embedded client alias using the default RocksDB backend.
#[cfg(feature = "rocksdb")]
pub type DefaultEmbeddedClient =
    EmbeddedClient<crate::RocksDBStorageEngine, crate::RocksDBStateMachine>;
