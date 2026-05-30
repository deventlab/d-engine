//! Public API layer for d-engine

mod embedded;
mod embedded_client;
mod read_handle;
mod standalone;

pub use read_handle::ReadHandle;
pub use standalone::StandaloneEngine;

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

#[cfg(test)]
mod embedded_client_test;

#[cfg(test)]
mod embedded_client_fast_path_test;

#[cfg(test)]
mod read_handle_test;

#[cfg(test)]
mod embedded_test;

#[cfg(test)]
mod embedded_env_test;

#[cfg(test)]
mod standalone_test;
