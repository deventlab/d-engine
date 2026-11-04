#![warn(missing_docs)]
#![doc = include_str!("docs/overview.md")]

// Re-export server components when server feature is enabled
// Re-export client components when client feature is enabled
#[cfg(feature = "client")]
pub use d_engine_client::*;
#[cfg(feature = "server")]
pub use d_engine_server::*;

/// Convenient prelude for importing common types
///
/// ```rust,ignore
/// use d_engine::prelude::*;
/// ```
pub mod prelude {
    #[cfg(feature = "client")]
    pub use d_engine_client::Client;
    #[cfg(feature = "client")]
    pub use d_engine_client::ClientApi;
    #[cfg(feature = "client")]
    pub use d_engine_client::ClientBuilder;
    #[cfg(feature = "rocksdb")]
    pub use d_engine_server::DefaultEmbeddedClient;
    #[cfg(feature = "rocksdb")]
    pub use d_engine_server::DefaultEmbeddedEngine;
    #[cfg(feature = "server")]
    pub use d_engine_server::EmbeddedClient;
    #[cfg(feature = "server")]
    pub use d_engine_server::EmbeddedEngine;
    #[cfg(feature = "server")]
    pub use d_engine_server::Error;
    #[cfg(feature = "server")]
    pub use d_engine_server::FileStateMachine;
    #[cfg(feature = "server")]
    pub use d_engine_server::FileStorageEngine;
    #[cfg(feature = "server")]
    pub use d_engine_server::Node;
    #[cfg(feature = "server")]
    pub use d_engine_server::Result;
    #[cfg(feature = "rocksdb")]
    pub use d_engine_server::RocksDBStateMachine;
    #[cfg(feature = "rocksdb")]
    pub use d_engine_server::RocksDBStorageEngine;
    #[cfg(feature = "server")]
    pub use d_engine_server::StandaloneEngine;
    #[cfg(feature = "server")]
    pub use d_engine_server::StateMachine;
    #[cfg(feature = "server")]
    pub use d_engine_server::StorageEngine;
}

/// Documentation modules
pub mod docs;
