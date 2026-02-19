#![warn(missing_docs)]
#![doc = include_str!("docs/overview.md")]

// Re-export server components when server feature is enabled
#[cfg(feature = "server")]
pub use d_engine_server::*;

// Re-export client components when client feature is enabled
#[cfg(feature = "client")]
pub use d_engine_client::*;

/// Convenient prelude for importing common types
///
/// ```rust,ignore
/// use d_engine::prelude::*;
/// ```
pub mod prelude {
    #[cfg(feature = "server")]
    pub use d_engine_server::{
        EmbeddedClient, EmbeddedEngine, Error, FileStateMachine, FileStorageEngine, Node,
        NodeBuilder, Result, StandaloneEngine, StateMachine, StorageEngine,
    };

    #[cfg(feature = "rocksdb")]
    pub use d_engine_server::{RocksDBStateMachine, RocksDBStorageEngine};

    #[cfg(feature = "client")]
    pub use d_engine_client::{Client, ClientApi, ClientBuilder};
}

/// Documentation modules
pub mod docs;
