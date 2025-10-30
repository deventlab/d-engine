//! Protocol buffer type extensions
//!
//! This module provides additional trait implementations and helper methods
//! for protobuf-generated types. The re-exports here are used by dependent crates
//! (d-engine-core, d-engine-runtime, d-engine-client) when they import from this crate.

pub mod common {
    include!("generated/d_engine.common.rs");
}

pub mod error {
    include!("generated/d_engine.common.error.rs");
}
pub mod server {
    pub mod cluster {
        include!("generated/d_engine.server.cluster.rs");
    }

    pub mod replication {
        include!("generated/d_engine.server.replication.rs");
    }

    pub mod election {
        include!("generated/d_engine.server.election.rs");
    }

    pub mod storage {
        include!("generated/d_engine.server.storage.rs");
    }
}

pub mod client {
    include!("generated/d_engine.client.rs");
}

pub mod exts;
