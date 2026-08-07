//! the test_utils folder here will share utils or test components between unit
//! tests and integration tests
mod buffered_raft_log_test_helpers;
mod common;
mod entry_builder;
pub mod mock;
mod replication_test_helpers;
mod snapshot;

#[cfg(test)]
mod common_test;

#[cfg(test)]
mod entry_builder_test;

#[cfg(test)]
mod log_capture;

#[cfg(test)]
pub use log_capture::*;

pub use buffered_raft_log_test_helpers::*;
pub use common::*;
pub use entry_builder::*;
pub use mock::*;
pub use replication_test_helpers::*;
pub use snapshot::*;

/// `db_path` is unused — kept as a no-op parameter so the many existing call
/// sites (each passing a unique `/tmp/test_xxx` path, back when this fed
/// `ClusterConfig.data_dir`) don't all need touching. data_dir isn't a
/// `d-engine-core` concept anymore (see #9).
pub fn node_config(_db_path: &str) -> crate::RaftNodeConfig {
    crate::RaftNodeConfig::new()
        .expect("RaftNodeConfig should be inited successfully")
        .validate()
        .expect("RaftNodeConfig should be validated successfully")
}
