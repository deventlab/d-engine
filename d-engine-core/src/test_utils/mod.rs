//! the test_utils folder here will share utils or test components betwee unit
//! tests and integrations tests
mod common;
mod entry_builder;
mod mock;
mod snapshot;

pub use common::*;
pub use entry_builder::*;
pub use mock::*;
pub use snapshot::*;

pub fn node_config(db_path: &str) -> crate::RaftNodeConfig {
    let mut s =
        crate::RaftNodeConfig::new().expect("RaftNodeConfig should be inited successfully.");
    s.cluster.db_root_dir = std::path::PathBuf::from(db_path);
    s
}
