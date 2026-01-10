//! the test_utils folder here will share utils or test components betwee unit
//! tests and integrations tests
mod common;
mod entry_builder;
pub mod mock;
mod snapshot;

#[cfg(test)]
mod common_test;

#[cfg(test)]
mod entry_builder_test;

pub use common::*;
pub use entry_builder::*;
pub use mock::*;
pub use snapshot::*;

pub fn node_config(db_path: &str) -> crate::RaftNodeConfig {
    let mut s = crate::RaftNodeConfig::new().expect("RaftNodeConfig should be inited successfully");
    s.cluster.db_root_dir = std::path::PathBuf::from(db_path);
    s.validate().expect("RaftNodeConfig should be validated successfully")
}
