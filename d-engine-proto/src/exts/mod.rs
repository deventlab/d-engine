//! Protocol buffer type extensions
//!
//! This module provides additional trait implementations and helper methods
//! for protobuf-generated types. While the imports appear unused at the module level,
//! they are re-exported for use by dependent crates (d-engine-core, d-engine, d-engine-client).

pub mod client_ext;
pub mod cluster_ext;
pub mod common_ext;
pub mod replication_ext;
pub mod storage_ext;

#[cfg(test)]
mod client_ext_test;
#[cfg(test)]
mod cluster_ext_test;
#[cfg(test)]
mod common_ext_test;
#[cfg(test)]
mod replication_ext_test;
#[cfg(test)]
mod storage_ext_test;
