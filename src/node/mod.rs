//! Core node implementation for the distributed consensus system
//!
//! Provides the fundamental building blocks for creating and managing
//! Raft cluster nodes with thread-safe lifecycle control and network integration

mod builder;
mod node;

pub use builder::*;
pub use node::*;

#[doc(hidden)]
mod type_config;
#[doc(hidden)]
pub use type_config::*;

/// Test Modules
#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod node_test;
