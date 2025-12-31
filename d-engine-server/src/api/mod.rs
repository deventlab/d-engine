//! Public API layer for d-engine

mod embedded;
mod standalone;

pub use embedded::EmbeddedEngine;
pub use standalone::StandaloneServer;

#[cfg(test)]
mod embedded_test;

#[cfg(test)]
mod embedded_env_test;

#[cfg(test)]
mod standalone_test;
