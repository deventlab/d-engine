//! Public API layer for d-engine

mod embedded;
mod standalone;

pub use embedded::EmbeddedEngine;
pub use standalone::StandaloneServer;

#[cfg(test)]
mod embedded_test;
