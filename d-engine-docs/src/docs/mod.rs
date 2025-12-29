//! # D-Engine Documentation
//!
//! Welcome to D-Engine documentation - a lightweight and strongly consistent Raft consensus
//! engine written in Rust. This section provides comprehensive resources for developers.
//!
//! ## Documentation Sections
//!
//! - [Architecture](self::architecture) - Core design principles and implementation details
//! - [Client Guide](self::client_guide) - Client application development and API usage
//! - [Performance](self::performance) - Optimization techniques and benchmarking
//! - [Server Guide](self::server_guide) - Server deployment and customization

pub mod quick_start_5min {
    #![doc = include_str!("quick-start-5min.md")]
}

pub mod quick_start_standalone {
    #![doc = include_str!("quick-start-standalone.md")]
}

pub mod integration_modes {
    #![doc = include_str!("integration-modes.md")]
}

pub mod real_world_examples {
    #![doc = include_str!("real-world-examples.md")]
}

pub mod architecture;
pub mod client_guide;
pub mod examples;
pub mod performance;
pub mod server_guide;
