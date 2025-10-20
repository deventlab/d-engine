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

pub mod overview {
    #![doc = include_str!("overview.md")]
}

pub mod architecture;
pub mod client_guide;
pub mod performance;
pub mod server_guide;
