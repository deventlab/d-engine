//! # Client Guide
//!
//! This module contains documentation and guides for client developers using D-Engine.
//!
//! ## Available Guides
//!
//! - [Read Consistency](read_consistency.md) - Understanding and configuring read consistency
//!   policies
//! - [Error Handling](error_handling.md) - Error categories and retry strategies
pub mod read_consistency {
    #![doc = include_str!("read_consistency.md")]
}

pub mod error_handling {
    #![doc = include_str!("error-handling.md")]
}
