//! # Performance Guide
//!
//! Performance optimization strategies and best practices for D-Engine deployments.
//! These documents help you maximize throughput, minimize latency, and optimize
//! resource utilization.
//!
//! ## Guides
//!
//! - [Throughput Optimization](self::throughput_optimization_guide) - Maximizing operation
//!   throughput
//! - [Benchmarking Guide](self::benchmarking_guide) - How to run embedded and standalone benchmarks

pub mod throughput_optimization_guide {
    #![doc = include_str!("throughput-optimization-guide.md")]
}

pub mod benchmarking_guide {
    #![doc = include_str!("benchmarking-guide.md")]
}
