//! Test utilities and mock implementations
//!
//! This module provides builders and fixtures for testing d-engine-server.
//!
//! # Features
//!
//! - **Mock Builder**: Easily construct mock Raft instances with custom components
//! - **Integration Helpers**: Utilities for integration testing
//!
//! # Example
//!
//! ```rust,ignore
//! use d_engine_server::test_utils::MockBuilder;
//! use tokio::sync::watch;
//!
//! let (tx, rx) = watch::channel(());
//! let mock_raft = MockBuilder::new(rx).build_raft();
//! ```
//!
//! # Note
//!
//! This module is only available during tests.

mod integration;
mod mock;

pub use integration::*;
pub use mock::*;
