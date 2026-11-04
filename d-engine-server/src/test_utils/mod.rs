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
// One source of truth for the shared-state-machine wrapper used by both unit and integration tests.
#[path = "../../tests/common/shared_state_machine.rs"]
mod shared_state_machine;

pub use integration::*;
pub use mock::*;
pub use shared_state_machine::SharedStateMachine;
