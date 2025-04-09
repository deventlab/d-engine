//! # dengine
//!
//! [![codecov](https://askmanyai-1255660719.cos.ap-beijing.myqcloud.com/pics/245cf90b-ce96-4f.svg)](https://codecov.io/gh/deventlab/dengine)
//! ![License](https://img.shields.io/badge/license-MIT%20%7C%20Apache--2.0-blue)
//! [![CI](https://github.com/deventlab/dengine/actions/workflows/ci.yml/badge.svg)](https://github.com/deventlab/dengine/actions/workflows/ci.yml)
//!
//! A lightweight, strongly-consistent Raft engine for building reliable distributed systems.
//!
//! ## Features
//! - **Strong Consistency**: Full Raft protocol implementation
//! - **Pluggable Storage**: Supports RocksDB, Sled, and in-memory backends
//! - **Observability**: Metrics, logging, and tracing
//! - **Runtime Agnostic**: Built for `tokio`
//! - **Extensible**: Decoupled protocol and application logic
//!
//! ## Quick Start
//! ```no_run
//! use dengine::NodeBuilder;
//! use tokio::sync::watch;
//!
//! #[tokio::main]
//! async fn main() {
//!     let (graceful_tx, graceful_rx) = watch::channel(());
//!
//!     let node = NodeBuilder::new(None, graceful_rx)
//!         .build()
//!         .start_rpc_server()
//!         .await
//!         .ready().unwrap();
//!
//!     if let Err(e) = node.run().await {
//!         error!("node stops: {:?}", e);
//!     } else {
//!         info!("node stops.");
//!     }
//! }
//! ```
//!
//! ## Core Concepts
//! ![Data Flow](https://www.mermaidchart.com/raw/67aa2040-9292-4aed-b5cd-44621245f1c4?theme=light&version=v0.1&format=svg)
//!
//! For production deployments, a minimum cluster size of **3 nodes** is required.

// #![warn(missing_docs)]

pub mod client;
pub mod config;
pub mod node;
pub mod proto;
#[doc(hidden)]
pub mod storage;

#[doc(hidden)]
pub use client::*;
#[doc(hidden)]
pub use config::*;
#[doc(hidden)]
pub use node::*;

mod constants;
mod core;
mod errors;
mod membership;
mod metrics;
mod network;
mod type_config;

#[doc(hidden)]
pub use core::*;

pub use errors::*;
pub(crate) use membership::*;
pub(crate) use metrics::*;
pub(crate) use network::*;
pub(crate) use storage::*;
#[doc(hidden)]
pub mod utils;
#[doc(hidden)]
pub use type_config::*;
#[doc(hidden)]
pub use utils::*;

//-----------------------------------------------------------
// Test utils
#[cfg(test)]
#[doc(hidden)]
pub mod test_utils;

//-----------------------------------------------------------
// Autometrics
/// autometrics: https://docs.autometrics.dev/rust/adding-alerts-and-slos
use autometrics::objectives::Objective;
use autometrics::objectives::ObjectiveLatency;
use autometrics::objectives::ObjectivePercentile;

#[doc(hidden)]
const API_SLO: Objective = Objective::new("api")
    .success_rate(ObjectivePercentile::P99_9)
    .latency(ObjectiveLatency::Ms10, ObjectivePercentile::P99);
