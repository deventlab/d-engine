#![warn(missing_docs)]

//! # d-engine - Distributed Coordination Engine
//!
//! **d-engine** is a lightweight distributed coordination engine written in Rust, designed for
//! embedding into applications that need strong consistency—the consensus layer for building
//! reliable distributed systems.
//!
//! ## 🚀 New to d-engine? Start Here
//!
//! Follow this learning path to get started quickly:
//!
//! ```text
//! 1. Is d-engine Right for You? (1 minute)
//!    ↓
//! 2. Choose Integration Mode (1 minute)
//!    ↓
//! 3a. Quick Start - Embedded (5 minutes)
//!    OR
//! 3b. Quick Start - Standalone (5 minutes)
//!    ↓
//! 4. Scale to Cluster (optional)
//! ```
//!
//! **→ Start: [Is d-engine Right for You?](docs::use_cases)**
//!
//! ## Crate Organization
//!
//! | Crate | Purpose |
//! |-------|---------|
//! | **d-engine-proto** | Protocol definitions (Prost) |
//! | **d-engine-core** | Core Raft algorithm & traits |
//! | **d-engine-client** | Client library for applications |
//! | **d-engine-server** | Server runtime implementation |
//! | **d-engine-docs** | Documentation (you are here) |
//!
//! ## Quick Start
//!
//! ### Embedded Mode (Rust)
//!
//! ```rust,ignore
//! use d_engine::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let engine = EmbeddedEngine::with_rocksdb("./data", None).await?;
//!     engine.wait_ready(std::time::Duration::from_secs(5)).await?;
//!
//!     let client = engine.client();
//!     client.put(b"key".to_vec(), b"value".to_vec()).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! **→ [5-Minute Embedded Guide](docs::quick_start_5min)**
//!
//! ### Standalone Mode (Any Language)
//!
//! ```bash
//! cd examples/three-nodes-cluster
//! make start-cluster
//! ```
//!
//! **→ [Standalone Guide](docs::quick_start_standalone)**
//!
//! ## Documentation Index
//!
//! ### Getting Started
//! - [Is d-engine Right for You?](docs::use_cases) - Common use cases
//! - [Integration Modes](docs::integration_modes) - Embedded vs Standalone
//! - [Quick Start - Embedded](docs::quick_start_5min)
//! - [Quick Start - Standalone](docs::quick_start_standalone)
//!
//! ### Guides by Role
//!
//! #### Client Developers
//! - [Read Consistency](docs::client_guide::read_consistency) - Choosing consistency policies
//! - [Error Handling](docs::client_guide::error_handling)
//! - [Service Discovery](docs::client_guide::service_discovery_pattern)
//!
//! #### Server Operators
//! - [Customize Storage Engine](docs::server_guide::customize_storage_engine)
//! - [Customize State Machine](docs::server_guide::customize_state_machine)
//! - [Log Purge Executor](docs::server_guide::customize_raft_log_purge_executor)
//! - [Consistency Tuning](docs::server_guide::consistency_tuning)
//! - [Watch Feature](docs::server_guide::watch_feature)
//!
//! #### Architecture Deep Dive
//! - [Raft Roles](docs::architecture::raft_role)
//! - [Election Design](docs::architecture::election_design)
//! - [Log Persistence](docs::architecture::raft_log_persistence_architecture)
//! - [Snapshot Design](docs::architecture::snapshot_module_design)
//! - [Node Join](docs::architecture::node_join_architecture)
//! - [Error Handling Principles](docs::architecture::error_handling_design_principles)
//!
//! ### Examples & Performance
//! - [Single Node Expansion](docs::examples::single_node_expansion) - Scale from 1 to 3 nodes
//! - [Throughput Optimization](docs::performance::throughput_optimization_guide)
//!
//! ## API Documentation
//!
//! - [d_engine_server](../d_engine_server/index.html) - Server runtime API
//! - [d_engine_client](../d_engine_client/index.html) - Client library API
//! - [d_engine_core](../d_engine_core/index.html) - Core Raft traits
//!
//! ## License
//!
//! MIT or Apache-2.0

pub mod docs;
