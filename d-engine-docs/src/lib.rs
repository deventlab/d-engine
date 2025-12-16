#![warn(missing_docs)]

//! # d-engine - A Lightweight Raft Consensus Engine
//!
//! Welcome to d-engine, a robust, production-ready Raft consensus engine written in Rust.
//!
//! ## Overview
//!
//! d-engine provides a battle-tested Raft implementation for building distributed systems with:
//! - **Fault tolerance**: Automatic leader election and recovery
//! - **Strong consistency**: Ensures all replicas are identical
//! - **Modularity**: Pluggable storage, state machines, and networking
//! - **Performance**: Optimized for throughput and latency
//!
//! ## Crate Organization
//!
//! The d-engine workspace consists of multiple specialized crates:
//!
//! | Crate | Purpose | Use Case |
//! |-------|---------|----------|
//! | **d-engine-proto** | Protocol definitions using Prost | Shared protocol between components |
//! | **d-engine-core** | Core Raft algorithm & traits | Understanding Raft consensus logic |
//! | **d-engine-client** | Client library for applications | Building client applications |
//! | **d-engine-server** | Server runtime implementation | Running d-engine nodes |
//! | **d-engine-docs** | Guides, tutorials, and architecture | Learning and reference |
//!
//! ## Quick Start: Choose Your Mode
//!
//! d-engine supports two integration modes. Pick the one that fits your stack:
//!
//! | Mode | Language | Setup Time | Latency | Best For |
//! |------|----------|-----------|---------|----------|
//! | **Embedded** | Rust only | 30 seconds | <0.1ms | Rust apps, microservices |
//! | **Standalone** | Any language (Go, Python, Java) | 1 minute | 1-2ms | Multi-language teams |
//!
//! ### Embedded Mode (Rust Apps)
//!
//! Start d-engine **inside your Rust application**:
//!
//! ```rust
//! use d_engine::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Embedded engine: zero serialization, <0.1ms latency
//!     let engine = EmbeddedEngine::with_rocksdb("./data", None).await?;
//!     engine.ready().await;
//!     engine.wait_leader(std::time::Duration::from_secs(5)).await?;
//!
//!     let client = engine.client();
//!     client.put(b"hello".to_vec(), b"world".to_vec()).await?;
//!     let value = client.get(b"hello".to_vec()).await?;
//!
//!     println!("Value: {:?}", value);
//!     Ok(())
//! }
//! ```
//!
//! **→ [Detailed Embedded Guide](./docs/quick-start-5min/index.html)**
//!
//! ### Standalone Mode (Any Language)
//!
//! Start d-engine as a **separate server**, connect with gRPC:
//!
//! ```bash
//! cd examples/three-nodes-cluster
//! make start-cluster    # Starts 3-node cluster
//! ```
//!
//! Then connect from Go, Python, or any gRPC-supported language.
//!
//! **→ [Detailed Standalone Guide](./docs/quick-start-standalone/index.html)**
//!
//! ## API Documentation
//!
//! ### For Server Operators
//!
//! - **[d_engine_server API](../d_engine_server/index.html)** - Server runtime and node management
//!   - How to [customize storage engines](./docs/server_guide/customize_storage_engine/index.html)
//!   - How to [implement state machines](./docs/server_guide/customize_state_machine/index.html)
//!   - [Performance tuning guide](./docs/performance/throughput_optimization_guide/index.html)
//!
//! ### For Client Developers
//!
//! - **[d_engine_client API](../d_engine_client/index.html)** - Client library for applications
//!   - [Read consistency policies](./docs/client_guide/read_consistency/index.html)
//!   - Error handling and retry strategies
//!   - Connection pooling and timeouts
//!
//! ### For System Designers
//!
//! - **[d_engine_core API](../d_engine_core/index.html)** - Core Raft implementation
//!   - Understanding trait-based design
//!   - Custom storage backend development
//!   - State machine implementation guide
//!
//! ## Architecture Documentation
//!
//! Deep dive into d-engine's design and implementation:
//!
//! - [Raft Roles and State Transitions](./docs/architecture/raft_role/index.html)
//!   - How leader, candidate, and follower states work
//! - [Election Design](./docs/architecture/election_design/index.html)
//!   - Why randomized timeouts prevent split-brain
//! - [Raft Log Persistence Architecture](./docs/architecture/raft_log_persistence_architecture/index.html)
//!   - How log entries are persisted and recovered
//! - [Snapshot Module Design](./docs/architecture/snapshot_module_design/index.html)
//!   - Snapshots for recovery optimization
//! - [New Node Join Architecture](./docs/architecture/new_node_join_architecture/index.html)
//!   - Dynamic cluster membership changes
//! - [Error Handling Design Principles](./docs/architecture/error_handling_design_principles/index.html)
//!   - How errors are categorized and handled
//! - [Single Responsibility Principle](./docs/architecture/single_responsibility_principle/index.html)
//!   - Design philosophy behind modularity
//!
//! ## Key Concepts
//!
//! ### Raft Protocol Basics
//!
//! Raft solves the consensus problem: keeping distributed systems in sync despite failures.
//!
//! **Three roles**:
//! - **Leader**: Accepts client requests and replicates logs
//! - **Candidate**: Competing to become leader (during elections)
//! - **Follower**: Replicates leader's logs
//!
//! **Key mechanisms**:
//! - **Log replication**: Leader sends entries to followers sequentially
//! - **Election**: When leader fails, followers start election with randomized timeouts
//! - **Snapshots**: Periodic snapshots reduce recovery time
//! - **Joint consensus**: Safe configuration changes using overlapping quorums
//!
//! ### Read Consistency
//!
//! d-engine supports different consistency levels:
//! - **Strong consistency**: Read from leader only (guarantees latest value)
//! - **Eventual consistency**: Read from any replica (may be stale)
//!
//! See [consistency policies guide](./docs/client_guide/read_consistency/index.html) for details.
//!
//! ### Storage Abstraction
//!
//! Pluggable backends support different requirements:
//! - **File-based storage**: Simple, local-only persistence
//! - **RocksDB**: High-performance embedded database
//! - **Custom storage**: Implement `LogStore` + `StateSnapshot` traits
//!
//! ## Common Patterns
//!
//! ### Implementing a Custom Storage Engine
//!
//! See [Customize Storage Engine guide](./docs/server_guide/customize_storage_engine/index.html) for:
//! - Trait requirements
//! - Persistence guarantees
//! - Concurrency considerations
//!
//! ### Implementing a Custom State Machine
//!
//! See [Customize State Machine guide](./docs/server_guide/customize_state_machine/index.html) for:
//! - Processing write commands
//! - Snapshot serialization
//! - Recovery from snapshots
//!
//! ### Configuring Log Purging
//!
//! See [Log Purge Executor guide](./docs/server_guide/customize_raft_log_purge_executor/index.html) for:
//! - Automatic log cleanup
//! - Retention policies
//! - Performance impact
//!
//! ## Performance
//!
//! d-engine is optimized for:
//! - **Throughput**: Batching and pipelining for high request rates
//! - **Latency**: Single-digit millisecond write latencies
//! - **Scalability**: Efficient leader-based replication
//!
//! See [Performance Tuning Guide](./docs/performance/throughput_optimization_guide/index.html) for:
//! - Configuration recommendations
//! - Benchmarking methodology
//! - Common bottlenecks and solutions
//!
//! ## Building Documentation
//!
//! ### Generate All Workspace Documentation
//!
//! ```bash
//! # Generate all crate docs (recommended)
//! cargo doc --workspace --no-deps --open
//!
//! # With all features enabled
//! cargo doc --workspace --all-features --no-deps --open
//!
//! # Include private items (for architecture understanding)
//! cargo doc --workspace --no-deps --document-private-items --open
//! ```
//!
//! ### Generate Specific Crate Documentation
//!
//! ```bash
//! # Server crate only
//! cargo doc -p d-engine-server --no-deps --open
//!
//! # Client crate only
//! cargo doc -p d-engine-client --no-deps --open
//!
//! # Core Raft implementation
//! cargo doc -p d-engine-core --no-deps --open
//! ```
//!
//! ### Using Makefile Commands
//!
//! ```bash
//! make docs          # Build documentation
//! make docs-open     # Build and open in browser
//! make docs-check    # Verify docs compile without warnings
//! ```
//!
//! ## Examples
//!
//! The `examples/` directory contains complete, runnable examples:
//!
//! - **three-nodes-cluster**: Basic 3-node cluster setup
//! - **sled-cluster**: Using Sled embedded database for storage
//! - **rocksdb-cluster**: Using RocksDB for storage
//! - **client_usage**: Complete client application example
//!
//! Run any example:
//! ```bash
//! cargo run --example three-nodes-cluster -- --node-id 1 --port 9000
//! ```
//!
//! ## Testing
//!
//! d-engine includes comprehensive tests:
//!
//! ```bash
//! # Run all tests
//! cargo test --workspace
//!
//! # Run tests for specific crate
//! cargo test -p d-engine-server
//!
//! # Run with output
//! cargo test --workspace -- --nocapture
//!
//! # Run integration tests
//! cargo test --test '*' --workspace
//! ```
//!
//! ## Troubleshooting
//!
//! ### Common Issues
//!
//! **Q: Cluster won't form**
//! - Ensure all nodes have different IDs
//! - Check network connectivity between nodes
//! - Verify ports are not in use
//!
//! **Q: High latency**
//! - Check network latency between nodes
//! - Verify storage I/O performance
//! - Consider batching multiple requests
//!
//! **Q: Memory usage growing**
//! - Check log purging is configured
//! - Verify snapshots are being taken
//! - Monitor state machine memory
//!
//! See [architecture documentation](./docs/architecture/index.html) for deeper understanding.
//!
//! ## Contributing
//!
//! Contributions welcome! Areas of interest:
//! - Performance optimizations
//! - Additional storage backends
//! - Protocol improvements
//! - Documentation and examples
//!
//! ## License
//!
//! Licensed under either of:
//! - Apache License, Version 2.0 (LICENSE-APACHE or <http://www.apache.org/licenses/LICENSE-2.0>)
//! - MIT license (LICENSE-MIT or <http://opensource.org/licenses/MIT>)
//!
//! at your option.

pub mod docs;
