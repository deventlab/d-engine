# d-engine 🚀

[![Crates.io](https://img.shields.io/crates/v/d-engine.svg)](https://crates.io/crates/d-engine)
[![docs.rs](https://docs.rs/d-engine/badge.svg)](https://docs.rs/d-engine)
[![codecov](https://codecov.io/gh/deventlab/d-engine/graph/badge.svg?token=K3BEDM45V8)](https://codecov.io/gh/deventlab/d-engine)
![Static Badge](https://img.shields.io/badge/license-MIT%20%7C%20Apache--2.0-blue)
[![CI](https://github.com/deventlab/d-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/deventlab/d-engine/actions/workflows/ci.yml)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/deventlab/d-engine)

**d-engine** is a lightweight and strongly consistent Raft consensus engine written in Rust. It is a base to build reliable and scalable distributed systems. **Designed for resource efficiency**, d-engine employs a single-threaded event-driven architecture that maximizes single CPU core performance while minimizing resource overhead. It plans to provide a production-ready implementation of the Raft consensus algorithm, with support for pluggable storage backends, observability, and runtime flexibility.

---

## Features

- **Strong Consistency**: Full implementation of the Raft protocol for distributed consensus.
- **Pluggable Storage**: Supports custom storage backends (e.g., RocksDB, Sled, Raw File).
- **Observability**: Built-in metrics, structured logging, and distributed tracing.
- **Runtime Agnostic**: Works seamlessly with `tokio`.
- **Extensible Design**: Decouples business logic from the protocol layer for easy customization.

---

## Quick Start

### Installation

Add d-engine to your `Cargo.toml`:

```toml
[dependencies]
d-engine = "0.1.2"
```

## Basic Usage (Single-Node Mode)

use d-engine::{RaftCore, MemoryStorage, Config};

```rust
async fn main() -> Result<()> {
    // Initializing Shutdown Signal
    let (graceful_tx, graceful_rx) = watch::channel(());

    // Build Node
    let node = NodeBuilder::new(settings, graceful_rx)
        .build()
        .start_rpc_server()
        .await
        .ready()
        .expect("start node failed.");

    println!("Application started. Waiting for CTRL+C signal...");

    // Start Node
    node.run().await?;

    // Listen on Shutdown Signal
    // graceful_shutdown(graceful_tx, node).await?;

    println!("Exiting program.");
    Ok(())
}
```

Note: For production use, a minimum of 3 nodes is required to ensure fault tolerance.

## Core Concepts

### Data Flow

```mermaid
sequenceDiagram
    participant Client
    participant Leader
    participant Raft_Log as Raft Log
    participant Followers
    participant State_Machine as State Machine

    Client->>Leader: Propose("SET key=value")
    Leader->>Raft_Log: Append Entry (Uncommitted)
    Leader->>Followers: Replicate via AppendEntries RPC
    Followers-->>Leader: Acknowledge Receipt
    Leader->>Raft_Log: Mark Entry Committed
    Leader->>State_Machine: Apply Committed Entry
    State_Machine-->>Client: Return Result
```

## Architecture Principles

- Single Responsibility Principle (SRP)
- Error Handling Design Principles

## Key Components

### Protocol Core (`src/core/`)

- **`election/`**: Leader election logic with configurable handlers and comprehensive test coverage.
- **`replication/`**: Log replication pipeline featuring batch buffering, handler management, and consistency guarantees.
- **`raft_role/`**: State machine implementations for all Raft roles (Leader, Follower, Candidate, Learner) with individual state handlers and tests.
- **`commit_handler/`**: Commit application pipeline with default implementation and test suite.
- **`state_machine_handler/`**: State machine operation handlers featuring snapshot policies (composite, log-size, time-based), snapshot assembly, and stream processing.

### Node Control Plane (`src/node/`)

- **`builder.rs`**: Type-safe node construction with configurable components and built-in testing.
- **`type_config/`**: Generic type configurations for protocol extensibility, including oneshot utilities and Raft type configurations.

### Storage Abstraction (`src/storage/`)

- **`raft_log.rs`**: Core Raft log operation definitions.
- **`adaptors/`**: Multiple storage engine implementations:
  - **File-based** (`file/`): Persistent storage using file system.
  - **RocksDB** (`rocksdb/`): High-performance embedded database storage.
- **`buffered/`**: Buffered Raft log layer for performance optimization.
- **`state_machine.rs`**: State machine operation definitions and test suites.

### Network Layer (`src/network/`)

- **`grpc/`**: Complete gRPC implementation:
  - `grpc_transport.rs`: Core network transport layer.
  - `grpc_raft_service.rs`: RPC service definitions and implementations.
  - `backgroup_snapshot_transfer.rs`: Background snapshot transfer management.
- **`connection_cache.rs`**: Connection pooling and management.
- **`health_checker.rs`** & **`health_monitor.rs`**: Node health monitoring and failure detection.

### Client Implementation (`src/client/`)

- **`cluster.rs`**: Cluster management and configuration.
- **`kv.rs`**: Key-value client interface with test coverage.
- **`pool.rs`**: Connection pooling for client applications.
- **`builder.rs`**: Client construction utilities.

### Configuration System (`src/config/`)

- Comprehensive configuration management covering:
  - Cluster settings (`cluster.rs`)
  - Raft parameters (`raft.rs`)
  - Network configurations (`network.rs`)
  - Security and TLS settings (`security.rs`, `tls.rs`)
  - Retry policies (`retry.rs`)
  - Monitoring configurations (`monitoring.rs`)

### Membership Management (`src/membership/`)

- **`raft_membership.rs`**: Cluster membership management with consistency guarantees.

## Performance Benchmarks

d-engine is designed for low-latency consensus operations while maintaining strong consistency. Below are key metrics compared to etcd 3.5:

**Test Setup**: d-engine v0.1.2 vs etcd 3.5， 10k ops, 8B keys, 256B values, Apple M2

| **Test Case**            | **Metric**  | **d-engine(rocksdb)** | **etcd**      | **Advantage**     |
| ------------------------ | ----------- | --------------------- | ------------- | ----------------- |
| **Basic Write**          | Throughput  | 423.61 ops/s          | 157.85 ops/s  | ✅ 2.68× d-engine |
| (1 connection, 1 client) | Avg Latency | 2,359 μs              | 6,300 μs      | ✅ 63% lower      |
|                          | p99 Latency | 3,915 μs              | 16,700 μs     | ✅ 77% lower      |
| **High Concurrency**     | Throughput  | 3,597.27 ops/s        | 5,439 ops/s   | ❌ 1.51× etcd     |
| (10 conns, 100 clients)  | Avg Latency | 2,774 μs              | 18,300 μs     | ✅ 85% lower      |
|                          | p99 Latency | 5,999 μs              | 32,400 μs     | ✅ 81% lower      |
| **Linear Read**          | Throughput  | 9,999 ops/s           | 85,904 ops/s  | ❌ 8.59× etcd     |
| (Strong consistency)     | Avg Latency | 997 μs                | 1,100 μs      | ✅ 9% lower       |
|                          | p99 Latency | 1,498 μs              | 3,200 μs      | ✅ 53% lower      |
| **Sequential Read**      | Throughput  | 42,002 ops/s          | 124,631 ops/s | ❌ 2.97× etcd     |
| (Eventual consistency)   | Avg Latency | 236 μs                | 700 μs        | ✅ 66% lower      |
|                          | p99 Latency | 514 μs                | 2,800 μs      | ✅ 82% lower      |

**Important Notes**

1. d-engine architecture uses **single-threaded** event-driven design
2. Tested on d-engine v0.1.2 (without snapshot functionality)
3. etcd 3.5 benchmark using official tools
4. All services co-located on same hardware (M2/16GB)

### View Benchmarks Detailed Reports

```bash
open benches/reports/
```

## Contribution Guide

### Prerequisites

- Rust 1.65+
- Tokio runtime
- Protobuf compiler

### Development Workflow

```bash
# Build and test
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

## Code Style

Follow Rust community standards (rustfmt, clippy).
Write unit tests for all new features.

## FAQ

**Why are 3 nodes required?**
Raft requires a majority quorum (N/2 + 1) to achieve consensus. A 3-node cluster can tolerate 1 node failure.

**How do I customize storage?**
Implement the Storage trait and pass it to RaftCore::new.

**Is d-engine production-ready?**
The current release (v0.0.1) focuses on correctness and reliability. Performance optimizations are planned for future releases.

## Supported Platforms

- Linux: x86_64, aarch64
- macOS: x86_64, aarch64

## License

d-eninge is licensed under the terms of the [MIT License](https://en.wikipedia.org/wiki/MIT_License#License_terms)
or the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0), at your choosing.
