# d-engine 🚀
[![Crates.io](https://img.shields.io/crates/v/d-engine.svg)](https://crates.io/crates/d-engine)
[![docs.rs](https://docs.rs/d-engine/badge.svg)](https://docs.rs/d-engine)
[![codecov](https://codecov.io/gh/deventlab/d-engine/graph/badge.svg?token=K3BEDM45V8)](https://codecov.io/gh/deventlab/d-engine)
![Static Badge](https://img.shields.io/badge/license-MIT%20%7C%20Apache--2.0-blue)
[![CI](https://github.com/deventlab/d-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/deventlab/d-engine/actions/workflows/ci.yml)

**d-engine** is a lightweight and strongly consistent Raft consensus engine written in Rust. It is a base to build reliable and scalable distributed systems. It plans to provide a production-ready implementation of the Raft consensus algorithm, with support for pluggable storage backends, observability, and runtime flexibility.

---

## Features

- **Strong Consistency**: Full implementation of the Raft protocol for distributed consensus.
- **Pluggable Storage**: Supports custom storage backends (e.g., RocksDB, Sled, in-memory).
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

### Protocol Core (src/core/)

- `election/`: Leader election and heartbeat management.
- `replication/`: Log replication pipeline with batch buffering and consistency guarantees.
- `raft_role/`: State transition handlers: (leader_state | follower_state | candidate_state)
- `commit_handler/`: Commit application pipeline with default/test implementations
- `state_machine_handler/`: State machine operation executors
- `timer/`: Critical timing components: "Election timeouts" and "Heartbeat intervals"

### Node Control Plane (src/node/)
- `builder.rs`: Type-safe node construction with configurable components
- `type_config/`: Generic type configurations for protocol extensibility
- `settings.rs`: Node configuration parameters and runtime tuning
- `errors.rs`: Unified error handling for node operations

### Storage Abstraction (src/storage/)
- `raft_log.rs`: Raft local log operation definitions.
- `sled_adapter/`: Storage implementations which is based on Sled DB
- `state_machine.rs`: State machine operation definitions

### Network Layer (src/network/)
- `grpc/`: gRPC implementation:
    - `grpc_transport.rs`: Network primitives
    - `grpc_raft_service.rs`: RPC service definitions
- `rpc_peer_channels.rs`: Peer-to-peer communication channels

## Performance Benchmarks 

d-engine is designed for low-latency consensus operations while maintaining strong consistency. Below are key metrics compared to etcd 3.5:

**Test Setup**: d-engine v0.1.2 vs etcd 3.5， 10k ops, 8B keys, 256B values, Apple M2

| Operation          | Consistency  | Metric       | d-engine | etcd    | Advantage           |
|--------------------|--------------|--------------|----------|---------|---------------------|
| **Write**          |              |              |          |         |                     |
| - Basic (1C/1W)    | Strong       | Throughput   | 370/s    | 158/s   | 2.3× (d-engine wins)|
|                    |              | p99 Latency  | 5.3ms    | 16.7ms  | 68% ↓ (d-engine wins)|
| - High (10C/100W)  | Strong       | Throughput   | 4,070/s  | 5,439/s | 0.75× (etcd wins)   |
|                    |              | p99 Latency  | 4.2ms    | 32.4ms  | 7.7× (d-engine wins)|
| **Read**           |              |              |          |         |                     |
| - Linear           | Strong       | Throughput   | 6,359/s  | 85k/s   | 13.4× (etcd wins)   |
|                    |              | p99 Latency  | 6.5ms    | 3.2ms   | 2× ↑ (etcd wins)    |
| - Sequential       | Eventual     | Throughput   | 19.8k/s  | 124k/s  | 6.3× (etcd wins)    |
|                    |              | p99 Latency  | 1.3ms    | 2.8ms   | 54% ↓ (d-engine wins)|

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
cargo clippy --all-targets --all-features
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
