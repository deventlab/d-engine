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
d-engine = "0.1.3"
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

## Performance Comparison (d-engine v0.1.3 vs etcd 3.5)

## ![d-engine vs etcd comparison](./benches/d-engine-bench/reportrs/v0.1.3/dengine_comparison_v0.1.3.png)

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
