# d-engine-server

[![Crates.io](https://img.shields.io/crates/v/d-engine-server.svg)](https://crates.io/crates/d-engine-server)
[![docs.rs](https://docs.rs/d-engine-server/badge.svg)](https://docs.rs/d-engine-server)

Complete Raft server with gRPC and storage - batteries included

---

## What is this?

This crate provides a **complete Raft server implementation** with gRPC networking, persistent storage, and cluster orchestration. It's the server runtime component of d-engine.

**d-engine** is a lightweight distributed coordination engine written in Rust, designed for embedding into applications that need strong consistency—the consensus layer for building reliable distributed systems.

---

## When to use this crate

- ✅ **Embedding server** in a larger Rust application
- ✅ Need **programmatic access** to server APIs
- ✅ Building **custom tooling** around d-engine
- ✅ Already have your own client implementation

---

## When NOT to use this crate

- ❌ **Most applications** → Use [`d-engine`](https://crates.io/crates/d-engine) for simpler dependency management
- ❌ **Need client + server** → Use [`d-engine`](https://crates.io/crates/d-engine) with `features = ["server", "client"]`
- ❌ **Quick start preferred** → Use [`d-engine`](https://crates.io/crates/d-engine) for unified API

---

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
d-engine-server = "0.2"
```

### Embedded Mode (zero-overhead local client)

```rust
use d_engine_server::EmbeddedEngine;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = EmbeddedEngine::start_with("config.toml").await?;
    engine.wait_ready(Duration::from_secs(5)).await?;

    let client = engine.client();
    client.put(b"key".to_vec(), b"value".to_vec()).await?;

    engine.stop().await?;
    Ok(())
}
```

### Standalone Mode (independent server)

```rust
use d_engine_server::StandaloneServer;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("CONFIG_PATH", "config.toml");
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    StandaloneServer::run(shutdown_rx).await?;
    Ok(())
}
```

---

## Features

This crate provides:

- **gRPC Server** - Production-ready Raft RPC implementation
- **Storage Backends** - File-based and RocksDB storage
- **Cluster Orchestration** - Node lifecycle and membership management
- **Snapshot Coordination** - Automatic log compaction
- **Watch API** (optional) - Real-time state change notifications
- **EmbeddedEngine API** - High-level API for embedded mode

---

## Architecture

```text
┌─────────────────────────────────────────┐
│ d-engine-server                         │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │ API Layer                       │   │
│  │  - EmbeddedEngine               │   │
│  │  - StandaloneServer             │   │
│  │  - LocalKvClient                │   │
│  └──────────────┬──────────────────┘   │
│                 │                       │
│  ┌──────────────▼──────────────────┐   │
│  │ Node (Raft orchestration)       │   │
│  │  - Leader election              │   │
│  │  - Log replication              │   │
│  │  - Membership changes           │   │
│  └───────┬──────────────┬──────────┘   │
│          │              │               │
│  ┌───────▼────────┐  ┌──▼────────────┐ │
│  │ StorageEngine  │  │ StateMachine  │ │
│  │ (Raft logs)    │  │ (KV store)    │ │
│  └────────────────┘  └───────────────┘ │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │ gRPC Services                   │   │
│  │  - Client RPC (read/write)      │   │
│  │  - Cluster RPC (membership)     │   │
│  │  - Internal RPC (replication)   │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

---

## Storage Backends

### File Storage (Default)

Simple file-based storage for development and small deployments.

```rust
use d_engine_server::{FileStorageEngine, FileStateMachine};

let storage = Arc::new(FileStorageEngine::new("./logs")?);
let sm = Arc::new(FileStateMachine::new("./sm").await?);
```

### RocksDB Storage (Production)

High-performance embedded database for production use.

```toml
[dependencies]
d-engine-server = { version = "0.2", features = ["rocksdb"] }
```

```rust
use d_engine_server::{RocksDBStorageEngine, RocksDBStateMachine};

let storage = Arc::new(RocksDBStorageEngine::new("./data/logs")?);
let sm = Arc::new(RocksDBStateMachine::new("./data/sm").await?);
```

---

## Custom Storage

Implement [`StateMachine`] and [`StorageEngine`] traits:

```rust
use d_engine_server::{StateMachine, StorageEngine};

struct MyStateMachine;
impl StateMachine for MyStateMachine {
    // Apply committed entries to your application state
}

struct MyStorageEngine;
impl StorageEngine for MyStorageEngine {
    // Persist Raft logs and metadata
}
```

See the [Server Guide](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/index.html) for detailed implementation instructions.

---

## EmbeddedEngine API

High-level API for embedding d-engine in Rust applications:

```rust
use d_engine_server::EmbeddedEngine;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start embedded engine with config file
    let engine = EmbeddedEngine::start_with("d-engine.toml").await?;

    // Wait for leader election
    let leader = engine.wait_ready(Duration::from_secs(5)).await?;
    println!("Leader elected: {}", leader.leader_id);

    // Get zero-overhead client
    let client = engine.client();
    client.put(b"key".to_vec(), b"value".to_vec()).await?;

    // Graceful shutdown
    engine.stop().await?;
    Ok(())
}
```

---

## Documentation

For comprehensive guides:

- [Customize Storage Engine](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/customize_storage_engine/index.html)
- [Customize State Machine](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/customize_state_machine/index.html)
- [Watch Feature Guide](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/watch_feature/index.html)
- [Consistency Tuning](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/consistency_tuning/index.html)

---

## Examples

See working examples in the repository:

- [Single Node Expansion](https://github.com/deventlab/d-engine/tree/main/examples/single-node-expansion)
- [Three Nodes Cluster](https://github.com/deventlab/d-engine/tree/main/examples/three-nodes-standalone)
- [Quick Start Embedded](https://github.com/deventlab/d-engine/tree/main/examples/quick-start-embedded)

---

## Related Crates

| Crate                                                         | Purpose                                           |
| ------------------------------------------------------------- | ------------------------------------------------- |
| [`d-engine`](https://crates.io/crates/d-engine)               | **Recommended** - Unified API for most users      |
| [`d-engine-client`](https://crates.io/crates/d-engine-client) | Client library for Rust applications              |
| [`d-engine-core`](https://crates.io/crates/d-engine-core)     | Pure Raft algorithm for custom integrations       |
| [`d-engine-proto`](https://crates.io/crates/d-engine-proto)   | Protocol definitions (foundation for all clients) |

---

## License

MIT or Apache-2.0
