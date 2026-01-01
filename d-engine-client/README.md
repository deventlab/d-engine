# d-engine-client

[![Crates.io](https://img.shields.io/crates/v/d-engine-client.svg)](https://crates.io/crates/d-engine-client)
[![docs.rs](https://docs.rs/d-engine-client/badge.svg)](https://docs.rs/d-engine-client)

Client library for interacting with d-engine Raft clusters

---

## What is this?

This crate provides a **high-level Rust client** for connecting to d-engine clusters over gRPC. It handles cluster discovery, leader redirection, and provides a type-safe API for key-value operations.

**d-engine** is a lightweight distributed coordination engine written in Rust, designed for embedding into applications that need strong consistency—the consensus layer for building reliable distributed systems.

---

## When to use this crate

- ✅ Building **client-only Rust applications** that connect to d-engine clusters
- ✅ Need **fine-grained control** over client behavior (timeouts, retries, connection pooling)
- ✅ Integrating d-engine into **existing Rust services** as a dependency
- ✅ Building **microservices** that communicate with a separate d-engine cluster

---

## When NOT to use this crate

- ❌ **Embedded mode (single process)** → Use [`d-engine`](https://crates.io/crates/d-engine) with `LocalKvClient` for zero overhead
- ❌ **Simplicity preferred** → Use [`d-engine`](https://crates.io/crates/d-engine) with `features = ["client"]` for simpler dependency management
- ❌ **Non-Rust applications** → Use [`d-engine-proto`](https://crates.io/crates/d-engine-proto) to generate clients for Go/Python/Java

---

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
d-engine-client = "0.2"
```

### Basic Usage

```rust
use d_engine_client::{Client, KvClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to cluster (automatic leader discovery)
    let client = Client::connect(vec![
        "http://localhost:50051",
        "http://localhost:50052",
        "http://localhost:50053",
    ]).await?;

    // Write data
    client.put(b"key".to_vec(), b"value".to_vec()).await?;

    // Read data
    if let Some(value) = client.get(b"key".to_vec()).await? {
        println!("Value: {:?}", value);
    }

    Ok(())
}
```

---

## Read Consistency

Choose consistency level based on your needs:

```rust
use d_engine_client::{Client, KvClient};

let client = Client::connect(endpoints).await?;

// Strong consistency (read from Leader, highest latency)
let value = client.get_linearizable(key).await?;

// Optimized with leader lease (balanced)
let value = client.get_lease(key).await?;

// Fast local reads (may return stale data)
let value = client.get_eventual(key).await?;
```

**Performance comparison:**

- **Linearizable**: ~20-30ms (requires quorum)
- **Lease**: ~5-10ms (leader lease optimization)
- **Eventual**: <1ms (local read, may be stale)

See [Read Consistency Guide](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/client_guide/read-consistency.md) for details.

---

## Features

This crate provides:

- **Automatic Cluster Discovery** - Connects to any node, discovers cluster topology
- **Leader Redirection** - Automatically retries writes on leader changes
- **Connection Pooling** - Efficient connection reuse with health checks
- **Configurable Timeouts** - Fine-grained control over request/connection timeouts
- **gRPC Compression** - Optional gzip compression for bandwidth savings
- **Type-Safe API** - Strongly-typed operations with proper error handling

---

## Architecture

```text
┌────────────────────┐         ┌────────────────────┐
│ Your Application   │         │ d-engine Cluster   │
│ (Rust)             │         │                    │
│                    │  gRPC   │  ┌──────────────┐  │
│  ┌──────────────┐  │ ◄─────► │  │ Leader       │  │
│  │ d-engine-    │  │         │  └──────────────┘  │
│  │ client       │  │         │  ┌──────────────┐  │
│  └──────────────┘  │         │  │ Follower     │  │
│                    │         │  └──────────────┘  │
└────────────────────┘         └────────────────────┘
```

---

## Advanced Configuration

```rust
use d_engine_client::ClientBuilder;
use std::time::Duration;

let client = ClientBuilder::new(vec![
    "http://node1:50051".into(),
    "http://node2:50052".into(),
])
.connect_timeout(Duration::from_secs(3))
.request_timeout(Duration::from_secs(1))
.enable_compression(true)
.build()
.await?;
```

---

## Error Handling

```rust
use d_engine_client::{Client, KvClient, ClientApiError};

match client.put(key, value).await {
    Ok(_) => println!("Success"),
    Err(ClientApiError::NotLeader { leader_id, leader_address }) => {
        println!("Not
 leader, retry at: {:?}", leader_address);
    }
    Err(ClientApiError::Timeout) => {
        println!("Request timeout, retry later");
    }
    Err(e) => println!("Error: {:?}", e),
}
```

See [Error Handling Guide](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/client_guide/error-handling.md) for comprehensive error handling strategies.

---

## Documentation

For comprehensive guides:

- [Read Consistency](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/client_guide/read-consistency.md)
- [Error Handling](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/client_guide/error-handling.md)
- [Service Discovery Pattern](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/client_guide/service-discovery-pattern.md)

---

## Examples

See working examples in the repository:

- [Client Usage Example](https://github.com/deventlab/d-engine/tree/main/examples/client_usage)
- [Service Discovery (Standalone)](https://github.com/deventlab/d-engine/tree/main/examples/service-discovery-standalone)

---

## Related Crates

| Crate                                                         | Purpose                                                 |
| ------------------------------------------------------------- | ------------------------------------------------------- |
| [`d-engine`](https://crates.io/crates/d-engine)               | **Recommended** - Unified API with simpler dependencies |
| [`d-engine-server`](https://crates.io/crates/d-engine-server) | Server implementation for running d-engine clusters     |
| [`d-engine-proto`](https://crates.io/crates/d-engine-proto)   | Protocol definitions for non-Rust clients               |
| [`d-engine-core`](https://crates.io/crates/d-engine-core)     | Pure Raft algorithm for custom integrations             |

---

## License

MIT or Apache-2.0
