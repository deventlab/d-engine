# Integration Modes: Embedded vs Standalone

**Decision time: 30 seconds**

Choose your integration mode based on your primary programming language and latency requirements.

---

## 30-Second Decision Guide

```text
┌─────────────────────────────────────┐
│ Is your application written in Rust?│
└──────────────┬──────────────────────┘
               │
         ┌─────┴─────┐
         │    YES    │─────────────────────────────────────┐
         └───────────┘                                     │
               │                                           │
         ┌─────┴─────────────────────────────┐            │
         │ Do you need <0.1ms KV operations? │            │
         └──────────────┬────────────────────┘            │
                        │                                  │
                  ┌─────┴─────┐                           │
                  │    YES    │                           │
                  └─────┬─────┘                           │
                        │                                  │
                        ▼                                  ▼
              ┌──────────────────┐              ┌─────────────────┐
              │ EMBEDDED MODE    │              │ STANDALONE MODE │
              │                  │              │                 │
              │ • In-process     │              │ • Separate      │
              │ • Zero overhead  │              │   server        │
              │ • <0.1ms latency │              │ • gRPC API      │
              │ • Rust-only      │              │ • 1-2ms latency │
              └──────────────────┘              │ • Any language  │
                                                └─────────────────┘
```

---

## Embedded Mode

### What is it?

d-engine runs **inside your Rust application process**:

- Direct memory access (no serialization)
- Function call latency (<0.1ms)
- Single binary deployment
- Rust API only

### When to use?

✅ **Use Embedded Mode if:**

- Your application is written in Rust
- You need ultra-low latency (<0.1ms for KV operations)
- You want zero serialization overhead
- You prefer single-binary deployment
- You're building latency-sensitive systems (trading, gaming, real-time analytics)

### Quick Start

```rust,ignore
use d_engine::EmbeddedEngine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start with config file
    let engine = EmbeddedEngine::start_with("d-engine.toml").await?;

    // Wait for leader election
    engine.wait_ready(Duration::from_secs(5)).await?;

    // Get zero-overhead KV client
    let client = engine.client();
    client.put(b"key".to_vec(), b"value".to_vec()).await?;

    engine.stop().await?;
    Ok(())
}
```

### Performance Characteristics

| Operation           | Latency              |
| ------------------- | -------------------- |
| **put()** (write)   | <0.1ms (single node) |
|                     | 1-5ms (3-node)       |
| **get()** (read)    | <0.1ms (local)       |
| **Leader election** | <100ms (single)      |
|                     | 1-2s (3-node)        |

---

## Standalone Mode

### What is it?

d-engine runs as a **separate server process**:

- gRPC-based communication
- Language-agnostic client libraries
- Network latency (1-2ms typical)
- Polyglot ecosystem support

### When to use?

✅ **Use Standalone Mode if:**

- Your application is **not** written in Rust (Go, Python, Java, etc.)
- You need language-agnostic deployment
- You prefer microservices architecture
- 1-2ms latency is acceptable
- You want to share one d-engine cluster across multiple applications

### Quick Start

**1. Start d-engine server:**

```bash
# Start single-node server
d-engine-server --config d-engine.toml
```

**2. Connect from any language:**

**Rust Client:**

```rust,ignore
use d_engine::{ClientBuilder, Client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ClientBuilder::new(vec!["http://127.0.0.1:9083".to_string()])
        .build()
        .await?;

    client.kv().put(b"key".to_vec(), b"value".to_vec()).await?;
    Ok(())
}
```

**Go Client (coming soon):**

```go
// Future: Go client library
client := dengine.NewClient("127.0.0.1:9083")
client.Put([]byte("key"), []byte("value"))
```

**Python Client (coming soon):**

```python
# Future: Python client library
client = dengine.Client("127.0.0.1:9083")
client.put(b"key", b"value")
```

### Performance Characteristics

| Operation           | Latency         |
| ------------------- | --------------- |
| **put()** (write)   | 1-2ms (local)   |
|                     | 2-10ms (remote) |
| **get()** (read)    | 1-2ms (gRPC)    |
| **Leader election** | 1-2s            |

---

## Comparison Table

| Feature              | Embedded Mode      | Standalone Mode         |
| -------------------- | ------------------ | ----------------------- |
| **Language**         | Rust only          | Any (via gRPC)          |
| **Deployment**       | In-process         | Separate server         |
| **Latency (KV ops)** | <0.1ms             | 1-2ms                   |
| **Communication**    | Direct memory      | gRPC network            |
| **Serialization**    | Zero               | Protocol Buffers        |
| **Use Case**         | Ultra-low latency  | Polyglot microservices  |
| **Single Binary**    | ✅ Yes             | ❌ No (server + app)    |
| **Cross-Language**   | ❌ No              | ✅ Yes                  |
| **Overhead**         | Minimal            | Network + serialization |
| **Complexity**       | Simple (1 process) | Moderate (2+ processes) |

---

## Migration Path

### Start Simple, Scale Later

You can start with **Embedded Mode** for development and migrate to **Standalone Mode** for production if needed:

**Development (Embedded):**

```rust,ignore
// Single node, in-process
let engine = EmbeddedEngine::start_with("d-engine.toml").await?;
let client = engine.client();
```

**Production (Standalone):**

```rust,ignore
// 3-node cluster, separate servers
let client = ClientBuilder::new(vec![
    "http://node1:9083".to_string(),
    "http://node2:9083".to_string(),
    "http://node3:9083".to_string(),
]).build().await?;
```

**Zero code changes** to your business logic - only configuration changes.

---

## Architecture Differences

### Embedded Mode Architecture

```text
┌─────────────────────────────────────────┐
│ Your Rust Application Process          │
│                                         │
│  ┌──────────────┐    ┌──────────────┐  │
│  │ Business     │◄───┤ LocalKvClient│  │
│  │ Logic        │    │ (memory)     │  │
│  └──────────────┘    └───────┬──────┘  │
│                              │          │
│                     ┌────────▼───────┐  │
│                     │ Raft Engine    │  │
│                     │ (d-engine-core)│  │
│                     └────────────────┘  │
└─────────────────────────────────────────┘
```

### Standalone Mode Architecture

```text
┌────────────────────┐         ┌────────────────────┐
│ Your Application   │         │ d-engine Server    │
│ (Any Language)     │         │                    │
│                    │         │  ┌──────────────┐  │
│  ┌──────────────┐  │  gRPC   │  │ Raft Engine  │  │
│  │ Business     │◄─┼─────────┼─►│              │  │
│  │ Logic        │  │         │  │ (d-engine-   │  │
│  └──────────────┘  │         │  │  core)       │  │
│                    │         │  └──────────────┘  │
└────────────────────┘         └────────────────────┘
     (Go, Python, Java, etc.)      (Separate process)
```

---

## Industry Comparison

### How does d-engine compare to etcd?

| Feature                | d-engine Embedded | d-engine Standalone | etcd Embedded      |
| ---------------------- | ----------------- | ------------------- | ------------------ |
| **Language**           | Rust only         | Any                 | Go only            |
| **KV Latency**         | <0.1ms (memory)   | 1-2ms (gRPC)        | 1-2ms (gRPC)       |
| **Communication**      | Direct memory     | gRPC                | gRPC (even embed!) |
| **Serialization**      | Zero              | Protobuf            | Protobuf           |
| **True Zero-Overhead** | ✅ Yes            | ❌ No               | ❌ No              |

**d-engine's Unique Value:**

- **Embedded Mode**: True zero-overhead integration (etcd embedded mode still uses gRPC)
- **Standalone Mode**: Same polyglot capabilities as etcd

---

## FAQ

### Q: Can I use both modes simultaneously?

**A:** No, but you can migrate between them without code changes (only config changes).

### Q: Which mode is more production-ready?

**A:** Both are production-ready. Choose based on your language and latency requirements.

### Q: Does Embedded Mode support clustering?

**A:** Yes! Embedded nodes communicate via network (gRPC) for Raft protocol, but your application uses local memory access.

### Q: Can I switch modes later?

**A:** Yes. Your business logic code remains the same, only connection setup changes.

### Q: Why is d-engine Embedded Mode faster than etcd embedded?

**A:** etcd embedded mode still uses gRPC for KV operations. d-engine Embedded provides direct memory access via `LocalKvClient`.

---

## Next Steps

### For Embedded Mode Users

1. [Quick Start: Embedded in 5 Minutes](./quick-start-5min.md)
2. [Advanced Embedded Usage](./server_guide/advanced-embedded.md)
3. [Custom Storage Engines](./server_guide/customize-storage-engine.md)

### For Standalone Mode Users

1. [Quick Start: Standalone Mode](./quick-start-standalone.md)
2. [Client API Guide](./client_guide/client-api.md)
3. [Multi-Language Clients](./client_guide/multi-language-clients.md)

---

**Created**: 2025-12-25  
**Last Updated**: 2025-12-25
