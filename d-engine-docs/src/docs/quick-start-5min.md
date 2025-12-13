# Quick Start: Embedded d-engine in 5 Minutes

**Goal**: Start d-engine in embedded mode, perform KV operations, and understand the core concepts.

---

## What is Embedded Mode?

d-engine runs **inside your Rust application process**:

- Zero serialization overhead (local function calls)
- <0.1ms latency (memory access)
- Single binary deployment
- Start with 1 node, scale to 3 nodes without code changes

---

## Prerequisites

- **Rust**: stable ([install](https://rustup.rs/))
- **Disk**: ~50MB free space
- **Time**: 5 minutes

---

## Step 1: Add Dependency (30 seconds)

Add d-engine to your `Cargo.toml`:

```toml
[dependencies]
d-engine = "0.2"  # Default includes server + rocksdb
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

---

## Step 2: Write Your First d-engine App (2 minutes)

Create `src/main.rs`:

```rust
use d_engine::prelude::*;
use std::error::Error;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Starting d-engine...\n");

    // Start embedded engine with RocksDB (auto-creates directories)
    let engine = EmbeddedEngine::with_rocksdb("./data", None).await?;

    // Wait for node initialization
    engine.ready().await;
    println!("✓ Node initialized");

    // Wait for leader election (single-node: instant)
    let leader = engine.wait_leader(Duration::from_secs(5)).await?;
    println!("✓ Leader elected: node {} (term {})\n", leader.leader_id, leader.term);

    // Get KV client (zero-overhead, in-process)
    let client = engine.client();

    // Store and retrieve data
    client.put(b"hello".to_vec(), b"world".to_vec()).await?;
    println!("✓ Stored: hello = world");

    if let Some(value) = client.get(b"hello".to_vec()).await? {
        println!("✓ Retrieved: hello = {}\n", String::from_utf8_lossy(&value));
    }

    // Graceful shutdown
    engine.stop().await?;
    println!("Done!");

    Ok(())
}
```

---

## Step 3: Run It (30 seconds)

```bash
cargo run
```

**Expected output**:

```
Starting d-engine...

✓ Node initialized
✓ Leader elected: node 1 (term 1)

✓ Stored: hello = world
✓ Retrieved: hello = world

Done!
```

**Congratulations!** You just embedded a distributed consensus engine in your application.

---

## What Just Happened?

### Behind the Scenes

```rust
EmbeddedEngine::with_rocksdb("./data", None).await?
```

This one line:

1. Created `./data/storage/` (Raft logs)
2. Created `./data/state_machine/` (KV data)
3. Initialized RocksDB storage engines
4. Built Raft node with node_id=1
5. Spawned `node.run()` in background (Raft protocol)
6. Returned immediately (non-blocking)

```rust
engine.ready().await
```

Waits for node initialization (gRPC server ready, ~100ms).

```rust
engine.wait_leader(Duration::from_secs(5)).await?
```

Waits for leader election:

- **Single-node**: Instant (<100ms, auto-elected)
- **Multi-node**: Waits for majority quorum (~1-2s)

```rust
let client = engine.client()
```

Returns `LocalKvClient` for zero-overhead KV operations.

---

## Core Concepts

### 1. Single-Node is a Valid Cluster

```
1 node = Raft cluster of 1
- Auto-elected as leader
- All writes commit immediately (quorum = 1)
- Fully durable (persisted to disk)
```

**Use case**: Development, testing, small workloads.

---

### 2. Local-First Operations

```rust
client.put(key, value).await?;  // <0.1ms (local memory + disk)
```

No network, no serialization. Just direct function calls to Raft core.

---

### 3. Automatic Lifecycle Management

```rust
let engine = EmbeddedEngine::with_rocksdb("./data", None).await?;
// ↑ Internally spawns node.run() in background

engine.stop().await?;
// ↑ Gracefully shuts down background task
```

No manual `tokio::spawn()`, no leaked tasks.

---

## API Reference

### EmbeddedEngine

```rust
// Quick-start (development)
EmbeddedEngine::with_rocksdb(data_dir: &str, config_path: Option<&str>) -> Result<Self>

// Production (custom storage)
EmbeddedEngine::start(
    config_path: Option<&str>,
    storage: Arc<impl StorageEngine>,
    state_machine: Arc<impl StateMachine>
) -> Result<Self>

// Wait for initialization (NOT election)
engine.ready().await

// Wait for leader election (event-driven, no polling)
engine.wait_leader(timeout: Duration) -> Result<LeaderInfo>

// Get KV client
engine.client() -> &LocalKvClient

// Subscribe to leader changes (optional, for monitoring)
engine.leader_notifier() -> watch::Receiver<Option<LeaderInfo>>

// Graceful shutdown
engine.stop().await -> Result<()>
```

### LocalKvClient

```rust
// Write (replicates to majority)
client.put(key: Vec<u8>, value: Vec<u8>) -> Result<PutResponse>

// Read (local, no network)
client.get(key: Vec<u8>) -> Result<Option<Vec<u8>>>

// Delete
client.delete(key: Vec<u8>) -> Result<DeleteResponse>
```

---

## Performance Characteristics

| Operation           | Single-Node         | 3-Node Cluster              |
| ------------------- | ------------------- | --------------------------- |
| **put()**           | <0.1ms (local)      | 1-5ms (network replication) |
| **get()**           | <0.1ms (local read) | <0.1ms (local read)         |
| **Leader election** | <100ms              | 1-2s (network RTT)          |
| **Failover**        | N/A (no peers)      | 1-2s (auto re-election)     |

---

## Common Patterns

### Pattern 1: Use Default /tmp Location

```rust
// Data stored in /tmp/d-engine/
let engine = EmbeddedEngine::with_rocksdb("", None).await?;
```

### Pattern 2: Custom Data Directory

```rust
// Data stored in ./my-app-data/
let engine = EmbeddedEngine::with_rocksdb("./my-app-data", None).await?;
```

### Pattern 3: Monitor Leader Changes

```rust
let mut leader_rx = engine.leader_notifier();

tokio::spawn(async move {
    while leader_rx.changed().await.is_ok() {
        match leader_rx.borrow().as_ref() {
            Some(info) => println!("Leader: {} (term {})", info.leader_id, info.term),
            None => println!("No leader (election in progress)"),
        }
    }
});
```

### Pattern 4: Handle Election Timeout

```rust
match engine.wait_leader(Duration::from_secs(10)).await {
    Ok(leader) => println!("Leader ready: {}", leader.leader_id),
    Err(_) => {
        eprintln!("Election timeout - check cluster configuration");
        return Err("No quorum".into());
    }
}
```

---

## Troubleshooting

### "Election timeout"

**Cause**: Node failed to initialize or panicked during startup.

**Fix**:

- Check console output for error messages
- Verify data directory is writable
- Ensure sufficient disk space

### "Failed to create data directory"

**Cause**: Permission denied or invalid path.

**Fix**:

```bash
# Check permissions
ls -la ./data

# Or use /tmp (always writable)
let engine = EmbeddedEngine::with_rocksdb("", None).await?;
```

### "Address already in use"

**Cause**: Previous instance still running.

**Fix**:

```bash
# Find and kill process
lsof -i :9081
kill <PID>

# Or use different port in config
```

---

## Next Steps

### Scale to 3-Node Cluster

See [examples/single-node-expansion.md](./examples/single-node-expansion.md):

- How to expand from 1 node to 3 nodes
- Configuration changes needed
- Testing cluster failover
- Zero code changes required

### Production-Grade 3-Node Cluster

See [examples/three-nodes-cluster.md](./examples/three-nodes-cluster.md):

- Start with 3 nodes simultaneously
- Production-ready configuration
- Benchmark reference setup
- Performance profiling

### Integration Modes

**Embedded Mode** (this guide):

- Runs inside your Rust application
- Zero serialization overhead
- <0.1ms latency

**Standalone Mode**:

- Runs as separate server process
- Language-agnostic (Go, Python, etc.)
- gRPC communication

### Advanced Usage

See [advanced-embedded.md](./advanced-embedded.md):

- Custom storage engines
- Custom state machines
- Fine-grained lifecycle control
- Performance tuning

---

## Key Takeaways

- ✅ **3-line startup**: `with_rocksdb()` → `ready()` → `wait_leader()`
- ✅ **Zero boilerplate**: No manual spawn, no shutdown channels
- ✅ **Event-driven**: No polling, <1ms notification latency
- ✅ **Production-ready**: Auto-creates directories, graceful shutdown
- ✅ **Single-node simplicity**: Perfect for development and small workloads

**This is what "embedded distributed engine" means**: complexity hidden, power exposed.

---

**Created**: 2025-11-28  
**Updated**: 2025-11-28
