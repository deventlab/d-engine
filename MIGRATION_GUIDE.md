# NodeBuilder API Migration Guide

## Overview

Starting from **v0.2.0**, d-engine introduces a simplified `NodeBuilder` API that unifies node initialization into a single async method: `start_server()`.

This guide helps you migrate from the old three-step API to the new unified API.

---

## What Changed

### Old API (v0.1.x)

```rust
let node = NodeBuilder::new(None, graceful_rx)
    .storage_engine(storage_engine)
    .state_machine(state_machine)
    .build()                    // Step 1: Build Raft core
    .await
    .start_rpc_server()         // Step 2: Start gRPC server
    .await
    .ready()                    // Step 3: Get ready node
    .expect("Failed to start node");

node.run().await?;
```

### New API (v0.2.0+)

```rust
let node = NodeBuilder::new(None, graceful_rx)
    .storage_engine(storage_engine)
    .state_machine(state_machine)
    .start_server()             // Single unified call
    .await?;

node.run().await?;
```

---

## Migration Steps

### Step 1: Remove `build()` call

**Before:**
```rust
.build()
.await
.start_rpc_server()
.await
.ready()?
```

**After:**
```rust
.start_server()
.await?
```

### Step 2: Update error handling

The new API returns `Result<Arc<Node>>` directly, so you can use `?` operator instead of chaining `.ready()`.

**Before:**
```rust
let node = NodeBuilder::new(None, graceful_rx)
    .storage_engine(storage_engine)
    .state_machine(state_machine)
    .build()
    .await
    .start_rpc_server()
    .await
    .ready()
    .expect("Failed to start node");
```

**After:**
```rust
let node = NodeBuilder::new(None, graceful_rx)
    .storage_engine(storage_engine)
    .state_machine(state_machine)
    .start_server()
    .await?;
```

### Step 3: Update async context if needed

Make sure your function is `async` or you use `block_on()` to handle the `.await`.

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

    let storage = Arc::new(FileStorageEngine::new(path)?);
    let state_machine = Arc::new(FileStateMachine::new(path).await?);

    let node = NodeBuilder::new(None, shutdown_rx)
        .storage_engine(storage)
        .state_machine(state_machine)
        .start_server()
        .await?;

    node.run().await?;
    Ok(())
}
```

---

## What Stayed the Same

These methods still work exactly as before:

```rust
NodeBuilder::new(config, shutdown_rx)
    .storage_engine(storage)          // Same
    .state_machine(state_machine)     // Same
    .with_custom_state_machine_handler(handler)  // Same
    .start_server()                   // New!
    .await?
```

---

## Examples

### Single Node Example

```rust
use d_engine::{NodeBuilder, FileStorageEngine, FileStateMachine};
use std::sync::Arc;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

    let path = PathBuf::from("/tmp/db");
    let storage = Arc::new(FileStorageEngine::new(path.join("storage"))?);
    let state_machine = Arc::new(FileStateMachine::new(path.join("state_machine")).await?);

    let node = NodeBuilder::new(None, shutdown_rx)
        .storage_engine(storage)
        .state_machine(state_machine)
        .start_server()
        .await?;

    println!("Node started successfully!");
    node.run().await?;
    Ok(())
}
```

### Three-Node Cluster

```rust
let node = NodeBuilder::new(
    Some("config.yaml"),  // Config file path
    shutdown_rx
)
.storage_engine(storage)
.state_machine(state_machine)
.start_server()
.await?;

node.run().await?;
```

### With RocksDB

```rust
use d_engine::{RocksDBStorageEngine, RocksDBStateMachine};

let storage = Arc::new(RocksDBStorageEngine::new("/data/storage")?);
let state_machine = Arc::new(RocksDBStateMachine::new("/data/state_machine")?);

let node = NodeBuilder::new(None, shutdown_rx)
    .storage_engine(storage)
    .state_machine(state_machine)
    .start_server()
    .await?;
```

---

## Why This Change?

### Benefits of the new API

1. **Simpler** - One call instead of three
2. **Clearer Intent** - `start_server()` is self-documenting
3. **Fewer Errors** - Less chance of forgetting `.ready()` call
4. **Type Safety** - Returns `Result` directly for better error handling
5. **Consistency** - Aligns with Rust async patterns

### What Happens Inside

The `start_server()` method internally:

1. Calls `build()` to initialize the Raft core
2. Calls `start_rpc_server()` to start the gRPC server
3. Calls `ready()` to return the initialized node

All three steps happen, just hidden behind a cleaner API.

---

## Troubleshooting

### Error: `cannot find method 'start_server'`

**Cause**: You're using d-engine < 0.2.0

**Solution**: Update your `Cargo.toml`:
```toml
d-engine = "0.2"  # or higher
```

### Error: `expected 'bool', found 'unit'`

**Cause**: Old code trying to use `.ready()` which no longer exists

**Solution**: Remove the `.ready()` call and use `?` instead:
```rust
// Old
.ready()?

// New
.start_server()
.await?
```

### Example builds but node doesn't start

**Cause**: Forgetting `.await` on `start_server()`

**Solution**: Make sure you have the `.await` call:
```rust
let node = NodeBuilder::new(None, shutdown_rx)
    .storage_engine(storage)
    .state_machine(state_machine)
    .start_server()
    .await?;  // Don't forget this!
```

---

## References

- **API Documentation**: Run `cargo doc --open` and search for `NodeBuilder`
- **Examples**: See `examples/` directory for complete working examples
- **Issues**: Report migration issues on GitHub

---

## Timeline

| Version | Status | API |
|---------|--------|-----|
| v0.1.x | ✅ Stable | `.build().start_rpc_server().await.ready()` |
| v0.2.0+ | ✅ Current | `.start_server().await` |

The old API is **not** supported in v0.2.0+. Please migrate to the new API.
