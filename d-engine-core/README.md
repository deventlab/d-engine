# d-engine-core

[![Crates.io](https://img.shields.io/crates/v/d-engine-core.svg)](https://crates.io/crates/d-engine-core)
[![docs.rs](https://docs.rs/d-engine-core/badge.svg)](https://docs.rs/d-engine-core)

Pure Raft consensus algorithm - for building custom Raft-based systems

---

## What is this?

This crate contains the **Raft consensus algorithm implementation** used internally by d-engine. It's extracted into a separate crate for modularity within the d-engine project.

**d-engine** is a lightweight distributed coordination engine written in Rust, designed for embedding into applications that need strong consistency—the consensus layer for building reliable distributed systems.

---

## When to use this crate

- ✅ **Contributing to d-engine** - Understanding internals
- ✅ **Research or educational purposes** - Studying Raft implementation
- ⚠️ **Custom integrations** - Possible but NOT yet recommended (see Project Status below)

---

## When NOT to use this crate

- ❌ **Production applications** → Use [`d-engine`](https://crates.io/crates/d-engine)
- ❌ **Building distributed systems** → Use [`d-engine`](https://crates.io/crates/d-engine)
- ❌ **Need stable API** → Use [`d-engine`](https://crates.io/crates/d-engine) (this crate's API may change)

---

## What you get

This crate focuses solely on the Raft consensus algorithm:

- **Leader Election** - Automatic leader election with randomized timeouts
- **Log Replication** - Reliable log replication to followers
- **Membership Changes** - Dynamic cluster membership (add/remove nodes)
- **Snapshot Support** - Log compaction via snapshots

**What you need to provide:**

- Storage implementation (persist logs and metadata)
- Network layer (send/receive RPCs)
- State machine (apply committed entries)

---

## Architecture

```text
┌─────────────────────────────────┐
│   Your Application              │
├─────────────────────────────────┤
│   d-engine-core (Raft)          │  ← You are here
├─────────────────────────────────┤
│   Your Storage │ Your Network   │  ← You implement
└─────────────────────────────────┘
```

---

## Quick Start

```rust
use d_engine_core::{Raft, StorageEngine, StateMachine, Config};
use std::sync::Arc;

// 1. Implement storage engine
struct MyStorage;

impl StorageEngine for MyStorage {
    // Persist Raft logs and metadata
    // See: https://docs.rs/d-engine-core/latest/d_engine_core/trait.StorageEngine.html
}

// 2. Implement state machine
struct MyStateMachine;
impl StateMachine for MyStateMachine {
    // Apply committed entries to your application state
    // See: https://docs.rs/d-engine-core/latest/d_engine_core/trait.StateMachine.html
}

// 3. Create Raft instance
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::default();
    let storage = Arc::new(MyStorage);
    let state_machine = Arc::new(MyStateMachine);

    let raft = Raft::new(1, storage, state_machine, config);

    // 4. Drive Raft with your event loop
    // Handle events, send RPCs, etc.

    Ok(())
}
```

---

## Comparison with other crates

| Crate                                                         | Purpose         | Networking       | Storage          |
| ------------------------------------------------------------- | --------------- | ---------------- | ---------------- |
| **d-engine-core**                                             | Algorithm only  | ❌ You implement | ❌ You implement |
| [`d-engine-server`](https://crates.io/crates/d-engine-server) | Complete server | ✅ gRPC          | ✅ RocksDB/File  |
| [`d-engine`](https://crates.io/crates/d-engine)               | Unified API     | ✅ gRPC          | ✅ RocksDB/File  |

---

## Key Traits

You need to implement these traits to use d-engine-core:

- **`StorageEngine`** - Persistent storage for Raft logs
- **`StateMachine`** - Application-specific state transitions
- **`LogStore`** - Log entry persistence
- **`MetaStore`** - Metadata persistence (term, voted_for)

See the [API documentation](https://docs.rs/d-engine-core) for detailed trait definitions.

---

## Documentation

For comprehensive guides:

- [Customize Storage Engine](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/server_guide/customize-storage-engine.md)
- [Customize State Machine](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/server_guide/customize-state-machine.md)
- [Raft Protocol Details](https://raft.github.io/)

---

## Examples

See how d-engine-server uses d-engine-core:

- [File Storage Implementation](https://github.com/deventlab/d-engine/blob/main/d-engine-server/src/storage/file_storage.rs)
- [RocksDB Storage Implementation](https://github.com/deventlab/d-engine/blob/main/d-engine-server/src/storage/rocksdb_storage.rs)

---

## Related Crates

| Crate                                                         | Purpose                                         |
| ------------------------------------------------------------- | ----------------------------------------------- |
| [`d-engine`](https://crates.io/crates/d-engine)               | **Recommended** - Start here for most use cases |
| [`d-engine-server`](https://crates.io/crates/d-engine-server) | Complete server with gRPC and storage           |
| [`d-engine-client`](https://crates.io/crates/d-engine-client) | Client library for Rust applications            |
| [`d-engine-proto`](https://crates.io/crates/d-engine-proto)   | Protocol definitions for non-Rust clients       |

---

## ⚠️ Project Status

**This crate is currently INTERNAL to d-engine and not yet ready for standalone use.**

- ✅ **Stable within d-engine-server** - Production-ready when used via d-engine (1000+ tests, Jepsen validated)
- ❌ **NOT tested as standalone library** - API may change without notice between minor versions
- 🚧 **Pre-1.0**: Direct use of this crate is **not recommended** yet

**Recommended approach:**

- Use [`d-engine`](https://crates.io/crates/d-engine) for production applications
- Use [`d-engine-server`](https://crates.io/crates/d-engine-server) if you need server-only functionality
- Only use `d-engine-core` directly if you're contributing to d-engine internals

**Future plans:** This crate may become a standalone Raft library in future versions, but it requires additional testing and API stabilization work before that goal can be achieved.

**Compatibility Promise:**

- **Before v1.0**: Breaking changes may occur between minor versions (e.g., v0.2 → v0.3)
- **After v1.0**: Breaking changes only in major versions, following [Semantic Versioning](https://semver.org/)
- All breaking changes documented in [MIGRATION_GUIDE.md](https://github.com/deventlab/d-engine/blob/main/MIGRATION_GUIDE.md)

---

## License

MIT or Apache-2.0
