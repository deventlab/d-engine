# d-engine-core

[![Crates.io](https://img.shields.io/crates/v/d-engine-core.svg)](https://crates.io/crates/d-engine-core)
[![docs.rs](https://docs.rs/d-engine-core/badge.svg)](https://docs.rs/d-engine-core)

Pure Raft consensus algorithm - for building custom Raft-based systems

---

## ⚠️ Internal Crate - Not Ready for Standalone Use

**Use [`d-engine`](https://crates.io/crates/d-engine) instead.**

This crate contains the pure Raft consensus algorithm used internally by d-engine. The API is unstable before v1.0.

```toml
# ❌ Don't use this directly
[dependencies]
d-engine-core = "0.2"

# ✅ Use this instead
[dependencies]
d-engine = "0.2"
```

---

## For Contributors

This crate provides the core Raft consensus algorithm:

- **Leader Election** - Automatic leader election with randomized timeouts
- **Log Replication** - Reliable log replication to followers
- **Membership Changes** - Dynamic cluster membership (add/remove nodes)
- **Snapshot Support** - Log compaction via snapshots

**Reference integration**: See how [d-engine-server](https://github.com/deventlab/d-engine/tree/main/d-engine-server) uses this crate.

**Key traits to understand**:

- `StorageEngine` - Persistent storage for Raft logs
- `StateMachine` - Application-specific state transitions
- `LogStore` - Log entry persistence
- `MetaStore` - Metadata persistence (term, voted_for)

See the [API documentation](https://docs.rs/d-engine-core) for detailed trait definitions.

---

## Future Vision

**Post-1.0 goal**: Become a standalone Raft library with stable API.

**Current status**: Internal to d-engine, API may change between minor versions.

---

## Documentation

For understanding d-engine internals:

- [Customize Storage Engine](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/customize_storage_engine/index.html)
- [Customize State Machine](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/customize_state_machine/index.html)

---

## License

MIT or Apache-2.0
