# Changelog

All notable changes to this project will be documented in this file.

---

## [v0.2.0] - 2025-12-11 [✅ Released]

### 🎯 Highlights for Developers

#### Workspace Structure - Modular Dependencies

**Problem**: v0.1.x pulled all dependencies even for client-only usage  
**Solution**: Feature flags `client`/`server`/`full` - depend only on what you need

```toml
# Client-only (lightweight)
d-engine = { version = "0.2", features = ["client"] }

# Embedded server (full engine)
d-engine = { version = "0.2", features = ["server"] }
```

**Impact**: Faster builds, smaller binaries

#### TTL/Lease - Automatic Key Expiration

**Use Case**: Distributed locks, session management, temporary state  
**API**: `client.put_with_ttl("session:123", data, Duration::from_secs(60))`  
**Feature**: Crash-safe (survives restart via absolute expiration time)

#### Watch API - Real-Time Key Monitoring

**Use Case**: Config change notifications, service discovery  
**Example**:

```rust,ignore
let mut watcher = client.watch("config/").await?;
while let Some(event) = watcher.next().await {
    println!("Changed: {:?}", event);
}
```

**Performance**: Lock-free, <0.1ms notification latency

#### EmbeddedEngine - Simplified Single-Node Start

**Before**: `build() → start_rpc_server() → ready()` (3 steps)  
**Now**: `start_server()` (1 step)  
**Benefit**: Scale from 1→3 nodes with zero code changes

#### LocalKvClient - Zero-Overhead Embedded Access

**When**: Your app and d-engine in same process  
**Benefit**: Skip gRPC serialization, direct memory access (<0.1ms)  
**Example**: See `examples/service-discovery-embedded/`

---

### 📚 New Examples

- `examples/quick-start/` - 5-minute single-node setup
- `examples/single-node-expansion/` - Dynamic 1→3 node scaling
- `examples/service-discovery-embedded/` - LocalKvClient usage
- `examples/service-discovery-standalone/` - Watch API pattern

---

### ⚠️ Migration Notes

#### API Changes (Backward Compatible)

- **Deprecated**: `NodeBuilder::build().start_rpc_server().await`
- **Recommended**: `NodeBuilder::start_server().await`
- Old API still works but marked deprecated

#### Workspace Structure

- **No Action Required**: `d-engine = "0.2"` works out of the box
- **Only Affects**: Users directly depending on internal crates (rare)

#### Storage Format (File-based State Machine)

- **Breaking**: WAL now stores absolute expiration time (not relative TTL)
- **Migration**: See [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md) for upgrade path
- **Benefit**: etcd-compatible lease semantics, crash-safe TTL

---

### 🚀 Performance & Quality

- Watch API: Lock-free, supports 10K+ concurrent watchers
- TTL cleanup: Lazy + scheduled hybrid, <1% CPU overhead
- 1000+ new integration tests covering edge cases
- Zero clippy warnings across entire codebase

---

## [v0.1.4] - 2025-10-12 [✅ Released]

### Features

- **Read Consistency Policies**: Implemented three-tier read consistency model with LeaseRead, LinearizableRead, and EventualConsistency support (#142)
- **Lease-Based Read Optimization**: Added leader-local reads with lease validation for improved read performance without sacrificing strong consistency (#142)

### Performance

- **Write Path Optimization**: Optimized RocksDB write path and Raft log loop for reduced latency (#141)
- **Zero-Copy Proto**: Migrated proto bytes fields to `bytes::Bytes` for zero-copy serialization (#140)
- **gRPC Compression**: Refactored gRPC compression configuration for Raft transport layer (#143)
- **Long-lived Peer Connections**: Optimized AppendEntries network layer with persistent peer task pools (#138)
- **Dedicated Read Thread Pool**: Offloaded state machine read operations to separate thread pool to improve throughput (#135)

### Testing

- **Multi-node Deployment**: Conducted comprehensive multi-node deployment testing for throughput validation (#137)
- **100K QPS Benchmark**: Achieved sustained 100,000+ QPS in high concurrency scenarios

---

## [v0.1.3] - 2025-09-XX [✅ Released]

### Features

- **Learner Join Flow**: Revised learner join process with promotion/fail semantics (#101)
- **Node Removal**: Automatic Node Removal (#102)
- **Learner Discovery**: Added auto-discovery support for new learner nodes (#89)
- **Snapshot Feature**: Implemented snapshot feature (#79)
- **Snapshot Compression**: Refactored compression logic from StateMachine to StateMachineHandler (#122)
- **Log Conflict Resolution**: Implemented first/last index for term to resolve replication conflicts (#45)
- **RocksDB Feature Flag**: Made RocksDB adapter optional via feature flag (#125)
- **Peer Connection Cache**: Enable RPC connection cache (#109)

### Fixes

- **Leaership Confirmation** Retry leadership noop confirmation until timeout (#106)

### Refactors

- **StateMachine API**: Made StateMachine trait more developer-friendly (#120)
- **StorageEngine API**: Made StorageEngine trait more developer-friendly (#119)

---

## [v0.1.2] - 2025-04-20 [✅ Released]

### Features

- **Benchmarking**: Added etcd v3.5 benchmarking on Mac Mini M2 (#59)
- **Client Example**: Created new crate client usage example (#71)
- **Raft Protocol**: Leader now sends empty log entry after election (#43)

### Fixes

- **Logging**: Replaced `log` crate with `tracing` implementation (#68)
- **Node Shutdown**: Fixed unexpected node termination after stress tests (#70)

### Refactors

- **Error Handling**: Separated protocol errors from system-level errors (#66)

---

## [v0.1.0] - 2025-04-11 [✅ Released]

### Added

- Initial implementation of core Raft consensus algorithm
  - Leader election
  - Log replication
  - State machine persistence
- Basic cluster communication layer using gRPC
  - Node-to-node heartbeat mechanism
  - AppendEntries RPC implementation
- Minimal working example demonstrating 3-node cluster setup

---

[//]: # "Version Links"
[v0.1.0]: https://github.com/deventlab/d-engine/releases/tag/v0.1.0
[v0.1.2]: https://github.com/deventlab/d-engine/releases/tag/v0.1.2
[v0.1.3]: https://github.com/deventlab/d-engine/releases/tag/v0.1.3
