# Changelog

All notable changes to this project will be documented in this file.

---

## [Unreleased v0.2.3]

### Added

- **CompareAndSwap (CAS) Operation** (#258): Atomic compare-and-swap primitive for distributed coordination
  - Use cases: Distributed locks, leader election, optimistic updates
  - API: `client.compare_and_swap(key, expected_value, new_value)`
  - Performance: Zero additional latency vs regular writes

### Changed

- **Unified Client API** (#258): Merged KV and cluster operations into single `ClientApi` trait
  - **Breaking**: `KvClient` → `ClientApi`, `KvError` → `ClientApiError`
  - Simplifies developer experience (single trait for all operations)
  - Both `GrpcKvClient` and `LocalKvClient` implement unified interface

- **WriteResult Message** (#258): Replaced `bool succeeded` with `WriteResult` message
  - Improves API extensibility (reserved fields for version tracking)
  - Better type safety for future features

### Migration Notes

- Replace `KvClient` with `ClientApi` in trait bounds
- Replace `KvError` with `ClientApiError` in error handling
- Update imports: `use d_engine::client::ClientApi;`

---

## [v0.2.2] - 2026-01-12 [✅ Released]

### 🎯 Key Improvements

- **ReadIndex Batching** - 440% linearizable read performance improvement (#236)
- **Embedded Mode Benchmarks** - Zero-copy performance validated (#233)
- **Cluster State APIs** - HA support (#234)
  - New: `is_leader()`, `node_id()`, `current_term()`, `wait_ready()`
  - Use case: Load balancers, health checks, leader discovery

---

### 🐛 Critical Fixes

- **Inconsistent Reads** (#228): Single-node mode returning stale data
- **Learner Promotion** (#212): Promotion stuck due to voter count bugs
- **Data Loss** (#242): Storage layer durability bugs
- **Snapshot Purge** (#235): Single-node cluster NoPeersAvailable error
- **Startup Race** (#209): `wait_ready()` timeout race condition

---

### ⚠️ Breaking Changes

**None** - All changes are backward compatible

---

## [v0.2.1] - 2026-01-01 [✅ Released]

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

#### StandaloneServer - One-Line Deployment

**Use Case**: Independent server process (production deployment)  
**API**: `run(shutdown_rx)` uses env config, `run_with(config_path, shutdown_rx)` uses explicit config  
**Benefit**: Blocks until shutdown, no manual lifecycle management

#### EmbeddedEngine - In-Process Integration

**Use Case**: Embed d-engine in your Rust application  
**API**: `start()` uses env config, `start_with(config_path)` uses explicit config, `start_custom(...)` for advanced usage  
**Benefit**: Zero gRPC overhead via LocalKvClient (<0.1ms latency)

#### LocalKvClient - Zero-Overhead Embedded Access

**When**: Your app and d-engine in same process  
**Benefit**: Skip gRPC serialization, direct memory access (<0.1ms)  
**Example**: See `examples/service-discovery-embedded/`

---

### 📚 New Examples

- `examples/quick-start/` - 5-minute single-node setup
- `examples/single-node-expansion/` - Dynamic 1→3 node scaling
- `examples/service-discovery-embedded/` - LocalKvClient zero-overhead access
- `examples/service-discovery-standalone/` - Watch API pattern

---

### ⚠️ Breaking Changes

**File-based State Machine WAL Format**

- WAL now uses absolute expiration time (not relative TTL)
- **Action Required**: See [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md) if upgrading from v0.1.x
- **New users**: No action needed

---

### 🚀 Performance & Quality

- Watch API: Lock-free, tested with 1000+ concurrent watchers, <0.1ms notification latency
- TTL cleanup: Lazy (read-time check) + Background (scheduled task), <1% CPU overhead
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
