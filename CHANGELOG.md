# Changelog

All notable changes to this project will be documented in this file.

---

## [v0.2.0] - 2025-11-12 [✅ Released]

### 🚀 Features

- **TTL/Lease Support**: Implemented time-to-live (TTL) functionality with configurable cleanup strategies (piggyback, lazy, scheduled) for automatic key expiration
- **Unified NodeBuilder API**: Simplified node startup with new `start_server()` method that combines `build()`, `start_rpc_server()`, and `ready()` into a single async call
- **Improved State Machine Initialization**: Enhanced state machine lifecycle with `try_inject_lease()` and `post_start_init()` hooks for transparent lease configuration
- **etcd-Compatible TTL Semantics**: TTL now uses absolute expiration time (compatible with etcd lease semantics) instead of relative TTL, ensuring correct behavior across restarts

### 🔄 Breaking Changes

- **WAL Format Change (File-based State Machine)**: ⚠️ **CRITICAL BREAKING CHANGE**
  - WAL entries now store absolute expiration time (`expire_at_secs: u64`) instead of relative TTL (`ttl_secs: u32`)
  - This enables crash-safe TTL semantics and etcd-compatible lease behavior
  - **Migration Required**: See [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md) for upgrade instructions
  - Existing WAL files from pre-v0.2.0 are **not compatible** and must be migrated

- **NodeBuilder API**: `build().start_rpc_server().await.ready()` is now replaced with `.start_server().await`
  - See [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md) for detailed migration instructions
  - Old API is no longer supported

### 📝 Documentation

- Added comprehensive MIGRATION_GUIDE.md for API changes
- Updated all README files with new `start_server()` API
- Updated server guide documentation for custom implementations
- Updated quick-start examples in overview documentation

### 🐛 Fixes

- Fixed clippy warning: empty line after doc comments in RocksDB state machine
- Fixed duplicate trace logging in BufferedRaftLog initialization
- Fixed log level filtering: RUST_LOG now correctly limits to DEBUG level (no more TRACE spam in tests)
- Fixed Zed editor clippy warnings in benchmark code
- Fixed unused imports in test utilities
- Fixed crash-safety bug: snapshot restore now persists TTL metadata to RocksDB CF
- Fixed WAL replay: expired entries are now correctly skipped during recovery

### ⚡ Performance

- **Benchmark Optimization**: Reduced TTL benchmark execution time by ~10x
  - `worst_case_all_expired`: 213s → 20s (10.6x faster)
  - `mixed_ttl_workload`: 200s → 20s (10x faster)
  - `piggyback_high_frequency`: 200s → 20s (10x faster)
  - Used `iter_batched` to separate setup from measurement
  - Reduced sample size to 10 for tests with sleep operations

### ✨ Quality

- All benchmarks pass clippy without warnings
- TTL benchmarks validate cleanup performance targets
- State machine benchmarks validate scaling characteristics
- Added tests for crash-safe WAL replay behavior
- Added tests for TTL persistence across snapshot restore

### 🔧 Internal Improvements

- Refactored RocksDB options configuration into `configure_db_options()` helper (DRY)
- Removed high-frequency trace logs from hot paths to reduce noise
- Improved test output clarity by filtering log levels correctly

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
