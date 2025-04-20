# Changelog
All notable changes to this project will be documented in this file.

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
# Release Roadmap

## 🚧 In Development - v0.2.0 Cluster Scalability
### Planned Features
- **Snapshot Support**
  - State compaction for log management
  - Snapshot creation/restoration API
- **Dynamic Membership**
  - `AddNode`/`RemoveNode` RPC endpoints
  - Configuration change protocol implementation

## ⏳ Planned - v0.3.0 Production Ready
### Milestone Targets
- **Performance Benchmarking**
  - Throughput: Target 100,000 RPS (requests per second)
- **Operational Tooling**
  - Cluster health monitoring dashboard

---
[//]: # (Version Links)
[v0.1.0]: https://github.com/deventlab/d-engine/releases/tag/v0.1.0