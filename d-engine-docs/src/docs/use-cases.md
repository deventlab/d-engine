# Is d-engine Right for You?

**Purpose**: Help you quickly determine if d-engine fits your needs.

---

## What d-engine Provides

- **Strong consistency** using Raft consensus algorithm
- **Leader election** with automatic failover
- **Watch API** for real-time change notifications
- **Two deployment modes**: Embedded (Rust) or Standalone (any language via gRPC)

---

## Common Use Cases

### ✅ Good Fit

**1. Distributed Coordination**

- Leader election
- Distributed locks
- Cluster membership management

**2. Metadata Storage**

- Service registry
- Configuration management
- Small shared state requiring strong consistency

**3. Embedded Rust Applications**

- Need low-latency local access
- Single binary deployment
- Type-safe integration

---

## When NOT to Use

### ❌ Not Suitable

- **Large-scale KV data storage**
- **Applications requiring SQL interface**

---

## Next Steps

If your use case matches → **[Choose Integration Mode](integration-modes.md)**

---

**Created**: 2025-12-30  
**Updated**: 2025-12-30
