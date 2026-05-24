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

## Migrating from etcd?

d-engine shares etcd's core ideas — Raft consensus, linearizable reads, watch streams —
but it is **not a drop-in replacement**. It is a different point on the trade-off spectrum:
simpler to operate, embeddable in Rust, but with a narrower API surface.

**As of v0.2.4, if you are evaluating d-engine as an etcd alternative, check these gaps first:**

| etcd feature                            | d-engine status                                                                                                                                                                                     |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Lease with keepalive                    | ✗ Not supported. d-engine has `put_with_ttl`, but keys cannot be renewed. Patterns relying on lease expiry for crash detection (service discovery, auto-releasing locks) are not directly portable. |
| Distributed lock (`Lock` API)           | ✗ No built-in primitive. Can be approximated with `compare_and_swap` + `watch`, but without lease keepalive, safe auto-release on crash is not possible today.                                      |
| Multi-key transactions (`txn`)          | Partial. d-engine has single-key `compare_and_swap`. Multi-key atomic operations are not supported.                                                                                                 |
| Watch from historical revision          | ✗ Watches start from the current state only. Use `scan_prefix` to rebuild state at reconnect, then resume watching from current. If your logic depends on `start_revision` to replay exact event history, this pattern will not fully substitute. |
| Auth / RBAC                             | ✗ Not available. All connected clients have full access.                                                                                                                                            |
| Snapshot / backup                       | ✗ Not exposed via client API today.                                                                                                                                                                 |
| Arbitrary range queries with limit/sort | Partial. Only prefix scans; no cursor-based pagination.                                                                                                                                             |

**What d-engine offers instead:**

- **Zero-ops embedding**: run coordination inside your Rust binary with no external process.  
  `etcd` always requires a separate cluster; d-engine does not.
- **Explicit consistency API**: choose `Linearizable`, `Lease`, or `Eventual` per read call —
  more granular than etcd's `serializable` flag.
- **Pluggable storage backend**: swap the storage engine without changing application code.

d-engine is a good fit when you want strong coordination guarantees with minimal
infrastructure overhead. If you need the full etcd API surface — especially leases,
distributed locks, or auth — etcd (or a compatible system) is the better choice today.

---

## Next Steps

If your use case matches → **[Choose Integration Mode](crate::docs::integration_modes)**

---

**Created**: 2025-12-30  
**Updated**: 2026-05-24
