# Read Consistency Policies

d-engine supports three read consistency levels to balance **consistency**, **performance**, and **availability**.

## Quick Reference

| Policy                  | Consistency | Latency | Throughput    | Use Case                              |
| ----------------------- | ----------- | ------- | ------------- | ------------------------------------- |
| **LinearizableRead**    | Strongest   | ~2ms    | Baseline      | Financial transactions, critical data |
| **LeaseRead**           | Strong      | ~0.3ms  | 7-20x faster  | General purpose, metadata queries     |
| **EventualConsistency** | Eventual    | ~0.1ms  | 10-20x faster | Dashboards, monitoring, analytics     |

_Throughput gain depends on workload: higher under low-medium concurrency, lower under extreme concurrency (1000+ clients). See [Performance](#performance-benchmarks) for benchmark results._

**Default**: `LinearizableRead` (strongest consistency)

## Usage

### Client-Side

```rust
use d_engine_client::KvClient;
use d_engine_proto::client::{ClientReadRequest, ReadConsistencyPolicy};

let mut client = KvClient::connect("http://localhost:50051").await?;

// Option 1: Use server default (LinearizableRead)
let request = ClientReadRequest {
    keys: vec![b"key1".to_vec()],
    ..Default::default()
};
let response = client.read(request).await?;

// Option 2: Override with specific policy
let mut request = ClientReadRequest::default();
request.set_consistency_policy(ReadConsistencyPolicy::LeaseRead);
request.keys = vec![b"key1".to_vec()];
let response = client.read(request).await?;
```

### Server-Side Configuration

```toml
# d-engine.toml
[raft.read_consistency]
default_policy = "LinearizableRead"  # Options: LinearizableRead, LeaseRead, EventualConsistency
allow_client_override = true         # Allow clients to specify different policy
lease_duration_ms = 500              # Lease validity period (only for LeaseRead)
```

## Policy Details

### LinearizableRead (Default)

**Guarantee**: Always returns the latest committed value.

**How it works**: Leader verifies its leadership with a quorum (majority of nodes) before every read. This is the Raft "ReadIndex" protocol.

**Trade-offs**:

- ✅ Strongest consistency - No stale reads possible
- ✅ No clock dependency - Works with clock drift
- ❌ Higher latency - Requires 1 network round-trip per read (~2ms)
- ❌ Leader-only - Cannot serve reads from followers

**When to use**:

- Financial transactions
- Critical metadata (cluster configuration)
- When you need absolute latest value

**Error handling**:

```rust
match client.read(request).await {
    Err(e) if e.code() == tonic::Code::FailedPrecondition => {
        // Leadership verification failed (network partition or leader lost quorum)
        // Retry or failover to new leader
    }
    Ok(response) => { /* ... */ }
    Err(e) => { /* other errors */ }
}
```

---

### LeaseRead

**Guarantee**: Returns a value committed within the last `lease_duration_ms` (default 500ms).

**How it works**:

1. Leader maintains a lease by successfully heartbeating to quorum every ~100ms
2. During valid lease, leader serves reads locally (no network verification)
3. When lease expires, automatically falls back to LinearizableRead

**Trade-offs**:

- ✅ Low latency - No network verification during lease (~0.3ms)
- ✅ High throughput - 7-20x faster than LinearizableRead
  - 7x under extreme concurrency (1000+ clients)
  - ~20x under typical workloads (<100 clients)
- ⚠️ **Requires clock synchronization** - NTP must be configured
- ⚠️ Possible stale reads during clock drift (bounded by `lease_duration_ms`)
- ❌ Leader-only - Cannot serve reads from followers

**When to use**:

- General purpose applications (balance consistency & performance)
- Metadata queries where slight staleness is acceptable
- High-throughput read workloads

**Clock requirements**:

- NTP configured and running
- Clock drift < 50ms (< 10% of `lease_duration_ms`)
- Monitor clock drift in production

**Automatic fallback**: When lease expires, d-engine automatically uses LinearizableRead to refresh the lease.

---

### EventualConsistency

**Guarantee**: Returns a valid committed state, but may be stale (typically <100ms behind leader).

**How it works**: Reads directly from local state machine without any verification. **Can be served by any node** (leader, follower, or candidate).

**Trade-offs**:

- ✅ Lowest latency - Pure local read (~0.1ms)
- ✅ Highest throughput - 10-20x faster than LinearizableRead
  - 10x under extreme concurrency (1000+ clients)
  - ~20x under typical workloads (<100 clients)
- ✅ Works on any node - Follower reads supported
- ✅ No clock dependency
- ❌ May return stale data (bounded by replication lag, typically <100ms)

**When to use**:

- Monitoring dashboards
- Analytics queries
- Read replicas
- Scenarios where slight staleness is acceptable

**Staleness bound**: Maximum staleness = `heartbeat_interval_ms` × `missed_heartbeats_before_election` (typically <200ms).

---

## Configuration Guide

### Choosing Default Policy

```toml
[raft.read_consistency]
# Conservative (strongest consistency)
default_policy = "LinearizableRead"

# Balanced (recommended for most applications)
default_policy = "LeaseRead"

# Aggressive (maximize throughput)
default_policy = "EventualConsistency"
```

### Tuning LeaseRead

```toml
[raft.read_consistency]
lease_duration_ms = 500  # Default

# Shorter lease (more frequent verification)
lease_duration_ms = 200  # Better safety, lower throughput

# Longer lease (fewer verifications)
lease_duration_ms = 1000 # Higher throughput, requires better clock sync
```

**Rule of thumb**: `lease_duration_ms` > `3 × heartbeat_interval_ms` (default: 500ms > 3 × 100ms)

### Client Override Policy

```toml
[raft.read_consistency]
allow_client_override = true   # Clients can specify different policy (default)
allow_client_override = false  # Enforce server default for all reads
```

## Performance Benchmarks

### High Concurrency Scenario (Stress Test)

**Test environment**:

- d-engine v0.2.0, 3-node cluster
- 100K requests, 1000 clients, 200 connections
- Workload: 8-byte key/value pairs

| Policy              | Throughput | Avg Latency | p99 Latency | vs Linearizable |
| ------------------- | ---------- | ----------- | ----------- | --------------- |
| LinearizableRead    | 12k ops/s  | 16.50ms     | 24.40ms     | Baseline        |
| LeaseRead           | 83k ops/s  | 2.40ms      | 7.87ms      | **6.9x faster** |
| EventualConsistency | 118k ops/s | 1.68ms      | 8.54ms      | **9.8x faster** |

**Note**: Under lower concurrency (<100 clients), throughput gains approach theoretical maximum (~20x) as network latency becomes the dominant factor. The stress test above represents worst-case performance under extreme load.

_Full benchmark report: [v0.2.0 results](../../../../benches/d-engine-bench/reports/v0.2.0/report_v0.2.0_final.md)_

## Troubleshooting

### "Leadership verification failed"

**Symptom**: LinearizableRead requests fail with `FailedPrecondition` error.

**Cause**: Network partition, or leader cannot reach quorum.

**Solutions**:

1. Check network connectivity between nodes
2. Verify cluster health: `d-engine-ctl cluster status`
3. Consider using LeaseRead for better availability during transient network issues

---

### LeaseRead falling back too often

**Symptom**: Logs show frequent "Lease expired, verifying leadership" messages.

**Cause**: Clock drift or missed heartbeats.

**Solutions**:

1. Verify NTP is running: `ntpq -p`
2. Check clock drift: `ntpstat`
3. Increase `lease_duration_ms` if clock drift is unavoidable
4. Reduce `heartbeat_interval_ms` to refresh lease more frequently

---

### EventualConsistency reads are too stale

**Symptom**: Follower reads lag significantly behind leader.

**Cause**: Replication lag (follower falling behind).

**Solutions**:

1. Check replication metrics: `raft.replication.lag_ms`
2. Verify follower health (CPU, disk I/O)
3. Use LeaseRead instead for bounded staleness

---

## Decision Tree

```
Need absolute latest value?
├─ YES → Use LinearizableRead
│
└─ NO → Can tolerate ~100ms staleness?
    ├─ YES → Use EventualConsistency (fastest)
    │        • Read from any node (leader/follower)
    │        • ~20x throughput
    │
    └─ NO → Is NTP configured?
        ├─ YES → Use LeaseRead (balanced)
        │        • ~7x throughput
        │        • Bounded staleness (~500ms)
        │
        └─ NO → Use LinearizableRead
                 • Clock-independent
                 • Guaranteed latest value
```
