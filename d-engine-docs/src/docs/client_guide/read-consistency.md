# Read Consistency Guide

**Quick decision**: Use `get_lease()` for most cases. Use `get_linearizable()` when you need strongest consistency.

---

## Quick Decision (30 seconds)

```rust
// Most applications (balanced performance + consistency)
let value = client.get_lease(b"key").await?;

// Strongest consistency (when you need absolute latest value)
let data = client.get_linearizable(b"critical_data").await?;

// Dashboards/analytics (best performance, stale OK)
let stats = client.get_eventual(b"metrics").await?;
```

**Decision tree**:

```
Need absolute latest value?
├─ YES → get_linearizable() (strongest consistency)
│
└─ NO → Can tolerate ~100ms staleness?
    ├─ YES → get_eventual() (20x faster, e.g., dashboards)
    └─ NO → get_lease() (7x faster, recommended default)
```

---

## Three Policies

### 1. LinearizableRead (Strongest)

**API**: `client.get_linearizable(key)`

**Guarantee**: Always returns the **most recent** committed value across the cluster.

**Performance**: ~2ms (requires network round-trip to verify quorum)

**Use when**: Critical operations requiring strongest consistency (e.g., financial transactions, inventory updates)

```rust
let value = client.get_linearizable(b"critical_data").await?;
```

---

### 2. LeaseRead (Recommended Default)

**API**: `client.get_lease(key)`

**Guarantee**: Returns value committed within last ~500ms.

**Performance**: ~0.3ms (7x faster than LinearizableRead)

**Use when**:

- Most production applications
- Metadata queries where slight staleness is acceptable

**Requirements**: NTP(Network Time Protocol) must be configured on all nodes.

```rust
let user = client.get_lease(b"user:123").await?;
```

---

### 3. EventualConsistency (Fastest)

**API**: `client.get_eventual(key)`

**Guarantee**: Returns valid committed state, may be ~100ms behind leader.

**Performance**: ~0.1ms (20x faster than LinearizableRead)

**Use when**:

- Dashboard metrics
- Analytics queries
- Monitoring data

**Bonus**: Can read from **any node** (leader/follower), not just leader.

```rust
let stats = client.get_eventual(b"dashboard_stats").await?;
```

---

## Server Default Policy

If you use `client.get(key)` without specifying a policy, the server's default is used:

```rust
// Uses server's default policy (configured in server's d-engine.toml)
let value = client.get(b"key").await?;
```

**Server configuration** (d-engine.toml):

```toml
[raft.read_consistency]
default_policy = "LeaseRead"           # Server default when client doesn't specify
allow_client_override = true           # Allow client to override (default: true)
```

**Why does server have a default?**

- **Centralized control**: Operators can enforce consistency requirements across all clients
- **Client simplicity**: Clients don't need to specify policy for every read
- **Flexibility**: Clients can override when needed (if `allow_client_override = true`)

**Example scenario**:

- Server sets `default_policy = "LeaseRead"` (balanced)
- Most clients use `client.get(key)` → gets LeaseRead
- Critical operations use `client.get_linearizable(key)` → overrides to LinearizableRead

---

## Performance Comparison

| Policy              | Latency | Throughput | Network RTT | Serves From |
| ------------------- | ------- | ---------- | ----------- | ----------- |
| LinearizableRead    | ~2ms    | Baseline   | 1 RTT       | Leader only |
| LeaseRead           | ~0.3ms  | ~7x        | 0 RTT       | Leader only |
| EventualConsistency | ~0.1ms  | ~20x       | 0 RTT       | Any node    |

_Measured on 3-node cluster, AWS same-region_

**Important**: All policies return **correct** committed data. Difference is whether you get the **latest** or **slightly older** committed state.

---

## Common Patterns

### Mixed Consistency

```rust
// Strongest consistency (latest value required)
let critical = client.get_linearizable(b"critical_key").await?;

// Balanced (slight staleness acceptable)
let data = client.get_lease(b"key").await?;

// Fast read (stale OK)
let stats = client.get_eventual(b"metrics").await?;
```

### Fallback on Timeout

```rust
use tokio::time::{timeout, Duration};

// Try linearizable first, fallback to lease on timeout
let value = match timeout(Duration::from_millis(100),
                         client.get_linearizable(b"key")).await {
    Ok(result) => result?,
    Err(_) => client.get_lease(b"key").await?,  // Fallback
};
```

---

## Key Takeaways

1. **Default to LeaseRead** (`get_lease()`) for most applications
2. **Use LinearizableRead** (`get_linearizable()`) only when you need absolute latest value
3. **Use EventualConsistency** (`get_eventual()`) for analytics/dashboards
4. **All policies are safe** - return valid committed data, never corrupted
5. **Server default applies** when you use `get()` without specifying policy

---

## Advanced Topics

### LeaseRead Clock Requirements

LeaseRead requires **synchronized clocks** (NTP). Clock drift must be < 50ms.

**If NTP is not available**: Use LinearizableRead instead.

### EventualConsistency Staleness

Maximum staleness ≈ ~100ms (typical).

### Server Configuration & Tuning

For detailed server-side configuration (lease duration tuning, monitoring metrics, deployment scenarios), see:

**[Server Guide: Consistency Tuning](../server_guide/consistency-tuning.md)**

---

## Troubleshooting

### "Leadership verification failed"

**Cause**: Network partition or leader lost quorum.

**Solution**: Check network connectivity, or try `get_lease()` for better availability.

### LeaseRead requires NTP

**Solution**: Install NTP (`sudo apt install ntp`), or use `get_linearizable()` if NTP unavailable.
