# Read Consistency Tuning Guide

This guide explains how to configure server-side read consistency policies for optimal performance and correctness trade-offs.

## Configuration Location

All read consistency settings are in your server's `config.toml`:

```toml
[raft.read_consistency]
default_policy = "LeaseRead"
lease_duration_ms = 250
allow_client_override = true
```

## Configuration Parameters

### `default_policy`

**Type**: `"LinearizableRead" | "LeaseRead" | "EventualConsistency"`

**Default**: `"LeaseRead"`

The consistency policy applied when clients don't specify one explicitly.

**Recommendations**:

- **Production systems**: `"LeaseRead"` (balanced performance and consistency)
- **Financial/critical systems**: `"LinearizableRead"` (strictest guarantees)
- **Analytics/monitoring**: `"EventualConsistency"` (maximum throughput)

### `lease_duration_ms`

**Type**: Integer (milliseconds)

**Default**: `250`

How long the leader considers its lease valid after a successful heartbeat quorum ACK.

**Trade-offs**:

- **Higher values**: better LeaseRead performance; larger staleness window on clock drift
- **Lower values**: more conservative; more sensitive to heartbeat jitter

**Valid range** (both constraints are enforced by `RaftConfig::validate()`):
- Lower bound: `lease_duration_ms ≥ 2 × heartbeat_interval` (with default 100ms heartbeat: ≥ 200ms)
- Upper bound: `lease_duration_ms < election_timeout_min` (default: < 500ms)

### `allow_client_override`

**Type**: Boolean

**Default**: `true`

Whether clients can specify per-request consistency policies that override `default_policy`.

- `true`: mixed workloads (e.g., most reads use LeaseRead, critical ops use LinearizableRead)
- `false`: uniform consistency enforced — client-specified policies are ignored

## Tuning by Deployment Scenario

### Scenario 1: Financial/Critical Systems

**Priority**: Correctness > Performance

```toml
[raft.read_consistency]
default_policy = "LinearizableRead"
lease_duration_ms = 250  # unused for LinearizableRead
allow_client_override = false  # enforce strict policy
```

**Result**:

- All reads verify quorum before serving
- No stale reads possible
- Higher latency (~2ms p50)

### Scenario 2: High-Throughput Analytics

**Priority**: Performance > Freshness

```toml
[raft.read_consistency]
default_policy = "EventualConsistency"
lease_duration_ms = 250  # unused for EventualConsistency
allow_client_override = true  # allow critical reads to override
```

**Result**:

- Reads served from local state machine on any node (leader or follower)
- Sub-millisecond latency

### Scenario 3: Production Web Service (Recommended)

**Priority**: Balanced

```toml
[raft.read_consistency]
default_policy = "LeaseRead"
lease_duration_ms = 250  # default; increase if read latency matters more than freshness
allow_client_override = true  # mixed workload support
```

**Result**:

- Strong consistency with low latency
- Clients can use LinearizableRead for critical operations

### Scenario 4: Unstable Network Environment

**Priority**: Reliability

```toml
[raft.read_consistency]
default_policy = "LeaseRead"
lease_duration_ms = 200  # shorter lease — faster fallback on partition
allow_client_override = true
```

**Result**:

- Faster fallback to Raft path when lease expires under partition
- Slightly higher fallback frequency under heartbeat jitter

## Clock Synchronization Requirements

LeaseRead relies on **bounded clock drift** between nodes.

### Setup NTP (Recommended)

```bash
# Ubuntu/Debian
sudo apt-get install ntp
sudo systemctl enable ntp
sudo systemctl start ntp

# Check drift
ntpq -p
```

**Acceptable drift**: <50ms
**Warning threshold**: >100ms (fallback to LinearizableRead)

### AWS/Cloud Environments

Most cloud providers offer time synchronization services:

- **AWS**: Amazon Time Sync Service (automatic)
- **GCP**: Google NTP servers (automatic)
- **Azure**: time.windows.com (configure)

**Typical drift**: <10ms (safe for LeaseRead)

## Performance Impact Summary

| Configuration                                              | Read Latency (p50) | Throughput | Clock Dependency |
| ---------------------------------------------------------- | ------------------ | ---------- | ---------------- |
| `default_policy = "LinearizableRead"`                      | 2.1ms              | Baseline   | None             |
| `default_policy = "LeaseRead"`, `lease_duration_ms = 250`  | 0.3ms              | ~7x        | Low (NTP)        |
| `default_policy = "LeaseRead"`, `lease_duration_ms = 400`  | 0.2ms              | ~8x        | Medium           |
| `default_policy = "EventualConsistency"`                   | 0.1ms              | ~20x       | None             |

_Measured on 3-node cluster, AWS same-region._

## Further Reading

- Client usage guide: [Read Consistency Guide](crate::docs::client_guide::read_consistency)
- Performance tuning: [Throughput Optimization Guide](crate::docs::performance::throughput_optimization_guide)
