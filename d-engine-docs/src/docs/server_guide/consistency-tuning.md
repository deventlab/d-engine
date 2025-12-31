# Read Consistency Tuning Guide

This guide explains how to configure server-side read consistency policies for optimal performance and correctness trade-offs.

## Configuration Location

All read consistency settings are in your server's `config.toml`:

```toml
[raft.read_consistency]
default_policy = "LeaseRead"
lease_duration_ms = 500
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

**Default**: `500`

**Range**: `100` to `5000`

How long the leader considers its lease valid after successful heartbeat.

**Trade-offs**:

- **Higher values** (e.g., 1000ms):
  - Better read performance (fewer verification round-trips)
  - Larger window for stale reads if clock drift exists
  - Recommended for stable cloud environments with NTP

- **Lower values** (e.g., 200ms):
  - Stronger consistency guarantees
  - More sensitive to network latency spikes
  - Recommended for environments with unreliable clock sync

**Formula**: `lease_duration_ms ≥ 2 × heartbeat_interval`

Default heartbeat interval is 100ms, so minimum recommended lease is 200ms.

### `allow_client_override`

**Type**: Boolean

**Default**: `true`

Whether clients can specify per-request consistency policies.

**Use cases**:

- `true`: Allow mixed workloads (critical reads + analytics reads)
- `false`: Enforce uniform consistency across all operations

## Tuning by Deployment Scenario

### Scenario 1: Financial/Critical Systems

**Priority**: Correctness > Performance

```toml
[raft.read_consistency]
default_policy = "LinearizableRead"
lease_duration_ms = 500  # Unused for LinearizableRead, but keep default
allow_client_override = false  # Enforce strict policy
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
lease_duration_ms = 500  # Unused for EventualConsistency
allow_client_override = true  # Allow critical reads to override
```

**Result**:

- Reads served immediately from any node
- Maximum throughput (~20x baseline)
- Sub-millisecond latency

### Scenario 3: Production Web Service (Recommended)

**Priority**: Balanced

```toml
[raft.read_consistency]
default_policy = "LeaseRead"
lease_duration_ms = 500  # 5x heartbeat interval
allow_client_override = true  # Mixed workload support
```

**Result**:

- Strong consistency with low latency
- Clients can use LinearizableRead for critical operations
- 7x better performance than LinearizableRead

### Scenario 4: Unstable Network Environment

**Priority**: Reliability

```toml
[raft.read_consistency]
default_policy = "LeaseRead"
lease_duration_ms = 200  # Shorter lease for safety
allow_client_override = true
```

**Result**:

- Faster fallback to LinearizableRead if lease expires
- Better handling of network partitions
- Slightly more verification overhead

## Monitoring Recommendations

Track these metrics to validate your configuration:

### Key Metrics

```bash
# Lease-related
raft.lease_renewal.success          # Should match heartbeat frequency
raft.lease_renewal.failed           # Should be near zero

# Linearizable reads
raft.linearizable_read.success      # Total count
raft.leadership_verification.duration_us  # Should be <2000 (2ms)

# Read coalescing effectiveness
raft.linearizable_read.coalesced    # Higher = better optimization
raft.pending_reads.queue_depth      # Should spike during high concurrency
```

### Health Indicators

**Healthy LeaseRead configuration**:

- `lease_renewal.success` rate: ~10/sec (100ms heartbeat interval)
- `lease_renewal.failed` < 1% of attempts
- `leadership_verification.duration_us` p99 < 5ms

**Signs of misconfiguration**:

- Frequent lease expirations → Increase `lease_duration_ms`
- High `linearizable_read.coalesced` but low throughput → Network bottleneck
- `leadership_verification.duration_us` p99 > 10ms → Check network latency

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
| `default_policy = "LeaseRead"`, `lease_duration_ms = 500`  | 0.3ms              | ~7x        | Low (NTP)        |
| `default_policy = "LeaseRead"`, `lease_duration_ms = 1000` | 0.2ms              | ~8x        | Medium           |
| `default_policy = "EventualConsistency"`                   | 0.1ms              | ~20x       | None             |

## Further Reading

- Client usage guide: [Read Consistency Guide](../client_guide/read-consistency.md)
- Performance tuning: [Throughput Optimization Guide](../performance/throughput-optimization-guide.md)
