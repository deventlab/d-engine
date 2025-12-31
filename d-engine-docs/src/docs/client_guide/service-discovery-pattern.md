# Service Discovery Pattern

This guide explains how to use d-engine for service discovery use cases like DNS servers, service registries, and configuration centers.

## Why d-engine for Service Discovery

Service discovery systems have specific requirements that align well with d-engine's design:

**Cost-Effective Start**

- Start with 1 node: $100/month vs etcd's 3-node requirement ($300/month)
- Scale to 3 nodes only when high availability is needed
- No external dependencies (Redis, PostgreSQL, etc.)

**Read-Heavy Workload Optimization**

- DNS queries are 99% reads, 1% writes
- EventualConsistency policy enables local reads (<0.1ms)
- Watch API eliminates polling overhead

**Local-First Architecture**

- Most reads served from local memory
- Network communication only for writes and coordination
- Embedded mode: zero alization overhead (Rust only)

## Core Capabilities

### 1. Watch API

Monitor key changes in real-time without polling.

**What it does:**

- Receive notifications when keys change (PUT/DELETE)
- FIFO ordering per key
- Lock-free implementation with minimal write path overhead

**Current Limitations:**

- Exact key match only (no prefix or range watch)
- At-most-once delivery (events may drop under high load)
- No event history (only future changes after watch starts)

See [Watch Feature Guide](../server_guide/watch-feature.md) for configuration and usage.

### 2. Read Consistency Policies

Choose between consistency and performance based on your needs.

| Policy              | Latency | Use Case                                           |
| ------------------- | ------- | -------------------------------------------------- |
| LinearizableRead    | High    | Critical operations requiring absolute latest data |
| LeaseRead           | Medium  | Most production applications (default)             |
| EventualConsistency | Lowest  | DNS queries, caching, monitoring                   |

**For service discovery**: EventualConsistency is typically sufficient. DNS tolerates 50-100ms staleness, and d-engine's EventualConsistency provides much lower staleness in practice.

See [Read Consistency Guide](read_consistency.md) for details.

### 3. Standard KV Operations

- `put(key, value)` - Register service endpoints
- `get(key)` - Query service information
- `delete(key)` - Unregister services

## Typical Pattern

### Service Registration (Write Path)

External controller or service instances write endpoint information:

```text
PUT /services/api-gateway/instance-1 -> "192.168.1.10:8080"
PUT /services/api-gateway/instance-2 -> "192.168.1.11:8080"
```

### Service Discovery (Read Path)

1. **Watch service keys** to receive change notifications
2. **Cache locally** in your application memory
3. **Serve queries from cache** using EventualConsistency reads
4. **Invalidate cache** when Watch events arrive

### Benefits of This Pattern

- **Low latency**: Local cache serves queries (<1ms)
- **Efficient updates**: Watch eliminates polling
- **Consistent**: EventualConsistency ensures reads from committed state
- **Scalable**: Minimal network traffic after initial sync

## Example Scenarios

### DNS Server (like CoreDNS)

**Requirements:**

- High read throughput (10K+ queries/sec)
- Stale reads acceptable (50-100ms)
- Cache invalidation on record changes

**How d-engine fits:**

- Watch DNS record keys (`dns.example.com/A`)
- EventualConsistency for query serving
- Update cache on Watch events

### Service Registry (like Consul)

**Requirements:**

- Service health checks
- Endpoint discovery
- Dynamic service registration

**How d-engine fits:**

- Services register endpoints via PUT
- Clients watch service keys
- Health checker updates TTL or deletes unhealthy endpoints

### Configuration Center

**Requirements:**

- Feature flags, runtime config
- Real-time updates without restart
- Version control

**How d-engine fits:**

- Store config as KV pairs
- Applications watch config keys
- Reload config on Watch events

## Current Limitations

### What d-engine v0.1.x Does NOT Support

1. **Prefix/Range Watch**
   - Cannot watch `/services/*` for all services
   - Must watch each key individually
   - Workaround: Maintain a service list key and watch it

2. **Event Replay**
   - No historical events before watch starts
   - Must read current state first, then watch

3. **Watch Persistence**
   - Watchers lost on server restart
   - Clients must re-register after reconnect

4. **Guaranteed Delivery**
   - Events may drop if buffers are full
   - Clients should re-read on suspected gaps

### Workarounds

**For prefix watch needs:**

```text
// Instead of: watch("/services/*")
// Do: Maintain an index key
PUT /services/_index -> "instance-1,instance-2,instance-3"
watch(/services/_index)  // Watch the index
// When index changes, read individual service keys
```

**For event replay needs:**

```text
// Pattern: Read-then-Watch
1. Read current state: GET /services/api-gateway/*
2. Start watch immediately after
3. Process both current state and future events
```

## When NOT to Use d-engine

Consider alternatives if you need:

- **Complex queries**: SQL-like joins, aggregations
- **Prefix/range watch**: Critical requirement (not workaround)
- **Event sourcing**: Need full event history replay
- **Multi-datacenter replication**: Not available in open-source version

For these scenarios, evaluate etcd, Consul, or specialized databases.

## Migration from etcd

If you're using etcd for service discovery:

**What works the same:**

- Basic KV operations (PUT/GET/DELETE)
- Watch single keys
- Linearizable/Lease read consistency

**What requires changes:**

- Prefix watch → Index key pattern
- Range operations → Individual key operations
- Transactions → Not yet supported

**Performance comparison:**

- d-engine: 5.6x faster than etcd (average throughput)
- See `benches/reports/` for detailed benchmarks

## Getting Started

1. **Start with single node**: Follow [Quick Start (5min)](../quick-start-5min.md)
2. **Enable Watch**: Check [Watch Feature Guide](../server_guide/watch-feature.md)
3. **Choose consistency policy**: See [Read Consistency Guide](read_consistency.md)
4. **Scale when needed**: Follow [Single Node Expansion](../examples/single-node-expansion.md)

## Production Checklist

- [ ] Enable Watch feature (`raft.watch.enabled = true`)
- [ ] Tune buffer sizes based on load (`event_queue_size`, `watcher_buffer_size`)
- [ ] Choose appropriate read consistency policy (EventualConsistency for most cases)
- [ ] Implement client reconnection logic
- [ ] Monitor dropped events (`enable_metrics = true`)
- [ ] Test failover scenarios (if using 3-node cluster)

## Further Reading

- [Watch Feature Guide](../server_guide/watch-feature.md) - Detailed Watch API documentation
- [Read Consistency Guide](read_consistency.md) - Understanding consistency trade-offs
- [Performance Tuning](../performance/throughput-optimization-guide.md) - Optimize for your workload

---

**Created**: 2025-12-03
**Status**: Production Ready (v0.1.x capabilities)
