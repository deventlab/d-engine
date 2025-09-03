# Raft Throughput Optimization Guide for Tonic gRPC in Rust

This guide provides empirically-tuned strategies to improve Raft performance using [tonic](https://docs.rs/tonic/latest/tonic/) with connection type isolation. These optimizations address critical bottlenecks in consensus systems where network transport impacts throughput and latency.

## Connection Type Strategy

We implement three distinct connection types to prevent head-of-line blocking:

| **Type**  | **Purpose**          | **Critical Operations**           | **Performance Profile**    |
| --------- | -------------------- | --------------------------------- | -------------------------- |
| `Control` | Consensus operations | Heartbeats, Votes, Config changes | Latency-sensitive (sub-ms) |
| `Data`    | Log replication      | AppendEntries, Log writes         | Throughput-optimized       |
| `Bulk`    | Large transfers      | Snapshot streaming                | Bandwidth-intensive        |

```rust,ignore
pub(crate) enum ConnectionType {
    Control,  // Elections/heartbeats
    Data,     // Log replication
    Bulk,     // Snapshot transmission
}
```

## Configuration Tuning

### Control Plane (`[network.control]`)

```toml
concurrency_limit = 8192
max_concurrent_streams = 500
connection_window_size = 2_097_152  # 2MB
http2_keep_alive_timeout_in_secs = 20  # Aggressive timeout

```

**Why it matters:**

- Ensures election timeouts aren't missed during load
- Prevents heartbeat delays that cause unnecessary leader changes
- Uses smaller windows for faster roundtrips

### Data Plane (`[network.data]`)

```toml
max_concurrent_streams = 500
connection_window_size = 6_291_456  # 6MB
request_timeout_in_ms = 200          # Batch-friendly

```

**Optimization rationale:**

- Larger windows accommodate log batches
- Stream count aligns with typical pipelining depth
- Timeout tuned for batch processing, not individual entries

### Bulk Plane (`[network.bulk]` - Recommended)

```toml
# SNAPSHOT-SPECIFIC SETTINGS (EXAMPLE)
connect_timeout_in_ms = 1000         # Slow-start connections
request_timeout_in_ms = 30000        # 30s for large transfers
concurrency_limit = 4                # Few concurrent streams
connection_window_size = 33_554_432  # 32MB window

```

**Snapshot considerations:**

- Requires 10-100x larger windows than data plane
- Higher timeouts for GB-range transfers
- Compression essential (Gzip enabled in implementation)

## Critical Code Implementation

### Connection Type Routing

```rust,ignore
// Control operations
membership.get_peer_channel(peer_id, ConnectionType::Control)

// Data operations
membership.get_peer_channel(peer_id, ConnectionType::Data)

// Snapshot transfers
membership.get_peer_channel(leader_id, ConnectionType::Bulk)

```

### gRPC Server Tuning

```rust,ignore
tonic::transport::Server::builder()
    .timeout(Duration::from_millis(control_config.request_timeout_in_ms))
    .max_concurrent_streams(control_config.max_concurrent_streams)
    .max_frame_size(Some(data_config.max_frame_size))
    .initial_connection_window_size(data_config.connection_window_size)

```

### Snapshot Streaming

```rust,ignore
async fn request_snapshot_from_leader(
    &self,
    leader_id: u32,
    ack_rx: mpsc::Receiver<SnapshotAck>,
    membership: Arc<MOF<T>>,
) -> Result<Box<tonic::Streaming<SnapshotChunk>>> {
    let channel = membership.get_peer_channel(leader_id, ConnectionType::Bulk).await?;
    let mut client = SnapshotServiceClient::new(channel)
        .send_compressed(CompressionEncoding::Gzip);

    client.stream_snapshot(tonic::Request::new(ReceiverStream::new(ack_rx)))
        .await
        .map(|res| Box::new(res.into_inner()))
}

```

## Performance Results

| **Metric**    | **Before**  | **After**   | **Delta**  |
| ------------- | ----------- | ----------- | ---------- |
| Throughput    | 368 ops/sec | 373 ops/sec | +1.3%      |
| p99 Latency   | 5543 µs     | 4703 µs     | **-15.2%** |
| p99.9 Latency | 14015 µs    | 11279 µs    | -19.5%     |

> Key improvement: 15% reduction in tail latency - critical for consensus stability

## Operational Recommendations

1. **Pre-warm connections** during node initialization
2. **Monitor connection types separately**:

   ```bash
   # Control plane
   netstat -an | grep ":9081" | grep ESTABLISHED | wc -l

   # Data plane
   netstat -an | grep ":9082" | grep ESTABLISHED

   ```

3. **Size bulk windows** for snapshot sizes:

   ```rust,ignore
   connection_window_size = max_snapshot_size * 1.2

   ```

4. **Compress snapshots**:

   ```rust,ignore
   .send_compressed(CompressionEncoding::Gzip)
   .accept_compressed(CompressionEncoding::Gzip)

   ```

## Anti-Patterns to Avoid

```rust,ignore
// DON'T: Use same connection for control and data
get_peer_channel(peer_id, ConnectionType::Data).await?;
client.request_vote(...)  // Control operation on data channel

// DO: Strict separation
get_peer_channel(peer_id, ConnectionType::Control).await?;
client.request_vote(...)

```

## Why Connection Isolation Matters

1. **Prevents head-of-line blocking**
   Large snapshots won't delay heartbeats
2. **Enables targeted tuning**
   Control: Low latency ↔ Data: High throughput ↔ Bulk: Bandwidth
3. **Improves fault containment**
   Connection issues affect only one operation type

```text
Key enhancements:
1. Added dedicated `Bulk` configuration section with snapshot-specific tuning
2. Implemented connection type routing in code samples
3. Included snapshot streaming implementation with compression
4. Added operational commands for monitoring
5. Provided anti-pattern examples
6. Explained why connection isolation matters
7. Maintained performance improvement highlights

The connection type strategy is crucial because:
- Snapshots can be 1000x larger than typical log entries
- Heartbeats require microsecond-level latency guarantees
- Log replication needs steady high throughput
Separating these onto dedicated connections prevents resource contention and tail latency spikes.
```

## Reference Deployment Configurations

Below are example configurations for different deployment scenarios.
Adjust values based on snapshot size, log append rate, and cluster size.

1. Single Node (Local Dev / Testing)
   • CPU: 4 cores
   • Memory: 8 GB
   • Network: Localhost

```toml
[network.control]
concurrency_limit = 10
max_concurrent_streams = 64
connection_window_size = 1_048_576   # 1MB

[network.data]
concurrency_limit = 20
max_concurrent_streams = 128
connection_window_size = 2_097_152   # 2MB

[network.bulk]
concurrency_limit = 2
connection_window_size = 8_388_608   # 8MB
request_timeout_in_ms = 10_000       # 10s
```

Tip: Single-node setups focus on low resource usage; bulk window size can be smaller since snapshots are local.

2. 3-Node Public Cloud Cluster (Medium)
   • Instance Type: 4 vCPU / 16 GB RAM (e.g., AWS m6i.large, GCP n2-standard-4)
   • Network: 10 Gbps

```toml
[network.control]
concurrency_limit = 20
max_concurrent_streams = 128
connection_window_size = 2_097_152   # 2MB

[network.data]
concurrency_limit = 30
max_concurrent_streams = 256
connection_window_size = 4_194_304   # 4MB

[network.bulk]
concurrency_limit = 4
connection_window_size = 33_554_432  # 32MB
request_timeout_in_ms = 30_000       # 30s for multi-GB snapshots
```

Tip: For public cloud, moderate concurrency and 32MB bulk windows ensure stable snapshot streaming without affecting heartbeats.

3. 5-Node High-Throughput Cluster (Production)
   • Instance Type: 8 vCPU / 32 GB RAM (e.g., AWS m6i.xlarge)
   • Network: 25 Gbps

```toml
[network.control]
concurrency_limit = 50
max_concurrent_streams = 256
connection_window_size = 4_194_304   # 4MB

[network.data]
concurrency_limit = 80
max_concurrent_streams = 512
connection_window_size = 8_388_608   # 8MB

[network.bulk]
concurrency_limit = 8
connection_window_size = 67_108_864  # 64MB
request_timeout_in_ms = 60_000       # 60s for large snapshots
```

Tip: Increase concurrency and window size to prevent log replication bottlenecks in high-throughput clusters.
Bulk plane is tuned for multi-GB snapshots without blocking the control plane.

## Network Environment Tuning Recommendations

These parameters are primarily **network-dependent**, not CPU/memory dependent.

Adjust them based on latency, packet loss, and connection stability.

| **Environment**                  | **tcp_keepalive_in_secs** | **http2_keep_alive_interval_in_secs** | **http2_keep_alive_timeout_in_secs** | **max_frame_size** | **Notes**                                                                |
| -------------------------------- | ------------------------- | ------------------------------------- | ------------------------------------ | ------------------ | ------------------------------------------------------------------------ |
| **Local / In-Cluster (LAN)**     | 60                        | 10                                    | 5                                    | 16_777_215 (16MB)  | Low latency & stable; defaults are fine                                  |
| **Cross-Region / Stable WAN**    | 60                        | 15                                    | 8                                    | 16_777_215 (16MB)  | Slightly longer keep-alive to avoid false disconnects                    |
| **Public Cloud / Moderate Loss** | 60                        | 20                                    | 10                                   | 33_554_432 (32MB)  | Higher interval & timeout for lossy links; larger frame helps batch logs |
| **High Latency / Unstable WAN**  | 120                       | 30                                    | 15                                   | 33_554_432 (32MB)  | Longer timeouts prevent spurious drops                                   |

**Guidelines:**

1. Keep-alive interval ≈ 1/3 of timeout.
2. Increase `max_frame_size` only if batch logs or snapshots exceed 16MB.
3. High-latency WAN: favor fewer reconnects over aggressive failure detection.
4. These settings are independent of CPU and memory; focus on network RTT and stability.

### RPC Timeout Guidance

`connect_timeout_in_ms` and `request_timeout_in_ms` depend on **network latency and I/O**, not CPU or memory.

| **Environment**                  | **connect_timeout_in_ms** | **request_timeout_in_ms** | **Notes**                                             |
| -------------------------------- | ------------------------- | ------------------------- | ----------------------------------------------------- |
| **Local / In-Cluster (LAN)**     | 50–100                    | 100–300                   | Very low RTT; fast retries                            |
| **Cross-Region / Stable WAN**    | 200–500                   | 300–1000                  | Higher RTT, moderate batch sizes                      |
| **Public Cloud / Moderate Loss** | 500–1000                  | 1000–5000                 | Compensate for packet loss and I/O latency            |
| **High Latency / Unstable WAN**  | 1000+                     | 5000+                     | Favor fewer reconnects; allow large batch replication |

**Tips:**

1. `connect_timeout_in_ms` covers TCP+TLS+gRPC handshake; increase for high-latency links.
2. `request_timeout_in_ms` should accommodate log batches and disk write delays on followers.
3. Timeouts mainly depend on **network RTT** and **disk I/O**, not hardware compute.
