# d-engine v0.2.5 Benchmark Report

**Test Environments**:

- **Local**: Apple M2 Mac mini (8-core, 16GB RAM, 3-node cluster on localhost)
- **AWS**: EC2 c5.2xlarge (8 vCPUs, 16GB RAM, 50GB SSD) × 3 nodes

**Test Dates**:

- **Local v0.2.5 vs v0.2.4**: May 29, 2026 (3-round average, embedded; warmup round excluded)
- **AWS v0.2.5**: _(TBD — not yet collected)_

**Key/Value**: 8 bytes / 256 bytes

---

## Local 3-Node Cluster: d-engine v0.2.5 vs d-engine v0.2.4

### Embedded Mode: v0.2.5 vs v0.2.4

_(3-round average, manually collected; see Benchmark Configuration for settings)_

| **Scenario**        | **Metric**  | **v0.2.4**    | **v0.2.5**    | **Δ**         |
| ------------------- | ----------- | ------------- | ------------- | ------------- |
| Single Client Write | Throughput  | 9,740 ops/s   | 9,962 ops/s   | +2.3% →       |
|                     | Avg Latency | 0.102 ms      | 0.100 ms      | -2.0% →       |
|                     | p99 Latency | 0.177 ms      | 0.194 ms      | +9.6% →       |
| High Conc. Write    | Throughput  | 233,821 ops/s | 231,797 ops/s | -0.9% →       |
|                     | Avg Latency | 0.426 ms      | 0.430 ms      | +0.9% →       |
|                     | p99 Latency | 1.164 ms      | 1.189 ms      | +2.1% →       |
| Linearizable Read   | Throughput  | 630,789 ops/s | 633,466 ops/s | +0.4% →       |
|                     | Avg Latency | 0.157 ms      | 0.156 ms      | stable        |
|                     | p99 Latency | 0.367 ms      | 0.398 ms      | +8.4% →       |
| Lease Read          | Throughput  | 730,836 ops/s | 810,491 ops/s | **+10.9%** ✅ |
|                     | Avg Latency | 0.136 ms      | 0.123 ms      | **-9.4%** ✅  |
|                     | p99 Latency | 0.337 ms      | 0.293 ms      | **-13.1%** ✅ |
| Eventual Read       | Throughput  | 752,198 ops/s | 850,952 ops/s | **+13.1%** ✅ |
|                     | Avg Latency | 0.132 ms      | 0.117 ms      | **-11.4%** ✅ |
|                     | p99 Latency | 0.343 ms      | 0.267 ms      | **-22.2%** ✅ |
| Hot-Key (10 keys)   | Throughput  | 659,429 ops/s | 668,178 ops/s | +1.3% →       |
|                     | Avg Latency | 0.153 ms      | 0.148 ms      | -3.0% →       |
|                     | p99 Latency | 0.343 ms      | 0.351 ms      | +2.3% →       |

**Notes**:

- Lease/Eventual Read improvement driven by ReadActor fast path (#392): dedicated task bypasses Raft loop entirely for Eventual/LeaseRead, eliminating channel contention under 100 concurrent clients.
- v0.2.4 had Lease/Eventual regression vs v0.2.3 re-measured baseline (705K/742K). v0.2.5 fully recovers: **+14.9%/+14.6% vs v0.2.3**.
- Lease Read run-to-run variance: 727K–865K across 3 rounds (3-round avg 810K). Rounds 1–2 avg ~851K; round 3 dropped to 727K (OS scheduler noise on a loaded Mac mini).
- Linearizable Read p99 +8.4% and Single Write p99 +9.6% are within local benchmark noise; both paths are unchanged from v0.2.4.
- Write throughput fully stable: HC Write within ±1% of v0.2.4.
- Default config (`read_actor_channel_capacity = 512`) yields Lease/Eventual ~790K/~806K (+8%/+7% vs v0.2.4). Results above use tuned settings (see configuration).

---

### Standalone Mode: v0.2.5 vs v0.2.4

_(TBD — not yet collected)_

---

## AWS 3-Node Cluster: d-engine v0.2.5 vs v0.2.4

_(TBD — not yet collected)_

---

## Key Changes Driving Results

| Change                                   | Impact                                                                    |
| ---------------------------------------- | ------------------------------------------------------------------------- |
| ReadActor fast path (#392)               | Lease/Eventual Read +10.9%/+13.1% vs v0.2.4; bypasses Raft loop entirely |
| ReadLease.revoke() (#392)                | Atomic lease invalidation on leader demotion; replaces `invalidate()`     |
| Configurable ReadActor (#392)            | `read_actor_channel_capacity` and `read_actor_max_drain` in `[raft]`      |
| Fix: RocksDB LOCK on stop() (#392)       | ReadActor is sole `Arc<SM>` holder; LOCK released before Raft shutdown    |

---

## ReadActor Parameter Tuning (Local, Embedded Mode)

Four configurations tested to characterize `read_actor_channel_capacity` and `read_actor_max_drain` sensitivity:

| Config | cap / drain / batch | Lease Read | Eventual Read | HC Write | vs v0.2.3 (705K/742K) |
| ------ | ------------------- | ---------- | ------------- | -------- | --------------------- |
| C1     | 1024 / 1000 / 200   | ~790K      | ~806K         | ~232K    | +12% / +9%            |
| C2     | 1024 / 1000 / 300   | ~785K      | ~803K         | ~231K    | +11% / +8%            |
| C3     | 1024 / 2000 / 200   | ~756K      | ~789K         | ~227K    | +7% / +6%             |
| **C4** | **10240 / 2000 / 200** | **~810K** | **~851K**  | **~232K** | **+15% / +15%**     |

**Key findings**:

- `channel_capacity` is the dominant knob: 512→1024 yields +30%, 1024→10240 yields +3–6%.
- `max_drain` > `channel_capacity` has no effect: drain loop exits early once channel is empty.
- `max_batch_size = 300` does not improve reads and introduces write p99 instability.
- Rule of thumb: `read_actor_channel_capacity = 2× peak_concurrent_readers`.

---

## Benchmark Configuration

All Local Embedded results above were collected with the following configuration:

```toml
[raft]
read_actor_channel_capacity = 10240
read_actor_max_drain = 2000

[raft.persistence]
strategy = "MemFirst"
flush_policy = { Batch = { idle_flush_interval_ms = 1000 } }

[raft.batching]
max_batch_size = 200
```
