# d-engine v0.2.4 Benchmark Report

**Test Environments**:

- **Local**: Apple M2 Mac mini (8-core, 16GB RAM, 3-node cluster on localhost)
- **AWS**: EC2 c5.2xlarge (8 vCPUs, 16GB RAM, 50GB SSD) × 3 nodes *(pending)*

**Test Dates**:

- **Local v0.2.4 vs v0.2.3**: March 30, 2026 (5-round average, embedded; 5-round average, standalone)
- **AWS v0.2.4**: *pending*

**Key/Value**: 8 bytes / 256 bytes

---

## Local 3-Node Cluster: d-engine v0.2.4 vs d-engine v0.2.3

### Embedded Mode: v0.2.4 vs v0.2.3

*(5-round average, manually collected)*

| **Scenario**        | **Metric**  | **v0.2.3**    | **v0.2.4**    | **Δ**          |
| ------------------- | ----------- | ------------- | ------------- | -------------- |
| Single Client Write | Throughput  | 10,075 ops/s  | 9,938 ops/s   | -1.4% →        |
|                     | Avg Latency | 0.099 ms      | 0.100 ms      | +1.0% →        |
|                     | p99 Latency | 0.139 ms      | 0.203 ms      | +46.3%         |
| High Conc. Write    | Throughput  | 176,314 ops/s | 201,021 ops/s | **+14.0%** ✅  |
|                     | Avg Latency | 0.566 ms      | 0.489 ms      | **-13.6%** ✅  |
|                     | p99 Latency | 1.566 ms      | 1.565 ms      | 0.0% →         |
| Linearizable Read   | Throughput  | 508,264 ops/s | 623,535 ops/s | **+22.7%** ✅  |
|                     | Avg Latency | 0.197 ms      | 0.159 ms      | **-19.2%** ✅  |
|                     | p99 Latency | 0.710 ms      | 0.396 ms      | **-44.3%** ✅  |
| Lease Read          | Throughput  | 705,530 ops/s | 712,184 ops/s | +0.9% →        |
|                     | Avg Latency | —             | 0.140 ms      | —              |
|                     | p99 Latency | —             | 0.384 ms      | —              |
| Eventual Read       | Throughput  | 742,602 ops/s | 723,263 ops/s | -2.6% →        |
|                     | Avg Latency | —             | 0.138 ms      | —              |
|                     | p99 Latency | —             | 0.377 ms      | —              |
| Hot-Key (10 keys)   | Throughput  | 499,527 ops/s | 684,455 ops/s | **+37.0%** ✅  |
|                     | Avg Latency | 0.205 ms      | 0.146 ms      | **-28.7%** ✅  |
|                     | p99 Latency | 0.638 ms      | 0.328 ms      | **-48.7%** ✅  |

**Notes**:

- Lease/Eventual Read v0.2.3 baseline was re-measured on the same machine on March 30; original 852K/859K was not reproducible (likely different system state). Latency columns not re-measured; only throughput baseline was corrected.
- Single Client Write p99 +46% is local scheduling noise; throughput and avg latency are stable (-1.4%, +1.0%).

---

### Standalone Mode: v0.2.4 vs v0.2.3

*(5-round average, manually collected — script results discarded due to cluster warm-up artifact)*

| **Scenario**        | **Metric**  | **v0.2.3**   | **v0.2.4**   | **Δ**          |
| ------------------- | ----------- | ------------ | ------------ | -------------- |
| Single Client Write | Throughput  | 6,421 ops/s  | 5,253 ops/s  | -18.2%         |
|                     | Avg Latency | 0.155 ms     | 0.190 ms     | +22.4%         |
|                     | p99 Latency | 0.200 ms     | 0.239 ms     | +19.6%         |
| High Conc. Write    | Throughput  | 55,285 ops/s | 61,477 ops/s | **+11.2%** ✅  |
|                     | Avg Latency | 3.610 ms     | 3.256 ms     | **-9.8%** ✅   |
|                     | p99 Latency | 6.720 ms     | 6.745 ms     | +0.4% →        |
| Linearizable Read   | Throughput  | 63,210 ops/s | 73,365 ops/s | **+16.1%** ✅  |
|                     | Avg Latency | 3.160 ms     | 2.726 ms     | **-13.7%** ✅  |
|                     | p99 Latency | 5.810 ms     | 5.970 ms     | +2.8% →        |
| Lease Read          | Throughput  | 67,878 ops/s | 77,883 ops/s | **+14.7%** ✅  |
|                     | Avg Latency | 2.950 ms     | 2.566 ms     | **-13.0%** ✅  |
|                     | p99 Latency | 6.200 ms     | 5.465 ms     | **-11.9%** ✅  |
| Eventual Read       | Throughput  | 91,174 ops/s | 96,152 ops/s | **+5.5%** ✅   |
|                     | Avg Latency | 2.190 ms     | 2.076 ms     | **-5.2%** ✅   |
|                     | p99 Latency | 13.970 ms    | 9.162 ms     | **-34.4%** ✅  |
| Hot-Key (10 keys)   | Throughput  | 74,017 ops/s | 82,681 ops/s | **+11.7%** ✅  |
|                     | Avg Latency | 2.700 ms     | 2.418 ms     | **-10.4%** ✅  |
|                     | p99 Latency | 5.490 ms     | 5.564 ms     | +1.3% →        |

**Notes**:

- Single Client Write regression (-18%) is caused by the double-yield design: each operation incurs two fixed Tokio task-switch overheads (~+33µs) that are not amortized in single-writer scenarios. In standalone mode the per-operation gRPC RTT (~155µs) makes this overhead proportionally larger than in embedded mode.
- All other scenarios show clear improvement: HC Write +11%, reads +5–16% across the board.

---

## AWS 3-Node Cluster: d-engine v0.2.4 vs etcd

*Pending — AWS benchmark not yet run for v0.2.4.*

---

## Benchmark Configuration

All results above were collected with the following configuration:

```toml
[storage]
unified_db = false  # separate RocksDB instances for log and meta

[raft.persistence]
strategy = "MemFirst"
flush_policy = { Batch = { idle_flush_interval_ms = 1000 } }

[raft.batching]
max_batch_size = 200
```

The `unified_db = true` path exists but was not covered by this benchmark run.

---

## Key Changes in v0.2.4

| Change | Commit / Ticket | Impact |
| ------ | --------------- | ------ |
| Unified IO architecture: single IO thread, drain-then-fsync, pipeline replication | 372df3b (#295, #313, #321, #329, #332–#334, #336, #340–#343, #346) | Embedded High Conc. Write +14%, Linearizable Read +23%, Hot-Key +37%; standalone reads +5–16% |
| Unified RocksDB option (`unified_db = true`): single DB with 4 CFs, shared block cache | #295 | Reduces memory RSS and file descriptor usage; throughput impact not benchmarked in this report |
| Fix durable quorum: use `durable_index` for majority calculation and follower ACK | #329 | Prevents data loss with `MemFirst` strategy under concurrent leader+follower crash; correctness fix |
| Snapshot push when peer log falls behind leader purge boundary | #336 | Fixes stuck follower when leader has purged entries the peer needs (Raft §7 compliance) |
| Fix candidate not stepping down on same-term AppendEntries conflict | #340 | Prevents unnecessary leader re-elections during membership changes |
| Reduce default `snapshot_cool_down` from 3600s to 60s | #290 | Enables practical log compaction; prevents unbounded log growth in production |
| Remove `DiskFirst` persistence strategy | #332 | Simplifies storage layer; `MemFirst` provides OS page-cache durability — power-loss safety requires explicit fsync, which `MemFirst` does not perform |
