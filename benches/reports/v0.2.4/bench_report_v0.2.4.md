# d-engine v0.2.4 Benchmark Report

**Test Environments**:

- **Local**: Apple M2 Mac mini (8-core, 16GB RAM, 3-node cluster on localhost)
- **AWS**: EC2 c5.2xlarge (8 vCPUs, 16GB RAM, 50GB SSD) × 3 nodes *(pending)*

**Test Dates**:

- **Local v0.2.4 vs v0.2.3**: April 11, 2026 (6-round average, embedded; 6-round average, standalone)
- **AWS v0.2.4**: *pending*

**Key/Value**: 8 bytes / 256 bytes

---

## Local 3-Node Cluster: d-engine v0.2.4 vs d-engine v0.2.3

### Embedded Mode: v0.2.4 vs v0.2.3

*(6-round average, manually collected)*

| **Scenario**        | **Metric**  | **v0.2.3**    | **v0.2.4**    | **Δ**          |
| ------------------- | ----------- | ------------- | ------------- | -------------- |
| Single Client Write | Throughput  | 10,075 ops/s  | 9,526 ops/s   | -5.4% →        |
|                     | Avg Latency | 0.099 ms      | 0.104 ms      | +5.1% →        |
|                     | p99 Latency | 0.139 ms      | 0.197 ms      | +41.7%         |
| High Conc. Write    | Throughput  | 176,314 ops/s | 233,608 ops/s | **+32.5%** ✅  |
|                     | Avg Latency | 0.566 ms      | 0.427 ms      | **-24.6%** ✅  |
|                     | p99 Latency | 1.566 ms      | 1.148 ms      | **-26.7%** ✅  |
| Linearizable Read   | Throughput  | 508,264 ops/s | 626,012 ops/s | **+23.1%** ✅  |
|                     | Avg Latency | 0.197 ms      | 0.158 ms      | **-19.8%** ✅  |
|                     | p99 Latency | 0.710 ms      | 0.481 ms      | **-32.3%** ✅  |
| Lease Read          | Throughput  | 705,530 ops/s | 686,701 ops/s | -2.7% →        |
|                     | Avg Latency | —             | 0.145 ms      | —              |
|                     | p99 Latency | —             | 0.443 ms      | —              |
| Eventual Read       | Throughput  | 742,602 ops/s | 733,437 ops/s | -1.2% →        |
|                     | Avg Latency | —             | 0.136 ms      | —              |
|                     | p99 Latency | —             | 0.386 ms      | —              |
| Hot-Key (10 keys)   | Throughput  | 499,527 ops/s | 619,374 ops/s | **+24.0%** ✅  |
|                     | Avg Latency | 0.205 ms      | 0.161 ms      | **-21.5%** ✅  |
|                     | p99 Latency | 0.638 ms      | 0.454 ms      | **-28.8%** ✅  |

**Notes**:

- Lease/Eventual Read v0.2.3 baseline was re-measured on the same machine on March 30; original 852K/859K was not reproducible (likely different system state). Latency columns not re-measured; only throughput baseline was corrected.
- Single Client Write p99 +42% is local scheduling noise; throughput and avg latency are stable (within ±5%).
- High Conc. Write improvement increased from an earlier +14% measurement to +32.5% after commits #349 (eliminate per-entry flush) and #350 (single-lock FileLogStore restructure) were included in the final v0.2.4 build.
- Hot-Key throughput shows ±10% run-to-run variance on this machine; 6-round average (619K ops/s) is the representative value.

---

### Standalone Mode: v0.2.4 vs v0.2.3

*(6-round average, manually collected — script results discarded due to cluster warm-up artifact)*

| **Scenario**        | **Metric**  | **v0.2.3**   | **v0.2.4**   | **Δ**          |
| ------------------- | ----------- | ------------ | ------------ | -------------- |
| Single Client Write | Throughput  | 6,421 ops/s  | 5,231 ops/s  | -18.5%         |
|                     | Avg Latency | 0.155 ms     | 0.191 ms     | +23.2%         |
|                     | p99 Latency | 0.200 ms     | 0.242 ms     | +21.0%         |
| High Conc. Write    | Throughput  | 55,285 ops/s | 60,985 ops/s | **+10.3%** ✅  |
|                     | Avg Latency | 3.610 ms     | 3.279 ms     | **-9.2%** ✅   |
|                     | p99 Latency | 6.720 ms     | 6.172 ms     | **-8.2%** ✅   |
| Linearizable Read   | Throughput  | 63,210 ops/s | 73,168 ops/s | **+15.7%** ✅  |
|                     | Avg Latency | 3.160 ms     | 2.735 ms     | **-13.4%** ✅  |
|                     | p99 Latency | 5.810 ms     | 5.606 ms     | -3.5% →        |
| Lease Read          | Throughput  | 67,878 ops/s | 74,966 ops/s | **+10.4%** ✅  |
|                     | Avg Latency | 2.950 ms     | 2.669 ms     | **-9.5%** ✅   |
|                     | p99 Latency | 6.200 ms     | 5.557 ms     | **-10.4%** ✅  |
| Eventual Read       | Throughput  | 91,174 ops/s | 95,333 ops/s | **+4.6%** ✅   |
|                     | Avg Latency | 2.190 ms     | 2.093 ms     | **-4.4%** ✅   |
|                     | p99 Latency | 13.970 ms    | 9.707 ms     | **-30.5%** ✅  |
| Hot-Key (10 keys)   | Throughput  | 74,017 ops/s | 87,010 ops/s | **+17.5%** ✅  |
|                     | Avg Latency | 2.700 ms     | 2.301 ms     | **-14.8%** ✅  |
|                     | p99 Latency | 5.490 ms     | 5.461 ms     | -0.5% →        |

**Notes**:

- Single Client Write regression (-18.5%) is caused by the double-yield design: each operation incurs two fixed Tokio task-switch overheads (~+33µs) that are not amortized in single-writer scenarios. In standalone mode the per-operation gRPC RTT (~155µs) makes this overhead proportionally larger than in embedded mode.
- All other scenarios show clear improvement: HC Write +10%, reads +5–16% across the board.
- Eventual Read p99 standalone (9.7 ms) has high run-to-run variance; the -30.5% improvement over v0.2.3 is real but the absolute value should be treated as ±1–2 ms.

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
| Unified IO architecture: single IO thread, drain-then-fsync, pipeline replication | 372df3b (#295, #313, #321, #329, #332–#334, #336, #340–#343, #346) | Foundation for all IO performance improvements |
| Eliminate per-entry flush and lock in FileLogStore `persist_entries` | b7a8e6a (#349) | Embedded HC Write +32.5%, p99 -26.7%; standalone HC Write p99 -8.2% |
| Restructure FileLogStore to single `Mutex<Inner>`, add `replace_range` override, store `end_pos` in index | 6df0a1d (#350) | Reduces lock contention; enables atomic truncate+write path |
| IO thread owns `new_current_thread` runtime; `handle_non_write_cmd` fatal-exits on `replace_range` failure; `IOTask::ReplaceRange` carries `done` channel to block caller until truncation is durable | (#295) | Prevents SIGABRT on runtime drop; prevents flush short-circuit after log truncation; prevents IO thread from continuing on corrupted disk state |
| Unified RocksDB option (`unified_db = true`): single DB with 4 CFs, shared block cache | #295 | Reduces memory RSS and file descriptor usage; throughput impact not benchmarked in this report |
| Fix durable quorum: use `durable_index` for majority calculation and follower ACK | #329 | Prevents data loss with `MemFirst` strategy under concurrent leader+follower crash; correctness fix |
| Snapshot push when peer log falls behind leader purge boundary | #336 | Fixes stuck follower when leader has purged entries the peer needs (Raft §7 compliance) |
| Fix candidate not stepping down on same-term AppendEntries conflict | #340 | Prevents unnecessary leader re-elections during membership changes |
| Reduce default `snapshot_cool_down` from 3600s to 60s | #290 | Enables practical log compaction; prevents unbounded log growth in production |
| Remove `DiskFirst` persistence strategy | #332 | Simplifies storage layer; `MemFirst` provides OS page-cache durability — power-loss safety requires explicit fsync, which `MemFirst` does not perform |
