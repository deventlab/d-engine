# d-engine v0.2.3 Benchmark Report

**Date**: February 18, 2026  
**Hardware**: Apple M2 Mac mini (8-core, 16GB RAM, 3-node cluster on localhost)  
**Rounds**: 3-round average  
**Key/Value**: 8 bytes / 256 bytes

---

## Embedded Mode: v0.2.3 vs v0.2.2

| **Scenario**        | **Metric**  | **v0.2.2**    | **v0.2.3**    | **Δ**         |
| ------------------- | ----------- | ------------- | ------------- | ------------- |
| Single Client Write | Throughput  | 441 ops/s     | 10,112 ops/s  | **+22.9x** ✅ |
|                     | Avg Latency | 2.26 ms       | 0.098 ms      | **-95.7%** ✅ |
|                     | p99 Latency | 3.31 ms       | 0.138 ms      | **-95.8%** ✅ |
| High Conc. Write    | Throughput  | 202,658 ops/s | 176,839 ops/s | -12.7%        |
|                     | Avg Latency | 0.49 ms       | 0.56 ms       | +14%          |
|                     | p99 Latency | 1.16 ms       | 1.88 ms       | +62%          |
| Linearizable Read   | Throughput  | 279,442 ops/s | 536,571 ops/s | **+92%** ✅   |
|                     | Avg Latency | 0.36 ms       | 0.185 ms      | **-49%** ✅   |
|                     | p99 Latency | 0.91 ms       | 0.537 ms      | **-41%** ✅   |
| Lease Read          | Throughput  | 492,252 ops/s | 797,970 ops/s | **+62%** ✅   |
|                     | Avg Latency | 0.20 ms       | 0.125 ms      | **-38%** ✅   |
|                     | p99 Latency | 0.50 ms       | 0.343 ms      | **-31%** ✅   |
| Eventual Read       | Throughput  | 501,721 ops/s | 819,096 ops/s | **+63%** ✅   |
|                     | Avg Latency | 0.20 ms       | 0.121 ms      | **-40%** ✅   |
|                     | p99 Latency | 0.48 ms       | 0.392 ms      | **-18%** ✅   |
| Hot-Key (10 keys)   | Throughput  | 305,695 ops/s | 522,914 ops/s | **+71%** ✅   |
|                     | Avg Latency | 0.33 ms       | 0.191 ms      | **-42%** ✅   |
|                     | p99 Latency | 0.77 ms       | 0.617 ms      | **-20%** ✅   |

---

## Standalone Mode: v0.2.3 vs v0.2.2

| **Scenario**        | **Metric**  | **v0.2.2**   | **v0.2.3**   | **Δ**         |
| ------------------- | ----------- | ------------ | ------------ | ------------- |
| Single Client Write | Throughput  | 544 ops/s    | 6,421 ops/s  | **+11.8x** ✅ |
|                     | Avg Latency | 1.84 ms      | 0.155 ms     | **-91.6%** ✅ |
|                     | p99 Latency | 2.83 ms      | 0.200 ms     | **-92.9%** ✅ |
| High Conc. Write    | Throughput  | 59,302 ops/s | 55,285 ops/s | -6.8%         |
|                     | Avg Latency | 3.37 ms      | 3.61 ms      | +7%           |
|                     | p99 Latency | 6.31 ms      | 6.72 ms      | +6%           |
| Linearizable Read   | Throughput  | 63,928 ops/s | 63,210 ops/s | -1.1%         |
|                     | Avg Latency | 3.14 ms      | 3.16 ms      | stable        |
|                     | p99 Latency | 5.25 ms      | 5.81 ms      | +11%          |
| Lease Read          | Throughput  | 74,739 ops/s | 67,878 ops/s | -9.2%         |
|                     | Avg Latency | 2.67 ms      | 2.95 ms      | +10%          |
|                     | p99 Latency | 5.60 ms      | 6.20 ms      | +11%          |
| Eventual Read       | Throughput  | 99,975 ops/s | 91,174 ops/s | -8.8%         |
|                     | Avg Latency | 2.0 ms       | 2.19 ms      | +10%          |
|                     | p99 Latency | 11.06 ms     | 13.97 ms     | +26%          |
| Hot-Key (10 keys)   | Throughput  | 69,610 ops/s | 74,017 ops/s | **+6.3%** ✅  |
|                     | Avg Latency | 2.87 ms      | 2.70 ms      | -6%           |
|                     | p99 Latency | 4.87 ms      | 5.49 ms      | +13%          |

---

## AWS 3-Node Cluster: d-engine v0.2.3 vs etcd

![d-engine v0.2.3 vs etcd AWS Benchmark](d-engine_comparison_v0.2.3.png)

**Hardware**: AWS EC2 c5.2xlarge (8 vCPUs, 16GB RAM, 50GB SSD) × 3 nodes  
**Date**: 2026-02-16 | 3-round average | Key/Value: 8 bytes / 256 bytes  
**etcd reference**: Official etcd benchmark (GCE, 8 vCPUs + 16GB + SSD × 3 nodes, etcd 3.2.0)²

### Throughput (ops/s)

| **Scenario**        | **Clients** | **d-engine v0.2.3 Embedded** | **d-engine v0.2.3 Standalone** | **etcd 3.2.0** | **Embedded vs etcd** |
| ------------------- | ----------- | ---------------------------- | ------------------------------ | -------------- | -------------------- |
| Single Client Write | 1           | 2,647                        | 1,679                          | 583            | **+4.5x** ✅         |
| High Conc. Write    | 1000        | 65,548                       | 35,559                         | 44,341         | **+47.8%** ✅        |
| Linearizable Read   | 1000        | 168,707                      | 49,635                         | 141,578        | **+19.2%** ✅        |
| Lease Read          | 1000        | 391,494                      | 62,424                         | —³             | —                    |
| Eventual Read       | 1000        | 395,880                      | 156,569                        | 185,758        | **+2.1x** ✅         |
| Hot-Key Read        | 1000        | 165,269                      | 58,173                         | —³             | —                    |

### Avg Latency (ms)

| **Scenario**        | **Clients** | **d-engine v0.2.3 Embedded** | **d-engine v0.2.3 Standalone** | **etcd 3.2.0** | **Embedded vs etcd** |
| ------------------- | ----------- | ---------------------------- | ------------------------------ | -------------- | -------------------- |
| Single Client Write | 1           | 0.377                        | 0.597                          | 1.6            | **-76.4%** ✅        |
| High Conc. Write    | 1000        | 1.524                        | 5.619                          | 22.0           | **-93.1%** ✅        |
| Linearizable Read   | 1000        | 0.592                        | 4.024                          | 5.5            | **-89.2%** ✅        |
| Lease Read          | 1000        | 0.255                        | 3.199                          | —³             | —                    |
| Eventual Read       | 1000        | 0.252                        | 1.272                          | 2.2            | **-88.5%** ✅        |
| Hot-Key Read        | 1000        | 0.617                        | 3.433                          | —³             | —                    |

² etcd data sourced from [etcd official benchmark documentation](https://etcd.io/docs/v3.6/op-guide/performance/), tested on GCE infrastructure. Different cloud platform; results are for reference only.  
³ etcd does not have an equivalent mode.

---

## Key Changes Driving Results

| Change                                | Impact                                                                |
| ------------------------------------- | --------------------------------------------------------------------- |
| Event-driven `pending_reads` (#272)   | Linearizable read +92% embedded, eliminates self-blocking deadlock    |
| Drain-based batch architecture (#266) | Lease/Eventual read +62-63% embedded, eliminates ~1ms timeout penalty |
| CAS correctness: SM apply wait (#263) | High conc. write -7~13% — correct Raft behavior, expected trade-off   |
| Standalone slight regressions         | Within noise floor of local Mac disk I/O variability                  |
