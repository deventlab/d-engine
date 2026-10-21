# d-engine v0.2.5 Benchmark: raft-engine log store (embedded and standalone)

Companion to `bench_report_v0.2.5.md`. Measured on **October 20, 2026** on two new instance sets (E and F).
Experimental: the log store is a spike (#460), not part of a release. Values are 3-round means; spread = (max - min) / mean.

## 1. What was measured

| Item | Value |
|---|---|
| Engine | The Raft log and the hard state are stored in raft-engine (a fork pinned at `e5ef995`: upstream master `097c499` with the `lz4-sys` pin relaxed); the state machine is RocksDB. Embedded: `EmbeddedEngine<RaftEngineStorage, RocksDBStateMachine>`; standalone: `three-nodes-standalone` built with `--features raft-engine` (`StandaloneEngine::run_custom`). Branch `spike/460-fdatasync` |
| Control | Stock v0.2.5: Raft log in a RocksDB WAL, `unified_db = false`; the October 18 report (set A). For standalone there is also a RocksDB run on set F itself (Section 3) |
| Instances | Same shapes as the other sets: 3 x c5.2xlarge, 50 GB gp3 root volume (3,000 IOPS, 125 MB/s), us-east-1. Set E: embedded, 07:24 to 07:27 UTC. Set F: embedded 14:20 to 14:23 UTC, standalone from 14:41 UTC. Instance IDs were not recorded |
| Bench | Embedded: `embedded-bench --mode local --batch`, 100 in-flight, leader output (set F: node2, node3, node3). Standalone: `standalone-bench` on the client, 200 in-flight, single client 1. 8 B keys, 256 B values, data wiped between rounds |
| Config | Snapshots disabled, as in the earlier reports. `[raft_engine]`: `target_file_size_mib = 1`, `purge_threshold_mib = 512`, `prefill_limit_mib = 512` (every start created 768 files, which is 512 x 1.5) |

## 2. Embedded, 100 in-flight (set F; set E in the third column)

Cells are ops/s (average latency). Control = October 18, RocksDB WAL.

| Scenario | raft-engine, set F | Spread | raft-engine, set E | Control | Set F vs control |
|---|---|---|---|---|---|
| Single client write | 836 (1.196 ms, p99 3.00 ms) | 2.8% | 693 (1.485 ms) | 337 (2.962 ms, p99 3.15) | +147.6% |
| High concurrency write | 19,860 (5.036 ms, p99 12.04 ms) | 4.8% | 20,271 (4.932 ms) | 18,198 (5.494 ms, p99 6.20) | +9.1% |
| Linearizable read | 368,068 (0.271 ms) | 4.0% | 363,803 | 373,820 | -1.5% |
| Lease read | 1,204,641 | 5.9% | 1,213,826 | 1,319,390 | -8.7% |
| Eventual read | 1,253,650 | 4.4% | 1,236,197 | 1,350,684 | -7.2% |
| Hot-key read | 356,768 (0.280 ms) | 6.6% | 360,246 | 369,349 | -3.4% |

Per round on set F, single client write: 850 / 830 / 827 ops/s; high concurrency write: 20,075 / 20,232 / 19,273 ops/s.
Throughput x latency is 1.0 and 100.0 for the writes, 99.7 for the reads that use 100 in-flight.

## 3. Standalone, 200 in-flight (set F)

"Same-set RocksDB" is a standalone run on set F whose build did not get the raft-engine feature, so it ran stock RocksDB.

| Scenario | raft-engine | Spread | Oct 18 RocksDB | Same-set RocksDB | vs Oct 18 |
|---|---|---|---|---|---|
| Single client write | 712 (1.404 ms, p99 3.15 ms) | 1.5% | 312 (3.204 ms, p99 3.40) | 299 (3.343 ms) | +128.2% |
| High concurrency write | 35,080 (5.696 ms, p99 12.28 ms) | 0.4% | 32,068 (6.228 ms, p99 7.43) | 31,818 (6.277 ms) | +9.4% |
| Linearizable read | 81,610 (2.448 ms) | 5.5% | 82,215 | 81,094 | -0.7% |
| Lease read | 93,372 | 5.8% | 95,219 | 93,077 | -1.9% |
| Eventual read | 212,625 (0.937 ms) | 1.1% | 217,464 | 212,492 | -2.2% |
| Hot-key read | 95,466 | 5.6% | 96,553 | 93,985 | -1.1% |

Throughput x latency is 1.0 for the single client and 199.2 to 199.8 for the rest. No timeouts or failed requests in any round.
The two RocksDB runs differ by 0.3% to 4.1%, so the October 18 control is a fair reference for the standalone writes.

## 4. Against etcd v3.7.2 (October 18, instance set B)

| Case | etcd | Ratio with raft-engine | Ratio with the RocksDB WAL |
|---|---|---|---|
| Standalone write, 1 in-flight | 605 ops/s | **1.18x** | 0.52x |
| Standalone write, 200 in-flight | 17,143 ops/s | **2.05x** | 1.87x |
| Standalone linearizable read | 90,569 ops/s | 0.90x | 0.91x |
| Standalone eventual vs etcd serializable read | 147,426 ops/s | 1.44x | 1.48x |
| Embedded write, 1 in-flight | 605 ops/s | 1.38x | 0.56x |
| Embedded write, 100 in-flight | 10,064 ops/s | 1.97x | 1.81x |

## 5. Evidence that a commit waits for a physical sync

- Standalone, set F: each server's `core_raft_fsync_duration_ms` averaged 0.867 to 0.948 ms in all nine node-rounds (about 12,600
  flushes each), against 2.68 to 2.78 ms for the RocksDB run on the same set. The fio overwrite floor is 0.78 to 0.92 ms.
- Embedded, set E (`verify-fsync.sh`, 07:11 UTC, 20,000 single-client writes): average `flush()` 0.894 ms, 20,022 `fdatasync` calls for
  20,001 commits, 288 journal transactions (1.4% of the commits). On the leader 95.3% of the flushes took 0.8 to 1.6 ms and 1.2% took 1.6
  to 3.2 ms, which matches the journal share; whether these are the same events was not checked. One follower had 0.4% of its flushes
  above 25 ms (up to 102 ms), not explained. For contrast, the run with the no-op sync made 19 `fdatasync` calls per node.
- Embedded, set F: no separate run. The single client write averages 1.196 ms, above the fastest sync seen on this disk (0.775 ms).
- An earlier run on October 20 showed 0.21 ms commits because the published raft-engine 0.4.2 has a no-op `Engine::sync()`; it is discarded.

## 6. Reading

- A sync that overwrites preallocated files makes the single client write 2.3 to 2.5 times faster in both modes. At 100 to 200 in-flight
  it is 9% faster and the p99 is higher (12 vs 6 to 7 ms). The sync is not the only limit at that concurrency; not investigated.
- Against etcd, the single client write goes from behind (0.52x, 0.56x) to ahead (1.18x, 1.38x). Reads do not depend on the log store.

## 7. Limits

- The control is from another instance set (A), except the standalone same-set run. Embedded reads differ by 3% to 9% between sets.
- Snapshots are off, so the log is never purged and the recycle pool is consumed during a run (about 33 files per run against 768).
- The standalone raft-engine path starts the state machine through `run_custom`, which does not attach the TTL lease manager; no
  benchmark uses TTL. The fork is a stop-gap for a no-op `Engine::sync()` in the published crate and a pinned `lz4-sys`.
- Not measured: 1,000 in-flight, a RocksDB embedded run on set F, the cause of the higher p99, power-loss behaviour, lz4 compression (off).
