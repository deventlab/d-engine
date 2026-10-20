# d-engine v0.2.5 Benchmark: raft-engine log store (embedded)

Companion to `bench_report_v0.2.5.md`. Measured on **October 20, 2026** (leader output logged 07:24:06 to 07:27:20 UTC).
Experimental: the log store is a spike (#460), not part of a release. Values are 3-round means; spread = (max - min) / mean.

## 1. What was measured

| Item | Value |
|---|---|
| Engine | `EmbeddedEngine<RaftEngineStorage, RocksDBStateMachine>`: the Raft log and the hard state are stored in raft-engine (a fork pinned at `e5ef995`, upstream master `097c499` with the `lz4-sys` pin relaxed); the state machine is RocksDB. Branch `spike/460-fdatasync` |
| Control | `DefaultEmbeddedEngine`: Raft log in a RocksDB WAL, state machine RocksDB, `unified_db = false`. Numbers from `bench_report_v0.2.5.md` (October 18, set A) |
| Instances | A new set (E): 3 x c5.2xlarge, 50 GB gp3 root volume (3,000 IOPS, 125 MB/s), us-east-1, same Terraform shapes as the other sets. Instance IDs were not recorded |
| Bench | `embedded-bench --mode local --batch --key-size 8 --value-size 256 --sequential-keys put`, one process per node, leader output (node2 in all three rounds), 100 in-flight, 3 rounds, data wiped between rounds |
| Log store config | `[raft_engine]` in each node config: 1 MiB segments (`target_file_size_mib = 1`), `purge_threshold_mib = 512`. `prefill_limit_mib = 512`: every start logged "created 768 files" in about 0.55 s, and raft-engine creates `prefill_limit / segment size x 1.5` files |
| Commit | The branch was built on the instances from GitHub; the exact commit was not recorded (the branch head when pushed was `3e1ad66a`) |

## 2. Results (leader, 3-round means)

Cells are ops/s (average latency, p99). "Control" is v0.2.5 with the RocksDB WAL from October 18 (a different instance set).

| Scenario | raft-engine | Spread | Control (RocksDB WAL) | Throughput vs control |
|---|---|---|---|---|
| Single client write | 693 (1.485 ms, p99 11.71 ms) | 42.0% | 337 (2.962 ms, p99 3.15 ms) | +105.4% |
| High concurrency write | 20,271 (4.932 ms, p99 10.37 ms) | 1.2% | 18,198 (5.494 ms, p99 6.20 ms) | +11.4% |
| Linearizable read | 363,803 (0.274 ms, p99 0.343 ms) | 7.2% | 373,820 (0.267 ms) | -2.7% |
| Lease read | 1,213,826 (0.001 ms) | 3.5% | 1,319,390 (0.001 ms) | -8.0% |
| Eventual read | 1,236,197 (0.001 ms) | 2.7% | 1,350,684 (0.001 ms) | -8.5% |
| Hot-key read (10 keys) | 360,246 (0.277 ms, p99 0.347 ms) | 2.0% | 369,349 (0.270 ms) | -2.5% |

Against v0.2.4, which does not sync (October 18): single client write -84.2%, high concurrency write -82.5%.
Check (throughput x average latency): 1.0 for the single client, 100.0 for high concurrency, 99.8 and 99.7 for the
two reads that use 100 in-flight; lease and eventual reads are client-limited at about 1.2, as in the main report.

Per round (ops/s, average ms, p99 ms):

| Round | Single client write | High concurrency write |
|---|---|---|
| 1 | 649, 1.540, 16.07 | 20,423, 4.895, 9.54 |
| 2 | 570, 1.754, 16.12 | 20,203, 4.948, 9.47 |
| 3 | 861, 1.160, 2.93 | 20,188, 4.951, 12.10 |

## 3. Evidence that a commit waits for a physical sync

An earlier run on October 20 (another instance set) showed 0.21 ms commits because the published raft-engine 0.4.2 has a
no-op `Engine::sync()`; that run is discarded. On this set the check `verify-fsync.sh` (20,000 single-client writes, 07:11 to
about 07:16 UTC, results `reports/fsync-verify-20261020-151149/` in d-engine-terraform) gave, on the leader: average `flush()`
0.894 ms (17,893 ms over 20,001 calls), 20,022 `fdatasync` calls under `strace` (11 `fsync`, 9 `fallocate`), 288 ext4 journal
transactions for about 20,000 commits, single-client write 891 ops/s at 1.122 ms. Followers: 0.897 and 1.038 ms per flush.

## 4. Reading

- The single-client write is about twice as fast as with the RocksDB WAL, and the average flush (0.89 ms) is at the fio
  overwrite floor on this volume (0.78 to 0.92 ms) rather than the append cost (2.71 ms). This supports the #460 hypothesis.
- At 100 in-flight the gain is only 11.4% in throughput and 10% in average latency, and p99 is higher (10.4 vs 6.2 ms). The
  sync is not the only limit at that concurrency; the cause was not investigated (flush time under load was not recorded).
- Tail latency is high where it should not be: single-client p99 was 16 ms in rounds 1 and 2 (2.9 ms in round 3), which gives the
  42% spread. Hypotheses, not tested: background write-back of the roughly 768 MiB of zero-filled files created at start
  competes for the 125 MB/s volume during the first scenario; 1 MiB segment rotation stalls. A round (restart, 5 s settle, six scenarios) takes about 70 s.
- Reads match the control within the usual spread of different instance sets; the log store is not on the read path.

## 5. Limits of this comparison

- The control is from another instance set and another day (October 18). Instance sets differ in disk behaviour, so a back-to-back
  RocksDB run on set E is still needed before any ratio above is quoted outside this directory.
- One set of instances, three rounds, leader only; the single-client write has a 42% spread.
- Not measured: standalone mode with raft-engine, 1,000 in-flight, the recycle pool state after a run, power-loss behaviour, the
  effect of 1 MiB vs larger segments, lz4 compression (disabled in the adapter).
- The fork is a stop-gap: it exists because the published crate has a no-op `Engine::sync()` and upstream pins `lz4-sys` to `=1.9.5`.
