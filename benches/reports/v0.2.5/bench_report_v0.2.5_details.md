# d-engine v0.2.5 Benchmark: Full Results

Companion to `bench_report_v0.2.5.md`. Environment: `bench_report_v0.2.5_environment.md`.
All values are 3-round means unless marked. Spread = (max - min) / mean between rounds.

## 1. Embedded, 100 in-flight (instance set A)

| Scenario | v0.2.4 ops/s | avg / p99 ms | v0.2.5 ops/s | avg / p99 ms | Throughput change |
|---|---|---|---|---|---|
| Single client write (1 in-flight) | 4,393 (spread 17.7%) | 0.228 / 0.305 | 337 (0.5%) | 2.962 / 3.153 | -92.3% |
| High concurrency write | 115,641 (5.4%) | 0.864 / 1.064 | 18,198 (1.1%) | 5.494 / 6.203 | -84.3% |
| Linearizable read | 441,087 (5.0%) | 0.226 / 0.269 | 373,820 (0.3%) | 0.267 / 0.329 | -15.3% |
| Lease read | 470,786 (10.3%) | 0.212 / 0.259 | 1,319,390 (1.8%) | 0.001 / 0.002 | +180.3% |
| Eventual read | 478,101 (12.8%) | 0.209 / 0.247 | 1,350,684 (1.7%) | 0.001 / 0.002 | +182.5% |
| Hot-key read (10 keys) | 467,864 (2.7%) | 0.213 / 0.255 | 369,349 (5.6%) | 0.270 / 0.337 | -21.1% |

Lease and eventual reads return in about a microsecond, so their in-flight count (throughput x latency) is
about 1, not 100: the client side is the limit there. Every other row measured 100 (single client: 1).

## 2. Standalone, 200 in-flight (instance set A)

| Scenario | v0.2.4 ops/s | avg / p99 ms | v0.2.5 ops/s | avg / p99 ms | Throughput change |
|---|---|---|---|---|---|
| Single client write | 2,042 (23.7%) | 0.494 / 0.650 | 312 (1.2%) | 3.204 / 3.400 | -84.7% |
| High concurrency write | 63,308 (round 3 only) | 3.155 / 4.727 | 32,068 (0.8%) | 6.228 / 7.428 | -49.3% |
| Linearizable read | 85,018 (round 3 only) | 2.349 / 3.729 | 82,215 (8.3%) | 2.432 / 4.692 | -3.3% |
| Lease read | 83,181 (7.4%) | 2.403 / 3.684 | 95,219 (7.1%) | 2.099 / 4.258 | +14.5% |
| Eventual read | 204,337 (1.7%) | 0.975 / 1.723 | 217,464 (0.8%) | 0.916 / 1.714 | +6.4% |
| Hot-key read (10 keys) | 104,149 (round 3 only) | 1.917 / 3.037 | 96,553 (2.3%) | 2.068 / 3.846 | -7.3% |

"Round 3 only": in rounds 1 and 2 of v0.2.4 the cluster was not ready when the bench started and the
client logged `ConnectionTimeout` / `DeadlineExceeded` (write: 5,488 and 16,076 ops/s; linearizable read and
hot-key also affected). Their means are meaningless, so only the clean round is used and the spread is unknown.
v0.2.5 had no errors in any round.

## 3. etcd v3.7.2 (instance set B, three fresh-cluster runs)

| Case | ops/s (spread) | avg ms | p99 ms | WAL fsync avg ms (members' range) |
|---|---|---|---|---|
| Write 1 in-flight | 605 (0.6%) | 1.654 | 5.07 | 1.05 (1.05 to 1.05) |
| Write 100, leader | 10,064 (0.3%) | 9.933 | 16.97 | 2.16 (2.11 to 2.24) |
| Write 200, leader | 17,143 (0.3%) | 11.667 | 19.67 | 2.37 (2.28 to 2.49) |
| Write 1,000, leader | 40,011 (0.5%) | 24.867 | 43.23 | 3.24 (3.10 to 3.44) |
| Write 1,000, all members | 42,405 (0.7%) | 23.433 | 44.47 | 3.45 (3.31 to 3.54) |
| Linearizable read 1 / 100 / 200 / 1,000 | 1,624 / 64,071 / 90,569 / 150,109 | 0.616 / 1.567 / 2.200 / 6.267 | 0.80 / 3.60 / 6.03 / 25.03 | - |
| Serializable read 1 / 100 / 200 / 1,000 | 4,217 / 148,709 / 147,426 / 202,464 | 0.237 / 0.633 / 1.233 / 4.300 | 0.40 / 3.60 / 6.40 / 21.27 | - |

The 1 in-flight average is 1 / throughput (the tool prints 0.1 ms steps). Reads run only a second or two,
so their measured in-flight count is below nominal: serializable 94 / 182 / 870 and linearizable 941 at
nominal 100 / 200 / 1,000; this flatters etcd's read latency slightly.

## 4. Matched comparison on AWS (d-engine / etcd)

Latency columns are etcd / d-engine, so values above 1 favor d-engine. The d-engine runs are from set A and
the etcd runs from set B (different instances of the same shape).

| Pair | etcd ops/s | d-engine ops/s | Throughput x | Avg latency x | p99 x |
|---|---|---|---|---|---|
| Write, 1 in-flight vs standalone | 605 | 312 | 0.52 | 0.52 | 1.49 |
| Write, 200 in-flight vs standalone | 17,143 | 32,068 | 1.87 | 1.87 | 2.65 |
| Linearizable read, 200 vs standalone | 90,569 | 82,215 | 0.91 | 0.90 | 1.29 |
| Serializable vs eventual read, 200 vs standalone | 147,426 | 217,464 | 1.48 | 1.35 | 3.73 |
| Write, 1 in-flight vs embedded | 605 | 337 | 0.56 | 0.56 | 1.61 |
| Write, 100 vs embedded | 10,064 | 18,198 | 1.81 | 1.81 | 2.74 |
| Linearizable read, 100 vs embedded | 64,071 | 373,820 | 5.83 | 5.88 | 10.93 |
| Serializable vs eventual read, 100 vs embedded | 148,709 | 1,350,684 | 9.08 | about 600 (0.633 ms vs 0.001 ms) | about 1,800 (3.60 ms vs 0.002 ms) |

Embedded has no network hop (in-process calls), so its read rows show the cost of the network client as
much as the engine. The latency ratio of the last row is not meaningful: embedded eventual reads return
in about a microsecond, at the resolution of the measurement. d-engine was not run on AWS at 1 or 1,000
in-flight for reads, nor at 200 in-flight in embedded mode, nor at 1,000 for writes, in this session.

## 5. Not measured in this session

d-engine on AWS at 1,000 in-flight and for single-client reads; etcd with v0.2.4 (it
does not fsync every commit); a d-engine run with a different RocksDB WAL configuration.
