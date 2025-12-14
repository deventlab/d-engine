# d-engine v0.1.4 Re-Benchmark Report

**Date:** December 13, 2025  
**Hardware:** Apple Mac mini M2 (8-core, 16GB) - Single machine, 3-node cluster  
**Purpose:** Establish accurate baseline for v0.2.0 comparison

---

## Context

This report provides re-measured v0.1.4 performance data using the same hardware and test methodology as v0.2.0 benchmarks. The original `report_20251011.md` data may have been from different test conditions. This remeasured data ensures a fair comparison between v0.1.4 and v0.2.0.

---

## Test Results (2025-12-13 Remeasured)

### Write Performance

| Test                          | Throughput   | Avg Latency | p50 Latency | p99 Latency |
| ----------------------------- | ------------ | ----------- | ----------- | ----------- |
| Single client write (10K)     | 535.35 ops/s | 1.87 ms     | 1.91 ms     | 3.05 ms     |
| High concurrency write (100K) | 60,411 ops/s | 3.31 ms     | 3.15 ms     | 6.31 ms     |

### Read Performance

| Test                        | Throughput    | Avg Latency | p50 Latency | p99 Latency |
| --------------------------- | ------------- | ----------- | ----------- | ----------- |
| Linearizable read (100K)    | 11,839 ops/s  | 16.88 ms    | 16.64 ms    | 27.81 ms    |
| Lease-based read (100K)     | 75,032 ops/s  | 2.66 ms     | 2.41 ms     | 14.05 ms    |
| Eventual consistency (100K) | 112,639 ops/s | 1.77 ms     | 1.11 ms     | 8.84 ms     |

---

## Raw Test Output

### Single Client Write (10K requests)

```
Summary:
Total time:	18.68 s
 Requests:	10000
Throughput:	535.35 ops/sec

Latency distribution (μs):
 Avg	1866.38
 Min	180
 Max	21887
 p50	1905
 p90	2241
 p99	3045
 p99.9	9975
```

### High Concurrency Write (100K requests)

```
Summary:
Total time:	1.67 s
 Requests:	100999
Throughput:	60410.77 ops/sec

Latency distribution (μs):
 Avg	3306.96
 Min	712
 Max	8759
 p50	3149
 p90	4655
 p99	6311
 p99.9	7559
```

### Linearizable Read (100K requests)

```
Summary:
Total time:	8.53 s
 Requests:	100999
Throughput:	11838.78 ops/sec

Latency distribution (μs):
 Avg	16875.99
 Min	677
 Max	65023
 p50	16639
 p90	17631
 p99	27807
 p99.9	55263
```

### Lease-based Read (100K requests)

```
Summary:
Total time:	1.35 s
 Requests:	100999
Throughput:	75031.70 ops/sec

Latency distribution (μs):
 Avg	2661.65
 Min	61
 Max	27663
 p50	2411
 p90	3881
 p99	14047
 p99.9	20463
```

### Eventual Consistency Read (100K requests)

```
Summary:
Total time:	0.90 s
 Requests:	100999
Throughput:	112639.20 ops/sec

Latency distribution (μs):
 Avg	1770.17
 Min	34
 Max	17679
 p50	1109
 p90	4603
 p99	8839
 p99.9	11863
```

---

## Comparison with Original Report

| Test                   | Original (2025-10-11) | Re-measured (2025-12-13) | Difference |
| ---------------------- | --------------------- | ------------------------ | ---------- |
| Single client write    | 622 ops/s             | 535.35 ops/s             | -13.9%     |
| High concurrency write | 67,303 ops/s          | 60,411 ops/s             | -10.2%     |

**Note:** The differences may be due to:

- Different machine load conditions at test time
- Cold start vs warm start effects
- OS/system updates or background processes
- Thermal throttling on M2 chip under sustained load

---

## Test Commands

```bash
# Single client write (10K requests)
./target/release/d-engine-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 1 --clients 1 --sequential-keys --total 10000 \
    --key-size 8 --value-size 256 put

# High concurrency write (100K requests)
./target/release/d-engine-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --sequential-keys --total 100000 \
    --key-size 8 --value-size 256 put

# Linearizable read (100K requests)
./target/release/d-engine-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --sequential-keys --total 100000 \
    --key-size 8 range --consistency l

# Lease-based read (100K requests)
./target/release/d-engine-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --sequential-keys --total 100000 \
    --key-size 8 range --consistency s

# Eventual consistency read (100K requests)
./target/release/d-engine-bench \
    --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082 --endpoints http://127.0.0.1:9083 \
    --conns 200 --clients 1000 --sequential-keys --total 100000 \
    --key-size 8 range --consistency e
```

---

## Usage Note

This report should be used as the v0.1.4 baseline when comparing against v0.2.0 performance, as both were tested on the same hardware under similar conditions, ensuring a fair and accurate comparison.

**Version:** d-engine v0.1.4  
**Test Date:** December 13, 2025  
**Benchmark Runs:** Single run per configuration  
**Cluster:** 3-node local deployment
