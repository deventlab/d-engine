# d-engine v0.1.4 Re-Benchmark Report

**Date:** December 5, 2025  
**Hardware:** Apple Mac mini M2 (8-core, 16GB) - Single machine, 3-node cluster  
**Purpose:** Establish accurate baseline for v0.2.0 comparison

---

## Context

This report provides re-measured v0.1.4 performance data. The original `report_20251011.md` data (622 ops/s, 67,303 ops/s) may have been from different test conditions.

---

## Results (2025-12-05)

| Test | Throughput | Avg Latency | p99 Latency |
|------|------------|-------------|-------------|
| Single client write | 522 ops/s | 1.91 ms | 3.62 ms |
| High concurrency write | 66,003 ops/s | 3.03 ms | 5.91 ms |
| Linearizable read | 12,127 ops/s | 16.48 ms | 24.26 ms |
| Lease-based read | 92,191 ops/s | 2.17 ms | 5.55 ms |
| Eventual consistency | 116,503 ops/s | 1.71 ms | 8.10 ms |

---

## Comparison with Original Report

| Test | Original (2025-10-11) | Re-measured (2025-12-05) | Note |
|------|----------------------|-------------------------|------|
| Single client write | 622 ops/s | 522 ops/s | -16% |
| High concurrency write | 67,303 ops/s | 66,003 ops/s | -2% |

The difference may be due to:
- Different machine load conditions
- Cold start vs warm start
- OS/system updates

---

## Test Commands

```bash
./target/release/d-engine-bench \
    --endpoints http://127.0.0.1:9081 \
    --endpoints http://127.0.0.1:9082 \
    --endpoints http://127.0.0.1:9083 \
    --conns 1 --clients 1 --sequential-keys \
    --total 100000 --key-size 8 --value-size 256 put
```
