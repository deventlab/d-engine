# Stability Test Instructions

This document explains how to run the stability (soak) test for the d-engine benchmark client.

---

## Prerequisites

- You must have a **three-node cluster** running successfully.
- To start the cluster, navigate to `three-nodes-cluster/docker/` and run:

```bash
docker-compose up -d
```

Ensure all three nodes are up and healthy before proceeding.

---

## **Running the Stability Test**

From the **parent folder** of this repository (the folder containing the Makefile), run:

```bash
make soak-test
```

This command will:

- Build the d-engine-soak-tester image if not already built.
- Start the soak test container that runs the soak-test.sh script inside the container.

---

## **About soak-test.sh**

The soak-test.sh script runs a series of benchmark tests continuously for up to 3 days, including:

- Basic write tests
- High concurrency write tests
- Strong consistency reads (linearizable)
- Eventual consistency reads (sequential)

It randomly selects test scenarios, performs system status checks, and inserts random delays between tests to simulate realistic workloads.

---

## **Environment Variables**

You can override the following environment variables to customize the soak test:

- ENDPOINTS: Comma-separated list of cluster node URLs (default: "http://localhost:9081,http://localhost:9082,http://localhost:9083")
- TOTAL: Total number of requests per test (default: 100)

---

## **Notes**

- The soak test requires network connectivity to the cluster nodes.
- The cluster nodes should be stable and reachable via the URLs provided in ENDPOINTS.
- Logs and results will be output inside the container’s standard output.

---

## **Interpreting the Stability Test Output**

The soak test runs continuously and prints periodic logs like the following:

```text
[Sat Aug  9 04:01:08 UTC 2025] ==== System Status ====
 04:01:08 up 3 days, 17:44,  0 user,  load average: 4.60, 4.51, 4.61

               total        used        free      shared  buff/cache   available
Mem:           7.7Gi       1.2Gi       3.8Gi       348Ki       2.9Gi       6.4Gi
Swap:          1.0Gi       159Mi       864Mi
Filesystem      Size  Used Avail Use% Mounted on
overlay         118G   28G   84G  25% /
...

[Sat Aug  9 04:01:08 UTC 2025] Running basic write test...

Initializing client connection with: ["http://192.168.0.13:9083/,http://192.168.0.12:9082/,http://192.168.0.11:9081/"]

Summary:

Total time:	0.07 s
 Requests:	10
Throughput:	150.92 ops/sec

Latency distribution (μs):

 Avg	6614.40
 Min	2130
 Max	20047
 p50	3043
 p90	16303
 p99	20047
 p99.9	20047

[Sat Aug  9 04:01:08 UTC 2025] Sleeping for 13 seconds...
```

---

### **What to Observe and How to Judge Stability**

- **System Status logs** show CPU load, memory usage, swap usage, and filesystem capacity.

  Watch for unusually high load, memory exhaustion, or disk space issues over time.

- **Test Summary** reports:
  - Total time and number of requests performed
  - Throughput (operations per second) — should remain relatively stable
  - Latency distribution percentiles (p50, p90, p99, etc.) — latency spikes or large increases indicate potential problems

---

### **Manual Monitoring Tips**

- Continuously monitor the output logs during the test duration (typically multiple days).
- Look for error messages or panics reported by the benchmark client — any repeated failures are signs of instability.
- Observe resource consumption trends from system status logs — rising load or memory usage may indicate leaks or bottlenecks.
- Check if throughput and latency remain steady or degrade over time.

---

### **Summary**

The soak test requires **manual observation** of its stdout logs to determine system stability. It provides useful metrics on both system health and benchmark performance which should be stable and consistent to consider the system stable under load.
