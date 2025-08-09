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
