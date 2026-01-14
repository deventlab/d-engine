# Jepsen Tests for dengine

This directory contains [Jepsen](https://jepsen.io/) tests for validating the correctness of the `d-engine` cluster under various failure scenarios.

## Prerequisites

- Docker & Docker Compose
- Java (for Leiningen / Clojure tooling)
- Rust toolchain (for building `dengine`)
- SSH keys available under `jepsen/sshkeys/` (mounted into the Jepsen container)
- **Example cluster(three-nodes-standalone) running:**

## Usage

The `Makefile` provides a set of common commands for running and managing Jepsen tests.

### 1. Restart the Docker stack

```bash
make restart-stack
```

    •	Cleans logs and database data in ../output/
    •	Restarts the Docker Compose cluster defined in ../docker-compose.yml
    •	Waits for the cluster to initialize

2. Run Jepsen tests

```bash
make test TIME_LIMIT=120 TEST_COMMAND=dengine_ctl
```

    •	Spins up the Jepsen container
    •	Executes the configured Jepsen test (lein run test)
    •	Runs against nodes defined by NODE1, NODE2, NODE3
    •	Default runtime is 60 seconds (override via TIME_LIMIT)

Environment variables you can override:
• TIME_LIMIT (default: 60)
• TEST_COMMAND (default: dengine_ctl)
• NODE1, NODE2, NODE3 (default: node1, node2, node3)
• ENDPOINTS (default: http://node1:9081,http://node2:9082,http://node3:9083)

3. Configure SSH inside container

If you need to manually set up the SSH agent in the Jepsen container:

```bash
make ssh-setup
```

4. View the latest report

```bash
make view
```

    •	Opens the latest Jepsen HTML report in your browser
    •	If auto-open fails, it will print the file path

5. Print report path

```bash
make report
```

    •	Prints the absolute path to the latest Jepsen report

6. Clean test artifacts

```bash
make clean
```

    •	Removes all artifacts under jepsen/store/

Output
• Test results are stored under jepsen/store/
• The latest symlink points to the most recent test run
• Reports include Jepsen logs and visualization of histories

⸻

Example

Run a 2-minute Jepsen test against a three-node cluster:

```bash
make test
make view
```

⸻

Notes
• Make sure your cluster nodes (node1, node2, node3) are reachable from inside the Jepsen container.
• For deeper debugging, you can docker exec -it docker-jepsen-1 bash and run lein run directly.
