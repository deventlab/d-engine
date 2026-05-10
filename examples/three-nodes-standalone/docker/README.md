# Distributed Engine Docker Setup

## Project Setup

### 1. Configure Environment

```bash
cd ../d-engine/

# Create required directories (run from d-engine workspace root)
mkdir -p examples/three-nodes-standalone/docker/output/{logs,db}
```

### 2. Build Docker Images

```bash
# Build core component (run from d-engine workspace root)
docker build -t demo:v0.2.4 -f examples/three-nodes-standalone/docker/Dockerfile .

# Build monitoring component
docker build -t prometheus:1.0 -f examples/three-nodes-standalone/docker/monitoring/prometheus/Dockerfile .

# Build Jepsen test container (run from d-engine-jepsen directory)
cd ../d-engine-jepsen/
docker compose build jepsen
```

### 3. Start the Cluster

```bash
# All docker-compose commands must be run from the docker/ directory so that
# relative volume paths (e.g. ../config) resolve correctly.
cd examples/three-nodes-standalone/docker/

# Start the main cluster
docker-compose up -d

# Start the monitoring system
docker-compose -f monitoring/docker-compose.yml up -d
```

### 4. Run Jepsen Tests

```bash
# From d-engine-jepsen directory
cd ../d-engine-jepsen/
make test

# Run a specific workload (register / bank / set)
make test WORKLOAD=bank
```
