# Distributed Engine Jepsen Testing with Docker

## Project Setup

### 1. Configure Environment

```bash
# Create required directories (run in the project root directory)
mkdir -p docker/output/{logs,db} docker/jepsen/store

# Create configuration symlink (run only once)
ln -sf $(pwd)/config docker/config
```

### 2. Build Docker Images

```bash
# Build core component (run in the project root directory)
docker build -t dengine:1.3 -f docker/Dockerfile .

# Build monitoring component
docker build -t prometheus:1.0 -f docker/monitoring/prometheus/Dockerfile .

# Build Jepsen test container
docker build -t jepsen:1.0 -f docker/jepsen/Dockerfile .
```

### 3. Start the Cluster

```bash
# Start the main cluster (run in the docker/ directory)
docker-compose -f docker/docker-compose.yml up -d

# Start the monitoring system
docker-compose -f docker/monitoring/docker-compose.yml up -d
```

### 4. Run the Tests

```bash
# Enter the Jepsen container
docker exec -it jepsen bash

# Run the test inside the container
lein run test --time-limit 60
```
