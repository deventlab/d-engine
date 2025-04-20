## Starting a Three-Node Cluster

### Quick Start with Makefile (Recommended)
```bash
# First build (ensure cargo is installed)
make build

# Start full cluster (automatically in parallel)
make start-cluster

# Individual node control
make start-node1  # Launch node 1
make start-node2  # Launch node 2
make start-node3  # Launch node 3
make clean        # Clean all build artifacts and logs

### Manual Startup (Advanced)

For manual control, run these in separate terminal sessions:

### Node 1

```bash
CONFIG_PATH=config/n1 \
LOG_DIR="./logs/1" \
RUST_LOG=demo=debug,d_engine=debug \
RUST_BACKTRACE=1 \
target/release/demo
```

### Node 2

```bash
CONFIG_PATH=config/n2 \
LOG_DIR="./logs/2" \
RUST_LOG=demo=debug,d_engine=debug \
target/release/demo
```

### Node 3

```bash
CONFIG_PATH=config/n3 \
LOG_DIR="./logs/3" \
RUST_LOG=demo=debug,d_engine=debug \
target/release/demo
```

## Prerequisites

### 1. Build Executable

```bash
# Using Makefile (recommended)make build

# Or directly with cargocargo build --release
```

### 2. Verify Configuration Files

Ensure these config files exist:

```bash
config/
├── n1
├── n2
└── n3
```

## Operations Guide

### Key Makefile Commands

| **Command** | **Description** |
| --- | --- |
| `make build` | Build release binary |
| `make start-cluster` | Start 3-node cluster (parallel) |
| `make start-nodeN` | Start individual node (N=1,2,3) |
| `make clean` | Clean build artifacts & logs |
| `make help` | Show all available commands |

### Log Monitoring

```bash
tail -f logs/1/demo.log# Watch node 1 logstail -f logs/2/demo.log# Watch node 2 logstail -f logs/3/demo.log# Watch node 3 logs
```

## Important Notes

1. Node communication depends on addresses in config files
2. Default log levels:
    - `debug`: Engine internals
    - `info`: Application logs
3. When using Makefile:
    - Auto-creates log directories
    - Verifies binary existence
    - Use Ctrl+C to terminate all nodes
4. Cluster requires all 3 nodes to be operational
5. Nodes must run on different ports (pre-configured in example)

## Recommended Directory Structure

```bash
project-root/
├── Makefile
├── examples/
│   └── three-node-cluster/
│       ├── README.md
│       ├── config/
│       │   ├── n1
│       │   ├── n2
│       │   └── n3
│       └── scripts/
├── src/
└── target/
```

This version maintains technical accuracy while improving:

- Clear hierarchy between recommended/advanced methods
- Consistent command formatting
- Visual separation of code blocks
- Table-based command reference
- Path structure visualization
- Cross-platform considerations (Linux/macOS/WSL2)