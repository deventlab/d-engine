## Starting a Three-Node Cluster
To launch a cluster with three nodes, open three separate terminal windows and run the following commands:

### Node 1
```shell
CONFIG_PATH=config/n1 \
LOG_DIR="./logs/1" \
RUST_LOG=demo=debug,dengine=debug \
RUST_BACKTRACE=1 \
target/release/demo
```

### Node 2
```shell
CONFIG_PATH=config/n2 \
LOG_DIR="./logs/2" \
RUST_LOG=demo=debug,dengine=debug \
target/release/demo
```

### Node 3
```shell
CONFIG_PATH=config/n3 \
LOG_DIR="./logs/3" \
RUST_LOG=demo=debug,dengine=debug \
target/release/demo
```

## Prerequisites

### 1. Ensure you've built the executable first:

```shell
cargo build --release
```

### 2. Verify the configuration files exist in:
- config/n1
- config/n2
- config/n3


## Notes
Each node must run in a separate terminal session.

Log levels are configured to:
- info for application logs

The cluster nodes will communicate using the addresses specified in their respective config files