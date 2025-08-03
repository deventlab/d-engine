# Makefile for DEngine cluster operations

.PHONY: build start-cluster clean clean-log-db help \
        start-node1 start-node2 start-node3 join-node4 join-node5 \
        perf-node1 perf-node2 perf-node3 perf-cluster
.DEFAULT_GOAL := help

# ===============================
# Build Targets
# ===============================
build:
	@echo "Building release binary..."
	cargo build --release --jobs 4

# ===============================
# Cluster Management (Normal Mode)
# ===============================
start-node1:
	@echo "Starting Node 1..."
	CONFIG_PATH=config/n1 \
	LOG_DIR="./logs/1" \
	RUST_LOG=demo=warn,d_engine=warn,timing=warn \
	RUST_BACKTRACE=1 \
	./target/release/demo

start-node2:
	@echo "Starting Node 2..."
	CONFIG_PATH=config/n2 \
	LOG_DIR="./logs/2" \
	RUST_LOG=demo=warn,d_engine=warn,timing=warn \
	./target/release/demo

start-node3:
	@echo "Starting Node 3..."
	CONFIG_PATH=config/n3 \
	LOG_DIR="./logs/3" \
	RUST_LOG=demo=warn,d_engine=warn,timing=warn \
	./target/release/demo

join-node4:
	@echo "Joining Learner 4..."
	CONFIG_PATH=config/n4 \
	LOG_DIR="./logs/4" \
	RUST_LOG=demo=warn,d_engine=warn,timing=warn \
	./target/release/demo

join-node5:
	@echo "Joining Learner 5..."
	CONFIG_PATH=config/n5 \
	LOG_DIR="./logs/5" \
	RUST_LOG=demo=warn,d_engine=warn,timing=warn \
	./target/release/demo

start-cluster: clean build
	@echo "Starting 3-node cluster in parallel..."
	$(MAKE) -j3 start-node1 start-node2 start-node3


# ===============================
# Performance Profiling with Samply
# ===============================
# Each target runs a node under `samply` profiler and outputs a .json profile

perf-node1:
	@echo "Profiling Node 1 with samply..."
	CONFIG_PATH=config/n1 \
	LOG_DIR="./logs/1" \
	RUST_LOG=demo=warn,d_engine=warn,timing=warn \
	RUST_BACKTRACE=1 \
	samply record -o ./samply_reports/1.profile.json -P 3001 ./target/release/demo

perf-node2:
	@echo "Profiling Node 2 with samply..."
	CONFIG_PATH=config/n2 \
	LOG_DIR="./logs/2" \
	RUST_LOG=demo=warn,d_engine=warn,timing=warn \
	samply record -o ./samply_reports/2.profile.json -P 3002 ./target/release/demo

perf-node3:
	@echo "Profiling Node 3 with samply..."
	CONFIG_PATH=config/n3 \
	LOG_DIR="./logs/3" \
	RUST_LOG=demo=warn,d_engine=warn,timing=warn \
	samply record -o ./samply_reports/3.profile.json -P 3003 ./target/release/demo

perf-cluster: clean build
	@echo "Starting 3-node cluster under samply profiling..."
	$(MAKE) -j3 perf-node1 perf-node2 perf-node3

# ===============================
# Utilities
# ===============================
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	rm -rf logs/
	rm -rf db/
	rm -rf samply_reports/

clean-log-db:
	@echo "Cleaning build artifacts..."
	rm -rf logs/
	rm -rf db/

help:
	@echo "DEngine Cluster Management Commands:"
	@echo "  build            - Build release binary"
	@echo "  start-node1..3   - Start individual cluster nodes"
	@echo "  join-node4..5    - Join learner nodes to cluster"
	@echo "  start-cluster    - Start all 3 nodes in parallel"
	@echo "  perf-node1..3    - Run individual nodes under samply profiler"
	@echo "  perf-cluster     - Run full 3-node cluster under samply profiling"
	@echo "  clean            - Remove build artifacts, logs, and profiles"
	@echo "  clean-log-db     - Remove only logs and database files"
	@echo "  help             - Show this help message"
