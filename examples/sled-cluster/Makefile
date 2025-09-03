# Makefile for DEngine cluster operations

.PHONY: build start-cluster clean clean-log-db help \
        start-node1 start-node2 start-node3 \
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
	NODE_ID=1 \
	DB_PATH="./db/1" \
	LOG_DIR="./logs/1" \
	METRICS_PORT=8081 \
	RUST_LOG=sled_demo=warn,d_engine=warn,timing=warn \
	RUST_BACKTRACE=1 \
	./target/release/sled-demo

start-node2:
	@echo "Starting Node 2..."
	CONFIG_PATH=config/n2 \
	NODE_ID=2 \
	DB_PATH="./db/2" \
	LOG_DIR="./logs/2" \
	METRICS_PORT=8082 \
	RUST_LOG=sled_demo=warn,d_engine=warn,timing=warn \
	./target/release/sled-demo

start-node3:
	@echo "Starting Node 3..."
	CONFIG_PATH=config/n3 \
	NODE_ID=3 \
	DB_PATH="./db/3" \
	LOG_DIR="./logs/3" \
	METRICS_PORT=8083 \
	RUST_LOG=sled_demo=warn,d_engine=warn,timing=warn \
	./target/release/sled-demo

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
	NODE_ID=1 \
	DB_PATH="./db/1" \
	LOG_DIR="./logs/1" \
	METRICS_PORT=8081 \
	RUST_LOG=sled_demo=warn,d_engine=warn,timing=warn \
	RUST_BACKTRACE=1 \
	samply record -o ./samply_reports/1.profile.json -P 3001 ./target/release/sled-demo

perf-node2:
	@echo "Profiling Node 2 with samply..."
	CONFIG_PATH=config/n2 \
	NODE_ID=2 \
	DB_PATH="./db/2" \
	LOG_DIR="./logs/2" \
	METRICS_PORT=8082 \
	RUST_LOG=sled_demo=warn,d_engine=warn,timing=warn \
	samply record -o ./samply_reports/2.profile.json -P 3002 ./target/release/sled-demo

perf-node3:
	@echo "Profiling Node 3 with samply..."
	CONFIG_PATH=config/n3 \
	NODE_ID=3 \
	DB_PATH="./db/3" \
	LOG_DIR="./logs/3" \
	METRICS_PORT=8083 \
	RUST_LOG=sled_demo=warn,d_engine=warn,timing=warn \
	samply record -o ./samply_reports/3.profile.json -P 3003 ./target/release/sled-demo

perf-cluster: clean build
	@echo "Starting 3-node cluster under samply profiling..."
	$(MAKE) -j3 perf-node1 perf-node2 perf-node3

# ===============================
# Utilities
# ===============================
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	rm -rf logs/*
	rm -rf db/*
	rm -rf samply_reports/*

clean-log-db:
	@echo "Cleaning build artifacts..."
	rm -rf logs/*
	rm -rf db/*

help:
	@echo "DEngine Cluster Management Commands:"
	@echo "  build            - Build release binary"
	@echo "  start-node1..3   - Start individual cluster nodes"
	@echo "  start-cluster    - Start all 3 nodes in parallel"
	@echo "  perf-node1..3    - Run individual nodes under samply profiler"
	@echo "  perf-cluster     - Run full 3-node cluster under samply profiling"
	@echo "  clean            - Remove build artifacts, logs, and profiles"
	@echo "  clean-log-db     - Remove only logs and database files"
	@echo "  help             - Show this help message"
