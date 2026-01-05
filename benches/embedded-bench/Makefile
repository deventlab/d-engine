# Makefile for embedded-bench
# Provides benchmark commands matching d-engine-bench/reports/v0.2.0/report_v0.2.0_final.md

.PHONY: help build clean test-single-write test-high-conc-write test-linearizable-read test-lease-read test-eventual-read test-hot-key all-tests

BENCH_BIN := ./target/release/embedded-bench
CONFIG_DIR := ./config

# Node selection (default: n1)
NODE ?= n1
CONFIG_PATH := $(CONFIG_DIR)/$(NODE).toml
SINGLE_NODE_CONFIG_PATH := $(CONFIG_DIR)/single_node.toml

# ===============================
# Global Variables
# ===============================
LOG_LEVEL ?= warn

# Common parameters (matching Standalone tests)
KEY_SIZE := 8
VALUE_SIZE := 256
SEQUENTIAL := --sequential-keys

# Optional write verification (VERIFY=true to enable)
VERIFY ?= false
VERIFY_FLAG := $(if $(filter true,$(VERIFY)),--verify-write,)

help:
	@echo "Embedded-bench Makefile - Performance Testing"
	@echo ""
	@echo "Build:"
	@echo "  make build              Build release binary"
	@echo "  make clean              Clean build artifacts"
	@echo ""
	@echo "Write Tests (only run on Leader - default: Node 1):"
	@echo "  make test-single-write        Single client write (10K requests)"
	@echo "  make test-high-conc-write     High concurrency write (100K requests)"
	@echo ""
	@echo "Write Verification:"
	@echo "  Add VERIFY=true to verify writes (reads back after each write)"
	@echo "  Example: make test-high-conc-write VERIFY=true"
	@echo ""
	@echo "Read Tests (can run on any node - specify with NODE=n1|n2|n3):"
	@echo "  make test-linearizable-read [NODE=n1]   Linearizable read (100K requests)"
	@echo "  make test-lease-read [NODE=n1]          Lease-based read (100K requests)"
	@echo "  make test-eventual-read [NODE=n1]       Eventual consistency read (100K requests)"
	@echo "  make test-hot-key [NODE=n1]             Hot-key test (100K requests, 10 keys)"
	@echo ""
	@echo "Run All:"
	@echo "  make all-tests          Run all benchmark tests"
	@echo ""
	@echo "Examples:"
	@echo "  make test-linearizable-read              # Run on n1 (default)"
	@echo "  make test-linearizable-read NODE=n2      # Run on n2"
	@echo "  make test-linearizable-read NODE=n3      # Run on n3"
	@echo ""
	@echo "Note: Ensure 3-node cluster is running before testing"
	@echo "  cd ./embedded-bench && make start-cluster"

build:
	@echo "Building embedded-bench..."
	cargo build --release --jobs 8

clean:
	@echo "Cleaning build artifacts and data directories..."
	cargo clean
	@echo "Removing data directories..."
	rm -rf ./data/*
	rm -rf ./logs/*
	rm -rf ./snapshots/*
	@echo "Clean complete!"

# ============================================
# Write Performance Tests
# ============================================

# Single client write (10K requests)
# Comparable to: d-engine-bench --conns 1 --clients 1 --total 10000
test-single-write: build
	@echo "========================================"
	@echo "Single Client Write (10K requests)"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(SINGLE_NODE_CONFIG_PATH) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--total 10000 \
		--clients 1 \
		--key-size $(KEY_SIZE) \
		--value-size $(VALUE_SIZE) \
		$(SEQUENTIAL) \
		$(VERIFY_FLAG) \
		put

# High concurrency write (100K requests)
# Comparable to: d-engine-bench --conns 200 --clients 1000 --total 100000
test-high-conc-write: build
	@echo "========================================"
	@echo "High Concurrency Write (100K requests)"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(CONFIG_PATH) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--total 100000 \
		--clients 1000 \
		--key-size $(KEY_SIZE) \
		--value-size $(VALUE_SIZE) \
		$(SEQUENTIAL) \
		$(VERIFY_FLAG) \
		put

# ============================================
# Read Performance Tests
# ============================================

# Linearizable read (100K requests)
# Comparable to: d-engine-bench --conns 200 --clients 1000 --total 100000 range --consistency l
test-linearizable-read: build
	@echo "========================================"
	@echo "Linearizable Read (100K requests)"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(CONFIG_PATH) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--total 100000 \
		--clients 1000 \
		--key-size $(KEY_SIZE) \
		--value-size $(VALUE_SIZE) \
		$(SEQUENTIAL) \
		get --consistency l

# Lease-based read (100K requests)
# Comparable to: d-engine-bench --conns 200 --clients 1000 --total 100000 range --consistency s
test-lease-read: build
	@echo "========================================"
	@echo "Lease-Based Read (100K requests)"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(CONFIG_PATH) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--total 100000 \
		--clients 1000 \
		--key-size $(KEY_SIZE) \
		--value-size $(VALUE_SIZE) \
		$(SEQUENTIAL) \
		get --consistency s

# Eventual consistency read (100K requests)
# Comparable to: d-engine-bench --conns 200 --clients 1000 --total 100000 range --consistency e
test-eventual-read: build
	@echo "========================================"
	@echo "Eventual Consistency Read (100K requests)"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(CONFIG_PATH) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--total 100000 \
		--clients 1000 \
		--key-size $(KEY_SIZE) \
		--value-size $(VALUE_SIZE) \
		$(SEQUENTIAL) \
		get --consistency e

# Hot-key test (100K requests, 10 keys)
# Comparable to: d-engine-bench --conns 200 --clients 1000 --total 100000 --key-space 10 range --consistency l
test-hot-key: build
	@echo "========================================"
	@echo "Hot-Key Test (100K requests, 10 keys)"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(CONFIG_PATH) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--total 100000 \
		--clients 1000 \
		--key-size $(KEY_SIZE) \
		--key-space 10 \
		$(SEQUENTIAL) \
		get --consistency l

# ============================================
# Run All Tests
# ============================================

all-tests: build
	@echo "========================================"
	@echo "Running All Tests in Batch Mode"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(CONFIG_PATH) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--batch \
		--key-size $(KEY_SIZE) \
		--value-size $(VALUE_SIZE) \
		$(SEQUENTIAL) \
		put
	@echo ""
	@echo "Compare results with Standalone mode:"
	@echo "  Standalone report: ../../benches/d-engine-bench/reports/v0.2.0/report_v0.2.0_final.md"
