# Makefile for embedded-bench
# Provides benchmark commands matching embedded-bench/reports/v0.2.0/report_v0.2.0_final.md

.PHONY: help build clean clean-log-db test-single-write test-high-conc-write test-linearizable-read test-lease-read test-eventual-read test-hot-key all-tests \
        all-ram-tests ramdisk-create clean-ram-log-db ramdisk-release

# ===============================
# Global Variables
# ===============================
BENCH_BIN := ./target/release/embedded-bench
CONFIG_DIR := ./config
LOG_LEVEL ?= warn

# Node selection (default: n1)
NODE ?= n1
CONFIG_PATH := $(CONFIG_DIR)/$(NODE).toml
SINGLE_NODE_CONFIG_PATH := $(CONFIG_DIR)/single_node.toml

# Metrics port per node — same scheme as examples/three-nodes-standalone
# (8081/8082/8083), reused because the two setups never run concurrently.
METRICS_PORT := $(if $(filter n2,$(NODE)),8082,$(if $(filter n3,$(NODE)),8083,8081))

# Common parameters (matching Standalone tests)
KEY_SIZE := 8
VALUE_SIZE := 256
SEQUENTIAL := --sequential-keys

# Concurrent client tasks (embedded-bench is in-process — no --conns knob exists
# in the binary, only --clients). Override: make test-high-conc-write CLIENTS=64
CLIENTS ?= 1000

# Optional write verification (VERIFY=true to enable)
VERIFY ?= false
VERIFY_FLAG := $(if $(filter true,$(VERIFY)),--verify-write,)

# RAM disk (macOS only, optional) — isolates this node's storage on its own
# independent RAM disk volume, to strip physical-disk latency out of a
# benchmark. Not a general fix for unrelated flakiness — see
# tickets/milestones/v0.2.5/446-perf-batching-measurement-2026-09-13.md.
# One-shot: `make all-ram-tests`. Manual/single-node: add RAMDISK=true to
# any test-* target (needs the other two nodes already running for quorum).
RAMDISK ?= false
RAMDISK_SIZE_MB ?= 1024
RAMDISK_SECTORS := $(shell echo $$(( $(RAMDISK_SIZE_MB) * 2048 )))
RAMDISK_VOLUMES := RAMDisk1 RAMDisk2 RAMDisk3
RAMDISK_INDEX := $(if $(filter n2,$(NODE)),2,$(if $(filter n3,$(NODE)),3,1))

ifeq ($(RAMDISK),true)
DATA_DIR := /Volumes/RAMDisk$(RAMDISK_INDEX)/$(NODE)
SINGLE_NODE_DATA_DIR := /Volumes/RAMDisk1/single-node
else
DATA_DIR := ./data/$(NODE)
SINGLE_NODE_DATA_DIR := ./data/single-node
endif

# On macOS with Homebrew: auto-detect compression lib paths to skip bundled C++
# compilation of RocksDB dependencies, which fails under macOS 26 + Xcode 26
# (Clang 16 lacks __builtin_ctzg/__builtin_clzg from LLVM 18+ SDK headers).
# brew --prefix resolves correctly on both Apple Silicon (/opt/homebrew) and
# Intel Mac (/usr/local). Silently no-ops when brew or a lib is absent.
SNAPPY_PREFIX    := $(shell brew --prefix snappy 2>/dev/null)
LZ4_PREFIX       := $(shell brew --prefix lz4    2>/dev/null)
ZSTD_PREFIX      := $(shell brew --prefix zstd   2>/dev/null)
BREW_ROCKSDB_ENV :=

ifneq ($(SNAPPY_PREFIX),)
ifneq ($(wildcard $(SNAPPY_PREFIX)/lib),)
  BREW_ROCKSDB_ENV += SNAPPY_LIB_DIR=$(SNAPPY_PREFIX)/lib
endif
endif
ifneq ($(LZ4_PREFIX),)
  BREW_ROCKSDB_ENV += LZ4_LIB_DIR=$(LZ4_PREFIX)/lib
endif
ifneq ($(ZSTD_PREFIX),)
  BREW_ROCKSDB_ENV += ZSTD_LIB_DIR=$(ZSTD_PREFIX)/lib
endif

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
	@echo "RAM Disk Cluster (macOS only, optional — isolates disk latency):"
	@echo "  make all-ram-tests NODE=nX       Create RAM disks, run full --batch suite on this node (like all-tests)"
	@echo "                                   Run in 3 terminals with NODE=n1/n2/n3 to form the cluster"
	@echo "  make ramdisk-create              Create RAMDisk1/2/3 if not already mounted"
	@echo "  make clean-ram-log-db            Wipe node data off the RAM disks (keeps them mounted)"
	@echo "  make ramdisk-release             Unmount RAMDisk1/2/3, freeing the memory"
	@echo "  Add RAMDISK=true to any single test-* target (needs the other 2 nodes already running)"
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
	$(BREW_ROCKSDB_ENV) cargo build --release --jobs 8

clean:
	@echo "Cleaning build artifacts and data directories..."
	cargo clean
	@echo "Removing data directories..."
	rm -rf ./data/*
	rm -rf ./logs/*
	rm -rf ./snapshots/*
	@echo "Clean complete!"

clean-log-db:
	@echo "Cleaning db and logs..."
	rm -rf ./logs/*
	rm -rf ./data/*
	rm -rf ./snapshots/*

# ============================================
# Write Performance Tests
# ============================================

# Single client write (10K requests)
# Comparable to: embedded-bench --clients 1 --total 10000
test-single-write: build
	@echo "========================================"
	@echo "Single Client Write (10K requests)"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(SINGLE_NODE_CONFIG_PATH) \
	DATA_DIR=$(SINGLE_NODE_DATA_DIR) \
	METRICS_PORT=$(METRICS_PORT) \
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
# Comparable to: embedded-bench --clients $(CLIENTS) --total 100000
test-high-conc-write: build
	@echo "========================================"
	@echo "High Concurrency Write (100K requests)"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(CONFIG_PATH) \
	DATA_DIR=$(DATA_DIR) \
	METRICS_PORT=$(METRICS_PORT) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--total 100000 \
		--clients $(CLIENTS) \
		--key-size $(KEY_SIZE) \
		--value-size $(VALUE_SIZE) \
		$(SEQUENTIAL) \
		$(VERIFY_FLAG) \
		put

# ============================================
# Read Performance Tests
# ============================================

# Linearizable read (100K requests)
# Comparable to: embedded-bench --clients $(CLIENTS) --total 100000 range --consistency l
test-linearizable-read: build
	@echo "========================================"
	@echo "Linearizable Read (100K requests)"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(CONFIG_PATH) \
	DATA_DIR=$(DATA_DIR) \
	METRICS_PORT=$(METRICS_PORT) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--total 100000 \
		--clients $(CLIENTS) \
		--key-size $(KEY_SIZE) \
		--value-size $(VALUE_SIZE) \
		$(SEQUENTIAL) \
		get --consistency l

# Lease-based read (100K requests)
# Comparable to: embedded-bench --clients $(CLIENTS) --total 100000 range --consistency s
test-lease-read: build
	@echo "========================================"
	@echo "Lease-Based Read (100K requests)"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(CONFIG_PATH) \
	DATA_DIR=$(DATA_DIR) \
	METRICS_PORT=$(METRICS_PORT) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--total 100000 \
		--clients $(CLIENTS) \
		--key-size $(KEY_SIZE) \
		--value-size $(VALUE_SIZE) \
		$(SEQUENTIAL) \
		get --consistency s

# Eventual consistency read (100K requests)
# Comparable to: embedded-bench --clients $(CLIENTS) --total 100000 range --consistency e
test-eventual-read: build
	@echo "========================================"
	@echo "Eventual Consistency Read (100K requests)"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(CONFIG_PATH) \
	DATA_DIR=$(DATA_DIR) \
	METRICS_PORT=$(METRICS_PORT) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--total 100000 \
		--clients $(CLIENTS) \
		--key-size $(KEY_SIZE) \
		--value-size $(VALUE_SIZE) \
		$(SEQUENTIAL) \
		get --consistency e

# Hot-key test (100K requests, 10 keys)
# Comparable to: embedded-bench --clients $(CLIENTS) --total 100000 --key-space 10 range --consistency l
test-hot-key: build
	@echo "========================================"
	@echo "Hot-Key Test (100K requests, 10 keys)"
	@echo "Node: $(NODE)"
	@echo "========================================"
	CONFIG_PATH=$(CONFIG_PATH) \
	DATA_DIR=$(DATA_DIR) \
	METRICS_PORT=$(METRICS_PORT) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--total 100000 \
		--clients $(CLIENTS) \
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
	DATA_DIR=$(DATA_DIR) \
	METRICS_PORT=$(METRICS_PORT) \
	RUST_LOG=embedded-bench=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BENCH_BIN) \
		--mode local \
		--batch \
		--clients $(CLIENTS) \
		--key-size $(KEY_SIZE) \
		--value-size $(VALUE_SIZE) \
		$(SEQUENTIAL) \
		put
	@echo ""
	@echo "Compare results with Standalone mode:"

# ============================================
# RAM Disk Cluster (macOS only, optional)
# ============================================
# Isolates each node's storage on its own independent RAM disk volume — use
# this to strip physical-disk latency out of a benchmark (e.g. to study
# fsync/scheduling behavior in isolation), not as a general fix for
# unrelated flakiness. See tickets/milestones/v0.2.5/446-perf-batching-measurement-2026-09-13.md.

# Ensures the RAM disks exist, then runs the full --batch suite for this
# node on its own RAM disk volume — same scope/shape as `make all-tests
# NODE=nX`, just on RAM disk. This is single-node, like all-tests: run it in
# 3 separate terminals with NODE=n1/n2/n3 to form the cluster, e.g.
#   make all-ram-tests NODE=n2 CLIENTS=100
all-ram-tests: ramdisk-create
	$(MAKE) all-tests RAMDISK=true NODE=$(NODE) CLIENTS=$(CLIENTS)

# Idempotent — mounts any of RAMDisk1/2/3 that aren't already present.
ramdisk-create:
	@[ "$$(uname)" = "Darwin" ] || { echo "RAM disk targets need macOS."; exit 1; }
	@for v in $(RAMDISK_VOLUMES); do \
		[ -d "/Volumes/$$v" ] || diskutil erasevolume HFS+ $$v `hdiutil attach -nomount ram://$(RAMDISK_SECTORS)` >/dev/null; \
	done

# Wipes node data off the RAM disks; keeps the volumes mounted.
clean-ram-log-db:
	@for v in $(RAMDISK_VOLUMES); do rm -rf /Volumes/$$v/*; done

# Unmounts the RAM disks entirely, releasing the memory back to the OS.
ramdisk-release:
	@for v in $(RAMDISK_VOLUMES); do diskutil eject /Volumes/$$v 2>/dev/null || true; done
