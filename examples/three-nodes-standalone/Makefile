# Makefile for DEngine cluster operations

.PHONY: build start-cluster clean clean-log-db help \
        start-node1 start-node2 start-node3 \
        perf-node1 perf-node2 perf-node3 perf-cluster \
        tokio-console-node1 tokio-console-node2 tokio-console-node3 tokio-console-cluster \
        start-ram-cluster ramdisk-create clean-ram-log-db ramdisk-release
.DEFAULT_GOAL := help

# ===============================
# Global Variables
# ===============================
LOG_LEVEL ?= warn

# Storage engine. Default: Raft log in raft-engine (experimental).
# ENGINE=rocksdb builds and runs the stock RocksDB-WAL binary. Each engine has its own
# target dir and its own data dir, so switching never mixes their files.
ENGINE ?= raft-engine
$(if $(filter raft-engine rocksdb,$(ENGINE)),,$(error ENGINE must be raft-engine or rocksdb, got '$(ENGINE)'))
TARGET_DIR := $(if $(filter raft-engine,$(ENGINE)),target-raft-engine,target)
CARGO_FEATURES := $(if $(filter raft-engine,$(ENGINE)),--features raft-engine,)
DB_ROOT := $(if $(filter raft-engine,$(ENGINE)),./db-raft-engine,./db)

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

# ===============================
# Build Targets
# ===============================
build:
	@echo "Building release binary (engine: $(ENGINE))..."
	$(BREW_ROCKSDB_ENV) cargo build --release --jobs 4 $(CARGO_FEATURES) --target-dir $(TARGET_DIR)

# ===============================
# Cluster Management (Normal Mode)
# ===============================
# Overridable so start-ram-cluster can point these at /Volumes/RAMDiskN
# without duplicating the node targets.
BIN ?= ./$(TARGET_DIR)/release/three-nodes-standalone
DB_PATH_1 ?= $(DB_ROOT)/1
DB_PATH_2 ?= $(DB_ROOT)/2
DB_PATH_3 ?= $(DB_ROOT)/3

start-node1:
	@echo "🚀 Starting Node 1..."
	@CONFIG_PATH=config/n1 \
	DB_PATH="$(DB_PATH_1)" \
	LOG_DIR="./logs/1" \
	METRICS_PORT=8081 \
	RUST_LOG=three_nodes_standalone=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	$(BIN) 2>&1 | sed 's/^/[Node1] /'

start-node2:
	@echo "🚀 Starting Node 2..."
	@CONFIG_PATH=config/n2 \
	DB_PATH="$(DB_PATH_2)" \
	LOG_DIR="./logs/2" \
	METRICS_PORT=8082 \
	RUST_LOG=three_nodes_standalone=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BIN) 2>&1 | sed 's/^/[Node2] /'

start-node3:
	@echo "🚀 Starting Node 3..."
	@CONFIG_PATH=config/n3 \
	DB_PATH="$(DB_PATH_3)" \
	LOG_DIR="./logs/3" \
	METRICS_PORT=8083 \
	RUST_LOG=three_nodes_standalone=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BIN) 2>&1 | sed 's/^/[Node3] /'


start-cluster:
	@echo "Starting 3-node cluster in parallel..."
	$(MAKE) -j3 start-node1 start-node2 start-node3

# ===============================
# RAM Disk (macOS only, optional)
# ===============================
# Isolates each node's storage on its own independent RAM disk volume — use
# this to strip physical-disk latency out of a benchmark (e.g. to study
# fsync/scheduling behavior in isolation), not as a general fix for
# unrelated flakiness.
RAMDISK_SIZE_MB ?= 1024
RAMDISK_SECTORS := $(shell echo $$(( $(RAMDISK_SIZE_MB) * 2048 )))
RAMDISK_VOLUMES := RAMDisk1 RAMDisk2 RAMDisk3

# One command: wipe stale node data, ensure the 3 RAM disks exist, start the
# cluster on them instead of ./db.
start-ram-cluster: clean-ram-log-db ramdisk-create
	@echo "Starting 3-node cluster on RAM disk..."
	$(MAKE) -j3 start-node1 start-node2 start-node3 \
		DB_PATH_1=/Volumes/RAMDisk1/n1 DB_PATH_2=/Volumes/RAMDisk2/n2 DB_PATH_3=/Volumes/RAMDisk3/n3

# Idempotent — mounts any of RAMDisk1/2/3 that aren't already present.
ramdisk-create:
	@[ "$$(uname)" = "Darwin" ] || { echo "RAM disk targets need macOS."; exit 1; }
	@for v in $(RAMDISK_VOLUMES); do \
		[ -d "/Volumes/$$v" ] || diskutil erasevolume HFS+ $$v `hdiutil attach -nomount ram://$(RAMDISK_SECTORS)` >/dev/null; \
	done

# Wipes this example's node directories off the RAM disks; keeps the volumes mounted so the
# next start-ram-cluster doesn't pay to recreate them. Nothing else on the volumes is touched.
clean-ram-log-db:
	@rm -rf /Volumes/RAMDisk1/n1 /Volumes/RAMDisk2/n2 /Volumes/RAMDisk3/n3

# Unmounts the RAM disks entirely, releasing the memory back to the OS.
ramdisk-release:
	@for v in $(RAMDISK_VOLUMES); do diskutil eject /Volumes/$$v 2>/dev/null || true; done


# ===============================
# Performance Profiling with Samply
# ===============================
# Each target runs a node under `samply` profiler and outputs a .json profile

perf-node1:
	@echo "Profiling Node 1 with samply..."
	CONFIG_PATH=config/n1 \
	DB_PATH="$(DB_PATH_1)" \
	LOG_DIR="./logs/1" \
	METRICS_PORT=8081 \
	RUST_LOG=three_nodes_standalone=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	samply record -o ./samply_reports/1.profile.json -P 3001 $(BIN)

perf-node2:
	@echo "Profiling Node 2 with samply..."
	CONFIG_PATH=config/n2 \
	DB_PATH="$(DB_PATH_2)" \
	LOG_DIR="./logs/2" \
	METRICS_PORT=8082 \
	RUST_LOG=three_nodes_standalone=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	samply record -o ./samply_reports/2.profile.json -P 3002 $(BIN)

perf-node3:
	@echo "Profiling Node 3 with samply..."
	CONFIG_PATH=config/n3 \
	DB_PATH="$(DB_PATH_3)" \
	LOG_DIR="./logs/3" \
	METRICS_PORT=8083 \
	RUST_LOG=three_nodes_standalone=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	samply record -o ./samply_reports/3.profile.json -P 3003 $(BIN)

perf-cluster: clean-log-db build
	@echo "Starting 3-node cluster under samply profiling..."
	$(MAKE) -j3 perf-node1 perf-node2 perf-node3


build-tokio-console:
	$(BREW_ROCKSDB_ENV) RUSTFLAGS="--cfg tokio_unstable" cargo build --release --jobs 4 $(CARGO_FEATURES) --target-dir $(TARGET_DIR)

tokio-console-node1:
	@echo "Starting Node 1 with tokio console monitoring..."
	TOKIO_CONSOLE=1  \
	TOKIO_CONSOLE_PORT=6670 \
	CONFIG_PATH=config/n1 \
	DB_PATH="$(DB_PATH_1)" \
	LOG_DIR="./logs/1" \
	METRICS_PORT=8081 \
	RUST_LOG=three_nodes_standalone=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	$(BIN)

tokio-console-node2:
	@echo "Starting Node 2 with tokio console monitoring..."
	TOKIO_CONSOLE=1  \
	TOKIO_CONSOLE_PORT=6671 \
	CONFIG_PATH=config/n2 \
	DB_PATH="$(DB_PATH_2)" \
	LOG_DIR="./logs/2" \
	METRICS_PORT=8082 \
	RUST_LOG=three_nodes_standalone=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	$(BIN)

tokio-console-node3:
	@echo "Starting Node 3 with tokio console monitoring..."
	TOKIO_CONSOLE=1 \
	TOKIO_CONSOLE_PORT=6672 \
	CONFIG_PATH=config/n3 \
	DB_PATH="$(DB_PATH_3)" \
	LOG_DIR="./logs/3" \
	METRICS_PORT=8083 \
	RUST_LOG=three_nodes_standalone=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	$(BIN)


tokio-console-cluster: clean-log-db build-tokio-console
	@echo "Starting 3-node cluster under tokio console monitoring ..."
	$(MAKE) -j3 tokio-console-node1 tokio-console-node2 tokio-console-node3


# ===============================
# Utilities
# ===============================
# Removes both engines' build output and local data. RAM disk data is wiped separately with
# `make clean-ram-log-db`.
clean:
	@echo "Cleaning build artifacts..."
	cargo clean --target-dir target
	cargo clean --target-dir target-raft-engine
	rm -rf logs/*
	rm -rf db/*
	rm -rf db-raft-engine/*
	rm -rf snapshots/*
	rm -rf samply_reports/*

clean-log-db:
	@echo "Cleaning db and logs..."
	rm -rf logs/*
	rm -rf db/*
	rm -rf db-raft-engine/*

help:
	@echo "DEngine Cluster Management Commands:"
	@echo "  build                           - Build release binary"
	@echo "  start-node1..3                  - Start individual cluster nodes"
	@echo "  start-cluster                   - Start all 3 nodes in parallel"
	@echo "  ENGINE=rocksdb                  - Any target: use the stock RocksDB-WAL build (default: raft-engine, experimental)"
	@echo "  perf-node1..3                   - Run individual nodes under samply profiler"
	@echo "  perf-cluster                    - Run full 3-node cluster under samply profiling"
	@echo "  tokio-console-cluster           - Run full 3-node cluster under tokio console monitoring"
	@echo "  start-ram-cluster               - (macOS) Clean+create RAM disks, start cluster on them"
	@echo "  ramdisk-create                  - (macOS) Create RAMDisk1/2/3 if not already mounted"
	@echo "  clean-ram-log-db                - Wipe node data off the RAM disks (keeps them mounted)"
	@echo "  ramdisk-release                 - Unmount RAMDisk1/2/3, freeing the memory"
	@echo "  clean                           - Remove both engines' builds, data, logs and profiles (RAM disks: use clean-ram-log-db)"
	@echo "  clean-log-db                    - Remove only logs and database files"
	@echo "  help                            - Show this help message"
