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
	@echo "Building release binary..."
	$(BREW_ROCKSDB_ENV) cargo build --release --jobs 4

# ===============================
# Cluster Management (Normal Mode)
# ===============================
# Overridable so start-ram-cluster can point these at /Volumes/RAMDiskN
# without duplicating the node targets.
DB_PATH_1 ?= ./db/1
DB_PATH_2 ?= ./db/2
DB_PATH_3 ?= ./db/3

start-node1:
	@echo "🚀 Starting Node 1..."
	@CONFIG_PATH=config/n1 \
	DB_PATH="$(DB_PATH_1)" \
	LOG_DIR="./logs/1" \
	METRICS_PORT=8081 \
	RUST_LOG=demo=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	./target/release/demo 2>&1 | sed 's/^/[Node1] /'

start-node2:
	@echo "🚀 Starting Node 2..."
	@CONFIG_PATH=config/n2 \
	DB_PATH="$(DB_PATH_2)" \
	LOG_DIR="./logs/2" \
	METRICS_PORT=8082 \
	RUST_LOG=demo=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	./target/release/demo 2>&1 | sed 's/^/[Node2] /'

start-node3:
	@echo "🚀 Starting Node 3..."
	@CONFIG_PATH=config/n3 \
	DB_PATH="$(DB_PATH_3)" \
	LOG_DIR="./logs/3" \
	METRICS_PORT=8083 \
	RUST_LOG=demo=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	./target/release/demo 2>&1 | sed 's/^/[Node3] /'


start-cluster:
	@echo "Starting 3-node cluster in parallel..."
	$(MAKE) -j3 start-node1 start-node2 start-node3

# ===============================
# RAM Disk (macOS only, optional)
# ===============================
# Isolates each node's storage on its own independent RAM disk volume — use
# this to strip physical-disk latency out of a benchmark (e.g. to study
# fsync/scheduling behavior in isolation), not as a general fix for
# unrelated flakiness. See tickets/milestones/v0.2.5/446-perf-batching-measurement-2026-09-13.md.
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

# Wipes node data off the RAM disks; keeps the volumes mounted so the next
# start-ram-cluster doesn't pay to recreate them.
clean-ram-log-db:
	@for v in $(RAMDISK_VOLUMES); do rm -rf /Volumes/$$v/*; done

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
	DB_PATH="./db/1" \
	LOG_DIR="./logs/1" \
	METRICS_PORT=8081 \
	RUST_LOG=demo=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	samply record -o ./samply_reports/1.profile.json -P 3001 ./target/release/demo

perf-node2:
	@echo "Profiling Node 2 with samply..."
	CONFIG_PATH=config/n2 \
	DB_PATH="./db/2" \
	LOG_DIR="./logs/2" \
	METRICS_PORT=8082 \
	RUST_LOG=demo=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	samply record -o ./samply_reports/2.profile.json -P 3002 ./target/release/demo

perf-node3:
	@echo "Profiling Node 3 with samply..."
	CONFIG_PATH=config/n3 \
	DB_PATH="./db/3" \
	LOG_DIR="./logs/3" \
	METRICS_PORT=8083 \
	RUST_LOG=demo=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	samply record -o ./samply_reports/3.profile.json -P 3003 ./target/release/demo

perf-cluster: clean build
	@echo "Starting 3-node cluster under samply profiling..."
	$(MAKE) -j3 perf-node1 perf-node2 perf-node3


build-tokio-console:
	$(BREW_ROCKSDB_ENV) RUSTFLAGS="--cfg tokio_unstable" cargo build --release --jobs 4

tokio-console-node1:
	@echo "Starting Node 1 with tokio console monitoring..."
	TOKIO_CONSOLE=1  \
	TOKIO_CONSOLE_PORT=6670 \
	CONFIG_PATH=config/n1 \
	DB_PATH="./db/1" \
	LOG_DIR="./logs/1" \
	METRICS_PORT=8081 \
	RUST_LOG=demo=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	./target/release/demo

tokio-console-node2:
	@echo "Starting Node 2 with tokio console monitoring..."
	TOKIO_CONSOLE=1  \
	TOKIO_CONSOLE_PORT=6671 \
	CONFIG_PATH=config/n2 \
	DB_PATH="./db/2" \
	LOG_DIR="./logs/2" \
	METRICS_PORT=8082 \
	RUST_LOG=demo=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	./target/release/demo

tokio-console-node3:
	@echo "Starting Node 3 with tokio console monitoring..."
	TOKIO_CONSOLE=1 \
	TOKIO_CONSOLE_PORT=6672 \
	CONFIG_PATH=config/n3 \
	DB_PATH="./db/3" \
	LOG_DIR="./logs/3" \
	METRICS_PORT=8083 \
	RUST_LOG=demo=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	./target/release/demo


tokio-console-cluster: clean build-tokio-console
	@echo "Starting 3-node cluster under tokio console monitoring ..."
	$(MAKE) -j3 tokio-console-node1 tokio-console-node2 tokio-console-node3


# ===============================
# Utilities
# ===============================
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	rm -rf logs/*
	rm -rf db/*
	rm -rf snapshots/*
	rm -rf samply_reports/*

clean-log-db:
	@echo "Cleaning db and logs..."
	rm -rf logs/*
	rm -rf db/*

help:
	@echo "DEngine Cluster Management Commands:"
	@echo "  build                           - Build release binary"
	@echo "  start-node1..3                  - Start individual cluster nodes"
	@echo "  start-cluster                   - Start all 3 nodes in parallel"
	@echo "  perf-node1..3                   - Run individual nodes under samply profiler"
	@echo "  perf-cluster                    - Run full 3-node cluster under samply profiling"
	@echo "  tokio-console-cluster           - Run full 3-node cluster under tokio console monitoring"
	@echo "  start-ram-cluster               - (macOS) Clean+create RAM disks, start cluster on them"
	@echo "  ramdisk-create                  - (macOS) Create RAMDisk1/2/3 if not already mounted"
	@echo "  clean-ram-log-db                - Wipe node data off the RAM disks (keeps them mounted)"
	@echo "  ramdisk-release                 - Unmount RAMDisk1/2/3, freeing the memory"
	@echo "  clean                           - Remove build artifacts, logs, and profiles"
	@echo "  clean-log-db                    - Remove only logs and database files"
	@echo "  help                            - Show this help message"
