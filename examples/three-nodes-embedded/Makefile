# Makefile for DEngine cluster operations

.PHONY: build start-cluster clean help \
        start-node1 start-node2 start-node3
.DEFAULT_GOAL := help


# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Cargo executable
CARGO ?= cargo

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
# Applied directly in recipes below (not folded into CARGO): a command-line
# `make CARGO=... build` override would otherwise silently skip these vars.

# ===============================
# Global Variables
# ===============================
LOG_LEVEL ?= warn
BIN := ./target/release/three-nodes-embedded
# ===============================
# Build Targets
# ===============================
build:
	@echo "Building release binary..."
	$(BREW_ROCKSDB_ENV) $(CARGO) build --release --jobs 4

# ===============================
# Cluster Management (Normal Mode)
# ===============================
start-node1:
	@echo "🚀 Starting Node 1..."
	@trap 'echo "[Node1] Received shutdown signal"; kill -INT $$PID 2>/dev/null; wait $$PID' INT TERM; \
	RUST_LOG=three-nodes-embedded=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	$(BIN)  \
		--port 8081 \
		--health-port 10001 \
		--config-path "config/n1.toml" \
		--data-dir "db/1" \
		2>&1 | sed 's/^/[Node1] /' & \
	PID=$$!; \
	wait $$PID; \
	EXIT_CODE=$$?; \
	exit $$EXIT_CODE

start-node2:
	@echo "🚀 Starting Node 2..."
	@trap 'echo "[Node2] Received shutdown signal"; kill -INT $$PID 2>/dev/null; wait $$PID' INT TERM; \
	RUST_LOG=three-nodes-embedded=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	$(BIN) \
		--port 8082 \
		--health-port 10002 \
		--config-path "config/n2.toml" \
		--data-dir "db/2" \
		2>&1 | sed 's/^/[Node2] /' & \
	PID=$$!; \
	wait $$PID; \
	EXIT_CODE=$$?; \
	exit $$EXIT_CODE

start-node3:
	@echo "🚀 Starting Node 3..."
	@trap 'echo "[Node3] Received shutdown signal"; kill -INT $$PID 2>/dev/null; wait $$PID' INT TERM; \
	RUST_LOG=three-nodes-embedded=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	$(BIN) \
		--port 8083 \
		--health-port 10003 \
		--config-path "config/n3.toml" \
		--data-dir "db/3" \
		2>&1 | sed 's/^/[Node3] /' & \
	PID=$$!; \
	wait $$PID; \
	EXIT_CODE=$$?; \
	exit $$EXIT_CODE


start-cluster: 	 build
	@echo "Starting 3-node cluster in parallel..."
	$(MAKE) -j3 start-node1 start-node2 start-node3


# ===============================
# Utilities
# ===============================
clean:
	@echo "Cleaning build artifacts..."
	$(BREW_ROCKSDB_ENV) $(CARGO) clean
	rm -rf logs/*
	rm -rf db/*
	rm -rf snapshots/*
	rm -rf samply_reports/*


help:
	@echo "DEngine Cluster Management Commands:"
	@echo "  build                           - Build release binary"
	@echo "  start-node1..3                  - Start individual cluster nodes"
	@echo "  start-cluster                   - Start all 3 nodes in parallel"
	@echo "  clean                           - Remove build artifacts, logs, and profiles"
	@echo "  help                            - Show this help message"
