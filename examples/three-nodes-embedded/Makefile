# Makefile for DEngine cluster operations

.PHONY: build start-cluster clean help \
        start-node1 start-node2 start-node3
.DEFAULT_GOAL := help

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
	cargo build --release --jobs 4

# ===============================
# Cluster Management (Normal Mode)
# ===============================
start-node1:
	@echo "🚀 Starting Node 1..."
	RUST_LOG=three-nodes-embedded=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	$(BIN)  \
		--port 8081 \
		--health-port 10001 \
		--config-path "config/n1" \
		2>&1 | sed 's/^/[Node1] /'

start-node2:
	@echo "🚀 Starting Node 2..."
	@CONFIG_PATH=config/n2 \
	RUST_LOG=three-nodes-embedded=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BIN) \
		--port 8082 \
		--health-port 10002 \
		--config-path "config/n2.toml" \
		2>&1 | sed 's/^/[Node2] /'

start-node3:
	@echo "🚀 Starting Node 3..."
	@CONFIG_PATH=config/n3 \
	RUST_LOG=three-nodes-embedded=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	$(BIN) \
		--port 8083 \
		--health-port 10003 \
		--config-path "config/n3.toml" \
		2>&1 | sed 's/^/[Node3] /'


start-cluster: 	 build
	@echo "Starting 3-node cluster in parallel..."
	$(MAKE) -j3 start-node1 start-node2 start-node3


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


help:
	@echo "DEngine Cluster Management Commands:"
	@echo "  build                           - Build release binary"
	@echo "  start-node1..3                  - Start individual cluster nodes"
	@echo "  start-cluster                   - Start all 3 nodes in parallel"
	@echo "  clean                           - Remove build artifacts, logs, and profiles"
	@echo "  help                            - Show this help message"
