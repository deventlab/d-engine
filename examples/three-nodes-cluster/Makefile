# Makefile for DEngine cluster operations

.PHONY: build start-cluster clean help
.DEFAULT_GOAL := help

# Build targets
build:
	@echo "Building release binary..."
	cargo build --release --jobs 4

# Cluster management
start-node1:
	@echo "Starting Node 1..."
	CONFIG_PATH=config/n1 \
	LOG_DIR="./logs/1" \
	RUST_LOG=demo=warn,d_engine=debug,timing=debug \
	RUST_BACKTRACE=1 \
	./target/release/demo

start-node2:
	@echo "Starting Node 2..."
	CONFIG_PATH=config/n2 \
	LOG_DIR="./logs/2" \
	RUST_LOG=demo=warn,d_engine=debug,timing=debug \
	./target/release/demo

start-node3:
	@echo "Starting Node 3..."
	CONFIG_PATH=config/n3 \
	LOG_DIR="./logs/3" \
	RUST_LOG=demo=warn,d_engine=debug,timing=debug \
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

# Utilities
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	rm -rf logs/
	rm -rf db/

clean-log-db:
	@echo "Cleaning build artifacts..."
	rm -rf logs/
	rm -rf db/

help:
	@echo "DEngine Cluster Management Commands:"
	@echo "  build         - Build release binary"
	@echo "  start-node1   - Start node 1"
	@echo "  start-node2   - Start node 2"
	@echo "  start-node3   - Start node 3"
	@echo "  join-node4    - Join learner 4"
	@echo "  join-node5    - Join learner 5"
	@echo "  start-cluster - Start all 3 nodes in parallel"
	@echo "  clean         - Remove build artifacts and logs"
	@echo "  help          - Show this help message"
