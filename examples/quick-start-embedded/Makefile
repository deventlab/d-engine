# d-engine embedded mode: quick-start example
#
# Single-node only. For multi-node cluster, see docs/scale-to-cluster.md

.PHONY: help build run clean

help:
	@echo "d-engine quick-start (single-node embedded mode)"
	@echo ""
	@echo "Commands:"
	@echo "  make build    - Build release binary"
	@echo "  make run      - Start embedded d-engine (default: WARN level)"
	@echo "  make clean    - Remove build artifacts and data"
	@echo ""
	@echo "Log Control:"
	@echo "  RUST_LOG=info make run       - Show info, warn, error"
	@echo "  RUST_LOG=debug make run      - Show debug and above"
	@echo "  RUST_LOG=d_engine=trace make run - Trace d-engine only"

build:
	cargo build --release

run: build
	@mkdir -p data/single-node
	@./target/release/quick-start

clean:
	rm -rf target/ data/ *.log
