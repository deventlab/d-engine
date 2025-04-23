# Release Management Makefile
.PHONY: all pre-release check-env install clean help

# Configuration
SCRIPT_PATH := scripts/pre-release-checklist.sh
CARGO_TOOLCHAIN := 1.84.0  # Update to your toolchain
CRATE_NAME := d_engine          # Update to your crate name


## Show this help
help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

all: pre-release

## Run all pre-release checks
pre-release: check-env install ## Run all pre-release checks
	@echo "\n\033[1;33m=== Starting Release Preparation ===\033[0m"
	@$(SCRIPT_PATH) || (echo "\033[0;31mPre-release checks failed!\033[0m" && exit 1)
	@echo "\n\033[1;32m✓ All checks passed. Ready for release!\033[0m"

## Verify environment prerequisites
check-env: ## Verify environment
	@command -v cargo >/dev/null 2>&1 || \
		(echo "\033[0;31mError: cargo not found!\033[0m" && exit 1)
	@rustup toolchain list | grep -q $(CARGO_TOOLCHAIN) || \
		(echo "\033[0;31mError: Required toolchain $(CARGO_TOOLCHAIN) missing!\033[0m" && exit 1)

## Install prerequisites
install: ## Install prerequisites
	@chmod +x $(SCRIPT_PATH)
	@rustup component add \
		clippy \
		rustfmt \
		rust-docs

## Clean build artifacts
clean: ## Clean build artifacts
	@cargo clean
	@rm -rf target/ coverage/ *.log

# Benchmark Targets (Optional)
bench: ## Run performance benchmarks
	@cargo bench --features unstable

# Documentation Targets
doc: ## Generate documentation
	@cargo doc --no-deps --open

# Format Verification
fmt-check: ## Check formatting
	@cargo fmt --all -- --check

# Run Clippy
clippy: ## Run Clippy
	@cargo clippy --all-targets --all-features -- -D warnings

# Test suite 
test: ## Run test suite
	@cargo test --lib -- --nocapture
	@cargo test --tests -- --nocapture
	@cargo test --doc
	@cargo bench -- --nocapture