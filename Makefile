# ============================================================================
# Makefile for d-engine Rust Workspace
# ============================================================================
# This Makefile provides convenient commands for managing a multi-crate
# Rust workspace including: formatting, linting, testing, and release tasks.
#
# Usage: make <target>
# Examples:
#   make help              # Show all available targets
#   make check             # Run all quality checks (fmt, clippy, test)
#   make fmt-fix           # Fix all formatting issues automatically
#   make clippy-fix        # Apply clippy suggestions automatically
#   make test              # Run all tests with logging
#   make pre-release       # Comprehensive pre-release validation
#
# ============================================================================

.PHONY: help all check fmt fmt-check clippy clippy-fix test \
        bench bench-save clean build build-release \
        pre-release install check-env audit deny \
        docs docs-all docs-check

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Cargo executable
CARGO ?= cargo

# Rust logging level for tests
RUST_LOG_LEVEL ?= d_engine_server=debug,d_engine_core=debug,d_engine_client=debug,d_engine=debug

# Backtrace level for debugging
RUST_BACKTRACE ?= 1

# Workspace member crates
WORKSPACE_MEMBERS := d-engine-proto d-engine-core d-engine-client d-engine-server d-engine

# Color codes for formatted output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
BLUE := \033[0;34m
MAGENTA := \033[0;35m
CYAN := \033[0;36m
NC := \033[0m # No Color

# ============================================================================
# HELP TARGET - Display all available commands
# ============================================================================

## help                 Display this help message with all available targets
help:
	@echo "$(BLUE)╔════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║  d-engine Workspace Build System                      ║$(NC)"
	@echo "$(BLUE)╚════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(CYAN)Available Targets:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(CYAN)Common Workflows:$(NC)"
	@echo "  $(YELLOW)Development:$(NC)"
	@echo "    make check          # Run all quality checks before committing"
	@echo "    make fmt-fix        # Auto-fix formatting issues"
	@echo "    make clippy-fix     # Apply clippy suggestions"
	@echo ""
	@echo "  $(YELLOW)Documentation:$(NC)"
	@echo "    make docs           # Build and open documentation in browser"
	@echo "    make docs-check     # Verify docs compile without warnings"
	@echo "    make docs-private   # Build docs with private items visible"
	@echo "    make docs-crate CRATE=name  # Build docs for specific crate"
	@echo ""
	@echo "  $(YELLOW)Testing:$(NC)"
	@echo "    make test           # Run all tests with nextest (fast, parallel)"
	@echo ""
	@echo "  $(YELLOW)Release:$(NC)"
	@echo "    make pre-release    # Full pre-release validation"
	@echo "    make build-release  # Build release artifacts"
	@echo ""
	@echo "  $(YELLOW)Maintenance:$(NC)"
	@echo "    make clean          # Remove all build artifacts"
	@echo "    make docs-clean     # Remove generated documentation"
	@echo "    make audit          # Check for security vulnerabilities"
	@echo ""

# ============================================================================
# ENVIRONMENT VERIFICATION
# ============================================================================

## check-env            Verify Rust environment prerequisites are installed
check-env:
	@echo "$(BLUE)Verifying Rust environment...$(NC)"
	@command -v $(CARGO) >/dev/null 2>&1 || \
		{ echo "$(RED)✗ Error: cargo not found in PATH$(NC)"; exit 1; }
	@command -v rustc >/dev/null 2>&1 || \
		{ echo "$(RED)✗ Error: rustc not found in PATH$(NC)"; exit 1; }
	@command -v rustup >/dev/null 2>&1 || \
		{ echo "$(RED)✗ Error: rustup not found in PATH$(NC)"; exit 1; }
	@echo "$(GREEN)✓ Cargo found: $$($(CARGO) --version)$(NC)"
	@echo "$(GREEN)✓ Rustc found: $$(rustc --version)$(NC)"

## install-tools        Install required Rust components (rustfmt, clippy, nextest)
install-tools: check-env
	@echo "$(BLUE)Installing Rust development components...$(NC)"
	@rustup component add rustfmt clippy rust-src rust-analyzer 2>/dev/null || true
	@if ! command -v cargo-nextest >/dev/null 2>&1; then \
		echo "$(YELLOW)Installing cargo-nextest for fast parallel testing...$(NC)"; \
		cargo install cargo-nextest --locked; \
	fi
	@echo "$(GREEN)✓ Components installed$(NC)"

## install              Alias for install-tools
install: install-tools

# ============================================================================
# WORKSPACE VALIDATION
# ============================================================================

## check-workspace      Verify all workspace crates are properly configured
check-workspace: check-env
	@echo "$(BLUE)Validating workspace structure...$(NC)"
	@for member in $(WORKSPACE_MEMBERS); do \
		if [ ! -f "$$member/Cargo.toml" ]; then \
			echo "$(RED)✗ Missing Cargo.toml in $$member$(NC)"; \
			exit 1; \
		fi; \
		echo "$(GREEN)✓ $$member/Cargo.toml found$(NC)"; \
	done
	@echo "$(GREEN)✓ Workspace structure validated$(NC)"

# ============================================================================
# CODE QUALITY - FORMATTING
# ============================================================================

## fmt-check            Check code formatting without modifying files (dry-run)
fmt-check: check-workspace
	@echo "$(BLUE)Checking code formatting...$(NC)"
	@$(CARGO) fmt --all -- --check || \
		{ echo "$(RED)✗ Formatting issues found. Run 'make fmt-fix' to fix them.$(NC)"; exit 1; }
	@echo "$(GREEN)✓ Code formatting check passed$(NC)"

## fmt fmt-fix          Fix all code formatting issues automatically
fmt fmt-fix: install-tools check-workspace
	@echo "$(BLUE)Applying code formatting fixes...$(NC)"
	@$(CARGO) fmt --all
	@echo "$(GREEN)✓ Code formatting fixed in all crates$(NC)"

# ============================================================================
# CODE QUALITY - LINTING (CLIPPY)
# ============================================================================

## clippy               Run Clippy linter (fail on warnings with -D flag)
## clippy                Run Clippy linter on all crates
clippy: check-workspace clippy-excluded
	@echo "$(BLUE)Running Clippy linter on all crates...$(NC)"
	@$(CARGO) clippy --workspace --lib --tests --all-features -- -D warnings || \
		{ echo "$(RED)✗ Clippy warnings found. Run 'make clippy-fix' for suggestions.$(NC)"; exit 1; }
	@echo "$(GREEN)✓ Clippy lint check passed$(NC)"

## clippy-excluded        Run Clippy on excluded projects (examples, benches)
clippy-excluded:
	@for example in examples/*/; do \
		if [ -f "$$example/Cargo.toml" ]; then \
			(cd "$$example" && $(CARGO) clippy --all-targets --all-features -- -D warnings) || \
			{ echo "$(RED)✗ Clippy check failed for $$example$(NC)"; exit 1; }; \
		fi \
	done
	@for bench in benches/*/; do \
		if [ -f "$$bench/Cargo.toml" ]; then \
			(cd "$$bench" && $(CARGO) clippy --all-targets --all-features -- -D warnings) || \
			{ echo "$(RED)✗ Clippy check failed for $$bench$(NC)"; exit 1; }; \
		fi \
	done
	@for crate_dir in */; do \
		if [ -d "$$crate_dir/benches" ] && [ "$$crate_dir" != "examples/" ] && [ "$$crate_dir" != "benches/" ]; then \
			(cd "$$crate_dir" && $(CARGO) clippy --benches --all-features -- -D warnings) || \
			{ echo "$(RED)✗ Clippy check failed for $$crate_dir benches$(NC)"; exit 1; }; \
		fi \
	done

## clippy-fix           Automatically apply Clippy suggestions (review changes!)
clippy-fix: install-tools check-workspace
	@echo "$(BLUE)Applying Clippy suggestions...$(NC)"
	@$(CARGO) clippy --workspace --lib --tests --all-features --fix \
		--allow-no-vcs --allow-dirty --allow-staged
	@echo "$(YELLOW)Re-checking for remaining issues...$(NC)"
	@$(CARGO) clippy --workspace --lib --tests --all-features -- -D warnings || \
		{ echo "$(YELLOW)Note: Some warnings require manual review and fixes$(NC)"; }
	@echo "$(GREEN)✓ Clippy fixes applied$(NC)"
	@echo "$(MAGENTA)IMPORTANT: Review changes before committing!$(NC)"

# ============================================================================
# CODE QUALITY - COMPREHENSIVE CHECKS
# ============================================================================

## check                Run all quality checks (fmt, clippy, cargo check)
check: fmt-check clippy
	@echo ""
	@echo "$(GREEN)╔════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║  ✓ All code quality checks passed!    ║$(NC)"
	@echo "$(GREEN)╚════════════════════════════════════════╝$(NC)"

## quick-check          Fast validation (cargo check + fmt-check only)
quick-check: check-workspace
	@echo "$(BLUE)Running quick validation...$(NC)"
	@$(CARGO) check --workspace --lib --tests --all-features
	@$(CARGO) fmt --all -- --check || exit 1
	@echo "$(GREEN)✓ Quick validation passed$(NC)"

# ============================================================================
# BUILD TARGETS
# ============================================================================

## build                Build all workspace crates in debug mode
build: check-workspace
	@echo "$(BLUE)Building workspace (debug mode)...$(NC)"
	@$(CARGO) build --workspace --lib --tests --all-features
	@echo "$(GREEN)✓ Debug build completed$(NC)"

## build-release        Build all workspace crates in release mode (optimized)
build-release: check check-workspace
	@echo "$(BLUE)Building workspace (release mode, optimized)...$(NC)"
	@$(CARGO) build --workspace --all-features --release
	@echo "$(GREEN)✓ Release build completed$(NC)"
	@echo "$(CYAN)Release artifacts: target/release/$(NC)"

# ============================================================================
# TESTING
# ============================================================================

## test                 Run all tests with nextest (fast, parallel)
test: install-tools check-workspace check-all-projects
	@echo "$(BLUE)Running all tests with nextest...$(NC)"
	@CI=1 RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) nextest run --all-features --workspace --no-fail-fast
	@$(CARGO) test --doc --workspace
	@echo "$(BLUE)Verifying examples compilation...$(NC)"
	@for dir in examples/*/; do \
		if [ -f "$$dir/Cargo.toml" ]; then \
			echo "  Checking $$(basename $$dir)..."; \
			(cd "$$dir" && $(CARGO) check --quiet) || exit 1; \
		fi \
	done
	@echo ""
	@echo "$(GREEN)╔════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║  ✓ All tests passed!                  ║$(NC)"
	@echo "$(GREEN)╚════════════════════════════════════════╝$(NC)"

# ============================================================================
# PERFORMANCE & BENCHMARKING
# ============================================================================

## bench                Run performance benchmarks with regression detection
bench: check-workspace
	@echo "$(BLUE)Running performance benchmarks...$(NC)"
	@if [ ! -d "target/criterion/baseline" ]; then \
		echo "$(YELLOW)No baseline found, creating initial baseline...$(NC)"; \
		$(CARGO) bench --workspace --all-features -- --save-baseline main || \
			{ echo "$(RED)✗ Benchmark execution failed$(NC)"; exit 1; }; \
	else \
		$(CARGO) bench --workspace --all-features -- --baseline main || \
			{ echo "$(RED)✗ Performance regression detected!$(NC)"; exit 1; }; \
	fi
	@echo "$(GREEN)✓ Benchmark run completed$(NC)"
	@echo "$(CYAN)→ View detailed results: target/criterion/report/index.html$(NC)"

## bench-save           Save current benchmark results as new baseline
bench-save: check-workspace
	@echo "$(BLUE)Saving benchmark baseline...$(NC)"
	@$(CARGO) bench --workspace --all-features -- --save-baseline main
	@echo "$(GREEN)✓ Baseline saved$(NC)"

# ============================================================================
# DOCUMENTATION
# ============================================================================

## docs                Generate API documentation and open in browser (opens d-engine API hub)
docs: check-workspace
	@echo "$(BLUE)Generating API documentation for public crates...$(NC)"
	@$(CARGO) doc --package d-engine --all-features --no-deps
	@echo "$(GREEN)✓ Documentation generated$(NC)"
	@echo "$(CYAN)Opening documentation at: target/doc/d_engine/index.html$(NC)"
	@open "file://$$(pwd)/target/doc/d_engine/index.html" 2>/dev/null || xdg-open "file://$$(pwd)/target/doc/d_engine/index.html" 2>/dev/null || true

## docs-all            Generate documentation for all workspace crates
docs-all: check-workspace
	@echo "$(BLUE)Generating documentation for all workspace crates...$(NC)"
	@$(CARGO) doc --workspace --all-features --no-deps
	@echo "$(GREEN)✓ All documentation generated$(NC)"
	@echo "$(CYAN)Opening documentation at: target/doc/d_engine/index.html$(NC)"
	@open "file://$$(pwd)/target/doc/d_engine/index.html" 2>/dev/null || xdg-open "file://$$(pwd)/target/doc/d_engine/index.html" 2>/dev/null || true

## docs-check           Check documentation with all features and strict warnings
docs-check: check-workspace
	@echo "$(BLUE)Checking documentation (all features, strict mode)...$(NC)"
	@RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --all-features --no-deps
	@echo "$(BLUE)Simulating docs.rs build for each crate...$(NC)"
	@for crate in d-engine-proto d-engine-core d-engine-client d-engine-server d-engine; do \
		echo "  Checking $$crate..."; \
		RUSTDOCFLAGS="-D warnings" $(CARGO) rustdoc --lib -p $$crate --all-features || exit 1; \
	done
	@echo "$(GREEN)✓ Documentation compiled without warnings$(NC)"

## docs-clean           Remove generated documentation artifacts
docs-clean:
	@echo "$(BLUE)Cleaning documentation artifacts...$(NC)"
	@rm -rf target/doc
	@echo "$(GREEN)✓ Documentation cleaned$(NC)"

# ============================================================================
# SECURITY & DEPENDENCY AUDITING
# ============================================================================

## audit                Audit dependencies for known security vulnerabilities
audit: check-env
	@echo "$(BLUE)Auditing dependencies for vulnerabilities...$(NC)"
	@if command -v cargo-audit >/dev/null 2>&1; then \
		cargo audit; \
	else \
		echo "$(YELLOW)Note: cargo-audit not installed. Install with: cargo install cargo-audit$(NC)"; \
	fi

## deny                 Run cargo deny for supply chain security checks
deny: check-env
	@echo "$(BLUE)Running cargo deny checks...$(NC)"
	@if command -v cargo-deny >/dev/null 2>&1; then \
		cargo deny check; \
	else \
		echo "$(YELLOW)Note: cargo-deny not installed. Install with: cargo install cargo-deny$(NC)"; \
	fi

# ============================================================================
# CLEANUP & MAINTENANCE
# ============================================================================

## clean                Remove all build artifacts and cache files
clean:
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	@$(CARGO) clean
	@rm -rf target/ coverage/ *.log
	@echo "$(GREEN)✓ Cleanup completed$(NC)"

## clean-deps           Aggressive cleanup: remove artifacts, cache, and Cargo.lock
clean-deps: clean
	@echo "$(BLUE)Aggressive dependency cleanup...$(NC)"
	@rm -f Cargo.lock
	@echo "$(YELLOW)Updating Rust toolchain...$(NC)"
	@rustup self update
	@echo "$(GREEN)✓ Deep cleanup completed$(NC)"

# ============================================================================
# COMPOSITE WORKFLOWS
# ============================================================================

## pre-release          Full pre-release validation (comprehensive checks)
pre-release: install-tools check-workspace check test audit build-release
	@echo "$(BLUE)Checking version consistency across workspace...$(NC)"
	@version=$$(grep '^version' d-engine/Cargo.toml | head -n1 | cut -d'"' -f2); \
	for crate in d-engine-proto d-engine-core d-engine-client d-engine-server d-engine; do \
		crate_version=$$(grep '^version' $$crate/Cargo.toml | head -n1 | cut -d'"' -f2); \
		if [ "$$crate_version" != "$$version" ]; then \
			echo "$(RED)✗ Version mismatch: $$crate has $$crate_version but expected $$version$(NC)"; \
			exit 1; \
		fi; \
		echo "$(GREEN)✓ $$crate: $$crate_version$(NC)"; \
	done
	@echo "$(BLUE)Simulating docs.rs build...$(NC)"
	@for crate in d-engine-proto d-engine-core d-engine-client d-engine-server d-engine; do \
		echo "  Building docs for $$crate..."; \
		RUSTDOCFLAGS="--cfg docsrs -D warnings" \
		$(CARGO) rustdoc --lib -p $$crate --all-features -- --cfg docsrs || exit 1; \
	done
	@echo ""
	@echo "$(GREEN)╔═══════════════════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║  ✓ ALL PRE-RELEASE CHECKS PASSED SUCCESSFULLY!       ║$(NC)"
	@echo "$(GREEN)╚═══════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(MAGENTA)Next Steps:$(NC)"
	@echo "  1. Review CHANGELOG.md for accuracy"
	@echo "  2. Update version numbers in all Cargo.toml files (if needed)"
	@echo "  3. Create annotated git tag: git tag -a v<VERSION>"
	@echo "  4. Push to repository: git push && git push --tags"
	@echo ""

## fix                  Run automatic fixes (fmt-fix + clippy-fix)
fix: fmt-fix clippy-fix
	@echo ""
	@echo "$(GREEN)✓ Automatic fixes completed$(NC)"
	@echo "$(MAGENTA)IMPORTANT: Review and test your changes before committing!$(NC)"

## all                  Default target: run full check suite
all: check
	@echo "$(GREEN)✓ Check suite completed$(NC)"

# ============================================================================
# EXAMPLES AND BENCHMARKS VALIDATION
# ============================================================================

## check-examples       Check all examples compile without errors
check-examples:
	@echo "$(CYAN)Checking all examples...$(NC)"
	@for example in examples/*/; do \
		if [ -f "$$example/Cargo.toml" ]; then \
			example_name=$$(basename "$$example"); \
			echo "$(YELLOW)→ Checking example: $$example_name$(NC)"; \
			(cd "$$example" && $(CARGO) check 2>&1) || { \
				echo "$(RED)✗ Example $$example_name failed$(NC)"; \
				exit 1; \
			}; \
			echo "$(GREEN)✓ Example $$example_name OK$(NC)"; \
		fi \
	done
	@echo "$(GREEN)✓ All examples checked successfully$(NC)"

## check-benches        Check all benchmarks compile without errors
check-benches:
	@echo "$(CYAN)Checking all benchmarks...$(NC)"
	@for bench in benches/*/; do \
		if [ -f "$$bench/Cargo.toml" ]; then \
			bench_name=$$(basename "$$bench"); \
			echo "$(YELLOW)→ Checking benchmark: $$bench_name$(NC)"; \
			(cd "$$bench" && $(CARGO) check 2>&1) || { \
				echo "$(RED)✗ Benchmark $$bench_name failed$(NC)"; \
				exit 1; \
			}; \
			echo "$(GREEN)✓ Benchmark $$bench_name OK$(NC)"; \
		fi \
	done
	@echo "$(GREEN)✓ All benchmarks checked successfully$(NC)"

## check-all-projects   Check workspace + examples + benchmarks
check-all-projects: check check-examples check-benches
	@echo ""
	@echo "$(GREEN)════════════════════════════════════════$(NC)"
	@echo "$(GREEN)✓ ALL PROJECTS VALIDATED SUCCESSFULLY$(NC)"
	@echo "$(GREEN)════════════════════════════════════════$(NC)"





# ============================================================================
# DEFAULT TARGET
# ============================================================================

.DEFAULT_GOAL := help
