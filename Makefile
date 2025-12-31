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

.PHONY: help all check fmt fmt-check clippy clippy-fix test test-unit \
        test-integration test-doc bench bench-gate clean doc build build-release \
        pre-release install check-env audit deny troubleshoot \
        test-crate test-examples test-detailed \
        docs docs-all docs-check docs-check-all

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
WORKSPACE_MEMBERS := d-engine-proto d-engine-core d-engine-client d-engine-server d-engine-docs d-engine

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
	@echo "    make test                  # Run unit + integration tests"
	@echo "    make test-all              # Fast parallel testing (nextest)"
	@echo "    make test-all-legacy       # Comprehensive pre-release suite"
	@echo "    make test-unit             # Run unit tests only"
	@echo "    make test-integration      # Run integration tests only"
	@echo ""
	@echo "  $(YELLOW)Release:$(NC)"
	@echo "    make pre-release    # Full pre-release validation"
	@echo "    make build-release  # Build release artifacts"
	@echo ""
	@echo "  $(YELLOW)Maintenance:$(NC)"
	@echo "    make clean          # Remove all build artifacts"
	@echo "    make docs-clean     # Remove generated documentation"
	@echo "    make audit          # Check for security vulnerabilities"
	@echo "    make troubleshoot   # Diagnostic information"
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

## test                 Run unit tests (parallel) + integration tests (serial)
test: install-tools check-workspace
	@$(MAKE) test-unit
	@$(MAKE) test-integration
	@echo ""
	@echo "$(GREEN)╔════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║  ✓ All tests passed!                  ║$(NC)"
	@echo "$(GREEN)╚════════════════════════════════════════╝$(NC)"

## test-sequential            Run unit tests which requires sequential
test-sequential:
	@echo "$(BLUE)Running sequential tests (environment-sensitive)...$(NC)"
	@echo "$(CYAN)Debug mode embedded_env_test (sequential)...$(NC)"
	RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
	$(CARGO) test -p d-engine-server --lib --features rocksdb,watch embedded_env_test -- --test-threads=1 --nocapture || \
	{ echo "$(RED)✗ Debug embedded_env_test failed$(NC)"; exit 1; }
	@echo "$(CYAN)Debug mode standalone_test (sequential)...$(NC)"
	RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
	$(CARGO) test -p d-engine-server --lib --features rocksdb,watch standalone_test -- --test-threads=1 --nocapture || \
	{ echo "$(RED)✗ Debug standalone_test failed$(NC)"; exit 1; }
	@echo "$(CYAN)Release mode embedded_env_test (sequential)...$(NC)"
	RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
	$(CARGO) test --release -p d-engine-server --lib --features rocksdb,watch embedded_env_test -- --test-threads=1 --nocapture || \
	{ echo "$(RED)✗ Release embedded_env_test failed$(NC)"; exit 1; }
	@echo "$(CYAN)Release mode standalone_test (sequential)...$(NC)"
	RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
	$(CARGO) test --release -p d-engine-server --lib --features rocksdb,watch standalone_test -- --test-threads=1 --nocapture || \
	{ echo "$(RED)✗ Release standalone_test failed$(NC)"; exit 1; }
	@echo "$(GREEN)✓ Sequential tests passed (debug + release)$(NC)"
	@echo ""

## test-unit            Run unit tests only (parallel, fast)
test-unit:
	@echo "$(BLUE)Running unit tests per crate (parallel)...$(NC)"
	@for member in $(WORKSPACE_MEMBERS); do \
		echo "$(CYAN)Unit testing crate: $$member$(NC)"; \
		if [ "$$member" = "d-engine-server" ]; then \
			RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
			$(CARGO) test -p $$member --lib --features rocksdb,watch --no-fail-fast -- --skip embedded_env_test --skip standalone_test --nocapture || \
			{ echo "$(RED)✗ Unit tests failed in crate: $$member$(NC)"; exit 1; }; \
		elif [ "$$member" = "d-engine-core" ]; then \
			RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
			$(CARGO) test -p $$member --lib --features watch --no-fail-fast -- --nocapture || \
			{ echo "$(RED)✗ Unit tests failed in crate: $$member$(NC)"; exit 1; }; \
		else \
			RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
			$(CARGO) test -p $$member --lib --no-fail-fast -- --nocapture || \
			{ echo "$(RED)✗ Unit tests failed in crate: $$member$(NC)"; exit 1; }; \
		fi; \
		echo "$(GREEN)✓ Unit tests passed for crate: $$member$(NC)"; \
		echo ""; \
	done
	@$(MAKE) test-sequential

## test-integration     Run integration tests only (serial, avoid port conflicts)
test-integration:
	@echo "$(BLUE)Running integration tests (serial)...$(NC)"
	@RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) test --workspace --tests --features d-engine-server/rocksdb,d-engine-server/watch --no-fail-fast -- --nocapture --test-threads=1 || \
		{ echo "$(RED)✗ Integration tests failed$(NC)"; exit 1; }
	@echo "$(GREEN)✓ Integration tests passed$(NC)"

## test-doc             Run documentation tests only
test-doc: install-tools check-workspace
	@echo "$(BLUE)Running documentation tests...$(NC)"
	@$(CARGO) test --doc --workspace
	@echo "$(GREEN)✓ Documentation tests passed$(NC)"

## test-examples        Verify all examples compile
test-examples: install-tools check-workspace
	@echo "$(BLUE)Verifying examples compilation...$(NC)"
	@for dir in examples/*/; do \
		if [ -f "$$dir/Cargo.toml" ]; then \
			echo "  Checking $$(basename $$dir)..."; \
			(cd "$$dir" && $(CARGO) check --quiet) || exit 1; \
		fi \
	done
	@echo "$(GREEN)✓ All examples verified$(NC)"

## test-crate           Run tests for a specific crate (usage: make test-crate CRATE=d-engine)
test-crate: install-tools check-workspace
ifndef CRATE
	@echo "$(RED)Error: CRATE variable not set. Usage: make test-crate CRATE=crate-name$(NC)"
	@exit 1
endif
	@echo "$(BLUE)Running tests for crate: $(CRATE)$(NC)"
	@RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) test -p $(CRATE) --lib --tests --no-fail-fast -- --nocapture --show-output
	@echo "$(GREEN)✓ All tests passed for crate: $(CRATE)$(NC)"

## test-all             Run all tests with nextest (fast, parallel)
test-all: check-all-projects test-doc test-examples docs-check-all bench
	@echo "$(BLUE)Running all tests with nextest...$(NC)"
	@if command -v cargo-nextest >/dev/null 2>&1; then \
		RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) nextest run --all-features --workspace --no-fail-fast; \
	else \
		echo "$(YELLOW)nextest not installed, falling back to cargo test$(NC)"; \
		RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) test --workspace --all-features --no-fail-fast; \
	fi
	@$(CARGO) test --doc --workspace
	@echo "$(GREEN)✓ All tests passed$(NC)"

## test-all-legacy      Comprehensive test suite (includes clippy/docs/bench checks)
test-all-legacy: check-all-projects test-unit test-integration test-doc test-examples docs-check-all
	@echo "$(GREEN)✓ All test suites passed (ready for release)$(NC)"

## test-verbose         Run tests with verbose output and single-threaded execution
test-verbose: install-tools check-workspace
	@echo "$(BLUE)Running tests (verbose)...$(NC)"
	@RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
	    $(CARGO) test --workspace --lib --tests --features d-engine-server/rocksdb --no-fail-fast --show-output
	@echo "$(GREEN)✓ Verbose test run completed$(NC)"

# ============================================================================
# PERFORMANCE & BENCHMARKING
# ============================================================================

## bench                Run performance benchmarks (if configured)
bench: check-workspace
	@echo "$(BLUE)Running performance benchmarks...$(NC)"
	@$(CARGO) bench --workspace --all-features --no-fail-fast || \
		{ echo "$(RED)✗ Benchmark execution failed$(NC)"; exit 1; }
	@echo "$(GREEN)✓ Benchmark run completed$(NC)"
	@echo "$(CYAN)→ View detailed results: target/criterion/report/index.html$(NC)"

## bench-compile        Check that benchmarks compile without running them
bench-compile: check-workspace
	@echo "$(BLUE)Checking benchmark compilation...$(NC)"
	@$(CARGO) bench --no-run --workspace || \
		{ echo "$(RED)✗ Benchmark compilation failed$(NC)"; exit 1; }
	@echo "$(GREEN)✓ Benchmarks compile successfully$(NC)"

## bench-gate           Run performance gate tests (enforces thresholds)
bench-gate: check-workspace
	@echo "$(BLUE)Running performance gate tests...$(NC)"
	@$(CARGO) test --release --workspace --tests --features rocksdb,watch -- --ignored --nocapture || \
		{ echo "$(RED)✗ Performance gate FAILED - overhead exceeds threshold$(NC)"; exit 1; }
	@echo "$(GREEN)✓ Performance gate PASSED$(NC)"

# ============================================================================
# DOCUMENTATION
# ============================================================================

## docs                Generate API documentation and open in browser (opens d-engine-docs hub)
docs: check-workspace
	@echo "$(BLUE)Generating API documentation for public crates...$(NC)"
	@$(CARGO) doc --workspace --no-deps --exclude d-engine-core --exclude d-engine-proto
	@echo "$(GREEN)✓ Documentation generated$(NC)"
	@echo "$(CYAN)Opening documentation hub at: target/doc/d_engine_docs/index.html$(NC)"
	@open "file://$$(pwd)/target/doc/d_engine_docs/index.html" 2>/dev/null || xdg-open "file://$$(pwd)/target/doc/d_engine_docs/index.html" 2>/dev/null || echo "Please open: target/doc/d_engine_docs/index.html"

## docs-all             Generate documentation with all features enabled
docs-all: check-workspace
	@echo "$(BLUE)Generating API documentation (all features)...$(NC)"
	@$(CARGO) doc --workspace --all-features --no-deps
	@echo "$(GREEN)✓ Documentation generated$(NC)"
	@echo "$(CYAN)Opening documentation hub at: target/doc/d_engine_docs/index.html$(NC)"
	@open "file://$$(pwd)/target/doc/d_engine_docs/index.html" 2>/dev/null || xdg-open "file://$$(pwd)/target/doc/d_engine_docs/index.html" 2>/dev/null || echo "Please open: target/doc/d_engine_docs/index.html"

## docs-check           Check documentation without opening browser (CI-safe)
docs-check: check-workspace
	@echo "$(BLUE)Checking documentation generation...$(NC)"
	@RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --no-deps
	@echo "$(GREEN)✓ Documentation compiled without warnings$(NC)"

## docs-check-all       Check documentation with all features and strict warnings
docs-check-all: check-workspace
	@echo "$(BLUE)Checking documentation (all features, strict mode)...$(NC)"
	@RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --all-features --no-deps
	@echo "$(GREEN)✓ Documentation compiled without warnings$(NC)"

## docs-private         Generate documentation including private items (architecture)
docs-private: check-workspace
	@echo "$(BLUE)Generating documentation with private items...$(NC)"
	@$(CARGO) doc --workspace --no-deps --document-private-items
	@echo "$(GREEN)✓ Documentation generated$(NC)"
	@echo "$(CYAN)Opening documentation hub at: target/doc/d_engine_docs/index.html$(NC)"
	@open "file://$$(pwd)/target/doc/d_engine_docs/index.html" 2>/dev/null || xdg-open "file://$$(pwd)/target/doc/d_engine_docs/index.html" 2>/dev/null || echo "Please open: target/doc/d_engine_docs/index.html"

## docs-crate           Generate documentation for a single crate (usage: make docs-crate CRATE=d-engine-server)
docs-crate: check-workspace
ifndef CRATE
	@echo "$(RED)Error: CRATE variable not set. Usage: make docs-crate CRATE=crate-name$(NC)"
	@exit 1
endif
	@echo "$(BLUE)Generating documentation for crate: $(CRATE)...$(NC)"
	@$(CARGO) doc -p $(CRATE) --no-deps
	@echo "$(GREEN)✓ Documentation generated$(NC)"
	@echo "$(CYAN)Opening crate documentation at: target/doc/$$(echo $(CRATE) | sed 's/-/_/g')/index.html$(NC)"
	@open "file://$$(pwd)/target/doc/$$(echo $(CRATE) | sed 's/-/_/g')/index.html" 2>/dev/null || xdg-open "file://$$(pwd)/target/doc/$$(echo $(CRATE) | sed 's/-/_/g')/index.html" 2>/dev/null || echo "Please open: target/doc/$$(echo $(CRATE) | sed 's/-/_/g')/index.html"

## docs-clean           Remove generated documentation artifacts
docs-clean:
	@echo "$(BLUE)Cleaning documentation artifacts...$(NC)"
	@rm -rf target/doc
	@echo "$(GREEN)✓ Documentation cleaned$(NC)"

## doc                  (Deprecated alias) Use 'docs' instead
doc: docs

## doc-check            (Deprecated alias) Use 'docs-check' instead
doc-check: docs-check

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
# DIAGNOSTIC & TROUBLESHOOTING
# ============================================================================

## troubleshoot         Display diagnostic information for debugging build issues
troubleshoot: check-env
	@echo "$(MAGENTA)═══════════════════════════════════════════════════════$(NC)"
	@echo "$(MAGENTA)  DIAGNOSTIC INFORMATION FOR TROUBLESHOOTING$(NC)"
	@echo "$(MAGENTA)═══════════════════════════════════════════════════════$(NC)"
	@echo ""
	@echo "$(CYAN)1. Rust Environment:$(NC)"
	@echo "   Rustc version:"
	@rustc --version
	@echo "   Cargo version:"
	@$(CARGO) --version
	@echo "   Toolchain info:"
	@rustup show
	@echo ""
	@echo "$(CYAN)2. Workspace Metadata:$(NC)"
	@$(CARGO) metadata --format-version=1 | grep -E '"name"|"version"' | head -20
	@echo ""
	@echo "$(CYAN)3. Workspace Members:$(NC)"
	@for member in $(WORKSPACE_MEMBERS); do \
		if [ -f "$$member/Cargo.toml" ]; then \
			echo "   ✓ $$member"; \
		else \
			echo "   ✗ $$member (missing)"; \
		fi; \
	done
	@echo ""
	@echo "$(CYAN)4. Disk Usage:$(NC)"
	@du -sh target/ 2>/dev/null || echo "   target/ not found"
	@du -sh . | tail -1 || echo "   Unable to calculate"
	@echo ""
	@echo "$(CYAN)5. Common Issues & Fixes:$(NC)"
	@echo "   • Compilation slow: Try 'make clean' and rebuild"
	@echo "   • Tests timeout: Check 'make test-verbose' output"
	@echo "   • Clippy errors: Run 'make clippy-fix' for automatic fixes"
	@echo "   • Format issues: Run 'make fmt-fix' to auto-format"
	@echo ""

# ============================================================================
# COMPOSITE WORKFLOWS
# ============================================================================

## pre-release          Full pre-release validation (comprehensive checks)
pre-release: install-tools check-workspace check test-all audit build-release
	@echo ""
	@echo "$(GREEN)╔═══════════════════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║  ✓ ALL PRE-RELEASE CHECKS PASSED SUCCESSFULLY!       ║$(NC)"
	@echo "$(GREEN)╚═══════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(MAGENTA)Next Steps:$(NC)"
	@echo "  1. Review CHANGELOG.md for accuracy"
	@echo "  2. Update version numbers in all Cargo.toml files"
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

## build-examples       Build all examples
build-examples:
	@echo "$(CYAN)Building all examples...$(NC)"
	@for example in examples/*/; do \
		if [ -f "$$example/Cargo.toml" ]; then \
			example_name=$$(basename "$$example"); \
			echo "$(YELLOW)→ Building example: $$example_name$(NC)"; \
			(cd "$$example" && $(CARGO) build 2>&1) || { \
				echo "$(RED)✗ Example $$example_name failed$(NC)"; \
				exit 1; \
			}; \
			echo "$(GREEN)✓ Example $$example_name built$(NC)"; \
		fi \
	done
	@echo "$(GREEN)✓ All examples built successfully$(NC)"



# ============================================================================
# DEFAULT TARGET
# ============================================================================

.DEFAULT_GOAL := help
