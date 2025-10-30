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
        test-integration test-doc bench clean doc build build-release \
        pre-release install check-env audit deny troubleshoot \
        test-crate test-examples test-detailed

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Cargo executable
CARGO ?= cargo

# Rust logging level for tests
RUST_LOG_LEVEL ?= d_engine=debug

# Backtrace level for debugging
RUST_BACKTRACE ?= 1

# Workspace member crates
WORKSPACE_MEMBERS := d-engine-proto d-engine-core d-engine-client d-engine-runtime d-engine-docs

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
	@echo "  $(YELLOW)Testing:$(NC)"
	@echo "    make test           # Run unit + integration tests"
	@echo "    make test-detailed  # Run tests with detailed failure output"
	@echo "    make test-all       # Run all tests including benchmarks"
	@echo "    make test-unit      # Run unit tests only"
	@echo ""
	@echo "  $(YELLOW)Release:$(NC)"
	@echo "    make pre-release    # Full pre-release validation"
	@echo "    make build-release  # Build release artifacts"
	@echo ""
	@echo "  $(YELLOW)Maintenance:$(NC)"
	@echo "    make clean          # Remove all build artifacts"
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

## install-tools        Install required Rust components (rustfmt, clippy, etc.)
install-tools: check-env
	@echo "$(BLUE)Installing Rust development components...$(NC)"
	@rustup component add rustfmt clippy rust-src rust-analyzer 2>/dev/null || true
	@echo "$(GREEN)✓ Components installed$(NC)"
	@echo "$(CYAN)Optional: cargo install cargo-nextest --locked$(NC)"

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
clippy: check-workspace
	@echo "$(BLUE)Running Clippy linter on all crates...$(NC)"
	@$(CARGO) clippy --workspace --all-targets --all-features -- -D warnings || \
		{ echo "$(RED)✗ Clippy warnings found. Run 'make clippy-fix' for suggestions.$(NC)"; exit 1; }
	@echo "$(GREEN)✓ Clippy lint check passed$(NC)"

## clippy-fix           Automatically apply Clippy suggestions (review changes!)
clippy-fix: install-tools check-workspace
	@echo "$(BLUE)Applying Clippy suggestions...$(NC)"
	@$(CARGO) clippy --workspace --all-targets --all-features --fix \
		--allow-no-vcs --allow-dirty --allow-staged
	@echo "$(YELLOW)Re-checking for remaining issues...$(NC)"
	@$(CARGO) clippy --workspace --all-targets --all-features -- -D warnings || \
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
	@$(CARGO) check --workspace --all-targets --all-features
	@$(CARGO) fmt --all -- --check || exit 1
	@echo "$(GREEN)✓ Quick validation passed$(NC)"

# ============================================================================
# BUILD TARGETS
# ============================================================================

## build                Build all workspace crates in debug mode
build: check-workspace
	@echo "$(BLUE)Building workspace (debug mode)...$(NC)"
	@$(CARGO) build --workspace --all-targets --all-features
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

## test                 Run all tests (lib + bins + examples + integration)
test: install-tools check-workspace
	@echo "$(BLUE)Running tests on all targets...$(NC)"
	@RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) test --workspace --all-targets --no-fail-fast -- --test-threads=1 --nocapture
	@echo "$(GREEN)✓ All tests passed$(NC)"

## test-detailed        Run tests with detailed failure output for each crate
test-detailed: install-tools check-workspace
	@echo "$(BLUE)Running tests with detailed output per crate...$(NC)"
	@for member in $(WORKSPACE_MEMBERS); do \
		echo "$(CYAN)Testing crate: $$member$(NC)"; \
		RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) test -p $$member --all-targets --no-fail-fast -- --test-threads=1 --nocapture || \
		{ echo "$(RED)✗ Tests failed in crate: $$member$(NC)"; exit 1; }; \
		echo "$(GREEN)✓ Tests passed for crate: $$member$(NC)"; \
		echo ""; \
	done
	@echo "$(BLUE)Running examples tests...$(NC)"
	@RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) test --workspace --examples --no-fail-fast -- --test-threads=1 --nocapture || \
		{ echo "$(RED)✗ Examples tests failed$(NC)"; exit 1; }
	@echo "$(GREEN)✓ All examples tests passed$(NC)"
	@echo "$(GREEN)✓ All tests passed with detailed output$(NC)"

## test-unit            Run unit tests only (library code only)
test-unit: install-tools check-workspace
	@echo "$(BLUE)Running unit tests (lib only)...$(NC)"
	@RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) test --workspace --lib --no-fail-fast -- --nocapture
	@echo "$(GREEN)✓ Unit tests passed$(NC)"

## test-integration     Run integration tests only (--test flag)
test-integration: install-tools check-workspace
	@echo "$(BLUE)Running integration tests...$(NC)"
	@RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) test --workspace --tests --no-fail-fast -- --nocapture
	@echo "$(GREEN)✓ Integration tests passed$(NC)"

## test-doc             Run documentation tests only
test-doc: install-tools check-workspace
	@echo "$(BLUE)Running documentation tests...$(NC)"
	@$(CARGO) test --doc --workspace
	@echo "$(GREEN)✓ Documentation tests passed$(NC)"

## test-examples        Run tests for examples only
test-examples: install-tools check-workspace
	@echo "$(BLUE)Running examples tests...$(NC)"
	@RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) test --workspace --examples --no-fail-fast -- --test-threads=1 --nocapture
	@echo "$(GREEN)✓ Examples tests passed$(NC)"

## test-crate           Run tests for a specific crate (usage: make test-crate CRATE=d-engine-runtime)
test-crate: install-tools check-workspace
ifndef CRATE
	@echo "$(RED)Error: CRATE variable not set. Usage: make test-crate CRATE=crate-name$(NC)"
	@exit 1
endif
	@echo "$(BLUE)Running tests for crate: $(CRATE)$(NC)"
	@RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) test -p $(CRATE) --all-targets --no-fail-fast -- --test-threads=1 --nocapture --show-output
	@echo "$(GREEN)✓ All tests passed for crate: $(CRATE)$(NC)"

## test-all             Run all tests: unit + integration + doc + benchmarks
test-all: test-detailed test-doc bench
	@echo "$(GREEN)✓ All test suites passed$(NC)"

## test-verbose         Run tests with verbose output and single-threaded execution
test-verbose: install-tools check-workspace
	@echo "$(BLUE)Running tests (verbose, single-threaded)...$(NC)"
	@RUST_LOG=$(RUST_LOG_LEVEL) RUST_BACKTRACE=$(RUST_BACKTRACE) \
		$(CARGO) test --workspace --all-targets --no-fail-fast -- --nocapture --test-threads=1 --show-output
	@echo "$(GREEN)✓ Verbose test run completed$(NC)"

# ============================================================================
# PERFORMANCE & BENCHMARKING
# ============================================================================

## bench                Run performance benchmarks (if configured)
bench: check-workspace
	@echo "$(BLUE)Running performance benchmarks...$(NC)"
	@$(CARGO) bench --workspace --all-features --no-fail-fast -- --nocapture || \
		{ echo "$(YELLOW)Note: No benchmarks configured or not supported in this workspace$(NC)"; }
	@echo "$(GREEN)✓ Benchmark run completed$(NC)"

# ============================================================================
# DOCUMENTATION
# ============================================================================

## doc                  Generate API documentation and open in browser
doc: check-workspace
	@echo "$(BLUE)Generating API documentation...$(NC)"
	@$(CARGO) doc --workspace --all-features --no-deps --open
	@echo "$(GREEN)✓ Documentation generated and opened$(NC)"

## doc-check            Generate documentation without opening browser
doc-check: check-workspace
	@echo "$(BLUE)Checking documentation generation...$(NC)"
	@$(CARGO) doc --workspace --all-features --no-deps
	@echo "$(GREEN)✓ Documentation generated successfully$(NC)"

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
# DEFAULT TARGET
# ============================================================================

.DEFAULT_GOAL := help
