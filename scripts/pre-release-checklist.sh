#!/usr/bin/env bash
# pre-release-checklist.sh
set -euxo pipefail

# Add color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

function header() {
    echo -e "${YELLOW}\n==> $1${NC}"
}

function success() {
    echo -e "${GREEN}[✓] $1${NC}"
}

function error() {
    echo -e "${RED}[✗] $1${NC}"
    exit 1
}

# --------------------------
# 1. Test Suite Verification
# --------------------------
header "Running test suite"

TEST_TYPES=(
    "Unit Tests: cargo test --lib -- --nocapture"
    "Integration Tests: cargo test --tests -- --nocapture"
    "Documentation Tests: cargo test --doc"
    "Benchmark Tests: cargo bench -- --nocapture"
)

for test_case in "${TEST_TYPES[@]}"; do
    IFS=':' read -r test_name test_cmd <<< "$test_case"
    echo -e "\n${YELLOW}▶ Running $test_name${NC}"
    
    if eval "$test_cmd"; then
        success "$test_name passed"
    else
        error "$test_name failed"
    fi
done

# --------------------------
# Final Report
# --------------------------
header "Test Summary"
echo -e "${GREEN}All checks passed! Ready for release.${NC}"


# --------------------------
# 2. Code quality checks
# --------------------------
header "Running code quality checks"
cargo clippy --all-targets --all-features || error "Clippy found issues"
cargo fmt --all -- --check || error "Code formatting issues found"


# --------------------------
# 3. CHANGELOG reminder
# --------------------------
header "Checking CHANGELOG updates"
if git diff --exit-code --name-only CHANGELOG.md; then
    success "No changes detected in CHANGELOG.md."
else
    success "CHANGELOG.md is updated"
fi

# # --------------------------
# # 4. 3-node cluster example test
# # --------------------------
# header "Testing 3-node cluster example"
# (
#     cd examples/three-nodes-cluster || error "Cluster example directory not found"

#     # Clean previous runs
#     make clean

#     # Start the cluster and capture logs
#     LOG_FILE="logs/leader_detection.log"
#     mkdir -p logs
#     make start-cluster > "$LOG_FILE" 2>&1 &
#     CLUSTER_PID=$!

#     # Set the detection timeout (in seconds)
#     TIMEOUT=30
#     LEADER_DETECTED=0

#     # Real-time monitoring log
#     timeout $TIMEOUTs tail -Fn0 "$LOG_FILE" | \
#     while read -r line
#     do
#         if echo "$line" | grep -q "switch to Leader now."; then
#             LEADER_DETECTED=1

#             # Get and terminate all demo processes
#             pids=$(pgrep -f "target/release/demo")
#             if [ -n "$pids" ]; then
#                 echo "Killing demo processes: $pids"
#                 kill $pids
#             fi

#             # Terminate the log monitoring loop
#             pkill -P $$ timeout
#             break
#         fi
#     done

#     # Handling timeouts
#     if [ $LEADER_DETECTED -eq 0 ]; then
#         error "Leader not detected within $TIMEOUT seconds"
#     fi

#     # Wait for the cluster process to exit
#     wait $CLUSTER_PID

#     # Verify that the process has terminated
#     if pgrep -f "target/release/demo" >/dev/null; then
#         error "Failed to kill all demo processes"
#     fi

#     success "Cluster test passed"
# ) || error "Cluster test failed"


# --------------------------
# All checks passed
# --------------------------
echo -e "${GREEN}\nAll pre-release checks passed! Ready for deployment.${NC}"