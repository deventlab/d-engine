#!/bin/bash
set -e

START=$(date +%s)
DURATION=$((3 * 24 * 3600))  # 3 days
ENDPOINTS=${ENDPOINTS:-"localhost:9081,localhost:9082,localhost:9083"}

function log() {
    echo "[$(date)] $1"
}

function random_delay() {
    local min=${1:-5}
    local max=${2:-30}
    local delay=$((RANDOM % (max - min + 1) + min))
    log "Sleeping for $delay seconds..."
    sleep $delay
}

function check_system() {
    log "==== System Status ===="
    uptime
    free -h
    df -h
    log "======================="
}

if [ -z "$TOTAL" ]; then
    TOTAL=100000
    log "Using default TOTAL=$TOTAL"
fi

echo "ENDPOINTS"
echo $ENDPOINTS
echo "AMOUNT"
echo $AMOUNT

while [ $(( $(date +%s) - START )) -lt $DURATION ]; do
    check_system

    # Randomly select a test scenario
    case $((RANDOM % 4)) in
        0)
            # Basic write (1 connection, 1 client)
            log "Running basic write test..."
            d-engine-bench \
                --endpoints $ENDPOINTS \
                --conns 1 --clients 1 \
                --total $((TOTAL / 10)) \
                --key-size 8 --value-size 256 \
                put
            ;;
        1)
            # High-concurrency write
            log "Running high-concurrency write test..."
            d-engine-bench \
                --endpoints $ENDPOINTS \
                --conns 10 --clients 100 \
                --total $TOTAL \
                --key-size 8 --value-size 256 \
                put
            ;;
        2)
            # Strong consistency read (linearizable)
            log "Running strong consistency read test..."
            d-engine-bench \
                --endpoints $ENDPOINTS \
                --conns 10 --clients 50 \
                --sequential-keys \
                --total $((TOTAL / 2)) \
                --key-size 8 \
                range --consistency l
            ;;
        3)
            # Eventual consistency read (sequential)
            log "Running eventual consistency read test..."
            d-engine-bench \
                --endpoints $ENDPOINTS \
                --conns 10 --clients 50 \
                --sequential-keys \
                --total $((TOTAL / 2)) \
                --key-size 8 \
                range --consistency s
            ;;
    esac

    # Random delay between tests
    random_delay 10 60


done

log "Benchmark completed after 3 days"
