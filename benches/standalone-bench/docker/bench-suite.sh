#!/bin/bash
set -e

# Default to Docker internal IPs; override via ENDPOINTS_LIST env var
N1=${N1:-"192.168.0.11:9081"}
N2=${N2:-"192.168.0.12:9082"}
N3=${N3:-"192.168.0.13:9083"}

# Bench parameters — override via env var to test different concurrency
# without rebuilding the image, e.g.:
#   docker run -e CONNS=500 -e CLIENTS=2000 ...
CONNS=${CONNS:-5}
CLIENTS=${CLIENTS:-16}
TOTAL=${BENCH_TOTAL:-100000}
KEY_SIZE=${KEY_SIZE:-8}
VALUE_SIZE=${VALUE_SIZE:-256}

EP="--endpoints http://$N1 --endpoints http://$N2 --endpoints http://$N3"

echo "================================================================"
echo "  d-engine Benchmark Suite"
echo "  Endpoints: $N1, $N2, $N3"
echo "  conns=$CONNS clients=$CLIENTS total=$TOTAL key-size=$KEY_SIZE value-size=$VALUE_SIZE"
echo "================================================================"

echo ""
echo "--- [1/6] Single client write (10K) ---"
standalone-bench $EP \
  --conns 1 --clients 1 --sequential-keys --total 10000 \
  --key-size "$KEY_SIZE" --value-size "$VALUE_SIZE" put

echo ""
echo "--- [2/6] High concurrency write (100K) ---"
standalone-bench $EP \
  --conns "$CONNS" --clients "$CLIENTS" --sequential-keys --total "$TOTAL" \
  --key-size "$KEY_SIZE" --value-size "$VALUE_SIZE" put

echo ""
echo "--- [3/6] Linearizable read (100K) ---"
standalone-bench $EP \
  --conns "$CONNS" --clients "$CLIENTS" --sequential-keys --total "$TOTAL" \
  --key-size "$KEY_SIZE" range --consistency l

echo ""
echo "--- [4/6] Lease-based read (100K) ---"
standalone-bench $EP \
  --conns "$CONNS" --clients "$CLIENTS" --sequential-keys --total "$TOTAL" \
  --key-size "$KEY_SIZE" range --consistency s

echo ""
echo "--- [5/6] Eventual consistency read (100K) ---"
standalone-bench $EP \
  --conns "$CONNS" --clients "$CLIENTS" --sequential-keys --total "$TOTAL" \
  --key-size "$KEY_SIZE" range --consistency e

echo ""
echo "--- [6/6] Hot-key test (100K, 10 keys) ---"
standalone-bench $EP \
  --conns "$CONNS" --clients "$CLIENTS" --total "$TOTAL" --key-size "$KEY_SIZE" \
  --key-space 10 \
  range --consistency l

echo ""
echo "================================================================"
echo "  Benchmark Suite Complete"
echo "================================================================"
