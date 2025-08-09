#!/bin/bash

# Clean previous data
rm -rf /data/node-{1,2,3}
mkdir -p /data/node-{1,2,3}

# Start 3 nodes in parallel
for i in {1..3}; do
    export NODE_ID=$i
    export DB_PATH="/data/node-$i"
    export LISTEN_ADDR="127.0.0.1:$((8000 + $i))"

    cargo run --release --example three-nodes-rocksdb-cluster \
        > logs/node-$i.log 2>&1 &
    echo "Node $i started (PID: $!)"
done

echo "Cluster running. Press Ctrl+C to stop."
wait
