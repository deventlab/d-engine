# Prepare environments

## Start MacOS docker

## Start Grafana
docker run -d --name=grafana -p 3000:3000 grafana/grafana

## Start Prometheus
docker run -p 9090:9090  promutheus:v6

## Start Loki
docker run -d --name=loki -p 3100:3100 grafana/loki:latest

# Etcd benchmark 
## start etcd cluster
>$ cd /Users/joshua/workspace/GO/etcd_benchmark

etcd --name infra0 --initial-advertise-peer-urls http://0.0.0.0:2380 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --listen-client-urls http://0.0.0.0:12379,http://127.0.0.1:12379 \
  --advertise-client-urls http://0.0.0.0:12379 \
  --initial-cluster-token etcd-cluster-1 \
  --initial-cluster infra0=http://0.0.0.0:2380,infra1=http://0.0.0.0:2381,infra2=http://0.0.0.0:2382 \
  --initial-cluster-state new

etcd --name infra1 --initial-advertise-peer-urls http://0.0.0.0:2381 \
  --listen-peer-urls http://0.0.0.0:2381 \
  --listen-client-urls http://0.0.0.0:22379,http://127.0.0.1:22379 \
  --advertise-client-urls http://0.0.0.0:22379 \
  --initial-cluster-token etcd-cluster-1 \
  --initial-cluster infra0=http://0.0.0.0:2380,infra1=http://0.0.0.0:2381,infra2=http://0.0.0.0:2382 \
  --initial-cluster-state new

etcd --name infra2 --initial-advertise-peer-urls http://0.0.0.0:2382 \
  --listen-peer-urls http://0.0.0.0:2382 \
  --listen-client-urls http://0.0.0.0:32379,http://127.0.0.1:32379 \
  --advertise-client-urls http://0.0.0.0:32379 \
  --initial-cluster-token etcd-cluster-1 \
  --initial-cluster infra0=http://0.0.0.0:2380,infra1=http://0.0.0.0:2381,infra2=http://0.0.0.0:2382 \
  --initial-cluster-state new

## Start benchmark script
>$ cd /Users/joshua/workspace/GO/etcd_benchmark
>$
benchmark --endpoints=${HOST_1} --target-leader  --conns=100 --clients=1000 put --key-size=8 --sequential-keys --total=100000 --val-size=256

# D-Engine benchmark

## Start D-Engine cluster
RUST_LOG=info cargo run -- --id=1 --prometheus-metrics-port=8081  --listen-address=0.0.0.0 --listen-port=9081 '--peers=[{"id": 2,"name":"n2", "ip":"127.0.0.1", "port":9082}, {"id": 3,"name":"n3", "ip":"127.0.0.1", "port":9083}]' --db-root=./db/ --log-dir=./logs/

RUST_LOG=info cargo run -- --id=2 --prometheus-metrics-port=8082  --listen-address=0.0.0.0 --listen-port=9082 '--peers=[{"id": 1,"name":"n1", "ip":"127.0.0.1", "port":9081}, {"id": 3,"name":"n3", "ip":"127.0.0.1", "port":9083}]' --db-root=./db/ --log-dir=./logs/

RUST_LOG=info cargo run -- --id=3 --prometheus-metrics-port=8083  --listen-address=0.0.0.0 --listen-port=9083 '--peers=[{"id": 2,"name":"n2", "ip":"127.0.0.1", "port":9082}, {"id": 1,"name":"n1", "ip":"127.0.0.1", "port":9081}]'  --db-root=./db/ --log-dir=./logs/

## Start benchmark script
> $ cd ./benchmarks/benchmark/

> $ cargo build --release --jobs 8

> $ ./target/release/benchmark --conns 100 --clients 1000 --key-size 8 --value-size 256 --total 10 --endpoints http://192.168.31.193:9081 --endpoints http://192.168.31.193:9082  --endpoints http://192.168.31.193:9083

> ./target/release/benchmark --conns 100 --clients 1000 --key-size 8 --value-size 256 --total 10 --endpoints http://127.0.0.1:9081 --endpoints http://127.0.0.1:9082  --endpoints http://127.0.0.1:9083 batch