# 📡 Prometheus + Node Exporter Integration Guide (for `d-engine` Cluster Monitoring)

This guide is part of the `d-engine` distributed system monitoring setup.

It explains how to:

- Install [`node_exporter`](https://github.com/prometheus/node_exporter) on each machine to expose system-level metrics (such as open file descriptor usage),
- Configure Prometheus to scrape metrics from all nodes in the `d-engine` cluster,
- Visualize these metrics in Grafana.

Monitoring these metrics is essential for identifying issues such as file descriptor exhaustion, which can lead to connection failures or degraded performance in `d-engine` nodes.

> This setup assumes that each `d-engine` node runs services on ports `8081`, `8082`, `8083`, and that `node_exporter` is accessible via port `9100`.

---

## (Optional): Install `node_exporter` on Each Node to Monitor System Metrics

Please comment the configure in prometheus.yml, if it is not needed.

Make sure to start node_exporter on each node, if you want to monitor system metrics.

Download the latest release from the [official GitHub page](https://github.com/prometheus/node_exporter/releases).

```bash
# Example for Linux amd64
wget https://github.com/prometheus/node_exporter/releases/download/v1.8.1/node_exporter-1.8.1.linux-amd64.tar.gz
tar -xzf node_exporter-1.8.1.linux-amd64.tar.gz
cd node_exporter-1.8.1.linux-amd64

# Start the exporter
./node_exporter
```
