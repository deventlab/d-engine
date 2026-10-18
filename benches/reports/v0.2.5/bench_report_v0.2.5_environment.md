# d-engine v0.2.5 Benchmark: Test Environment

Companion to `bench_report_v0.2.5.md`. Everything here was measured on **October 18, 2026**.

## 1. Instance sets

Three sets of AWS instances were used, all created from the same Terraform configuration (same shapes),
hours apart. A number is only comparable with another number from the same set, or with the caveat that
the instances differ.

| Set | Used for |
|---|---|
| A | d-engine v0.2.4 and v0.2.5 runs (v0.2.4 first, v0.2.5 about 12 minutes later) |
| B | etcd v3.7.2 runs (three fresh-cluster runs) and the second fio calibration |
| C | the first fio calibration and node-to-node ping |

## 2. AWS shape

| Item | Value |
|---|---|
| Region / zone | us-east-1, one availability zone, one subnet (10.0.1.0/24), no placement group |
| Server nodes | 3 x c5.2xlarge (8 vCPU, 16 GiB) |
| Client machine | 1 x c5.4xlarge (16 vCPU, 32 GiB), used by the standalone and etcd benches |
| OS | Ubuntu 20.04 LTS (Canonical AMI), ext4 root filesystem |
| Disk | 50 GB gp3 root volume, 3,000 IOPS, 125 MB/s. The data of every system under test is on this volume |
| Network | security group allows all traffic between members; client to node RTT 0.11 to 0.15 ms (set B, ping averages); node to node 0.09 and 0.18 ms (set C, ping averages to two peers) |

## 3. d-engine runs (set A)

- v0.2.5 = `main` at `b5fce385`. v0.2.4 = tag `v0.2.4`. Both built on the instances with the release profile.
- **Embedded**: `embedded-bench --mode local --batch --key-size 8 --value-size 256 --sequential-keys put`,
  one process per node, the leader's output is used. 100 in-flight (v0.2.4's `--batch` hard-codes 100;
  v0.2.5 gets `--clients 100`). The bench runtime has 2 worker threads on both versions (v0.2.4's bench was
  patched to match v0.2.5's).
- **Standalone**: `three-nodes-standalone` on each node, `standalone-bench` on the client with three
  endpoints. `--conns 200 --clients 200` (v0.2.4 limits in-flight requests by `--conns`, v0.2.5 by `--clients`;
  both are 200). Single-client write: `--conns 1 --clients 1`.
- Payload: 8 B keys, 256 B values, sequential keys; hot-key uses 10 keys. 10,000 requests for the
  single-client write, 100,000 for the others. 3 rounds; cluster restarted and data wiped before each round.
- Configuration (same file serves both versions; each ignores keys it does not know):
  `max_batch_size = 200`, snapshot off, `unified_db = false`, backpressure 10,000 writes / 50,000 reads,
  election timeout 1,500 to 2,500 ms and lease 500 ms (standalone), ReadActor 2,048/1,024 (embedded) and
  10,240/2,000 (standalone). Replication settings are left at their defaults.
- **Persistence**: v0.2.4 `MemFirst` (asynchronous flush, 1,000 ms idle interval). v0.2.5 advances the durable
  index only after `fdatasync` (RPO=0). The raft log is stored in RocksDB.

## 4. etcd runs (set B)

- etcd v3.7.2 official linux-amd64 binary, downloaded on the instances. Three members, one per node, data
  directory on the root volume, default settings apart from the cluster flags.
- The `benchmark` tool is not in the release package. It was built from the v3.7.2 source and
  cross-compiled to a static linux/amd64 binary (Go 1.26.8), then run on the client machine.
- Commands are etcd's documented ones, except that `--target-leader` no longer exists in v3.7.2, so the
  leader's address is passed instead. 8 B keys, 256 B values, sequential keys. Reads fetch one key through
  all three members.
- Cases per run: writes at 1, 100, 200 and 1,000 in-flight to the leader and 1,000 in-flight to all
  members; linearizable and serializable reads at 1, 100, 200 and 1,000 in-flight. 10,000 requests at 1
  in-flight, 100,000 otherwise. `--conns` equals `--clients`, except at 1,000 in-flight (100 connections, as
  in etcd's documentation).
- Three runs, each on a freshly wiped cluster. WAL fsync averages come from the change of
  `etcd_disk_wal_fsync_duration_seconds` before and after each case.

## 5. Published etcd numbers (not used for any comparison here)

etcd's documentation table comes from Google Cloud: 3 servers with 8 vCPU, 16 GB and a 50 GB SSD, a client with
16 vCPU and 30 GB, Ubuntu 17.04, etcd 3.2.0, Go 1.8.3. The disk type is not stated. The cloud, disk, software
version and age all differ from this report, so the earlier draft's comparison with those numbers was removed.

## 6. Calibration and other runs

- **fio** (`--ioengine=sync --fdatasync=1`, 1 KiB, iodepth 1, 20 s) on the same volume type: append write
  2.710 ms (set C) and 2.718 ms (set B) average `fdatasync`; preallocated overwrite 1.288 ms (set B).
  4 KiB overwrite 0.873 ms and 16 KiB overwrite 2.855 ms (set C).
- **Local runs** (not in the tables here): Mac mini M2, three nodes on loopback, each on its own RAM disk, where
  `fdatasync` costs about 0.05 ms. They show the RPO=0 cost when the disk is almost free.
