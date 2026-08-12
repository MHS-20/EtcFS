# EtcFS

[![CI](https://github.com/mhs-20/EtcFS/actions/workflows/ci.yml/badge.svg)](https://github.com/mhs-20/EtcFS/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/mhs-20/EtcFS/branch/main/graph/badge.svg)](https://codecov.io/gh/mhs-20/EtcFS)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue)](https://mhs-20.github.io/EtcFS/)

**A cluster-aware filesystem for shared raw block devices — the piece AWS and Kubernetes tell you to bring yourself.**

AWS EBS Multi-Attach will attach one io2 volume to sixteen instances at once. Kubernetes will hand it to you as a `ReadWriteMany` `volumeMode: Block` volume. Both then stop, and [the EBS CSI driver's documentation says why](https://github.com/kubernetes-sigs/aws-ebs-csi-driver/blob/master/docs/multi-attach.md): using it safely "requires application-level coordination (e.g. via I/O fencing)", and failure to do so "can result in data loss and silent data corruption". Put ext4 on a Multi-Attach volume and mount it twice and you will destroy it. The platform gives you the shared device and declines to make it safe.

EtcFS is what goes on top. **etcd/Raft is the only source of durable truth**, and the shared device holds nothing but file bytes. No on-disk filesystem format, no kernel module, no bespoke distributed lock manager — a userspace FUSE daemon on each node presents POSIX semantics, backed by etcd for everything structural (namespace, inode metadata, locks, allocation) and direct block I/O for file content. The I/O fencing that AWS says you need is three independent layers, one of them enforced by the drive itself — see [Fencing](https://mhs-20.github.io/EtcFS/architecture/fencing/self-fencing-watchdog/).

Traditional cluster filesystems (GFS2, OCFS2) keep durable truth *on disk* — inodes, bitmaps, a journal — and bolt a distributed lock manager on top to arbitrate access to it. EtcFS inverts that: etcd's replicated Raft log *is* the durable truth for every structural fact, and the disk is demoted to a flat, unformatted array of bytes addressed by extents `(logical_offset, disk_offset, length)` recorded in etcd. Atomicity, consistency, and metadata recovery come from etcd's existing quorum-replicated log almost for free, instead of a bespoke recovery protocol — at the cost of every structural operation being an etcd round trip, mitigated by client-side caching and keeping the hot data path (reads/writes to already-allocated extents) on direct block I/O with no etcd round trip at all.

Status: implemented and under hardening. See [State](docs/index.md#state) before relying on this for real data.

## Quick start

Requires Go 1.24+, a C11 compiler, `libfuse3-dev`.

```bash
make all      # bin/etcfuse-meta (Go), bin/etcfuse (C), bin/etcfsctl
make check    # lint + test — also wired as a pre-push git hook (make hooks)
```

A full 3-node cluster on one machine, no cloud account needed:

```bash
docker compose -f deploy/docker/docker-compose.yml up -d --build
# FUSE mount at /mnt/etcfuse inside each etcfuse<N> container
docker compose -f deploy/docker/docker-compose.yml down -v
```

Or install a released binary/package/container and provision real
infrastructure with the Terraform module — see
**[Deployment](docs/deployment/index.md)**.

```bash
etcfuse-meta --listen=/tmp/etcfuse.sock --etcd-endpoints=http://127.0.0.1:2379 \
  --node-id=n1 --cluster-name=my-cluster --lease-ttl=10s --block-device=/dev/nvme1n1
etcfuse --socket=/tmp/etcfuse.sock --node-id=n1 /mnt/etcfuse

etcfsctl --etcd-endpoints=http://127.0.0.1:2379 status
```

Full flag reference and every config knob:
[Configuration](docs/deployment/configuration.md).

## Documentation

The [documentation site](https://mhs-20.github.io/EtcFS/) covers everything
beyond this quick start:

| | |
|---|---|
| **[Deployment](docs/deployment/index.md)** | Terraform module, binaries/containers, configuration, `etcfsctl`, Prometheus + Grafana |
| **[Architecture](https://mhs-20.github.io/EtcFS/architecture/fuse/fuse-architecture/)** | FUSE layer, metadata model, storage substrate, consistency, fencing, reliability, cluster ops — one doc per subsystem |
| **[Reports](https://mhs-20.github.io/EtcFS/reports/chaos-reports/2026-07-30-fresh-cluster-per-scenario/)** | Chaos-testing and benchmark results, by date |
| **[Background](https://mhs-20.github.io/EtcFS/background/etcd_raft_research/)** | Research behind the design decisions: etcd/Raft internals, cluster-FS survey, VFS/FUSE, userspace FS patterns |

Read the relevant subsystem doc before making a design decision that touches
fencing, the write path, or the metadata schema.

## Contributing

[`CONTRIBUTING.md`](CONTRIBUTING.md) — build/test setup, the commit
convention the release automation reads, what a change to a safety-critical
path needs. [`SECURITY.md`](SECURITY.md) — how to report a vulnerability
privately. [`AGENTS.md`](AGENTS.md) — conventions for AI agents working in
this repo.
