# Voltra Deployment Guide

## Docker Quickstart (single node)

```bash
docker compose -f docker-compose.single.yml up -d
```

The server will be reachable at:
- WebSocket: `ws://localhost:3000`
- Metrics / HTTP API: `http://localhost:3001`

---

## 3-Node Raft Cluster with Docker Compose

```bash
docker compose up -d
```

This starts three Voltra nodes (`voltra-1`, `voltra-2`, `voltra-3`) wired into a Raft consensus cluster. Only `voltra-1` exposes ports to the host. The other two nodes are reachable only within the Docker network.

After the cluster is up, bootstrap Raft on the leader:

```bash
# Initialize single-node cluster on node 1
curl -X POST http://localhost:3001/raft/init

# Add node 2 as a learner
curl -X POST http://localhost:3001/raft/add-learner \
  -H 'Content-Type: application/json' \
  -d '{"node_id": 2, "addr": "voltra-2:3001"}'

# Add node 3 as a learner
curl -X POST http://localhost:3001/raft/add-learner \
  -H 'Content-Type: application/json' \
  -d '{"node_id": 3, "addr": "voltra-3:3001"}'

# Promote all three to voters
curl -X POST http://localhost:3001/raft/change-membership \
  -H 'Content-Type: application/json' \
  -d '[1, 2, 3]'
```

Check cluster health:

```bash
curl http://localhost:3001/raft/metrics
```

---

## Bare Metal with systemd

1. Copy the binary to the target machine:

```bash
scp target/release/voltra user@server:/tmp/voltra
```

2. Run the installer (requires root):

```bash
sudo bash deploy/install.sh
```

The installer:
- Downloads the latest release binary from GitHub to `/usr/local/bin/voltra`
- Creates the `voltra` system user
- Creates `/var/lib/voltra` owned by `voltra`
- Installs `deploy/voltra.service` to `/etc/systemd/system/`
- Runs `systemctl enable --now voltra`

3. Verify:

```bash
systemctl status voltra
journalctl -u voltra -f
```

---

## Production Checklist

- [ ] **TLS termination** — place Voltra behind a reverse proxy (nginx, Caddy, Traefik) that handles TLS. Voltra does not terminate TLS itself.
- [ ] **API key** — set `VOLTRA_API_KEY` (or `api_key` in `voltra.toml`) to a strong random secret. All WebSocket clients must supply `Authorization: Bearer <key>`.
- [ ] **Firewall ports** — expose port 3000 (WebSocket) to clients. Keep port 3001 (metrics/admin) firewalled; it has no authentication by default.
- [ ] **WAL backup** — schedule regular backups of the WAL directory (`/var/lib/voltra/wal` or the path set in `voltra.toml`). The WAL is the primary durability mechanism.
- [ ] **Memory limits** — Voltra is an in-memory database. Set a Docker memory limit or systemd `MemoryMax=` that is at least 2x your expected working set. Eviction is not automatic.
- [ ] **Cluster secret** — in a multi-node deployment, set `VOLTRA_CLUSTER_SECRET` on all nodes to authenticate inter-node Raft RPCs.
- [ ] **Snapshot interval** — tune `VOLTRA_SNAPSHOT_INTERVAL` (default 1 000 000 WAL entries) to control how often the server writes a full snapshot. Lower values speed up crash recovery at the cost of more disk I/O.
- [ ] **Log level** — set `RUST_LOG=info` in production. `RUST_LOG=debug` produces very high volume output.
