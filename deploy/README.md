# NeonDB Deployment Guide

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

This starts three NeonDB nodes (`neondb-1`, `neondb-2`, `neondb-3`) wired into a Raft consensus cluster. Only `neondb-1` exposes ports to the host. The other two nodes are reachable only within the Docker network.

After the cluster is up, bootstrap Raft on the leader:

```bash
# Initialize single-node cluster on node 1
curl -X POST http://localhost:3001/raft/init

# Add node 2 as a learner
curl -X POST http://localhost:3001/raft/add-learner \
  -H 'Content-Type: application/json' \
  -d '{"node_id": 2, "addr": "neondb-2:3001"}'

# Add node 3 as a learner
curl -X POST http://localhost:3001/raft/add-learner \
  -H 'Content-Type: application/json' \
  -d '{"node_id": 3, "addr": "neondb-3:3001"}'

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
scp target/release/neondb user@server:/tmp/neondb
```

2. Run the installer (requires root):

```bash
sudo bash deploy/install.sh
```

The installer:
- Downloads the latest release binary from GitHub to `/usr/local/bin/neondb`
- Creates the `neondb` system user
- Creates `/var/lib/neondb` owned by `neondb`
- Installs `deploy/neondb.service` to `/etc/systemd/system/`
- Runs `systemctl enable --now neondb`

3. Verify:

```bash
systemctl status neondb
journalctl -u neondb -f
```

---

## Production Checklist

- [ ] **TLS termination** — place NeonDB behind a reverse proxy (nginx, Caddy, Traefik) that handles TLS. NeonDB does not terminate TLS itself.
- [ ] **API key** — set `NEONDB_API_KEY` (or `api_key` in `neondb.toml`) to a strong random secret. All WebSocket clients must supply `Authorization: Bearer <key>`.
- [ ] **Firewall ports** — expose port 3000 (WebSocket) to clients. Keep port 3001 (metrics/admin) firewalled; it has no authentication by default.
- [ ] **WAL backup** — schedule regular backups of the WAL directory (`/var/lib/neondb/wal` or the path set in `neondb.toml`). The WAL is the primary durability mechanism.
- [ ] **Memory limits** — NeonDB is an in-memory database. Set a Docker memory limit or systemd `MemoryMax=` that is at least 2x your expected working set. Eviction is not automatic.
- [ ] **Cluster secret** — in a multi-node deployment, set `NEONDB_CLUSTER_SECRET` on all nodes to authenticate inter-node Raft RPCs.
- [ ] **Snapshot interval** — tune `NEONDB_SNAPSHOT_INTERVAL` (default 1 000 000 WAL entries) to control how often the server writes a full snapshot. Lower values speed up crash recovery at the cost of more disk I/O.
- [ ] **Log level** — set `RUST_LOG=info` in production. `RUST_LOG=debug` produces very high volume output.
