# Deployment

---

## Single Binary

Build a release binary:

```bash
cargo build --release
```

The binary is at `target/release/neondb` (or `neondb.exe` on Windows). It has no runtime dependencies — copy it to any server and run it.

```bash
scp target/release/neondb user@server:/usr/local/bin/
ssh user@server "neondb start --host 0.0.0.0"
```

---

## Docker

A `Dockerfile` and `docker-compose.yml` are included in the project root.

### Single node

```bash
docker compose up -d
```

The compose file starts one Voltra container with WebSocket on port 3000 and the admin HTTP endpoint on port 3001.

### Three-node cluster

```yaml
# docker-compose.cluster.yml (example)
services:
  node1:
    image: neondb
    environment:
      NEONDB_HOST: 0.0.0.0
      NEONDB_PORT: 3000
      NEONDB_METRICS_PORT: 3001
      NEONDB_API_KEY: changeme
      NEONDB_WAL_PATH: /data/neondb.wal
      NEONDB_SNAPSHOT_DIR: /data/snapshots
    volumes:
      - node1_data:/data
    ports:
      - "3000:3000"
      - "3001:3001"

  node2:
    image: neondb
    environment:
      NEONDB_PORT: 3000
      NEONDB_METRICS_PORT: 3001
      NEONDB_API_KEY: changeme
      NEONDB_WAL_PATH: /data/neondb.wal
    volumes:
      - node2_data:/data
    ports:
      - "3010:3000"
      - "3011:3001"

  node3:
    image: neondb
    environment:
      NEONDB_PORT: 3000
      NEONDB_METRICS_PORT: 3001
      NEONDB_API_KEY: changeme
      NEONDB_WAL_PATH: /data/neondb.wal
    volumes:
      - node3_data:/data
    ports:
      - "3020:3000"
      - "3021:3001"
```

After starting all three nodes, bootstrap the Raft cluster (see [docs/cluster.md](cluster.md)).

---

## Systemd (Linux bare-metal)

```bash
sudo cp target/release/neondb /usr/local/bin/neondb
sudo mkdir -p /var/lib/neondb/snapshots

sudo tee /etc/systemd/system/neondb.service > /dev/null << 'EOF'
[Unit]
Description=Voltra Game Backend
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=neondb
Group=neondb
ExecStart=/usr/local/bin/neondb start --host 0.0.0.0
Environment=NEONDB_API_KEY=REPLACE_WITH_STRONG_KEY
Environment=NEONDB_WAL_PATH=/var/lib/neondb/neondb.wal
Environment=NEONDB_SNAPSHOT_DIR=/var/lib/neondb/snapshots
Environment=NEONDB_FSYNC_INTERVAL_MS=100
Environment=NEONDB_METRICS_PORT=3001
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

# Create a dedicated system user
sudo useradd -r -s /bin/false neondb
sudo chown -R neondb:neondb /var/lib/neondb

sudo systemctl daemon-reload
sudo systemctl enable --now neondb
sudo systemctl status neondb
```

---

## TLS

Voltra does not terminate TLS internally. Terminate TLS at a reverse proxy in front of it.

### Caddy (automatic Let's Encrypt)

```
# Caddyfile
game.example.com {
    reverse_proxy localhost:3000 {
        # WebSocket upgrade is handled automatically by Caddy
    }
}
```

### nginx

```nginx
server {
    listen 443 ssl;
    server_name game.example.com;

    ssl_certificate     /etc/letsencrypt/live/game.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/game.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "Upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400;
    }
}
```

Clients connect to `wss://game.example.com` and the proxy upgrades to WebSocket over TLS.

---

## Production Checklist

- [ ] Set a strong `NEONDB_API_KEY` (at least 32 random characters).
- [ ] Set `NEONDB_WAL_PATH` to a persistent, fsync-capable disk path (not OS temp).
- [ ] Set `NEONDB_SNAPSHOT_DIR` to a persistent disk path.
- [ ] Set `NEONDB_FSYNC_INTERVAL_MS=100` (or lower if you need stronger durability).
- [ ] Configure TLS via a reverse proxy (Caddy or nginx).
- [ ] Set `NEONDB_MAX_CONNECTIONS` to a value that reflects your server RAM.
- [ ] Configure `NEONDB_REDUCER_TIMEOUT_MS` to prevent runaway reducers.
- [ ] Add at least one `[[scheduler]]` entry for session cleanup if you track sessions.
- [ ] Back up the WAL directory and snapshot directory on a schedule.
- [ ] Monitor `/health` and `/metrics` from an external health checker.
- [ ] For clusters: set `NEONDB_CLUSTER_SECRET` to prevent unauthorized peer joins.
- [ ] Run `neondb seed seed.json` to pre-populate initial game data.
- [ ] Set `RUST_LOG=warn` in production to reduce log volume.

### WAL Backup

The WAL is a plain binary file at `$NEONDB_WAL_PATH`. Snapshots are in `$NEONDB_SNAPSHOT_DIR`. To back up:

```bash
# Stop or pause writes briefly, then copy both directories
rsync -a /var/lib/neondb/ backup-host:/backups/neondb/$(date +%Y%m%d)/
```

To restore: copy the WAL and snapshot files back, then start the server. Startup replays the WAL automatically.

### Memory Limits

Voltra's in-memory store grows without bound as rows are added. Monitor RSS with `/metrics` and set OS-level limits if needed:

```
# systemd memory limit
MemoryMax=4G
```

JS reducer heap is not capped (a hard limit is not exposed by Boa 0.19). WASM reducers are capped by `ResourceLimiter` in Wasmtime. If you run untrusted JS reducers, use the WASM backend instead.
