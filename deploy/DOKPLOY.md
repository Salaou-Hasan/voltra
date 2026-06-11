# Deploying NeonDB on your own hardware with Dokploy

[Dokploy](https://dokploy.com) is a self-hostable PaaS (an open-source Heroku/Vercel)
that runs on your own server and deploys apps from Git via Docker. This guide
takes you from a bare Linux box to a running, persistent, auto-restarting,
backed-up NeonDB node.

Everything here is single-node (the right call until you've measured past ~15K
CCU on one machine — see the benchmark notes in `CLAUDE.md`).

---

## 0. Prerequisites

- A Linux server (Ubuntu 22.04/24.04 or Debian 12 recommended). 2 GB RAM is
  enough to start; for a real game give it 4 cores / 8 GB+.
- A domain name pointing at the server (optional but recommended for TLS).
- SSH access as root or a sudo user.

---

## 1. Install Dokploy on your server

SSH into the server and run the official installer (this is Dokploy's own
script — it installs Docker, Docker Swarm, Traefik, and the Dokploy UI):

```bash
curl -sSL https://dokploy.com/install.sh | sh
```

When it finishes it prints a URL like `http://<your-server-ip>:3000`.

> **Port note:** Dokploy's own UI defaults to port **3000**, which is also
> NeonDB's default WebSocket port. We move NeonDB off 3000 in step 4 to avoid
> the clash (or change Dokploy's port during install). Pick one — don't let
> both want 3000.

Open the URL, create your admin account.

---

## 2. Get the code where Dokploy can reach it

Dokploy deploys from a Git repository. Two options:

**A. Push this repo to GitHub/GitLab** (private is fine — you'll connect the
provider in Dokploy → Settings → Git).

**B. Self-host the repo** with Dokploy's built-in Git, or point at any HTTPS
Git URL.

Either way, Dokploy needs the repo containing `Dockerfile` and
`docker-compose.dokploy.yml` (both at the repo root, already committed).

---

## 3. Create the NeonDB service in Dokploy

1. **Projects → Create Project** → name it `neondb`.
2. Inside the project: **Create Service → Compose**.
3. **Provider:** select your Git provider and the NeonDB repo + branch.
4. **Compose Path:** `docker-compose.dokploy.yml`
5. Leave the build to Dokploy — the compose file builds the image from
   `Dockerfile` automatically.

---

## 4. Set environment variables (Dokploy → your service → Environment)

Paste this, replacing the API key with a long random secret
(`openssl rand -hex 32` makes a good one):

```env
NEONDB_API_KEY=replace-with-openssl-rand-hex-32
```

The compose file already sets everything else (host `0.0.0.0`, ports, durable
paths on the volume, hourly backups). If Dokploy's UI is on 3000, also add:

```env
# Move NeonDB's WebSocket port off 3000 so it doesn't fight Dokploy's UI.
NEONDB_PORT=8080
```

…and change the published port in `docker-compose.dokploy.yml` from
`"3000:3000"` to `"8080:8080"` (or edit it in Dokploy's compose editor).

**Optional protocol toggles** (set to `0` to turn a listener off entirely):

```env
NEONDB_REDIS_PORT=6379     # 0 disables the Redis-compatible port
NEONDB_PG_PORT=5432        # 0 disables the PostgreSQL-compatible port
NEONDB_REDIS_PASSWORD=...   # if you expose Redis to the network
NEONDB_PG_PASSWORD=...      # if you expose PostgreSQL to the network
```

---

## 5. Deploy

Click **Deploy**. The first build takes ~10–15 minutes (Rust compiles
wasmtime + rquickjs from source). Watch the build logs in Dokploy.

When it's up, **Logs** should show:

```
[neondb] Listening on 0.0.0.0:8080
[redis] RESP listener on 0.0.0.0:6379
[pg] PostgreSQL wire listener on 0.0.0.0:5432
```

---

## 6. Verify it's healthy

From your laptop (replace host/port):

```bash
# Liveness + stats
curl http://<server-ip>:3001/healthz
# → {"status":"ok","total_rows":0,"active_connections":0,...}

# Redis works
redis-cli -h <server-ip> -p 6379 PING        # → PONG

# PostgreSQL works
psql "host=<server-ip> port=5432 user=you dbname=neondb" -c "SELECT version();"
```

The container's own `HEALTHCHECK` hits `/healthz` too, so Dokploy will show the
service as healthy and auto-restart it if it ever goes down.

---

## 7. Lock it down (do this before real traffic)

1. **Firewall the admin port.** Port **3001** is the metrics + admin console.
   Do NOT expose it to the internet. Either drop the `"3001:3001"` line from the
   compose (you can still reach it from the host via `docker exec`/localhost), or
   restrict it with `ufw`:
   ```bash
   ufw allow 8080      # game clients
   ufw deny  3001      # admin — local only
   ufw enable
   ```
2. **Keep `NEONDB_API_KEY` set.** Without it, anyone who can reach the WS port can
   call reducers. The server logs a SECURITY WARNING at boot if it's missing on a
   non-loopback bind — don't ignore it.
3. **TLS via Traefik (recommended).** Point a domain at the server, then in
   Dokploy → your service → **Domains**, add `game.yourdomain.com` → container
   port `8080`. Dokploy/Traefik provisions a Let's Encrypt cert automatically and
   terminates TLS; your clients connect to `wss://game.yourdomain.com`. (Traefik
   passes WebSocket upgrades through correctly — no extra config needed.)

---

## 8. Backups & data

- Backups run hourly into the `neondb-data` volume
  (`/var/lib/neondb/data/backups`, keeping the last 24). Change the cadence with
  `NEONDB_BACKUP_INTERVAL_SECS` / `NEONDB_BACKUP_KEEP`.
- To copy a backup off the box:
  ```bash
  docker cp <container>:/var/lib/neondb/data/backups ./neondb-backups
  ```
- The named volume `neondb-data` survives redeploys. To wipe and start fresh,
  delete the volume in Dokploy → Volumes (this destroys all data — be sure).

---

## 9. Updating

Push a commit → Dokploy can auto-deploy (enable **Auto Deploy** + a webhook in
the service settings) or click **Redeploy**. The volume persists across
redeploys, so data is preserved. For zero data loss on schema-changing updates,
take a manual backup first (`POST /backup` on the admin port, or
`neondb backup` via `docker exec`).

---

## Connecting your game

- **Unity / Godot:** use the bundled clients (`neondb init --template unity` or
  `godot`) and point them at `wss://game.yourdomain.com` (or
  `ws://<server-ip>:8080` without TLS). The API key goes in the client's auth
  field.
- **Any Redis client:** connect to port 6379.
- **Any Postgres client / ORM:** connect to port 5432, database `neondb`.

That's a production-shaped single node: persistent, auto-restarting, backed up,
TLS-terminated, and firewalled.
