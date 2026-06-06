# NeonDB → Local Coolify Deployment Guide

## Your Setup
- **Hardware**: Windows 11 (self-hosted)
- **Coolify Location**: WSL2 Ubuntu
- **NeonDB Container**: Docker Desktop (Windows)
- **Network**: localhost (same machine)

## Installation Progress
Coolify is installing in WSL2. This will take 5-15 minutes. The installer will:
1. Download Coolify Docker images
2. Set up Coolify application
3. Create admin account
4. Initialize database

## After Coolify Installation Completes

### Step 1: Access Coolify Web Dashboard
```
URL: http://localhost:3000
(When installation finishes, you'll see the access URL and credentials)
```

### Step 2: Login to Coolify
- Use the credentials from installation output
- Set up your admin account

### Step 3: Add NeonDB Application

In Coolify Dashboard:
1. Click **"Applications"** → **"Add Application"** → **"Docker Compose"**
2. **Paste this configuration**:

```yaml
services:
  neondb:
    image: neondb-neondb:latest
    ports:
      - "8000:8000"
      - "8001:8001"
    volumes:
      - neondb-data:/data/wal
    environment:
      NEONDB_HOST: 0.0.0.0
      NEONDB_PORT: 8000
      NEONDB_METRICS_PORT: 8001
      NEONDB_WAL_PATH: /data/wal/neondb.wal
      NEONDB_WAL_BATCH_SIZE: "100000"
      NEONDB_WAL_BATCH_INTERVAL_MS: "100"
      NEONDB_UNSAFE_NO_FSYNC: "false"
      NEONDB_MAX_CONNECTIONS: "200"
      NEONDB_REDUCER_TIMEOUT_MS: "5000"
      NEONDB_SHARD_ID: "0"
      NEONDB_SHARD_COUNT: "1"
      RUST_LOG: info
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "8000"]
      interval: 30s
      timeout: 5s
      retries: 5
      start_period: 5s
    restart: unless-stopped
    networks:
      - coolify
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 1G
        reservations:
          cpus: '1'
          memory: 512M

volumes:
  neondb-data:
    driver: local

networks:
  coolify:
    external: true
```

3. **Name your application**: `neondb`
4. **Click "Deploy"**

### Step 4: Verify Deployment
- Check Coolify dashboard for "Running" status
- Test metrics: `curl http://localhost:8001/metrics`
- Monitor logs in Coolify UI

## Network Access from WSL2

Since Coolify runs in WSL2 and NeonDB runs in Docker Desktop (Windows), they're on the same network:
- **Coolify can reach**: `host.docker.internal:8000` (NeonDB WebSocket)
- **You can access**: `http://localhost:3000` (Coolify) and `http://localhost:8000` (NeonDB)

## Key NeonDB Settings Pre-Configured

| Setting | Value | Purpose |
|---------|-------|---------|
| WAL Batch Size | 100,000 | Optimize write throughput |
| Batch Interval | 100ms | Balance latency vs batching |
| Max Connections | 200 | Handle concurrent clients |
| Shard Config | 0/1 | Single node (ready to scale) |
| Memory Limit | 1GB | Resource constrained setup |
| CPU Limit | 2 cores | Respects Windows 11 limits |

## Monitoring

### From Coolify UI
- Application status and logs
- Resource usage
- Restart history

### From Windows
```powershell
# Check NeonDB container
docker-compose logs neondb -f

# Check metrics
Invoke-WebRequest http://localhost:8001/metrics
```

### From WSL2
```bash
# Check Coolify status
docker ps

# View Coolify logs
docker logs coolify -f
```

## Scaling to Multiple Nodes (Future)

Once you're comfortable with single-node, you can:
1. Deploy additional NeonDB instances with different `NEONDB_SHARD_ID` (1, 2, 3...)
2. Add load balancer (HAProxy) in Coolify
3. Update `NEONDB_SHARD_COUNT` to total number of shards

## Troubleshooting

### Coolify won't start
```bash
# Check WSL2 status
wsl -d Ubuntu -e docker ps

# View Coolify logs
wsl -d Ubuntu -e docker logs coolify
```

### NeonDB not visible in Coolify
- Ensure Docker Desktop is running
- Restart Docker Desktop
- Ensure `neondb-neondb:latest` image exists: `docker images`

### Connection issues
- Coolify (WSL2) needs to reach Docker Desktop (Windows)
- Use `host.docker.internal` if running NeonDB inside Coolify
- Or keep NeonDB in Docker Desktop and reference via localhost

## Files Needed

You have everything ready:
- ✅ `docker-compose.yml` - For reference (already built)
- ✅ `NEONDB` binary - Running in Docker
- ✅ `Configuration` - All env vars documented
- ✅ `COOLIFY_DEPLOYMENT.md` - Full reference guide

## Expected Coolify Installation Time
- Download: ~2-3 minutes
- Setup: ~5-10 minutes
- **Total**: 7-13 minutes

Check console output for:
```
✓ Successfully installed Coolify
Coolify is available at: http://localhost:3000
```

---

**Next Step**: Wait for Coolify installation to complete, then follow Step 1 above.
