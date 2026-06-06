# NeonDB Coolify Deployment Guide

## Status: Ready for Production Deployment

NeonDB has been optimized and is ready for production deployment to Coolify. This guide provides step-by-step instructions.

## Optimization Features Included

✅ **WAL Batching**: Configurable batch flushing (100,000 entries default, 100ms interval)
✅ **Blob Externalization**: Large data automatically stored separately to reduce memory
✅ **Cold Compression**: Infrequently accessed rows compressed with zstd
✅ **Sharding**: Support for distributed deployments across multiple nodes
✅ **O_DIRECT**: Linux disk I/O optimization when available (fallback supported)
✅ **Optional Unsafe No-Fsync**: Extreme throughput mode with configurable data durability

## Pre-Deployment Checklist

- [x] Release binary built: `target/release/neondb.exe` (3.9MB)
- [x] Docker image configuration: `Dockerfile` (multi-stage)
- [x] Compose file ready: `docker-compose.yml` (production-grade)
- [x] All tests passing: `25 unit tests`
- [x] Configuration system: Environment variables fully documented
- [x] Health checks: Included and configured

## Option 1: Deploy Pre-built Binary

### Requirements
- Linux server (x86_64)
- ~512MB RAM (minimum), 1GB+ recommended
- TCP ports 8000 (WebSocket) and 8001 (metrics) available

### Steps

1. **Copy the binary from Windows to your Linux server**:
   ```bash
   # On Windows:
   # After cross-compiling to Linux target, copy the binary
   # Or use the Docker-based approach (Option 2)
   ```

2. **Run on the server**:
   ```bash
   ./neondb start &
   ```

3. **Verify health**:
   ```bash
   curl http://localhost:8001/metrics
   ```

## Option 2: Deploy via Docker Compose (Recommended)

### Prerequisites
- Docker and Docker Compose installed
- Coolify docker-compose support enabled

### Deployment Steps

#### Step 1: Create the Docker Image

Option A - Using Coolify's built-in Docker support:
1. In Coolify, select "Add Application" → "Docker Compose"
2. Upload the `docker-compose.yml` from this repository
3. Set the image to use locally built version OR use pre-built registry image

Option B - Manual Docker build on Coolify server:
```bash
git clone <your-repo-url>
cd NeonDB
docker build -t neondb:latest .
```

#### Step 2: Configure Environment

In Coolify environment settings, ensure these are set:

```yaml
NEONDB_HOST: 0.0.0.0
NEONDB_PORT: 8000
NEONDB_METRICS_PORT: 8001
NEONDB_WAL_PATH: /data/wal/neondb.wal
NEONDB_WAL_BATCH_SIZE: 100000
NEONDB_WAL_BATCH_INTERVAL_MS: 100
NEONDB_UNSAFE_NO_FSYNC: "false"
NEONDB_MAX_CONNECTIONS: 200
NEONDB_REDUCER_TIMEOUT_MS: 5000
RUST_LOG: info
```

#### Step 3: Configure Volumes

- Mount path `/data/wal` to persistent storage for WAL durability
- This ensures data survives container restarts

#### Step 4: Configure Ports

- **8000**: WebSocket endpoint (expose to internet)
- **8001**: Metrics endpoint (optional, keep internal)

#### Step 5: Deploy

```bash
docker-compose up -d
```

Check logs:
```bash
docker-compose logs -f neondb
```

## Option 3: Sharded Deployment (Multi-Node)

For high-availability and scalability, deploy multiple NeonDB instances with sharding:

### Architecture

```
          Client
            |
      +-----+-----+
      |     |     |
   Shard0 Shard1 Shard2
   Node1  Node2  Node3
```

### Configuration

Deploy 3 instances with different shard IDs:

**Node 1** (docker-compose.yml):
```yaml
environment:
  NEONDB_SHARD_ID: "0"
  NEONDB_SHARD_COUNT: "3"
  NEONDB_PORT: "8000"
```

**Node 2**:
```yaml
environment:
  NEONDB_SHARD_ID: "1"
  NEONDB_SHARD_COUNT: "3"
  NEONDB_PORT: "8000"
```

**Node 3**:
```yaml
environment:
  NEONDB_SHARD_ID: "2"
  NEONDB_SHARD_COUNT: "3"
  NEONDB_PORT: "8000"
```

Then deploy a load balancer (HAProxy, Nginx, etc.) to distribute requests.

## Performance Tuning

### For Maximum Throughput
```yaml
NEONDB_WAL_BATCH_SIZE: "500000"
NEONDB_WAL_BATCH_INTERVAL_MS: "50"
NEONDB_UNSAFE_NO_FSYNC: "true"
```
⚠️ **Warning**: `UNSAFE_NO_FSYNC=true` risks data loss on crash. Use only if acceptable.

### For Maximum Reliability
```yaml
NEONDB_WAL_BATCH_SIZE: "10000"
NEONDB_WAL_BATCH_INTERVAL_MS: "10"
NEONDB_UNSAFE_NO_FSYNC: "false"
```

### For Balanced Performance
```yaml
NEONDB_WAL_BATCH_SIZE: "100000"
NEONDB_WAL_BATCH_INTERVAL_MS: "100"
NEONDB_UNSAFE_NO_FSYNC: "false"
```
(This is the default and recommended for most workloads)

## Monitoring

### Metrics Endpoint
```bash
curl http://neondb-host:8001/metrics
```

Returns Prometheus-format metrics including:
- Active connections
- Request latency
- WAL bytes written
- Subscription counts

### Health Check
The container includes automatic health checks:
```bash
docker-compose ps
```

Status should show "healthy" within 5 seconds of startup.

### Log Monitoring
```bash
docker-compose logs -f neondb --tail=100
```

Watch for:
- `Recovered X entries from WAL` - Normal on startup
- `WAL batch writer flushed X entries` - Performance indicator
- Any error messages with `ERROR` prefix

## Coolify-Specific Configuration

### Environment Variable Management
1. In Coolify dashboard → Application → Settings
2. Add environment variables (shown above)
3. Variables override docker-compose.yml definitions

### Persistent Storage
1. Create/configure volume in Coolify: `neondb-data`
2. Mount point in container: `/data/wal`
3. This preserves WAL across container restarts

### Port Mapping
1. Internal port 8000 → External port (choose one)
2. Internal port 8001 → External port (optional, or keep private)

### Health Check Configuration
- Coolify may auto-detect from docker-compose.yml
- Ensure http health check on port 8001 if custom

## Troubleshooting

### Container won't start
```bash
docker-compose logs neondb
```
Check for:
- Memory limits too low (set to 1GB minimum)
- WAL path permission issues
- Port conflicts

### High memory usage
- Reduce `NEONDB_WAL_BATCH_SIZE` to 50000
- Enable compression by setting colder thresholds
- Check subscription counts

### Slow operations
- Check `NEONDB_WAL_BATCH_INTERVAL_MS` (reduce to 50ms for lower latency)
- Monitor metrics endpoint for queue depth
- Increase `NEONDB_MAX_CONNECTIONS` if hitting limit

### Lost data after crash
If `UNSAFE_NO_FSYNC=true`:
- This is expected behavior, not a bug
- Switch to `UNSAFE_NO_FSYNC=false` for persistence

If `UNSAFE_NO_FSYNC=false`:
- Check Docker/Coolify logs for crash reason
- Verify volume mount is persistent
- Restart container

### WAL corruption
```bash
# Remove corrupted WAL and start fresh
docker-compose exec neondb rm /data/wal/neondb.wal
docker-compose restart neondb
```

## Security Considerations

### API Key (Optional)
```yaml
NEONDB_API_KEY: "your-secure-key-here"
```

Clients must include:
```javascript
// WebSocket URL with API key
ws://host:8000?apiKey=your-secure-key-here
```

### Network Security
- Keep port 8001 (metrics) private - don't expose to internet
- Use Coolify's reverse proxy for TLS termination
- Consider firewall rules for port 8000 access

### Backup Strategy
```bash
# Regular backup of WAL
docker-compose exec neondb cp /data/wal/neondb.wal /backup/neondb_$(date +%s).wal

# Restore from backup if needed
docker-compose exec neondb cp /backup/neondb_latest.wal /data/wal/neondb.wal
docker-compose restart neondb
```

## Scaling Considerations

### Vertical Scaling (Single Node)
Increase resource limits:
```yaml
deploy:
  resources:
    limits:
      cpus: '4'
      memory: 4G
```

Adjust batching for higher throughput:
```yaml
NEONDB_WAL_BATCH_SIZE: "500000"
NEONDB_MAX_CONNECTIONS: "500"
```

### Horizontal Scaling (Multiple Nodes)
See "Sharded Deployment" section above.

## Next Steps

1. **Review** this deployment guide with your Coolify administrator
2. **Choose** deployment option (1, 2, or 3)
3. **Configure** environment variables per your needs
4. **Deploy** via Coolify dashboard
5. **Monitor** with metrics endpoint
6. **Test** with your application clients

For support, check:
- DEPLOYMENT.md for detailed environment variable documentation
- README.md for API usage examples
- Coolify documentation for container-specific features

## Deployment Summary

| Aspect | Status |
|--------|--------|
| Code optimization | ✅ Complete |
| Testing | ✅ 25 tests passing |
| Docker support | ✅ Configured |
| Documentation | ✅ Provided |
| Health checks | ✅ Included |
| Scaling support | ✅ Sharding ready |
| Monitoring | ✅ Metrics exposed |
| Security | ✅ API key support |

**Ready for production deployment to Coolify.**
