# Voltra Operations Runbook

Day-2 operations for a running Voltra instance: backups, health checks, disaster
recovery, and tuning knobs. Read this before you take a production deployment off
its training wheels.

---

## WAL and Snapshot Lifecycle

Voltra persistence is a two-layer system:

1. **WAL** (`voltra.wal` by default) — every committed reducer call appends one
   entry. fsync'd in batches every `fsync_interval_ms` (default 100 ms).
2. **Snapshots** — a full dump of `TableStore` to `snapshot_dir/snapshot_<seq>.bin`
   every `snapshot_interval` WAL entries (default 100,000 in `voltra.toml`).

On startup the server loads the **latest snapshot**, then replays only the WAL
entries written **after** the snapshot's `last_seq`. The WAL file is not truncated
automatically — see "WAL rotation" below.

### Default paths

| What            | Default                      | Override                  |
| --------------- | ---------------------------- | ------------------------- |
| WAL file        | `$TEMP/voltra.wal`           | `wal_path` / `VOLTRA_WAL_PATH` |
| Snapshot dir    | `$TEMP/voltra_snapshots/`    | `snapshot_dir` / `VOLTRA_SNAPSHOT_DIR` |
| Blob store dir  | `$TEMP/voltra_blobs/`        | `VOLTRA_BLOB_PATH`        |

On Linux `$TEMP` is `/tmp` which is wiped on reboot — **set these explicitly in
production**.

### Manual snapshot trigger

There is no CLI flag to force a snapshot today. The supported way to take an
immediate snapshot is:

1. Reduce `snapshot_interval` temporarily via `VOLTRA_SNAPSHOT_INTERVAL=1`.
2. Issue any reducer call (e.g. `voltra call noop`).
3. Restart the server with the normal interval restored.

A snapshot is also written automatically when the server reaches
`snapshot_interval` writes since the last snapshot. Restarting the server
**after** `snapshot_interval` writes is safe; restarting **before** simply
replays the WAL from the previous snapshot.

### WAL rotation

Voltra does **not** auto-rotate the WAL file. It grows unbounded between
restarts. Recommended rotation procedure:

```bash
# 1. Stop the server (graceful).
systemctl stop voltra

# 2. Confirm a recent snapshot exists.
ls -lh /var/lib/voltra/snapshots/

# 3. Move the old WAL aside (do NOT delete until the next start succeeds).
mv /var/lib/voltra/voltra.wal /var/lib/voltra/voltra.wal.old

# 4. Start. The server loads the snapshot, finds an empty WAL, starts fresh.
systemctl start voltra

# 5. Verify health, then archive the old WAL.
curl http://127.0.0.1:3001/healthz
mv /var/lib/voltra/voltra.wal.old /backup/wal/$(date +%Y%m%d).wal
```

Recommended cadence: rotate after each successful snapshot, or weekly — whichever
comes first.

---

## Health Checks

Voltra exposes a metrics HTTP server on `metrics_port` (default `port + 1` =
`3001`).

| Endpoint        | Purpose                                              |
| --------------- | ---------------------------------------------------- |
| `GET /healthz`  | Returns `OK` (HTTP 200) when the server is alive.    |
| `GET /metrics`  | Prometheus text format — connections, write/read TPS, reducer latency histograms. |
| `GET /stats`    | JSON: row counts per table, WAL size, snapshot info. |
| `POST /seed`    | Bulk-load rows for dev/test (bypasses WAL).          |

Example liveness probe (Kubernetes / Docker / systemd):

```bash
curl --fail --max-time 2 http://127.0.0.1:3001/healthz || exit 1
```

---

## Backup Recipe

A complete backup is **snapshot directory + current WAL**. Both are needed to
guarantee no committed write is lost.

```bash
#!/usr/bin/env bash
set -euo pipefail

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR=/backup/voltra/$DATE
mkdir -p "$BACKUP_DIR"

# 1. Snapshot directory (multiple files OK — restore uses the highest seq).
cp -a /var/lib/voltra/snapshots "$BACKUP_DIR/"

# 2. Current WAL. Copying while the server is running is safe — the WAL is
#    append-only and fsync'd; you'll get a consistent prefix.
cp /var/lib/voltra/voltra.wal "$BACKUP_DIR/"

# 3. Optional: blob store (only if you write large blobs via BlobStore).
cp /var/lib/voltra/blobs.bin "$BACKUP_DIR/" 2>/dev/null || true

# 4. Config — restoring without it is a coin-flip.
cp /etc/voltra/voltra.toml "$BACKUP_DIR/"

# 5. Compress + ship offsite.
tar -czf "$BACKUP_DIR.tar.gz" -C /backup/voltra "$DATE"
rm -rf "$BACKUP_DIR"
```

Run via `cron` every 6 hours. Keep 14 days of daily backups + 12 months of
monthly ones.

---

## Disaster Recovery

To restore from backup onto a fresh host:

```bash
# 1. Stop the server if running.
systemctl stop voltra

# 2. Untar the backup.
tar -xzf /backup/voltra/20260607_030000.tar.gz -C /tmp/restore

# 3. Place files where the config expects them.
mkdir -p /var/lib/voltra
cp -a /tmp/restore/*/snapshots /var/lib/voltra/
cp    /tmp/restore/*/voltra.wal /var/lib/voltra/
cp    /tmp/restore/*/blobs.bin  /var/lib/voltra/ 2>/dev/null || true
cp    /tmp/restore/*/voltra.toml /etc/voltra/

# 4. Start. The server auto-loads the latest snapshot and replays the WAL.
systemctl start voltra
journalctl -u voltra -f   # watch replay finish

# 5. Sanity check.
curl http://127.0.0.1:3001/stats
```

Expected behaviour: `Loaded snapshot @ seq N` then `Replayed M WAL entries
(N+1..N+M)` in the logs, then `WebSocket listener accepting connections`.

---

## Capacity Sizing

Rough numbers from the in-memory table layer (DashMap):

| Resource     | Per row | At 100k rows | At 1M rows | At 10M rows |
| ------------ | ------- | ------------ | ---------- | ----------- |
| Heap (idle)  | ~200 B  | ~20 MB       | ~200 MB    | ~2 GB       |
| Heap (typical row w/ 10 fields) | ~1 KB | ~100 MB | ~1 GB | ~10 GB |
| Snapshot file | ~row size + 80 B header | ~80 MB | ~800 MB | ~8 GB |

WAL overhead: roughly the size of the serialized delta + 32 B header + 8 B CRC.
At 50k writes/sec averaging 200 B per delta: ~12 GB/hour. Rotate accordingly.

A 4 GB VM comfortably hosts ~2–3M rows of typical game state with headroom for
fan-out buffers. A 24 GB VM (Oracle Free A1.Flex) can host 20M rows easily.

---

## Tuning Knobs (Environment Variables)

Every `[server]` TOML key has an env-var equivalent. Env vars override TOML.

| Env var                            | TOML key                       | Default                  | Notes |
| ---------------------------------- | ------------------------------ | ------------------------ | ----- |
| `VOLTRA_HOST`                      | `host`                         | `127.0.0.1`              | Bind address. `0.0.0.0` for external. |
| `VOLTRA_PORT`                      | `port`                         | `3000`                   | WebSocket port. |
| `VOLTRA_METRICS_PORT`              | `metrics_port`                 | `port + 1`               | HTTP metrics port. |
| `VOLTRA_WAL_PATH`                  | `wal_path`                     | `$TEMP/voltra.wal`       | Set explicitly in prod. |
| `VOLTRA_FSYNC_INTERVAL_MS`         | `fsync_interval_ms`            | `100`                    | 0 = fsync per write. |
| `VOLTRA_WAL_BATCH_SIZE`            | `wal_batch_size`               | `100000`                 | Entries per flush. |
| `VOLTRA_WAL_BATCH_INTERVAL_MS`     | `wal_batch_interval_ms`        | `100`                    | Flush ceiling. |
| `VOLTRA_UNSAFE_NO_FSYNC`           | `unsafe_no_fsync`              | `false`                  | `1` = never fsync. Benchmarks only. |
| `VOLTRA_SNAPSHOT_INTERVAL`         | `snapshot_interval`            | `100000`                 | Entries between snapshots. |
| `VOLTRA_SNAPSHOT_DIR`              | `snapshot_dir`                 | `$TEMP/voltra_snapshots` | Persistent dir in prod. |
| `VOLTRA_SHARD_ID`                  | `shard_id`                     | `0`                      | This node's shard index. |
| `VOLTRA_SHARD_COUNT`               | `shard_count`                  | `1`                      | Total shards in cluster. |
| `VOLTRA_MAX_CONNECTIONS`           | `max_connections`              | `500`                    | Concurrent WebSocket clients. |
| `VOLTRA_REDUCER_TIMEOUT_MS`        | `reducer_timeout_ms`           | `5000`                   | Per-call timeout. |
| `VOLTRA_API_KEY`                   | `api_key`                      | (none)                   | Bearer token. **REQUIRED in prod.** |
| `VOLTRA_TUNE_SYSTEM`               | `tune_system`                  | `false`                  | Linux: raise file descriptor limit. |
| `VOLTRA_REUSE_PORT`                | `reuse_port`                   | `true`                   | SO_REUSEPORT. |
| `VOLTRA_TWO_FRAME_PROTOCOL`        | `two_frame_protocol`           | `false`                  | Optional shared-body delta encoding. |
| `VOLTRA_SQL_TIMEOUT_MS`            | `sql_timeout_ms`               | `5000`                   | Admin SQL query timeout. |
| `VOLTRA_MAX_BLOB_SIZE`             | `max_blob_size_bytes`          | `16777216` (16 MiB)      | Reject larger blobs. |
| `VOLTRA_BLOB_PATH`                 | (none)                         | `$TEMP/voltra_blobs`     | Blob store directory. |
| `VOLTRA_PERMISSIONS`               | `[permissions]`                | (open)                   | JSON `{"reducer":["role"]}`. |
| `VOLTRA_PERMISSIONS_DEFAULT_POLICY`| `permissions_default_policy`   | `open`                   | `open` or `closed`. |
| `RUST_LOG`                         | `log_level`                    | `info`                   | `debug`/`info`/`warn`/`error`. |

### Throughput tuning quick-reference

| Goal                            | Knob to change                | How                            |
| ------------------------------- | ----------------------------- | ------------------------------ |
| Lower restart time              | `snapshot_interval` ↓         | Try `10000` for write-heavy.   |
| Higher write throughput         | `fsync_interval_ms` ↑         | `500` trades durability window for TPS. |
| Lower write latency (P99)       | `fsync_interval_ms` ↓         | `10` or even `0`.              |
| Survive Hugmany clients         | `max_connections` ↑, `VOLTRA_TUNE_SYSTEM=1` | Plus OS `ulimit -n 65535`. |
| Catch buggy reducers fast       | `reducer_timeout_ms` ↓        | `1000` for typical web traffic. |

---

## Common Failure Modes

| Symptom                                          | Likely cause                                  | Fix                                                |
| ------------------------------------------------ | --------------------------------------------- | -------------------------------------------------- |
| "Server did not become ready" on start           | port collision (3000 or 3001)                 | `ss -tlnp` to find the squatter; change `port`.    |
| `/healthz` returns 200 but reducers time out     | WAL fsync I/O stalled                         | Check disk latency, free space, `iostat`.          |
| Restart takes minutes                            | WAL has millions of entries since last snapshot | Force a snapshot (see above), then rotate WAL.   |
| Out-of-memory on snapshot                        | row count grew beyond heap                    | Add RAM, or trim cold tables via custom reducer.   |
| Subscribers stop receiving deltas, calls still work | per-client write task panicked                | Check logs for "subscriber sink closed"; clients reconnect. |
| `cargo build` deadlock during tests              | nested `cargo build` from inside `cargo test` | Already fixed in Session 38 — ensure binary is pre-built. |
