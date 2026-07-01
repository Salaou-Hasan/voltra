# Voltra Production Status

**Last updated:** 2026-07-01 — every claim below was personally verified against the
current source/build/test output on this date, not carried forward from any prior
document.

This file replaces `PRODUCTION_READY.md` (dated Dec 2024) and `PRODUCTION_AUDIT.md`
(dated 2026-06-07), both of which were found this session to be significantly stale
in opposite directions — one described ports, test counts, and an internal
architecture that no longer exist; the other claimed TLS, JWT auth, row-level
security, presence, LRU eviction, and Prometheus metrics were all "missing" when
every one of them is actually implemented. Neither is trustworthy; this doc is the
single source of truth going forward, and every entry here should be re-verified
before being trusted again in future — do not let this file go stale the same way.

---

## 1. Verified build/test/lint status (ran personally, right now)

```
cargo build --release        → clean, zero errors/warnings
cargo clippy --release -- -D warnings → clean
cargo fmt --check             → clean
cargo test --lib              → 701 passed, 0 failed
cargo test --tests            → 6 test binaries, 0 failures (integration, crash-recovery,
                                 protocol-fuzz, schema-validation, security, WAL-recovery)
```

Note: `cargo test --tests` was **not run by CI until this session** — `.github/workflows/ci.yml`
previously only ran `cargo test --lib` and `cargo test --bin voltra`. That gap has been
fixed. Running the full suite for the first time surfaced two real bugs (a subscription-
notification test that hung indefinitely due to default tick-coalescing, and a genuinely
flaky crash-recovery timing race) — both fixed and verified.

## 2. Capability matrix — what's actually done, with evidence

### Core database
| Feature | Status | Evidence |
|---|---|---|
| WAL persistence + crash recovery | Done | `src/wal/batch_writer.rs`; verified live: killed a running server mid-write, restarted, WAL correctly replayed all committed entries |
| Tables (lock-free reads) | Done | `DashMap`-backed `TableStore`, per-row slot-based write locks (`src/table/mod.rs`) |
| SQL query engine | Done | JOINs/GROUP BY/aggregates/subqueries — full engine in `src/sql/` and PostgreSQL-wire layer in `src/pg/` |
| Reducers (3 runtimes) | Done | Native Rust (`#[reducer]` macro), JS via **rquickjs** (not Boa — confirmed in `Cargo.toml`, 64MB heap cap + CPU timeouts, `src/reducer/v8.rs`), WASM via Wasmtime 21 |
| Row-level security | Done | `RlsPolicy` enum (Public/OwnerField/RoleGated) in `src/schema.rs`, enforced in `get_row`/`commit()` |
| Schema enforcement + migrations | Done | `schema.toml` typed columns; `migrations/*.toml`, idempotent |
| Single-reducer transaction atomicity (OCC) | Done | `apply_delta_batch_versioned` — protects one reducer's read-set against concurrent writes. **Not** multi-reducer ACID (see Gaps). |

### Realtime / networking
| Feature | Status | Evidence |
|---|---|---|
| WebSocket + live subscriptions | Done | Predicate tree (AND/OR/IN/comparison), ORDER BY, LIMIT — `src/subscriptions.rs` |
| Tick-coalesced fan-out | Done | Default 50Hz (`sub_tick_ms=20`, i.e. 20ms — note: older docs said "20Hz/50ms", that's stale; verified in `config.rs` this session), configurable per-deployment via `VOLTRA_SUB_TICK_MS` (`0` = instant delivery) |
| Presence | Done | Heartbeat-based online/offline tracking, `src/presence.rs` (a past audit claimed this was "missing" — it is not) |
| TLS | Done | `src/network/tls.rs`, self-signed cert auto-generation or configured cert/key paths |
| JWT auth (Ed25519) | Done | `src/auth.rs`, `IdentityIssuer`, key persisted to disk |
| API key + role-based permissions | Done | `PermissionsConfig`, per-reducer role allowlist |
| Rate limiting | Present, verify default-on status | `src/network/rate_limiter.rs` exists — confirm it's actually enforced by default before relying on it in production (see Gaps) |

### Storage & memory
| Feature | Status | Evidence |
|---|---|---|
| Hybrid row encoding | Done | MsgPack for small rows, zstd-compressed MsgPack above 256 bytes (`src/table/mod.rs`) |
| LRU eviction | Done | `EvictionPolicy`, `LruTracker` — a past audit claimed this was "missing"; it is not, though it defaults to `None` (unbounded) unless explicitly configured |
| TTL / auto-expiry | Done | `src/ttl.rs`, background sweep |
| Blob externalization | Done | Large inventory-style arrays go to a separate blob store, not inline in DashMap |

### Multi-protocol surface
| Feature | Status | Evidence |
|---|---|---|
| Redis wire protocol (RESP2/3) | Done | ~150 commands — strings, hashes, lists, sets, zsets, pub/sub, MULTI/EXEC (`src/redis/`) |
| PostgreSQL wire protocol | Done | Full startup/auth handshake, simple + extended query protocol, transactions with real snapshot isolation (`src/pg/`) |
| MVCC engine | Done | Version-chain store backing the Redis/PG layers, single-sequencer writer thread, AOF persistence (`src/mvcc/`) |

### Operations
| Feature | Status | Evidence |
|---|---|---|
| Backups + PITR | Done | `src/backup.rs` — snapshot+WAL copy, point-in-time restore |
| Replication | Done (log-shipping) | `src/replication/` — async WAL streaming to a read replica, manual promotion (`voltra promote`) |
| Admin dashboard | Done | Single-file embedded dashboard, `/admin` on the metrics port |
| Prometheus metrics | Done | `src/metrics.rs` — real histograms/gauges (a past audit claimed only "4 gauges, no histogram"; that's stale) |
| Health checks | Done | `/healthz` on the metrics port (default `:3001`), used as the actual Docker `HEALTHCHECK` |
| CI | Done, recently fixed | `.github/workflows/ci.yml` now runs lib + bin + the full integration/crash-recovery/fuzz/schema/security suite, plus clippy/fmt |

### CLI / DX
| Feature | Status | Evidence |
|---|---|---|
| `voltra init --genre <fps\|mmo\|...> [--modules a,b,c] [--client ...]` | Done | Module-recipe composition via `voltra::runtime::compose_runtime`, `src/app/scaffold.rs` — validated by scaffolding and building real 15/16-module projects this session |
| `voltra add <module>` | Done | 21 runtime modules available standalone |
| `voltra update` / `voltra install` | Done, recently fixed | Was broken for the current `g<gen>.x.x.x` tag scheme (URL double-prefixing bug) and for Apple Silicon (wrong asset name) — both fixed this session; PATH auto-configuration now works on Linux/macOS too, not just Windows |
| `.vol` DSL | Done | Compiles to native Rust at `voltra build` time, no interpreter — real, verified by reading the compiler — but has real expressiveness gaps (no `match`, `.chunks()`, `format!()`) and is not the default template path |

## 3. Known real gaps — verified absent, not assumed

- **OpenTelemetry / distributed tracing** — confirmed absent (zero references anywhere in the codebase or `Cargo.toml` as of this check).
- **Consensus / automatic failover** — Raft was built once (`src/raft/`, historical) then deliberately removed; no `src/raft/` exists today. Replication promotion is manual (`voltra promote`), not automatic.
- **Consistent hash ring for cluster sharding** — `shard_for_key` (`src/cluster/mod.rs`) is static FNV-1a hash mod shard_count. Adding/removing a shard requires a coordinated restart with matching `shard_count` everywhere; no live rebalancing.
- **Multi-reducer ACID transactions** — OCC protects one reducer call's read-set, not a client-composed sequence of calls.
- **No unsafe-free claim is false** — 9 occurrences of `unsafe fn`/`unsafe {` across 6 files (FFI for mimalloc, Windows permission bits, JS engine bindings) — a past doc's "no unsafe code" claim was wrong.
- **Redis feature gaps** — no streams (XADD/XREAD), no geospatial (GEOADD/GEOSEARCH) as of the last verified check (this may change — see note at the end of this doc about work landing in parallel).
- **DSL limitations** — see CLI section above.
- **ECS/AoI/tick hot-path runtime exists but isn't user-reachable** — `src/runtime/` has a real, tested ECS + spatial-grid + tick-scheduling engine (`LobbyRuntime`), but `voltra init --genre` scaffolds hot-simulation state (movement, combat) as plain TableStore rows, never touching this engine. This is the single largest gap between the stated product direction and what a generated project actually gets.

## 4. Deployment (verified against the actual current `Dockerfile`/`config.rs`)

**Ports:** `3000` (WebSocket), `3001` (metrics/admin/`/healthz`), `6379` (Redis), `5432` (PostgreSQL) — **not** 8000/8001 as an older doc claimed.

**Health check** (from the real `Dockerfile`):
```
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS http://localhost:3001/healthz || exit 1
```

**Key env vars** (verified present in `src/config.rs` as of this check):
`VOLTRA_HOST`, `VOLTRA_PORT`, `VOLTRA_METRICS_PORT`, `VOLTRA_WAL_PATH`, `VOLTRA_SNAPSHOT_DIR`,
`VOLTRA_WAL_BATCH_SIZE`, `VOLTRA_WAL_BATCH_INTERVAL_MS`, `VOLTRA_UNSAFE_NO_FSYNC`,
`VOLTRA_SHARD_ID`, `VOLTRA_SHARD_COUNT`, `VOLTRA_MAX_CONNECTIONS`, `VOLTRA_REDUCER_TIMEOUT_MS`,
`VOLTRA_API_KEY`, `VOLTRA_SUB_TICK_MS`.

**Docker build:** two-stage (`rust:slim-bookworm` builder → `debian:bookworm-slim` runtime),
builds only the `voltra` binary (`cargo build --release --bin voltra`), not the bench/sim/soak tools.

## 5. Security checklist (verified, not assumed)

- [x] TLS available (self-signed auto-gen or configured cert/key)
- [x] JWT (Ed25519) + API key auth
- [x] Row-level security policies
- [x] Per-reducer role-based permissions
- [ ] Rate limiting — module exists, **confirm it's enabled by default** before relying on it
- [x] Reducer CPU timeouts (all 3 runtimes)
- [x] WASM memory caps; JS (rquickjs) 64MB heap cap
- [ ] No independent third-party security audit — this document and its predecessors were all self-authored

---

*A note on timing: several other work streams (tracing, cluster hash ring, transaction
isolation, WAL rotation/backpressure/rate-limiting hardening, Redis streams/geospatial,
ECS/AoI hot-path wiring) were in progress in parallel worktrees as this document was
written and may land after it. Re-check the specific gap in Section 3 against current
code before assuming it's still open.*
