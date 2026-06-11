# ── Stage 1: builder ─────────────────────────────────────────────────────────
FROM rust:1.83-slim-bookworm AS builder

# Build deps: NeonDB pulls in wasmtime (cranelift), rquickjs, ring (TLS), zstd.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config libssl-dev clang \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/neondb
COPY . .

# IMPORTANT: the repo's rust-toolchain.toml pins the *Windows* GNU toolchain
# (the maintainer builds on Windows-without-MSVC). Inside this Linux image that
# pin is wrong and would make cargo try to fetch a windows target. Remove it so
# the container uses the image's native linux toolchain.
RUN rm -f rust-toolchain.toml

# Build only the server binary (skip the bench/sim/soak bins — faster image).
RUN cargo build --release --bin neondb

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/neondb/target/release/neondb /usr/local/bin/neondb

# Data dir (mount a volume here to persist WAL + snapshots + MVCC across restarts).
RUN mkdir -p /var/lib/neondb
WORKDIR /var/lib/neondb

# WS 3000 · metrics/admin 3001 · Redis 6379 · PostgreSQL 5432
EXPOSE 3000 3001 6379 5432

# The real liveness endpoint is /healthz (NOT /health).
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS http://localhost:3001/healthz || exit 1

ENTRYPOINT ["neondb", "start"]
