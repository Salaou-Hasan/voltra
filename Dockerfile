# Multi-stage build for NeonDB Server

# Stage 1: Build
FROM rust:latest as builder

WORKDIR /app

# Copy source code
COPY Cargo.toml ./
COPY src src/
COPY benches benches/

# Build in release mode with optimizations
RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies (minimal)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/target/release/neondb /usr/local/bin/neondb

# Create data directory for WAL
RUN mkdir -p /data/wal

# Expose WebSocket port and metrics port
EXPOSE 8000 8001

# Environment defaults - optimization enabled
ENV NEONDB_HOST=0.0.0.0
ENV NEONDB_PORT=8000
ENV NEONDB_METRICS_PORT=8001
ENV NEONDB_WAL_PATH=/data/wal/neondb.wal
ENV NEONDB_FSYNC_INTERVAL_MS=0
ENV NEONDB_WAL_BATCH_SIZE=100000
ENV NEONDB_WAL_BATCH_INTERVAL_MS=100
ENV NEONDB_UNSAFE_NO_FSYNC=false
ENV NEONDB_SHARD_ID=0
ENV NEONDB_SHARD_COUNT=1
ENV RUST_LOG=info
ENV NEONDB_MAX_CONNECTIONS=100
ENV NEONDB_REDUCER_TIMEOUT_MS=5000

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD nc -z localhost 8000 || exit 1

# Run the server
ENTRYPOINT ["neondb", "start"]
