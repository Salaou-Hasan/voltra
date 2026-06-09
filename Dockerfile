# Stage 1: builder
FROM rust:1.83-slim-bookworm AS builder
RUN apt-get update && apt-get install -y build-essential pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/neondb
COPY . .
RUN cargo build --release

# Stage 2: runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/neondb/target/release/neondb /usr/local/bin/neondb
RUN mkdir -p /var/lib/neondb
WORKDIR /var/lib/neondb
EXPOSE 3000 3001
HEALTHCHECK --interval=10s --timeout=5s --retries=3 CMD curl -fsS http://localhost:3001/health || exit 1
ENTRYPOINT ["neondb", "start"]
