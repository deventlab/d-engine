# ------------------------
# Builder Stage
# ------------------------
FROM rust:slim-bullseye AS builder

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    pkg-config \
    libzstd-dev \
    protobuf-compiler \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/bench

# Copy bench project files
COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY src ./src

# Build bench binary
RUN cargo build --release --bin standalone-bench

# ------------------------
# Runtime Stage
# ------------------------
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    bash \
    curl \
    jq \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy built binary
COPY --from=builder /usr/src/bench/target/release/standalone-bench /usr/local/bin/standalone-bench

# Copy soak test script
COPY ./docker/soak-test.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/soak-test.sh

WORKDIR /app

# Set default endpoints (can be overridden)
ENV ENDPOINTS="http://localhost:9081,http://localhost:9082,http://localhost:9083"
ENV TOTAL=100

# Run soak test by default
CMD ["bash", "-c", "sleep 5 && /usr/local/bin/soak-test.sh"]
