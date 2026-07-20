# ------------------------
# Builder Stage
# ------------------------
FROM rust:slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    pkg-config \
    libzstd-dev \
    libsnappy-dev \
    liblz4-dev \
    protobuf-compiler \
    build-essential \
    ca-certificates \
    clang \
    libclang-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Build context is the d-engine workspace root.
# Run: docker build -t standalone-bench:1.0 -f benches/standalone-bench/docker/Dockerfile .
WORKDIR /build

ENV RUSTC_WRAPPER=""

COPY . .

WORKDIR /build/benches/standalone-bench
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/benches/standalone-bench/target \
    cargo build --release --jobs 2 --bin standalone-bench && \
    cp target/release/standalone-bench /usr/local/bin/standalone-bench

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
COPY --from=builder /usr/local/bin/standalone-bench /usr/local/bin/standalone-bench

# Copy test scripts
COPY benches/standalone-bench/docker/soak-test.sh /usr/local/bin/
COPY benches/standalone-bench/docker/bench-suite.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/soak-test.sh /usr/local/bin/bench-suite.sh

WORKDIR /app

# Set default endpoints (can be overridden)
ENV ENDPOINTS="http://localhost:9081,http://localhost:9082,http://localhost:9083"
ENV TOTAL=100

# Run soak test by default
CMD ["bash", "-c", "sleep 5 && /usr/local/bin/soak-test.sh"]
