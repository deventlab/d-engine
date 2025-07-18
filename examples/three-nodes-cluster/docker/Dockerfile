# ------------------------
# Builder Stage (Optimized for ARM64)
# ------------------------
FROM rust:slim-bullseye AS builder

WORKDIR /usr/src/demo

# Install build dependencies with cleanup
RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Cache dependencies first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "" > src/lib.rs && \
    cargo build --release && \
    rm -rf src target/release/deps/demo*

# Copy actual source code
COPY . .

# Final build
RUN cargo build --release # --locked

# ------------------------
# Runtime Stage (ARM64 Optimized)
# ------------------------
FROM debian:bookworm-slim

# Install runtime dependencies in single layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openssh-server \
    ca-certificates \
    curl \
    unzip \
    sudo \
    iptables \
    libc6 \
    tzdata && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /var/run/sshd && \
    ssh-keygen -A

# Configure system
RUN echo 'root:root' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# Set timezone
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install ARM64 Promtail
RUN curl -LO "https://github.com/grafana/loki/releases/download/v3.0.0/promtail-linux-arm64.zip" && \
    unzip promtail-linux-arm64.zip && \
    mv promtail-linux-arm64 /usr/local/bin/promtail && \
    chmod +x /usr/local/bin/promtail && \
    rm promtail-linux-arm64.zip

# Copy built binary
COPY --from=builder /usr/src/demo/target/release/demo /usr/local/bin/demo

# Create directories
RUN mkdir -p /app/{logs,db,snapshots} /etc/promtail

# Copy configuration
COPY docker/monitoring/promtail/config.yml /etc/promtail/config.yml

WORKDIR /app

# Start services
CMD ["sh", "-c", "/usr/sbin/sshd -D & CONFIG_PATH=$CONFIG_PATH RUST_LOG=demo=$LOG_LEVEL,d_engine=$LOG_LEVEL,hyper=warn,sled=warn demo  & promtail --config.file=/etc/promtail/config.yml > /app/logs/promtail.log 2>&1"]
