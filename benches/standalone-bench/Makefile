# Benchmark Test Automation
.PHONY: all clean build basic-write high-concurrency-write linear-read serial-read soak-test

ENDPOINTS = --endpoints http://127.0.0.1:9081 \
            --endpoints http://127.0.0.1:9082 \
            --endpoints http://127.0.0.1:9083

SOAK_TEST_ENDPOINTS ?= http://127.0.0.1:9083,http://127.0.0.1:9082,http://127.0.0.1:9081
SOAK_TEST_TOTAL ?= 100
SOAK_COMPOSE_FILE ?= docker/docker-compose.yml

# On macOS with Homebrew: auto-detect compression lib paths to skip bundled C++
# compilation of RocksDB dependencies, which fails under macOS 26 + Xcode 26
# (Clang 16 lacks __builtin_ctzg/__builtin_clzg from LLVM 18+ SDK headers).
# brew --prefix resolves correctly on both Apple Silicon (/opt/homebrew) and
# Intel Mac (/usr/local). Silently no-ops when brew or a lib is absent.
SNAPPY_PREFIX    := $(shell brew --prefix snappy 2>/dev/null)
LZ4_PREFIX       := $(shell brew --prefix lz4    2>/dev/null)
ZSTD_PREFIX      := $(shell brew --prefix zstd   2>/dev/null)
BREW_ROCKSDB_ENV :=
ifneq ($(wildcard $(SNAPPY_PREFIX)/lib),)
  BREW_ROCKSDB_ENV += SNAPPY_LIB_DIR=$(SNAPPY_PREFIX)/lib
endif
ifneq ($(LZ4_PREFIX),)
  BREW_ROCKSDB_ENV += LZ4_LIB_DIR=$(LZ4_PREFIX)/lib
endif
ifneq ($(ZSTD_PREFIX),)
  BREW_ROCKSDB_ENV += ZSTD_LIB_DIR=$(ZSTD_PREFIX)/lib
endif

all: clean build basic-write high-concurrency-write linear-read serial-read

clean:
	@echo "Cleaning previous builds..."
	@cargo clean

build:
	@echo "Building release binary..."
	@cargo update
	$(BREW_ROCKSDB_ENV) cargo build --release --jobs 4

# Basic Write Test (1 connection, 1 client)
basic-write:
	@echo "\n=== Basic Write Test (1C/1W) ==="
	@./target/release/standalone-bench \
		${ENDPOINTS} \
		--conns 1 \
		--clients 1 \
		--sequential-keys \
		--total 10000 \
		--key-size 8 \
		--value-size 256 \
		put

# High Concurrency Write Test (10 connections, 100 clients)
high-concurrency-write:
	@echo "\n=== High Concurrency Write Test (10C/100W) ==="
	@./target/release/standalone-bench \
		${ENDPOINTS} \
		--conns 10 \
		--clients 100 \
		--sequential-keys \
		--total 10000 \
		--key-size 8 \
		--value-size 256 \
		put

# Linearizable Read Test
linear-read:
	@echo "\n=== Linearizable Read Test ==="
	@./target/release/standalone-bench \
		${ENDPOINTS} \
		--conns 10 \
		--clients 100 \
		--sequential-keys \
		--total 10000 \
		--key-size 8 \
		range --consistency l

# Serializable Read Test
serial-read:
	@echo "\n=== Serializable Read Test ==="
	@./target/release/standalone-bench \
		${ENDPOINTS} \
		--conns 10 \
		--clients 100 \
		--sequential-keys \
		--total 10000 \
		--key-size 8 \
		range --consistency s

build-soak-tester:
	docker build -f docker/Dockerfile -t d-engine-soak-tester .

soak-test: build-soak-tester
	@docker compose -f $(SOAK_COMPOSE_FILE) down
	@docker compose -f $(SOAK_COMPOSE_FILE) up -d
