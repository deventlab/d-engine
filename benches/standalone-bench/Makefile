# Benchmark Test Automation
.PHONY: all clean build basic-write high-concurrency-write linear-read serial-read soak-test

ENDPOINTS = --endpoints http://127.0.0.1:9081 \
            --endpoints http://127.0.0.1:9082 \
            --endpoints http://127.0.0.1:9083

SOAK_TEST_ENDPOINTS ?= http://127.0.0.1:9083,http://127.0.0.1:9082,http://127.0.0.1:9081
SOAK_TEST_TOTAL ?= 100
SOAK_COMPOSE_FILE ?= docker/docker-compose.yml

all: clean build basic-write high-concurrency-write linear-read serial-read

clean:
	@echo "Cleaning previous builds..."
	@cargo clean

build:
	@echo "Building release binary..."
	@cargo update
	@cargo build --release --jobs 4

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
