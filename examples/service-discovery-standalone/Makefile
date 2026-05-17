.PHONY: help check-cluster \
        run-watcher run-watcher-prefix \
        register register-node2 register-node3 \
        unregister unregister-node2

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Prerequisites:'
	@echo '  Start a cluster: cd ../three-nodes-standalone && make start-cluster'
	@echo ''
	@echo '--- Mode 1: exact-key watch ---'
	@echo '  run-watcher       Watch /services/payment/node1 (Terminal 1)'
	@echo '  register          Register payment/node1 -> 10.0.0.1:8080'
	@echo '  unregister        Deregister payment/node1'
	@echo ''
	@echo '--- Mode 2: prefix watch ---'
	@echo '  run-watcher-prefix  Watch all /services/payment/* (Terminal 1)'
	@echo '  register-node2    Register payment/node2 -> 10.0.0.2:8080'
	@echo '  register-node3    Register payment/node3 -> 10.0.0.3:8080'
	@echo '  unregister-node2  Deregister payment/node2'

check-cluster:
	@echo "Checking connectivity to 127.0.0.1:9081..."
	@nc -z 127.0.0.1 9081 || (echo "❌ Cluster not reachable. Please start three-nodes-standalone first." && exit 1)
	@echo "✅ Cluster reachable"

# ---------------------------------------------------------------------------
# Mode 1 — exact-key watch
# ---------------------------------------------------------------------------

run-watcher: check-cluster ## Watch /services/payment/node1 (exact key)
	cargo run --bin watcher -- --key "/services/payment/node1"

register: check-cluster ## Register payment/node1 -> 10.0.0.1:8080
	cargo run --bin admin -- register --name payment --instance node1 --endpoint "10.0.0.1:8080"

unregister: check-cluster ## Deregister payment/node1
	cargo run --bin admin -- unregister --name payment --instance node1

# ---------------------------------------------------------------------------
# Mode 2 — prefix watch
# ---------------------------------------------------------------------------

run-watcher-prefix: check-cluster ## Watch all /services/payment/* (prefix)
	cargo run --bin watcher -- --key "/services/payment/" --prefix

register-node2: check-cluster ## Register payment/node2 -> 10.0.0.2:8080
	cargo run --bin admin -- register --name payment --instance node2 --endpoint "10.0.0.2:8080"

register-node3: check-cluster ## Register payment/node3 -> 10.0.0.3:8080
	cargo run --bin admin -- register --name payment --instance node3 --endpoint "10.0.0.3:8080"

unregister-node2: check-cluster ## Deregister payment/node2
	cargo run --bin admin -- unregister --name payment --instance node2
