.PHONY: help run-watcher register unregister check-cluster

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@echo '  run-watcher       Start the watcher client (reads & watches)'
	@echo '  register          Register a demo service (writes)'
	@echo '  unregister        Unregister the demo service (deletes)'
	@echo '  check-cluster     Check if d-engine cluster is reachable'
	@echo ''
	@echo 'Prerequisites:'
	@echo '  Ensure d-engine cluster is running (cd ../three-nodes-standalone && make start-cluster)'

check-cluster:
	@echo "Checking connectivity to 127.0.0.1:9081..."
	@nc -z 127.0.0.1 9081 || (echo "❌ Cluster not reachable. Please start three-nodes-standalone first." && exit 1)
	@echo "✅ Cluster reachable"

run-watcher: check-cluster ## Start the watcher
	cargo run --bin watcher -- --key "services/api-gateway/node1"

register: check-cluster ## Register service: api-gateway -> 192.168.1.10:8080
	cargo run --bin admin -- register --name api-gateway --instance node1 --endpoint "192.168.1.10:8080"

unregister: check-cluster ## Unregister service: api-gateway
	cargo run --bin admin -- unregister --name api-gateway --instance node1
