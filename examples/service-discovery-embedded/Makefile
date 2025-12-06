.PHONY: help run clean

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@echo '  run         Start the embedded service discovery demo'
	@echo '  clean       Remove data directory (./data)'

run: ## Start the demo
	cargo run --bin server

clean: ## Clean up data directory
	rm -rf data
	@echo "✓ Data directory cleaned"
