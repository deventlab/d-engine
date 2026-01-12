.PHONY: run clean

run:
	@echo "Starting quick-start-standalone example..."
	@echo "Make sure 3-node cluster is running: cd ../three-nodes-standalone && make start-cluster"
	@go mod tidy
	@go run main.go

clean:
	@go clean
	@rm -f go.sum
