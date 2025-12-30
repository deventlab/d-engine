# Makefile for d-engine-proto

.PHONY: generate-go clean-go help

# Generate Go protobuf code from proto files
# Output: go/proto/**/*.pb.go
generate-go:
	@echo "Checking protoc plugins..."
	@which protoc-gen-go > /dev/null || (echo "ERROR: protoc-gen-go not found. Install with: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" && exit 1)
	@which protoc-gen-go-grpc > /dev/null || (echo "ERROR: protoc-gen-go-grpc not found. Install with: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest" && exit 1)
	@echo "✓ Plugins found"
	@echo "Generating Go protobuf code..."

	# Generate protobuf code with source_relative paths
	# Output goes directly to go/ (not go/proto/) to match module path
	@protoc --go_out=go --go_opt=paths=source_relative \
	        --go-grpc_out=go --go-grpc_opt=paths=source_relative \
	        proto/client/*.proto \
	        proto/server/*.proto \
	        proto/common.proto \
	        proto/error.proto

	# Reorganize generated files to match Go package structure
	# Reason: protoc generates files next to .proto, but Go requires
	# package foo to be in foo/ directory

	# Move top-level proto files (error, common)
	@mkdir -p go/error go/common
	@mv go/proto/error.pb.go go/error/ 2>/dev/null || true
	@mv go/proto/common.pb.go go/common/ 2>/dev/null || true

	# Move client directory
	@mv go/proto/client go/ 2>/dev/null || true

	# Move server subdirectories
	@mkdir -p go/server
	@mv go/proto/server/cluster go/server/ 2>/dev/null || true
	@mv go/proto/server/storage go/server/ 2>/dev/null || true
	@mv go/proto/server/election go/server/ 2>/dev/null || true
	@mv go/proto/server/replication go/server/ 2>/dev/null || true

	# Clean up empty proto directory
	@rm -rf go/proto

	@echo "✓ Go code generated in go/"

# Clean generated Go code
clean-go:
	@echo "Cleaning generated Go code..."
	@rm -rf go/proto go/client go/server go/error go/common
	@echo "✓ Cleaned"

# Show available targets
help:
	@echo "Available targets:"
	@echo "  generate-go  - Generate Go protobuf code from .proto files"
	@echo "  clean-go     - Remove generated Go code"
	@echo "  help         - Show this help message"
