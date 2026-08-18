.PHONY: proto clean build run test run-workers

# Protocol Buffers generation
proto:
	@echo "Generating Go code from Protocol Buffers..."
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/wallet/wallet.proto \
		proto/wallet/portfolio.proto \
		proto/wallet/blockchain.proto \
		proto/wallet/import.proto
	@echo "Protocol Buffers generation complete."

# Build the application
build:
	@echo "Building application..."
	go build -o bin/wallet-service cmd/server/main.go
	@echo "Build complete."

# Run the application
run: build
	@echo "Starting wallet service..."
	./bin/wallet-service

# Run tests
test:
	@echo "Running tests..."
	go test ./...

# Clean generated files
clean:
	@echo "Cleaning generated files..."
	rm -rf bin/
	@echo "Clean complete."

# Run blockchain workers
run-workers:
	@echo "Starting blockchain workers..."
	@go run cmd/server/main.go

# Default task
all: proto build 