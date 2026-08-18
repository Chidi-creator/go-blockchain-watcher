# gRPC Integration for Wallet Service

This document explains how gRPC has been integrated into the wallet service and how to use it.

## What is gRPC?

gRPC is a high-performance, open-source remote procedure call (RPC) framework developed by Google. It allows a client application to directly call methods on a server application as if it were a local object, making distributed computing more efficient.

Key features of gRPC:
- Uses Protocol Buffers (protobuf) as its interface definition language and underlying message exchange format
- Supports multiple programming languages (language-agnostic)
- Provides bidirectional streaming
- Implements HTTP/2 for transport, enabling multiplexing requests over a single connection
- Offers built-in authentication, load balancing, and timeouts

## How gRPC Benefits Our Wallet Service

1. **Faster Communication**: gRPC is significantly faster than REST APIs due to its binary serialization and HTTP/2 transport. This is crucial for high-frequency operations like checking balances or price updates.

2. **Type Safety**: Protocol Buffers enforce strict typing, reducing runtime errors and ensuring consistency across services.

3. **Clear Service Definition**: The proto files provide a clear contract between services with auto-generated client/server code, making it easier to understand and maintain the API.

4. **Streaming Support**: We can now stream real-time updates for blockchain transactions and balance changes, which is perfect for wallet applications.

5. **Cross-Service Communication**: Better integration between our wallet, blockchain, and portfolio services allows for more efficient communication.

6. **Reduced Bandwidth**: The compact binary format saves network usage compared to JSON, which is important for mobile clients.

## Project Structure

The gRPC integration consists of the following components:

- **Protocol Definitions**: Located in `proto/wallet/` directory
- **gRPC Server**: Implemented in `interfaces/grpc/server.go`
- **Service Handlers**: Located in `interfaces/grpc/handlers/` directory
- **Example Client**: Provided in `examples/grpc_client.go`

## Protocol Definitions

We have organized our Protocol Buffer definitions into three main files:

1. `wallet.proto`: Defines the WalletService and related message types
2. `portfolio.proto`: Defines the PortfolioService and related message types
3. `blockchain.proto`: Defines the BlockchainService and related message types

## How to Generate Go Code from Proto Files

To generate the Go code from the proto files, run:

```bash
make proto
```

This will create the corresponding `.pb.go` files that contain the generated code for your services.

## Starting the gRPC Server

The gRPC server is automatically started alongside the HTTP server when you run the wallet service:

```bash
go run cmd/server/main.go
```

By default, the gRPC server runs on port 50051.

## Configuration

The gRPC server can be configured through environment variables:

```
GRPC_PORT=50051                  # Port for the gRPC server
GRPC_TLS_ENABLED=false           # Whether TLS is enabled
GRPC_TLS_CERT_FILE=cert.pem      # Path to TLS certificate file
GRPC_TLS_KEY_FILE=key.pem        # Path to TLS key file
```

## Example Usage

Here's a simple example of how to use the gRPC client:

```go
package main

import (
	"context"
	"log"
	"time"

	pb "github.com/zap/wallet-service/proto/wallet"
	"google.golang.org/grpc"
)

func main() {
	// Set up a connection to the server
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create a client
	client := pb.NewWalletServiceClient(conn)

	// Set up a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// Get wallet portfolio
	response, err := client.GetWalletPortfolio(ctx, &pb.WalletRequest{
		WalletId: "your-wallet-id-here",
	})
	if err != nil {
		log.Fatalf("Error calling GetWalletPortfolio: %v", err)
	}

	log.Printf("Wallet portfolio: %v", response)
}
```

## Streaming Example

For real-time updates, you can use the streaming endpoints:

```go
// Set up a client
client := pb.NewWalletServiceClient(conn)

// Watch for wallet updates
stream, err := client.WatchWalletUpdates(ctx, &pb.WalletRequest{
	WalletId: "your-wallet-id-here",
})
if err != nil {
	log.Fatalf("Error setting up wallet updates stream: %v", err)
}

// Process stream updates
for {
	update, err := stream.Recv()
	if err == io.EOF {
		// End of stream
		break
	}
	if err != nil {
		log.Fatalf("Error receiving update: %v", err)
	}
	log.Printf("Received update: %v", update)
}
```

## gRPC Tools

For development and testing, you can use the following tools:

- **grpcurl**: Command-line tool for interacting with gRPC servers
- **BloomRPC**: GUI client for gRPC services similar to Postman
- **gRPC Web**: For browser clients to interact with gRPC services

## Next Steps

1. Implement the remaining service methods
2. Add proper error handling
3. Implement authentication and authorization
4. Set up TLS for secure communication
5. Implement interceptors for logging and metrics
6. Create comprehensive integration tests 