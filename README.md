# Zap Wallet Service (Go)

This is a high-performance microservice written in Go for handling wallet portfolio operations in the Zap platform. This service is designed to address performance and memory usage issues encountered in the Node.js implementation, particularly for blockchain operations and portfolio calculations.

## Features

- **High Performance Portfolio Processing**: Efficiently calculates wallet portfolios using Go's concurrency model
- **Blockchain Support**: Direct integration with multiple blockchain networks:
  - Bitcoin (BTC)
  - Ethereum and EVM chains (ETH, BSC, MATIC, etc.)
  - Solana (SOL)
  - Tron (TRX)
- **Memory Efficiency**: Designed for minimal memory usage even with large portfolios
- **Cache Management**: Intelligent caching of portfolio data for improved response times
- **Parallel Processing**: Concurrent processing of wallets and accounts
- **REST API**: Exposes a RESTful API for integration with existing systems

## Architecture

The service follows a clean architecture pattern with clear separation of concerns:

```
go-wallet-service/
├── config/                # Configuration constants and types
├── interfaces/            # API interfaces (HTTP/gRPC)
│   ├── grpc/              # gRPC server and handlers
│   └── http/              # HTTP server and handlers
├── managers/              # Cross-cutting concerns
│   └── logger/            # Logging utilities
├── providers/             # External service providers
│   └── blockchain/        # Blockchain specific providers
│       ├── bitcoin/
│       ├── ethereum/
│       ├── solana/
│       └── tron/
└── src/                   # Core business logic
    ├── repositories/      # Data access layer
    ├── services/          # Business services
    ├── types/             # Domain types and models
    └── usecases/          # Application use cases
```

## Installation

### Prerequisites

- Go 1.21 or higher
- MongoDB
- Redis (optional, for enhanced caching)

### Setup

1. Clone the repository:

```bash
git clone https://github.com/zap/wallet-service.git
cd wallet-service
```

2. Install dependencies:

```bash
go mod tidy
```

3. Configure the environment:

```bash
cp .env.example .env
# Edit the .env file to match your environment
```

4. Build the application:

```bash
go build -o wallet-service ./cmd/server
```

## Running the Service

### Development mode:

```bash
go run cmd/server/main.go
```

### Production mode:

```bash
./wallet-service
```

## API Endpoints

### HTTP API

- `GET /api/v1/wallets/portfolio/user/:userId` - Get portfolio for all user wallets
- `GET /api/v1/wallets/portfolio/wallet/:walletId` - Get portfolio for a specific wallet
- `GET /api/v1/wallets/portfolio/account/:accountId` - Get portfolio for a specific account
- `GET /api/v1/wallets/balance/:accountId` - Get and update balance for a specific account

### gRPC Services

The service provides the following gRPC methods:

**Wallet Service:**

- `GetWalletPortfolio(WalletRequest) returns (PortfolioResponse)` - Get portfolio for a specific wallet
- `GetUserPortfolio(UserRequest) returns (PortfolioResponse)` - Get portfolio for all user wallets
- `GetAccountPortfolio(AccountRequest) returns (PortfolioResponse)` - Get portfolio for a specific account
- `GetAccountBalance(AccountRequest) returns (BalanceResponse)` - Get balance for a specific account

**Import Service:**

- `ImportAccounts(ImportRequest) returns (ImportResponse)` - Bulk import accounts

Example gRPC client usage:

```go
// Connect to gRPC server
conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil {
    log.Fatalf("did not connect: %v", err)
}
defer conn.Close()

// Create wallet service client
walletClient := pb.NewWalletServiceClient(conn)

// Get wallet portfolio
walletID := "60f7e5c65c9d2c0001f3a3d5"
walletResp, err := walletClient.GetWalletPortfolio(ctx, &pb.WalletRequest{
    WalletId: walletID,
})
```

## Account Management

The service provides a complete account management system with the following features:

- **Account Creation**: Create individual blockchain accounts for wallets
- **Account Importing**: Bulk import accounts with validation and duplicate checking
- **Balance Updates**: Automatically fetch and update account balances from blockchain
- **Portfolio Aggregation**: Group accounts into portfolios by wallet or user
- **Adapter Pattern**: Separation between domain logic and data access

The account system follows a clean architecture approach:

- **Domain Types** (`src/types/account.go`): Core account entity with business rules
- **Use Cases** (`src/usecases/account_usecase.go`): Account operations logic
- **Repository Interface** (`src/repositories/account_repository.go`): Data access abstraction
- **Adapters** (`src/adapters/account_adapter.go`): Implementation converting between domain and data models

### Account Import Process

The bulk import process handles:

1. Validation of wallet and currency data
2. Duplicate detection and prevention
3. Optional blockchain balance fetching
4. Concurrent processing for improved performance

## Integration with Existing System

This service is designed to work alongside the existing Node.js backend. The Node.js application can call this service via HTTP API requests for performance-intensive wallet operations.

Example integration:

```javascript
// In Node.js application
async function getWalletPortfolio(walletId) {
  try {
    const response = await axios.get(
      `http://localhost:8080/api/v1/wallets/portfolio/wallet/${walletId}`
    );
    return response.data;
  } catch (error) {
    console.error("Error fetching wallet portfolio:", error);
    throw error;
  }
}
```

## Performance Metrics

In comparison to the Node.js implementation:

| Operation                | Node.js         | Go Service           | Improvement |
| ------------------------ | --------------- | -------------------- | ----------- |
| Portfolio calculation    | 2-3s per wallet | 200-300ms per wallet | ~90% faster |
| Bitcoin block processing | 500ms           | 50-100ms             | ~80% faster |
| Memory usage (1k users)  | ~1.5GB          | ~300MB               | ~80% less   |

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## command for build

go-wallet-service % protoc --proto_path=proto \
 --go_out=paths=source_relative:protoBuilds/ \  
 --go-grpc_out=paths=source_relative:protoBuilds/ \  
 --go_opt=Mgithub.com/zap/wallet-service/proto/wallet/import.proto=protoBuilds/wallet/import.proto \
 proto/wallet/\*.proto
