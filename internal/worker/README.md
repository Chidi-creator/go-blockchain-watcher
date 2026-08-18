# Worker Package

This package contains the core implementations of various worker services. The worker implementations are separated from their entry points to enable reusability and easier testing.

## Directory Structure

```
internal/worker/
├── base.go           # Common interfaces and types shared across all workers
├── combined/         # Combined worker implementation
├── evm/              # EVM worker implementation
├── changenow/        # ChangeNow exchange worker implementation
├── btc/              # Bitcoin worker implementation (planned)
├── solana/           # Solana worker implementation (planned)
├── tron/             # Tron worker implementation (planned)
```

## Architecture

Each worker package follows a similar structure:

1. A main `Worker` struct that implements the `worker.Worker` interface
2. Specialized processor implementations as needed
3. Helper functions specific to the worker's domain

The worker implementations are designed to be:

- **Configurable**: Workers can be configured through the standard config packages.
- **Testable**: Business logic is separated from I/O concerns, enabling unit testing.
- **Reusable**: Core functionality can be reused across multiple entry points.

## Interfaces

The package defines the following key interfaces:

### Worker

```go
type Worker interface {
    Start(ctx context.Context) error
}
```

All worker implementations must fulfill this interface, which allows for a standard way to start and manage worker processes.

## Worker Components

### Combined Worker

The combined worker orchestrates multiple worker types in a single process, running them concurrently.

### EVM Worker

The EVM worker handles blockchain-related tasks for Ethereum Virtual Machine compatible chains.

### ChangeNow Worker

The ChangeNow worker integrates with the ChangeNow exchange API for transaction processing.

## Testing

Worker implementations should include unit tests that mock external dependencies like Redis, MongoDB, and API clients. 