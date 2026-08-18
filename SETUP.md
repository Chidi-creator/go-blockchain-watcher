# Wallet Service Setup Guide

This document provides a step-by-step guide to setting up and running the wallet service.

## Prerequisites

- Go 1.21.0 or later
- MongoDB
- Redis
- Protocol Buffers compiler (protoc v5.29.1 or compatible)
- Git

## Installation Steps

### Set Up Environment Variables

Create a `.env` file in the project root:

```bash
cp .env.example .env
```

Edit the `.env` file to configure your:

- MongoDB connection
- Redis connection
- Blockchain node URLs
- API keys
- Other service-specific settings

### 3. Install Dependencies

```bash
go mod download
```

### 4. Set Up Protocol Buffers (if making changes to proto files)

Install the required protoc plugins:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
```

Generate code from proto files:

```bash
protoc --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. proto/wallet/*.proto
```

### 5. Build the Service

```bash
go build -o wallet-service
```

To avoid the `-no_pie` warning on macOS:

```bash
go build -buildmode=pie -o wallet-service
```

### 6. Run the Service

```bash
./wallet-service
```

Or directly with Go:

```bash
go run main.go
```

## Running with Docker

### 1. Build the Docker Image

```bash
docker build -t wallet-service .
```

### 2. Run with Docker Compose

```bash
docker-compose up -d
```

This will start the wallet service and its dependencies (MongoDB, Redis, etc.).

## API Documentation

The API documentation is available at:

- Swagger UI: http://localhost:8080/swagger/index.html (when service is running)
- OpenAPI Spec: See `docs/api.yaml`

## Troubleshooting

### Common Issues

1. **MongoDB Connection Errors**:

   - Verify MongoDB is running
   - Check connection string in `.env`
   - Ensure network access to MongoDB server

2. **Redis Connection Issues**:

   - Verify Redis is running
   - Check Redis connection settings in `.env`

3. **Protocol Buffer Generation Errors**:

   - Ensure you have compatible versions of protoc and plugins
   - Use the exact command in step 4 of this guide
   - Check for import errors in proto files

4. **Build Warnings on macOS**:
   - Use the `-buildmode=pie` flag when building
   - Or set `export CGO_LDFLAGS="-Wl,-pie"`

### Getting Help

For additional help, please:

1. Check the project documentation in `docs/`
2. Open an issue on GitHub
3. Contact the development team

## Architecture Overview

The wallet service is structured as follows:

- `cmd/` - Entry points for different executables
- `interfaces/` - API interfaces (HTTP, gRPC)
- `models/` - Domain models
- `proto/` - Protocol Buffer definitions
- `src/` - Main source code
  - `repositories/` - Data access layer
  - `services/` - Business logic
  - `usecases/` - Application use cases
- `config/` - Configuration handling
- `managers/` - Cross-cutting concerns like logging, auth, etc.

## Development Workflow

1. Make code changes
2. Update tests
3. Run tests locally
4. Update proto files if needed and regenerate code
5. Build and run locally
6. Submit pull request
