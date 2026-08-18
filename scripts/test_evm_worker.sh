#!/bin/bash

# Set the test environment
export TEST_ENV=true

# Test with Ethereum chain first
export EVM_CHAIN="eth"

# Run the tests
echo "Running EVM worker tests for Ethereum (ETH)"
go test -v ./cmd/run_evm_worker -run TestEVMWorker

# Test transaction processing
echo "Running transaction processing tests"
go test -v ./cmd/run_evm_worker -run TestProcessTransactions

# Test block monitoring
echo "Running block monitoring tests"
go test -v ./cmd/run_evm_worker -run TestProcessBlock 