#!/bin/bash

# Set the test environment
export TEST_ENV=true

# Run the test
go test -v ./managers/workers -run TestSolanaBlockMonitorQueue

# Run the repeat job test
go test -v ./managers/workers -run TestSolanaBlockMonitorRepeatJob

# Run the start block monitor test
go test -v ./managers/workers -run TestSolanaBlockMonitorStartBlockMonitor 