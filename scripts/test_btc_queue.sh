#!/bin/bash

# Set the test environment
export TEST_ENV=true

# Run the test
go test -v ./managers/workers -run TestBitcoinBlockMonitorQueue

# Run the repeat job test
go test -v ./managers/workers -run TestBitcoinBlockMonitorRepeatJob

# Run the start block monitor test
go test -v ./managers/workers -run TestBitcoinBlockMonitorStartBlockMonitor 