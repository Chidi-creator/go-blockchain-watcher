#!/bin/bash

# Set log level to debug for detailed logging
export LOG_LEVEL=debug

# Set the target chain
export EVM_CHAIN="eth"

# Display information about API configuration
echo "============================================================="
echo "Ethereum Worker - Ankr API Configuration"
echo "============================================================="
echo "This worker requires an Ankr API key for optimal performance"
echo "You can provide your API key in one of two ways:"
echo " 1. Set the ANKR_API_KEY environment variable"
echo " 2. Pass it as an argument to this script"
echo "-------------------------------------------------------------"

# Set the Ankr API key from environment or command-line argument
if [ -z "$ANKR_API_KEY" ]; then
  # Check if key exists as a parameter
  if [ ! -z "$1" ]; then
    export ANKR_API_KEY="$1"
    echo "✅ Using provided Ankr API key parameter"
  else
    echo "⚠️  WARNING: No Ankr API key found!"
    echo "    Service may be rate-limited or face authentication errors."
    echo "    Get an API key at: https://www.ankr.com/rpc/"
    read -p "Do you want to continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      echo "Exiting..."
      exit 1
    fi
  fi
else
  echo "✅ Using Ankr API key from environment"
fi

echo "-------------------------------------------------------------"

# Build the EVM worker
echo "Building Ethereum worker..."
go build -o eth_worker ./cmd/run_evm_worker

if [ $? -ne 0 ]; then
  echo "❌ Build failed. Please check for errors above."
  exit 1
fi

echo "✅ Build successful"
echo "-------------------------------------------------------------"

# Run the worker
echo "Starting Ethereum worker..."
# ./eth_worker
./eth_worker --chain eth --log-level debug
# Check exit code
if [ $? -ne 0 ]; then
  echo "❌ Worker exited with an error. Check logs above for details."
  echo "   Common issues include:"
  echo "   - Invalid or missing Ankr API key"
  echo "   - Network connectivity problems"
  echo "   - Rate limiting from the API provider"
  echo "   - Missing dependencies or configuration"
  exit 1
fi 