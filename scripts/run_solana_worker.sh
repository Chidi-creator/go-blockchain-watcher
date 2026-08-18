#!/bin/bash

# Set log level to debug for detailed logging
export LOG_LEVEL=debug

# Display information about Ankr API key requirements
echo "============================================================="
echo "Solana Worker - Ankr API Key Configuration"
echo "============================================================="
echo "This worker requires an Ankr API key to access Solana RPC."
echo "You can provide it in one of two ways:"
echo " 1. Set the SOLANA_ANKR_API_KEY environment variable"
echo " 2. Pass it as an argument to this script"
echo "-------------------------------------------------------------"

# Set the Ankr API key from environment or use the default
if [ -z "$SOLANA_ANKR_API_KEY" ]; then
  # Check if key exists as a parameter
  if [ ! -z "$1" ]; then
    export SOLANA_ANKR_API_KEY="$1"
    echo "✅ Using provided Ankr API key parameter"
  else
    echo "⚠️  WARNING: No Solana Ankr API key found!"
    echo "    Service may fail with authentication errors."
    echo "    Get an API key at: https://www.ankr.com/rpc/solana/"
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

# Build the Solana worker
echo "Building Solana worker..."
go build -o solana_worker ./cmd/run_solana_worker

if [ $? -ne 0 ]; then
  echo "❌ Build failed. Please check for errors above."
  exit 1
fi

echo "✅ Build successful"
echo "-------------------------------------------------------------"

# Run the worker
echo "Starting Solana worker..."
./solana_worker

# Check exit code
if [ $? -ne 0 ]; then
  echo "❌ Worker exited with an error. Check logs above for details."
  echo "   Common issues include:"
  echo "   - Invalid or missing Ankr API key"
  echo "   - Network connectivity problems"
  echo "   - Missing dependencies or configuration"
  exit 1
fi 