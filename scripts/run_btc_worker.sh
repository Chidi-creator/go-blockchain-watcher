#!/bin/bash

# Set log level to debug for detailed logging
export LOG_LEVEL=debug

# Display information about Bitcoin API configuration
echo "============================================================="
echo "Bitcoin Worker - API Configuration"
echo "============================================================="
echo "This worker requires a Bitcoin blockchain.info API access"
echo "You can provide your API URL in one of two ways:"
echo " 1. Set the BTC_MAINNET_BASE_URL environment variable"
echo " 2. Use the default blockchain.info API"
echo "-------------------------------------------------------------"

# Set the Bitcoin API URL from environment or use the default
if [ -z "$BTC_MAINNET_BASE_URL" ]; then
  export BTC_MAINNET_BASE_URL="https://blockchain.info"
  echo "✅ Using default blockchain.info API URL"
else
  echo "✅ Using custom Bitcoin API URL from environment"
fi

echo "-------------------------------------------------------------"

# Build the Bitcoin worker
echo "Building Bitcoin worker..."
go build -o btc_worker ./cmd/run_btc_worker

if [ $? -ne 0 ]; then
  echo "❌ Build failed. Please check for errors above."
  exit 1
fi

echo "✅ Build successful"
echo "-------------------------------------------------------------"

# Run the worker
echo "Starting Bitcoin worker..."
./btc_worker

# Check exit code
if [ $? -ne 0 ]; then
  echo "❌ Worker exited with an error. Check logs above for details."
  echo "   Common issues include:"
  echo "   - Network connectivity problems to blockchain.info"
  echo "   - Rate limiting from the API provider"
  echo "   - Missing dependencies or configuration"
  exit 1
fi 