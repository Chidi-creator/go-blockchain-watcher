#!/bin/bash

# Set log level to debug for detailed logging
export LOG_LEVEL=debug

# Display information about TRON API configuration
echo "============================================================="
echo "Tron Worker - API Configuration"
echo "============================================================="
echo "This worker requires a TRON API key for TronGrid API access"
echo "You can provide your API key in one of two ways:"
echo " 1. Set the TRON_API_KEY environment variable"
echo " 2. Pass it as an argument to this script"
echo "-------------------------------------------------------------"

# Set the TRON API key from environment or command-line argument
if [ -z "$TRON_API_KEY" ]; then
  # Check if key exists as a parameter
  if [ ! -z "$1" ]; then
    export TRON_API_KEY="$1"
    echo "✅ Using provided Tron API key parameter"
  else
    echo "⚠️  WARNING: No Tron API key found!"
    echo "    Service may be rate-limited or face authentication errors."
    echo "    Get an API key at: https://www.trongrid.io/"
    read -p "Do you want to continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      echo "Exiting..."
      exit 1
    fi
  fi
else
  echo "✅ Using Tron API key from environment"
fi

# Set the TRON API URL from environment or use default
if [ -z "$TRON_API_URL" ]; then
  export TRON_API_URL="https://api.trongrid.io"
  echo "✅ Using default TronGrid API URL"
else
  echo "✅ Using custom Tron API URL from environment"
fi

echo "-------------------------------------------------------------"

# Build the Tron worker
echo "Building Tron worker..."
go build -o tron_worker ./cmd/run_tron_worker

if [ $? -ne 0 ]; then
  echo "❌ Build failed. Please check for errors above."
  exit 1
fi

echo "✅ Build successful"
echo "-------------------------------------------------------------"

# Run the worker
echo "Starting Tron worker..."
./tron_worker

# Check exit code
if [ $? -ne 0 ]; then
  echo "❌ Worker exited with an error. Check logs above for details."
  echo "   Common issues include:"
  echo "   - Invalid or missing Tron API key"
  echo "   - Network connectivity problems"
  echo "   - Rate limiting from the API provider"
  echo "   - Missing dependencies or configuration"
  exit 1
fi 