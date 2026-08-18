#!/bin/bash

# Source environment variables if .env file exists
if [ -f .env ]; then
  export $(cat .env | grep -v '^#' | xargs)
fi

# Define colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting EVM Worker...${NC}"

# Set default values if not set in environment
CONCURRENCY=${EVM_CONCURRENCY:-5}
LOG_LEVEL=${LOG_LEVEL:-info}
CHAIN=${EVM_CHAIN:-""}

# Check for docker or local run mode
if [ "$1" == "docker" ]; then
  echo -e "${YELLOW}Building and running EVM worker in Docker...${NC}"
  docker build -t evm-worker -f build/docker/evm_worker.Dockerfile .
  docker run -it --rm \
    --env-file .env \
    --name evm-worker \
    evm-worker
else
  echo -e "${YELLOW}Running EVM worker locally...${NC}"
  
  # Build the worker if needed
  if [ ! -f ./evm_worker ] || [ ./cmd/workers/evm/main.go -nt ./evm_worker ]; then
    echo -e "${GREEN}Building EVM worker...${NC}"
    go build -o evm_worker ./cmd/workers/evm
    if [ $? -ne 0 ]; then
      echo -e "${RED}Build failed!${NC}"
      exit 1
    fi
  fi
  
  # Run the worker
  ./evm_worker --concurrency=${CONCURRENCY} \
               --log-level=${LOG_LEVEL} \
               --chain=${CHAIN} \
               "$@"
fi