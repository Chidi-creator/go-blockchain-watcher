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

echo -e "${GREEN}Starting ChangeNow Worker...${NC}"

# Set default values if not set in environment
CONCURRENCY=${CHANGENOW_CONCURRENCY:-5}
LOG_LEVEL=${LOG_LEVEL:-info}

# Check for docker or local run mode
if [ "$1" == "docker" ]; then
  echo -e "${YELLOW}Building and running ChangeNow worker in Docker...${NC}"
  docker build -t changenow-worker -f build/docker/changenow_worker.Dockerfile .
  docker run -it --rm \
    --env-file .env \
    --name changenow-worker \
    changenow-worker
else
  echo -e "${YELLOW}Running ChangeNow worker locally...${NC}"
  
  # Build the worker if needed
  if [ ! -f ./changenow_worker ] || [ ./cmd/workers/changenow/main.go -nt ./changenow_worker ]; then
    echo -e "${GREEN}Building ChangeNow worker...${NC}"
    go build -o changenow_worker ./cmd/workers/changenow
    if [ $? -ne 0 ]; then
      echo -e "${RED}Build failed!${NC}"
      exit 1
    fi
  fi
  
  # Run the worker
  ./changenow_worker --concurrency=${CONCURRENCY} \
                     --log-level=${LOG_LEVEL} \
                     "$@"
fi 