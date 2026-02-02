#!/usr/bin/env bash
set -e

# Resolve project root based on script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Go to docker directory
cd "$SCRIPT_DIR/docker"

docker compose run --rm app
