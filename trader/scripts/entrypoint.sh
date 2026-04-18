#!/usr/bin/env bash
set -euo pipefail

# Load .env file if present (local dev only; k8s injects env vars directly)
if [ -f ".env" ]; then
  # shellcheck disable=SC2046
  export $(grep -v '^[[:space:]]*#' .env | xargs)
  echo "[entrypoint] Loaded .env file"
fi

# exec CMD (python -m src.main) if arguments provided
if [ "$#" -gt 0 ]; then
  exec "$@"
fi
