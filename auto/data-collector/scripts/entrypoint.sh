#!/bin/sh
set -e

WATCHLIST="crypto_watchlist.json"

# Backfill any gap between existing data and now via Kraken API (non-fatal)
echo "Running backfill..."
crypto-data-collector backfill --watchlist "$WATCHLIST" || echo "Backfill failed (non-fatal), continuing to serve..."

# Start the serve loop
exec crypto-data-collector serve --watchlist "$WATCHLIST"
