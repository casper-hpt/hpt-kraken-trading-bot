#!/bin/sh
set -e

SEED_DIR="/app/src/data"
WATCHLIST="crypto_watchlist.json"

# Auto-seed if crypto_bars_15m is empty and a ZIP exists
if ls "$SEED_DIR"/*.zip 1>/dev/null 2>&1; then
    # Wait for QuestDB to be queryable
    echo "Checking if crypto_bars_15m needs seeding..."
    COUNT=$(crypto-data-collector bootstrap >/dev/null 2>&1 && \
        python -c "
import requests, sys
try:
    r = requests.get('http://${QUESTDB_HOST:-localhost}:${QUESTDB_HTTP_PORT:-9000}/exec',
                      params={'query': 'SELECT count() cnt FROM crypto_bars_15m;'}, timeout=10)
    ds = r.json().get('dataset', [[0]])
    print(ds[0][0] if ds else 0)
except Exception:
    print(0)
" 2>/dev/null || echo 0)

    if [ "$COUNT" = "0" ]; then
        for zip in "$SEED_DIR"/*.zip; do
            echo "Seeding from $zip ..."
            crypto-data-collector seed --watchlist "$WATCHLIST" --zip "$zip"
        done
    else
        echo "crypto_bars_15m already has $COUNT rows, skipping seed."
    fi
else
    echo "No ZIP files in $SEED_DIR, skipping seed."
fi

# Backfill any gap between seed data and now via Kraken API (non-fatal)
echo "Running backfill to close gap between seed data and now..."
crypto-data-collector backfill --watchlist "$WATCHLIST" || echo "Backfill failed (non-fatal), continuing to serve..."

# Start the serve loop
exec crypto-data-collector serve --watchlist "$WATCHLIST"
