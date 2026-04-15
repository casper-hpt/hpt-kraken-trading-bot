# crypto-data-collector

Kraken OHLCV data ingestor for QuestDB. Fetches 15-minute crypto bars from the Kraken public API on a recurring schedule and writes them to QuestDB via ILP/HTTP.

## Quick Start

```bash
# Create the external network (once)
docker network create 2apollo_net

# Start QuestDB + crypto-data-collector
docker compose up -d
```

The service will create the QuestDB schema automatically and begin ingesting bars every 15 minutes.

## Architecture

```
Kraken REST API  --->  crypto-data-collector  --->  QuestDB (ILP/HTTP)
                         |
              crypto_watchlist.json
```

- **crypto-data-collector** fetches OHLC bars from Kraken's public `/OHLC` endpoint for each symbol in the watchlist.
- Bars are written to QuestDB's `crypto_bars_15m` table via ILP/HTTP, with deduplication on `(ts, symbol)`.
- The watchlist is synced to QuestDB's `watchlist` table on each cycle.

## Commands

```bash
crypto-data-collector bootstrap                          # Create QuestDB schema only
crypto-data-collector run-once --watchlist watchlist.json # Run one ingestion cycle
crypto-data-collector serve    --watchlist watchlist.json # Run ingestion loop (15-min cadence)
crypto-data-collector seed     --watchlist watchlist.json --zip /path/to/Kraken_OHLCVT.zip  # Bulk load from ZIP
```

## Bulk Seeding

To backfill historical data, download the Kraken OHLCVT bulk ZIP from [Kraken's Google Drive](https://drive.google.com/drive/folders/1aoA6SKgPbS_p3pYStXUXFvmjqShJ2jv9) and run:

```bash
# From inside the container
#crypto-data-collector seed --zip /src/data/ohlcvt_bulk.zip

# Or use the standalone script
python scripts/seed_cache.py --zip /src/data/ohlcvt_bulk.zip
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `QUESTDB_HOST` | `localhost` | QuestDB hostname |
| `QUESTDB_HTTP_PORT` | `9000` | QuestDB HTTP port (REST + ILP) |
| `INGEST_INTERVAL_MINUTES` | `15` | Minutes between ingestion cycles |
| `LOG_LEVEL` | `INFO` | Logging level |

## Watchlist

Edit `crypto_watchlist.json` to control which symbols are ingested:

```json
{
    "BTC": { "symbol": "BTC" },
    "ETH": { "symbol": "ETH" }
}
```

The compose file bind-mounts this file so changes take effect on the next cycle without rebuilding.

## Local Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## Querying the Data

QuestDB exposes a web console at `http://localhost:9000` and a Postgres-compatible wire protocol on port `8812`. See [wiki/CONNECT.md](wiki/CONNECT.md) for client examples.

## Send bulk data to project

```bash
# pip install gdown
# python scripts/download_ohlcvt.py
scp ~/Downloads/Kraken_OHLCVT.zip casper@192.168.2.18:/home/casper/Desktop/2apollo/trading-bot/kraken_data/src/data/
```

## Deploy to Artifact Registry

```bash
# One-time: authenticate Docker with GCP Artifact Registry
gcloud auth configure-docker northamerica-northeast2-docker.pkg.dev

# Build
docker build -t northamerica-northeast2-docker.pkg.dev/home-2apollo/home-2apollo-quant/crypto-data-collector:latest .

# Push
docker push northamerica-northeast2-docker.pkg.dev/home-2apollo/home-2apollo-quant/crypto-data-collector:latest
```

## Wiki

- [SETUP.md](wiki/SETUP.md) — QuestDB schema and table details
- [CONNECT.md](wiki/CONNECT.md) — Client connection examples
- [DEPLOY.md](wiki/DEPLOY.md) — Deploying to Google Cloud Artifact Registry
