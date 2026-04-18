# data-collector — Development

## Setup

```bash
cd auto/data-collector
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Running Locally

Start QuestDB first, then run the collector:

```bash
# From repo root — start only QuestDB
docker compose up -d questdb

# Then in auto/data-collector with venv active:
crypto-data-collector bootstrap          # create schema
crypto-data-collector run-once           # single ingestion cycle
crypto-data-collector serve              # continuous loop
```

## Tests

```bash
pytest tests/
```

## Project Layout

```
data-collector/
  src/crypto_data_collector/
    main.py              # CLI entry point (bootstrap / run-once / serve / seed)
    config.py            # All env-var config
    kraken_client.py     # Kraken public REST API client
    questdb_schema.py    # Table creation + WAL migration
    questdb_writer.py    # ILP/HTTP write to QuestDB
    questdb_rest.py      # REST query helpers
    watchlist.py         # Watchlist loading and QuestDB sync
    scheduler.py         # Interval loop
    metrics.py           # Prometheus metrics
  scripts/
    entrypoint.sh        # Docker entrypoint
    backfill.py          # Historical backfill helper
  crypto_watchlist.json  # Active symbol list
```

## Bulk Historical Seeding

Download the Kraken OHLCVT ZIP from Kraken's Google Drive, then:

```bash
crypto-data-collector seed --zip /path/to/Kraken_OHLCVT.zip
```

Or transfer to a remote host:
```bash
scp ~/Downloads/Kraken_OHLCVT.zip user@host:/path/to/data/
```

## Updating Dependencies

```bash
pip-compile requirements/requirements-dc.in -o requirements/requirements-dc.txt
pip install -r requirements/requirements-dc.txt
```
