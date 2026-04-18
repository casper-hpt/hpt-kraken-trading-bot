# data-collector

Kraken OHLCV data ingestor. Fetches 15-minute crypto bars from the Kraken public REST API on a recurring schedule and writes them to QuestDB via ILP/HTTP. All other services depend on the data this service produces.

## Architecture

```
Kraken REST API (/OHLC endpoint)
        │
        ▼
  data-collector
        │  crypto_watchlist.json (symbol list)
        ▼
  QuestDB
    ├── crypto_bars_15m  (OHLCV bars, deduped on ts + symbol)
    └── watchlist        (active symbol snapshots)
```

## Commands

| Command | Description |
|---------|-------------|
| `crypto-data-collector serve` | Main mode — ingestion loop every 15 min |
| `crypto-data-collector run-once` | Single ingestion cycle then exit |
| `crypto-data-collector bootstrap` | Create QuestDB schema only |
| `crypto-data-collector seed --zip <path>` | Bulk-load historical data from Kraken ZIP |

## Data Tables

- **`crypto_bars_15m`** — 15-min OHLCV bars, partitioned by day, WAL, deduped on `(ts, symbol)`. See [SETUP.md](SETUP.md) for full schema.
- **`watchlist`** — snapshot of active symbols written each cycle, partitioned by month.

## Key Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `QUESTDB_HOST` | `localhost` | QuestDB hostname |
| `QUESTDB_HTTP_PORT` | `9000` | QuestDB HTTP port |
| `INGEST_INTERVAL_MINUTES` | `15` | Minutes between ingestion cycles |
| `PROMETHEUS_PORT` | `9092` | Metrics endpoint |
| `LOG_LEVEL` | `INFO` | Log verbosity |

## Watchlist

Edit `crypto_watchlist.json` to control which symbols are collected. The compose file bind-mounts this file so changes take effect on the next cycle without rebuilding.

## Related Wiki

- [SETUP.md](SETUP.md) — QuestDB schema and table details
- [CONNECT.md](CONNECT.md) — Client connection examples and common queries
