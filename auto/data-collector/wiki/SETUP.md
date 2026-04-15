# QuestDB Structure

This document describes the QuestDB schema, how tables are organized, and how data flows into them.

## Database Overview

The service uses **QuestDB 9.3.2**, a time-series database. All tables use:

- **WAL** (Write-Ahead Logging) for durability
- **DEDUP UPSERT KEYS** for automatic deduplication
- **Partitioning** for query performance

## Tables

### `crypto_bars_15m` — 15-Minute OHLCV Bars

Stores intraday price/volume data fetched from the Kraken public API.

```sql
CREATE TABLE IF NOT EXISTS crypto_bars_15m (
    ts TIMESTAMP,
    symbol SYMBOL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    source SYMBOL,
    ingested_at TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
  DEDUP UPSERT KEYS(ts, symbol);
```

| Column | Type | Description |
|--------|------|-------------|
| `ts` | TIMESTAMP | Bar timestamp (UTC, designated timestamp) |
| `symbol` | SYMBOL | Crypto symbol (e.g. `BTC`, `ETH`) |
| `open` | DOUBLE | Opening price |
| `high` | DOUBLE | High price |
| `low` | DOUBLE | Low price |
| `close` | DOUBLE | Closing price |
| `volume` | DOUBLE | Trade volume |
| `source` | SYMBOL | Data source (`kraken`) |
| `ingested_at` | TIMESTAMP | When the row was written |

**Partitioning:** By day. Each day's data lives in its own partition for fast time-range queries.

**Deduplication:** On `(ts, symbol)`. Re-ingesting the same bar for the same symbol and timestamp overwrites the existing row rather than creating a duplicate.

**Data flow:** New bars are inserted via ILP/HTTP on every 15-minute cycle. Only bars with `ts > max(ts)` for that symbol are written, so the dedup key is a safety net.

---

### `watchlist` — Active Symbol List

Tracks which symbols are being ingested. A new snapshot is written on every ingestion cycle.

```sql
CREATE TABLE IF NOT EXISTS watchlist (
    updated_at TIMESTAMP,
    symbol SYMBOL
) TIMESTAMP(updated_at) PARTITION BY MONTH WAL
  DEDUP UPSERT KEYS(updated_at, symbol);
```

| Column | Type | Description |
|--------|------|-------------|
| `updated_at` | TIMESTAMP | When this snapshot was written (designated timestamp) |
| `symbol` | SYMBOL | Crypto symbol |

**Partitioning:** By month. Snapshots accumulate over time, giving a historical record of which symbols were active.

**Deduplication:** On `(updated_at, symbol)`. If the same cycle writes the same symbol twice, it deduplicates.

**Data flow:** On each ingestion cycle, the full watchlist is written as a new snapshot. Query the latest snapshot with `LATEST ON`.

---

## Querying Patterns

**Get the latest bar per symbol:**

```sql
SELECT *
FROM crypto_bars_15m
LATEST ON ts PARTITION BY symbol;
```

**Get the current watchlist:**

```sql
SELECT *
FROM watchlist
LATEST ON updated_at PARTITION BY symbol;
```

## Schema Migrations

The `SchemaManager` handles migrations automatically. When the service starts (`bootstrap`, `seed`, `run-once`, or `serve`), it:

1. Runs `CREATE TABLE IF NOT EXISTS` for both tables
2. Checks if existing tables need WAL conversion
3. Converts non-WAL tables to WAL if needed

This means you can update the code and restart the service without manually altering the database.

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 9000 | HTTP | Web Console + REST `/exec` + ILP/HTTP ingestion |
| 9009 | TCP | ILP/TCP (optional) |
| 8812 | PGWire | Postgres-compatible queries |
| 9003 | HTTP | Health checks and metrics |

See [CONNECT.md](CONNECT.md) for client connection examples.
