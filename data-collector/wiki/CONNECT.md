# Connecting to the Data

This guide explains how to query the crypto bars data from a separate program.

## Connection Options

QuestDB exposes two query interfaces:

| Protocol | Port | Use Case |
|----------|------|----------|
| PGWire (Postgres) | 8812 | Best for most applications - use any Postgres client |
| REST API | 9000 | HTTP queries via `/exec?query=...` |

## Schema

### `crypto_bars_15m` — 15-Minute OHLCV Bars

| Column | Type | Description |
|--------|------|-------------|
| `ts` | TIMESTAMP | Bar timestamp (UTC) |
| `symbol` | SYMBOL | Crypto symbol (e.g., `BTC`) |
| `open` | DOUBLE | Opening price |
| `high` | DOUBLE | High price |
| `low` | DOUBLE | Low price |
| `close` | DOUBLE | Closing price |
| `volume` | DOUBLE | Trade volume |
| `source` | SYMBOL | Data source (`kraken`) |
| `ingested_at` | TIMESTAMP | When the row was inserted |

The table is partitioned by day and deduplicates on `(ts, symbol)`.

### `watchlist` — Active Symbols

| Column | Type | Description |
|--------|------|-------------|
| `updated_at` | TIMESTAMP | When the watchlist was synced |
| `symbol` | SYMBOL | Crypto symbol |

The watchlist is synced on each ingestion cycle. Query the latest snapshot using `LATEST ON`.

## Python Examples

### Using psycopg (sync)

```python
import psycopg

conn = psycopg.connect(
    host="localhost",
    port=8812,
    user="admin",
    password="quest",
    dbname="qdb",
)

with conn.cursor() as cur:
    cur.execute("""
        SELECT ts, symbol, close, volume
        FROM crypto_bars_15m
        WHERE symbol = 'BTC'
          AND ts >= dateadd('d', -5, now())
        ORDER BY ts DESC
    """)
    rows = cur.fetchall()
```

### Using asyncpg (async)

```python
import asyncpg

async def get_bars(symbol: str, days: int = 5):
    conn = await asyncpg.connect(
        host="localhost",
        port=8812,
        user="admin",
        password="quest",
        database="qdb",
    )
    rows = await conn.fetch("""
        SELECT ts, open, high, low, close, volume
        FROM crypto_bars_15m
        WHERE symbol = $1
          AND ts >= dateadd('d', $2, now())
        ORDER BY ts
    """, symbol, -days)
    await conn.close()
    return rows
```

### Using pandas + SQLAlchemy

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://admin:quest@localhost:8812/qdb")

df = pd.read_sql("""
    SELECT ts, symbol, open, high, low, close, volume
    FROM crypto_bars_15m
    WHERE symbol = 'ETH'
      AND ts >= dateadd('d', -30, now())
""", engine)
```

### Using REST API (requests)

```python
import requests

query = """
SELECT ts, symbol, close
FROM crypto_bars_15m
WHERE symbol = 'SOL'
LIMIT 10
"""

resp = requests.get(
    "http://localhost:9000/exec",
    params={"query": query},
)
data = resp.json()
# data["dataset"] contains rows, data["columns"] contains column metadata
```

## Common Queries

**Latest bar for each symbol:**
```sql
SELECT symbol, ts, close
FROM crypto_bars_15m
LATEST ON ts PARTITION BY symbol;
```

**Daily OHLCV aggregation:**
```sql
SELECT
    symbol,
    timestamp_floor('d', ts) AS day,
    first(open) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close) AS close,
    sum(volume) AS volume
FROM crypto_bars_15m
WHERE ts >= dateadd('d', -30, now())
SAMPLE BY 1d ALIGN TO CALENDAR;
```

**Bars from today only:**
```sql
SELECT *
FROM crypto_bars_15m
WHERE ts >= timestamp_floor('d', now())
ORDER BY symbol, ts;
```

**Current watchlist:**
```sql
SELECT symbol
FROM watchlist
LATEST ON updated_at PARTITION BY symbol
ORDER BY symbol;
```

## Connection Defaults

If running via `docker compose up`:

- **Host:** `localhost`
- **PGWire Port:** `8812`
- **REST Port:** `9000`
- **User:** `admin`
- **Password:** `quest`
- **Database:** `qdb`
