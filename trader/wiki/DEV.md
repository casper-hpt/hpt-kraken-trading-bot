# trader — Development

## Setup

```bash
cd auto/trader
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Running Locally

The trader requires a live QuestDB instance with data. Start the full stack first:

```bash
# From repo root
docker compose up -d questdb data-collector
```

Then run the trader against it:

```bash
cd auto/trader
source .venv/bin/activate

# Dry-run mode — evaluates strategy but skips all Kraken API calls
DRY_RUN=true QUESTDB_HOST=localhost python -m src.main

# EMA refit — re-optimizes EMA parameters and writes ema_params.json
python -m src.main refit
```

## Tests

```bash
pytest tests/
```

## Project Layout

```
trader/
  src/
    config.py            # All env-var config
    main.py              # Entry point (serve / refit)
    engine/
      engine.py          # Rebalance loop
      strategy.py        # Position evaluation logic
      momentum.py        # Momentum score computation
      ema_filter.py      # EMA trend filter
      ema_refit.py       # EMA parameter optimization
    kraken_api/
      client.py          # Kraken REST API client
      auth.py            # HMAC-SHA512 request signing
      models.py          # Order/balance types
    market/
      questdb_client.py  # Bar data queries + caching
    positions/
      positions_cache.py # Position state management
    data/
      ema_params.json    # Optimized EMA pairs (per symbol)
```

## Environment Variables

Copy `auto/.env.example` to `auto/.env` and set at minimum:

```bash
KRAKEN_API_KEY=...
KRAKEN_API_SECRET=...
DRY_RUN=true   # recommended while developing
```

## Updating Dependencies

```bash
pip-compile requirements/requirements-t.in -o requirements/requirements-t.txt
pip install -r requirements/requirements-t.txt
```
