# Local Deployment

## 1. Configure environment

Copy the example file and fill in your credentials:

```bash
cp .env.example .env
```

Open `.env` and set the required values:

| Variable | Where to get it |
|----------|----------------|
| `KRAKEN_API_KEY` | Kraken → Settings → API |
| `KRAKEN_API_SECRET` | Kraken → Settings → API |
| `LLM_API_KEY` | Harris Partners portal |

Everything else has a working default and can be left as-is.

> Set `DRY_RUN=true` to paper-trade — the trader will evaluate the strategy and log orders but send nothing to Kraken.

## 2. Start the stack

```bash
docker compose up -d
```

This starts: QuestDB, data-collector, trader, signal-service, prometheus, grafana.

## 3. Verify

```bash
docker compose ps          # all services should be "running"
docker compose logs -f     # stream all logs
```

| Service | URL |
|---------|-----|
| QuestDB console | http://localhost:9000 |
| Grafana | http://localhost:3000 (admin / admin) |
| Prometheus | http://localhost:9090 |

## Common operations

**View logs for one service:**
```bash
docker compose logs -f trader
docker compose logs -f signal-service
docker compose logs -f data-collector
```

**Stop everything:**
```bash
docker compose down
```

**Restart a single service after a config change:**
```bash
docker compose up -d --no-deps trader
```

**Re-run EMA refit (weekly):**
```bash
docker compose run --rm ema-refit
```

**Edit the watchlist** — update `CRYPTO_LIST` in `auto/.env`, then restart the data-collector:
```bash
docker compose up -d --no-deps data-collector
```
