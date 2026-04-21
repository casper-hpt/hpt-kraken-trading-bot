# Harris Partners Bot


## Repository Structure

```
auto/
  data-collector/   # Ingests crypto 15m bars from Kraken into QuestDB
  trader/           # Momentum + EMA strategy + Kraken trade execution
  llm-signal/       # LLM-powered news signal classifier
  grafana/          # Grafana dashboard provisioning
  prometheus/       # Prometheus scrape config
web/                # React web UI (watchlist + service controls)
docker-compose.yml  # Local development environment
KUBE_DEPLOY.md      # Kubernetes deployment guide
```

## Architecture

```
[data-collector] → QuestDB → [trader] → Kraken API
                                  ↑
                           [llm-signal] → QuestDB (signals)
                                  ↓
                           Prometheus ← scrape
                                  ↓
                           Grafana
```

- **data-collector** polls Kraken on a 15-min schedule, writes OHLCV bars to QuestDB
- **llm-signal** fetches crypto news (RSS + GDELT), classifies via LLM, writes structured signals to QuestDB
- **trader** reads bars and signals from QuestDB, evaluates momentum + EMA strategy, executes trades on Kraken

## Tech Stack

- **Language:** Python 3.12
- **Database:** QuestDB (tables: `crypto_bars_15m`, `crypto_watchlist`, `crypto_signals`)
- **Metrics:** Prometheus + Grafana (all services expose `/metrics`)
- **Containers:** Docker, images pushed to GCP Artifact Registry
- **Deployment:** k3s Kubernetes

## Quick Start (Local)

```bash
cp auto/.env.example auto/.env
# Edit auto/.env — add KRAKEN_API_KEY, KRAKEN_API_SECRET, LLM_API_KEY

docker compose up -d
```

Services start at:
- QuestDB console: http://localhost:9000
- Grafana: http://localhost:3000 (admin / admin)
- Prometheus: http://localhost:9090
- Web UI: http://localhost:8080

## Service Wikis

| Service | Info | Dev | Deploy |
|---------|------|-----|--------|
| data-collector | [INFO](auto/data-collector/wiki/INFO.md) | [DEV](auto/data-collector/wiki/DEV.md) | [DEPLOY](auto/data-collector/wiki/DEPLOY.md) |
| trader | [INFO](auto/trader/wiki/INFO.md) | [DEV](auto/trader/wiki/DEV.md) | [DEPLOY](auto/trader/wiki/DEPLOY.md) |
| llm-signal | [INFO](llm-signal/wiki/INFO.md) | [DEV](llm-signal/wiki/DEV.md) | [DEPLOY](llm-signal/wiki/DEPLOY.md) |

## Deployment

See [KUBE_DEPLOY.md](KUBE_DEPLOY.md) for full Kubernetes deployment instructions.

## Conventions

- Each service has: `src/`, `tests/`, `scripts/`, `requirements/`, `Dockerfile`, `pyproject.toml`
- Services run as non-root user `appuser` (UID 10001) in production containers
- Config via `.env` locally (see `auto/.env.example`), K8s ConfigMaps/Secrets in prod
- All services expose Prometheus metrics on a configurable `PROMETHEUS_PORT`
- Never commit `.env` files
