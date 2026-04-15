# 2Apollo Bot - Project Reference

## Overview

Monorepo for 2Apollo automated trading system. One crypto trading portfolio, containerized, pushed to Google Cloud Artifact Registry, and deployed via k3s Kubernetes.

## Repository Structure

```
auto/
  crypto-data-collector/   # Ingests crypto 15m bars from Kraken into QuestDB
  crypto-trader/           # Momentum + EMA strategy + Kraken trade execution
  llm/                     # Slack bot powered by local Ollama
  k8s/
    portfolio/             # K8s manifests for infra + services
    utils/                 # prometheus.yaml, grafana.yaml
    llm/                   # hpt-llm.yaml
```

## Architecture

```
[crypto-data-collector] → QuestDB → [crypto-trader] → Kraken API
                                           ↓
                                       Prometheus ← scrape
                                           ↓
                                       Grafana
```

- **crypto-data-collector** polls Kraken, writes 15m bars to QuestDB
- **crypto-trader** reads bars, evaluates momentum + EMA signals, executes trades directly via Kraken API keys from env

## Tech Stack

- **Language:** Python 3.12
- **Build:** setuptools via pyproject.toml per service
- **Database:** QuestDB (tables: `crypto_bars_15m`, `crypto_watchlist`)
- **Metrics:** Prometheus + Grafana (all services expose `/metrics`)
- **Container runtime:** Docker, images pushed to GCP Artifact Registry
- **Deployment:** k3s Kubernetes (manifests in `k8s/`)

## Services

### crypto-data-collector
- Docker base: `python:3.12-slim`
- Package: `crypto-data-collector`
- Deps in: `requirements/requirements-cdc.txt`
- Entry: `crypto-data-collector serve` (via scripts/entrypoint.sh)
- Env: `QUESTDB_HOST`, `QUESTDB_HTTP_PORT` (9000), `INGEST_INTERVAL_MINUTES`, `PROMETHEUS_PORT` (9092)

### crypto-trader
- Docker base: `python:3.12-slim`
- Package: `crypto-trader` (flat `src/` layout)
- Deps in: `requirements/requirements-ct.txt`
- Entry: `python -m src.main` (via scripts/entrypoint.sh)
- Env: `QUESTDB_*`, `KRAKEN_API_KEY`, `KRAKEN_API_SECRET`, `DRY_RUN`, `CYCLE_INTERVAL` (15 min), `MAX_POSITIONS` (5), `SETTLEMENT_DELAY_SECONDS` (5.0), `PROMETHEUS_PORT` (9095), `POSITIONS_PATH` (/data/positions.json), `VERBOSE`

### hpt-llm
- Docker base: `python:3.12-slim`
- Entry: `python -m hpt_llm`
- Env: `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`, `OLLAMA_BASE_URL`, `OLLAMA_MODEL`

## Kubernetes Manifests (`k8s/`)

| File | Resource types | Purpose |
|---|---|---|
| `portfolio/questdb.yaml` | StatefulSet, Service, PVC | QuestDB (10Gi storage) |
| `portfolio/crypto-data-collector.yaml` | ConfigMap, Deployment, Service | |
| `portfolio/crypto-trader.yaml` | ConfigMap, Secret, Deployment, Service, PVC | |
| `utils/prometheus.yaml` | ConfigMap, Deployment, Service | Prometheus + scrape config |
| `utils/grafana.yaml` | ConfigMaps, Deployment, Service | Grafana + datasources + dashboards |

See `KUBE_DEPLOY.md` for full deployment instructions.

## QuestDB Tables

| Table | Written by | Read by |
|---|---|---|
| `crypto_bars_15m` | crypto-data-collector | crypto-trader |
| `crypto_watchlist` | crypto-data-collector | crypto-trader |

## Development

Each service is independent with its own virtualenv:
```bash
cd auto/<service>
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Run tests per service:
```bash
pytest tests/
```

## Docker

Build any service:
```bash
docker build -t <service-name> auto/<service>/
```

## Conventions

- Each service has: `src/`, `tests/`, `scripts/`, `requirements/`, `Dockerfile`, `pyproject.toml`
- Services run as non-root user `appuser` (UID 10001) in production containers
- Environment config via `.env` files locally (see root `.env.example`), K8s ConfigMaps/Secrets in prod
- All services expose Prometheus metrics on configurable `PROMETHEUS_PORT`
- Never commit `.env` files (covered by `.gitignore`)
