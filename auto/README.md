# 2Apollo Bot

Monorepo for the 2Apollo automated trading system and LLM assistant. Contains a crypto trading portfolio and an LLM-powered Slack bot. All services are containerized, pushed to Google Cloud Artifact Registry, and deployed via k3s Kubernetes.

## Repository Structure

```
auto/
  crypto-data-collector/   # Ingests crypto 15m bars from Kraken into QuestDB
  crypto-trader/           # Evaluates momentum strategy + executes Kraken trades
  llm/                     # Slack bot powered by local Ollama with tool support
  k8s/
    portfolio/             # K8s manifests for portfolio infra + services
      questdb.yaml
      kafka.yaml
      prometheus.yaml
      grafana.yaml
      crypto-data-collector.yaml
      crypto-trader.yaml
    utils/
      prometheus.yaml
      grafana.yaml
    llm/
      hpt-llm.yaml
```

## Architecture

### Trading Pipeline

```
[crypto-data-collector] → QuestDB → [crypto-trader] → Kraken API
                                           ↓
                                       Prometheus ← scrape
                                           ↓
                                       Grafana
```

- **crypto-data-collector** polls Kraken on interval, writes 15m OHLCV bars to QuestDB
- **crypto-trader** reads bars from QuestDB, computes momentum + EMA signals, executes trades on Kraken

### LLM Assistant

```
Slack (Socket Mode) → hpt-llm → Ollama (local) → tools (e.g. Bitcoin price)
```

- **hpt-llm** connects to Slack via Socket Mode (no ingress needed), forwards messages to a local Ollama model, and supports extensible tool calling (auto-discovered from `llm/src/hpt_llm/tools/`)

## Tech Stack

- **Language:** Python 3.12
- **Build:** setuptools via pyproject.toml per service
- **Database:** QuestDB (tables: `crypto_bars_15m`, `crypto_watchlist`)
- **Metrics:** Prometheus + Grafana (all services expose `/metrics`)
- **Container runtime:** Docker, images pushed to GCP Artifact Registry
- **Deployment:** k3s Kubernetes (manifests in `k8s/`)

## Services

### Portfolio Services

| Service | Path | Description |
|---------|------|-------------|
| crypto-data-collector | `auto/crypto-data-collector/` | Ingests crypto 15m bars from Kraken into QuestDB |
| crypto-trader | `auto/crypto-trader/` | Momentum + EMA strategy evaluation and trade execution on Kraken |

### LLM Service

| Service | Path | Description |
|---------|------|-------------|
| hpt-llm | `auto/llm/` | Slack bot backed by local Ollama with extensible tool support |

## Development

Each service is independent with its own virtualenv:

```bash
cd auto/<service>
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/
```

## Docker

Build any service:

```bash
docker build -t <service-name> auto/<service>/
```

## Deployment

See [KUBE_DEPLOY.md](KUBE_DEPLOY.md) for full Kubernetes deployment instructions.

## Conventions

- Each service has: `src/`, `tests/`, `scripts/`, `requirements/`, `Dockerfile`, `pyproject.toml`
- Services run as non-root user `appuser` (UID 10001) in production containers
- Environment config via `.env` files locally (see `.env.example`), K8s ConfigMaps/Secrets in prod
- All services expose Prometheus metrics on configurable `PROMETHEUS_PORT`
- Never commit `.env` files (covered by `.gitignore`)
