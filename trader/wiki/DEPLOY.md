# trader — Deployment

## Docker (local)

```bash
# From repo root
docker compose up -d trader
docker compose logs -f trader
```

## Google Cloud Artifact Registry

### One-time setup

```bash
gcloud auth configure-docker northamerica-northeast2-docker.pkg.dev
```

### Build and push

```bash
REGISTRY=northamerica-northeast2-docker.pkg.dev/home-2apollo/home-2apollo-quant

# Apple Silicon → must cross-compile for amd64
docker build --platform linux/amd64 \
  -t $REGISTRY/trader:latest \
  auto/trader/

docker push $REGISTRY/trader:latest
```

### Update running deployment

```bash
kubectl rollout restart deployment/trader
kubectl rollout status deployment/trader
kubectl logs -f deployment/trader
```

## Kubernetes Secrets

The trader requires Kraken credentials as a K8s secret:

```bash
kubectl create secret generic trader-secret \
  --from-literal=KRAKEN_API_KEY='your-key' \
  --from-literal=KRAKEN_API_SECRET='your-secret'
```

## Persistent Volumes

Two volumes are required:

| Mount | Purpose |
|-------|---------|
| `/app/src/data` | `ema_params.json` — shared with ema-refit job |
| `/data` | `positions.json` — survives pod restarts |

## EMA Refit Job

Run periodically (e.g. weekly via cron) to re-optimize EMA parameters:

```bash
# Docker Compose
docker compose run --rm ema-refit

# Kubernetes (one-shot job)
kubectl create job ema-refit --image=$REGISTRY/trader:latest \
  -- python -m src.main refit
```
