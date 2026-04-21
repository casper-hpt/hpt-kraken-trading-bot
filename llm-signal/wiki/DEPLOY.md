# llm-signal — Deployment

## Docker (local)

```bash
# From repo root
docker compose up -d llm-signal
docker compose logs -f llm-signal
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
  -t $REGISTRY/llm-signal:latest \
  llm-signal/

docker push $REGISTRY/llm-signal:latest
```

### Update running deployment

```bash
kubectl rollout restart deployment/llm-signal
kubectl rollout status deployment/llm-signal
kubectl logs -f deployment/llm-signal
```

## Kubernetes Secrets

The llm-signal service requires the LLM API key as a K8s secret:

```bash
kubectl create secret generic llm-signal-secret \
  --from-literal=LLM_API_KEY='your-llm-api-key'
```

## Key Environment Variables in Production

Set via K8s ConfigMap:

| Variable | Recommended |
|----------|-------------|
| `LLM_MODEL` | `gpt-4o-mini` |
| `LLM_MAX_ITEMS_PER_CYCLE` | `20` |
| `SIGNAL_POLL_INTERVAL_MINUTES` | `60` |
| `GDELT_ENABLED` | `true` |
| `QUESTDB_HOST` | `questdb` (K8s service name) |
| `PROMETHEUS_PORT` | `9093` |
