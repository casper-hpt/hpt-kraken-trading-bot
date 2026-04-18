# signal-service — Deployment

## Docker (local)

```bash
# From repo root
docker compose up -d signal-service
docker compose logs -f signal-service
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
  -t $REGISTRY/signal-service:latest \
  auto/signal-service/

docker push $REGISTRY/signal-service:latest
```

### Update running deployment

```bash
kubectl rollout restart deployment/signal-service
kubectl rollout status deployment/signal-service
kubectl logs -f deployment/signal-service
```

## Kubernetes Secrets

The signal-service requires the LLM API key as a K8s secret:

```bash
kubectl create secret generic signal-service-secret \
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
