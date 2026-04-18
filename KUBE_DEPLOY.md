# Kubernetes Deployment Guide

Deploy the full 2Apollo stack (infrastructure + crypto portfolio services) to a k3s cluster.

> **Note:** K8s manifests live under `auto/k8s/`. Infrastructure manifests are in `k8s/utils/`, portfolio service manifests are in `k8s/portfolio/`.

## Prerequisites

- k3s cluster running with `kubectl` configured
- Docker installed locally
- gcloud CLI installed and authenticated (`gcloud auth login`)
- Docker authenticated with Artifact Registry:
  ```bash
  gcloud auth configure-docker northamerica-northeast2-docker.pkg.dev
  ```

## 1. Create Image Pull Secret

k3s needs credentials to pull images from GCP Artifact Registry.

```bash
gcloud iam service-accounts list --format="value(email)"
```

```bash
# Create a GCP service account key (one-time)
gcloud iam service-accounts keys create key.json \
  --iam-account=home-2apollo@appspot.gserviceaccount.com

# Create the Kubernetes secret
kubectl create secret docker-registry gcr-secret \
  --docker-server=northamerica-northeast2-docker.pkg.dev \
  --docker-username=_json_key \
  --docker-password="$(cat key.json)" \
  --docker-email=noreply@home-2apollo.iam.gserviceaccount.com

# Clean up the key file
rm key.json
```

## 2. Build and Push All Images

From the repo root, build and push each service:

The k3s node runs on amd64 Linux. If building from an Apple Silicon Mac, you must specify `--platform linux/amd64`.

```bash
REGISTRY=northamerica-northeast2-docker.pkg.dev/home-2apollo/home-2apollo-quant

docker build --platform linux/amd64 -t $REGISTRY/data-collector:latest auto/data-collector/
docker build --platform linux/amd64 -t $REGISTRY/trader:latest          auto/trader/
docker build --platform linux/amd64 -t $REGISTRY/signal-service:latest  auto/signal-service/

docker push $REGISTRY/data-collector:latest
docker push $REGISTRY/trader:latest
docker push $REGISTRY/signal-service:latest
```

## 3. Create Secrets

Create secrets before deploying. If you prefer to manage secrets via the YAML files directly, edit the `stringData` values in-place and skip this step.

**trader** (Kraken API keys):
```bash
kubectl create secret generic trader-secret \
  --from-literal=KRAKEN_API_KEY='your-api-key' \
  --from-literal=KRAKEN_API_SECRET='your-api-secret'
```

**signal-service** (LLM API key):
```bash
kubectl create secret generic signal-service-secret \
  --from-literal=LLM_API_KEY='your-llm-api-key'
```

If creating secrets via `kubectl create secret`, remove the `Secret` resource from the corresponding YAML file before applying.

## 4. Deploy Infrastructure

Deploy infrastructure first. Portfolio services depend on QuestDB being available.

### QuestDB

```bash
kubectl apply -f auto/k8s/portfolio/questdb.yaml
kubectl rollout status statefulset/questdb
```

Verify QuestDB is accepting connections:
```bash
kubectl get pods -l app=questdb
```

### Prometheus

```bash
kubectl apply -f auto/k8s/utils/prometheus.yaml
kubectl rollout status deployment/prometheus
```

### Grafana

```bash
kubectl apply -f auto/k8s/utils/grafana.yaml
kubectl rollout status deployment/grafana
```

Grafana is exposed as a NodePort service. Access the dashboard:
```bash
# Find the assigned NodePort
kubectl get svc grafana

# Default login: admin / admin
```

### Verify Infrastructure

```bash
kubectl get pods -l component=infrastructure
kubectl get svc -l component=infrastructure
```

Expected services:

| Service | Ports | Purpose |
|---------|-------|---------|
| `questdb` | 9000, 8812, 9009 | HTTP/REST, PGWire, ILP/TCP |
| `prometheus` | 9090 | Metrics scraping |
| `grafana` | 3000 (NodePort) | Dashboards |

## 5. Deploy Application Services

### Data Collector

```bash
kubectl apply -f auto/k8s/portfolio/data-collector.yaml
kubectl rollout status deployment/data-collector
```

### Trader

```bash
kubectl apply -f auto/k8s/portfolio/trader.yaml
kubectl rollout status deployment/trader
```

### Signal Service

```bash
kubectl apply -f auto/k8s/portfolio/signal-service.yaml
kubectl rollout status deployment/signal-service
```

### Deploy Everything at Once

If all secrets and infrastructure are already configured:
```bash
kubectl apply -R -f auto/k8s/
```

## 6. Verify

```bash
# Check all pods
kubectl get pods

# Check all services
kubectl get svc

# Check Prometheus scrape targets are up
kubectl port-forward svc/prometheus 9090:9090
# Open http://localhost:9090/targets in browser

# Check Grafana dashboards
kubectl port-forward svc/grafana 3000:3000
# Open http://localhost:3000 (admin / admin)

# Check QuestDB web console
kubectl port-forward svc/questdb 9000:9000
# Open http://localhost:9000

# Check logs for a specific service
kubectl logs -f deployment/trader
kubectl logs -f deployment/signal-service
```

## Common Operations

### View logs
```bash
kubectl logs -f deployment/<service-name>
# For StatefulSets (questdb):
kubectl logs -f statefulset/<name>
```

### Restart a service
```bash
kubectl rollout restart deployment/<service-name>
```

### Update a service image
```bash
REGISTRY=northamerica-northeast2-docker.pkg.dev/home-2apollo/home-2apollo-quant

# Rebuild and push (--platform linux/amd64 required when building from Apple Silicon)
docker build --platform linux/amd64 -t $REGISTRY/<service-name>:latest auto/<service-name>/
docker push $REGISTRY/<service-name>:latest

# Restart to pull new image
kubectl rollout restart deployment/<service-name>
```

### Update environment config
```bash
# Edit the ConfigMap in the YAML, then re-apply
kubectl apply -f auto/k8s/portfolio/<service-name>.yaml

# Restart to pick up changes
kubectl rollout restart deployment/<service-name>
```

### Update secrets
```bash
kubectl delete secret <secret-name>
kubectl create secret generic <secret-name> \
  --from-literal=KEY='new-value'
kubectl rollout restart deployment/<service-name>
```

### Scale a service
```bash
kubectl scale deployment/<service-name> --replicas=0  # stop
kubectl scale deployment/<service-name> --replicas=1  # start
```

### Access web UIs via port-forward
```bash
kubectl port-forward svc/questdb 9000:9000     # QuestDB console
kubectl port-forward svc/grafana 3000:3000     # Grafana dashboards
kubectl port-forward svc/prometheus 9090:9090  # Prometheus UI
```

## Full Stack Reference

### Manifest Paths

```
auto/k8s/
  portfolio/
    questdb.yaml
    data-collector.yaml
    trader.yaml
    signal-service.yaml
  utils/
    prometheus.yaml
    grafana.yaml
```

### Infrastructure

| Component | YAML | Type | Ports | Storage |
|-----------|------|------|-------|---------|
| QuestDB | `auto/k8s/portfolio/questdb.yaml` | StatefulSet | 9000, 8812, 9009 | 10Gi PVC |
| Prometheus | `auto/k8s/utils/prometheus.yaml` | Deployment | 9090 | emptyDir |
| Grafana | `auto/k8s/utils/grafana.yaml` | Deployment | 3000 | none |

### Application Services

| Service | YAML | Metrics Port | Secrets Required |
|---------|------|-------------|-----------------|
| data-collector | `auto/k8s/portfolio/data-collector.yaml` | 9092 | None |
| trader | `auto/k8s/portfolio/trader.yaml` | 9095 | `KRAKEN_API_KEY`, `KRAKEN_API_SECRET` |
| signal-service | `auto/k8s/portfolio/signal-service.yaml` | 9093 | `LLM_API_KEY` |
