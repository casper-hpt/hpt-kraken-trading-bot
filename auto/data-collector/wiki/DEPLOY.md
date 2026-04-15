# Deploy to Google Cloud Artifact Registry

## Prerequisites

- Docker installed
- gcloud CLI installed and authenticated (`gcloud auth login`)

## 1. Authenticate Docker with Artifact Registry (one-time setup)

```bash
gcloud auth configure-docker northamerica-northeast2-docker.pkg.dev
```

## 2. Build and tag the image

```bash
docker build -t northamerica-northeast2-docker.pkg.dev/home-2apollo/home-2apollo-quant/crypto-data-collector:latest .
```

## 3. Push the image

```bash
docker push northamerica-northeast2-docker.pkg.dev/home-2apollo/home-2apollo-quant/crypto-data-collector:latest
```
