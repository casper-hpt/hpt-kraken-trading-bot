# hpt-llm

Slack bot powered by a local Ollama model with extensible tool support. Runs on k3s via Socket Mode (no ingress needed).

## Project Structure

```
src/hpt_llm/
├── bot.py          # Slack event handlers
├── config.py       # env-based settings
├── llm.py          # Ollama chat loop + tool dispatch
└── tools/
    ├── base.py     # @register_tool decorator & registry
    └── bitcoin.py  # example tool
../k8s/llm/
└── hpt-llm.yaml    # k8s Deployment manifest
```

## Adding a New Tool

Drop a file in `src/hpt_llm/tools/`. It will be auto-discovered at startup.

```python
# src/hpt_llm/tools/weather.py
from .base import register_tool

@register_tool("get_weather", "Get weather for a city", {
    "type": "object",
    "properties": {"city": {"type": "string"}},
    "required": ["city"],
})
def get_weather(city: str) -> str:
    ...
```

## Connecting to Ollama

The bot needs to reach an Ollama instance. There are three connection modes depending on where you're running:

| Mode | Where you're running | `OLLAMA_URL` | Setup |
|------|---------------------|-------------|-------|
| **k8s / Docker** | Container on the Ollama host | `http://192.168.2.38:11434/api/chat` | Default in k8s manifest — uses LAN IP since `localhost` inside a pod refers to the pod, not the host |
| **Home (local dev)** | Python directly on home network | `http://192.168.2.38:11434/api/chat` | Default in `config.py` — no extra setup needed |
| **Remote (SSH tunnel)** | Python directly, outside home network | `http://localhost:11434/api/chat` | Requires SSH tunnel (see below) |

### Remote development via SSH tunnel

When running outside your home network, set up an SSH port-forward through your NoIP domain to tunnel Ollama traffic:

```bash
# Open the tunnel (runs in background)
ssh -f -N -L 11434:localhost:11434 casper@192.168.2.38

ssh -f -N -L 11434:localhost:11434 casper@2apollo-machine.ddns.net # requires credentials preloaded

# Override the URL to use the tunnel
export OLLAMA_URL=http://localhost:11434/api/chat

python -m hpt_llm
```

This forwards local port `11434` through the SSH connection to the Ollama host on your home network. The bot then hits `localhost:11434` which transparently routes through the tunnel.

To stop the tunnel:
```bash
# Find and kill the SSH process
kill $(lsof -ti:11434)
```

## Local Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .

# Set required env vars (or use a .env file)
export SLACK_BOT_TOKEN=xoxb-...
export SLACK_APP_TOKEN=xapp-...

# Home network — default OLLAMA_URL works as-is
python -m hpt_llm

# Remote — start SSH tunnel first, then override the URL
# ssh -f -N -L 11434:192.168.2.38:11434 <user>@<your-noip-domain>
# export OLLAMA_URL=http://localhost:11434/api/chat
# python -m hpt_llm
```

## Deploying to k3s

### 1. Build and push the image

```bash
REGISTRY=northamerica-northeast2-docker.pkg.dev/home-2apollo/home-2apollo-quant

docker build --platform linux/amd64 -t $REGISTRY/hpt-llm:latest .
docker push $REGISTRY/hpt-llm:latest
```

### 2. Create the Kubernetes secret

```bash
kubectl create secret generic hpt-llm-secrets \
  --from-literal=SLACK_BOT_TOKEN=xoxb-... \
  --from-literal=SLACK_APP_TOKEN=xapp-...
```

### 3. Update deployment config

Edit `../k8s/llm/hpt-llm.yaml` to set the correct:
- `OLLAMA_URL` — the Ollama endpoint reachable from within the cluster (e.g. `http://ollama.default.svc.cluster.local:11434/api/chat` if Ollama runs in-cluster, or your LAN IP if it runs on a separate host)
- `OLLAMA_MODEL` — the model to use
- Resource requests/limits as needed

### 4. Apply the deployment

```bash
kubectl apply -f k8s/llm/hpt-llm.yaml
```

### 5. Check it's running

```bash
kubectl get pods -l app=hpt-llm
kubectl logs -l app=hpt-llm -f
```

## Kafka Output

The nightly theory job publishes a structured trading signal to the `news-signals` Kafka topic. Example message:

```json
{
  "theme": "Middle East shipping disruptions threaten oil supply",
  "confidence": 0.72,
  "novelty": 0.55,
  "market_relevance": 0.80,
  "time_urgency": 0.65,
  "cross_source_agreement": 0.70,
  "tradability": 0.75,
  "expected_half_life_days": 14,
  "directional_clarity": 0.68,
  "tickers": [
    {"symbol": "XOM", "direction": "long", "score": 0.72},
    {"symbol": "CVX", "direction": "long", "score": 0.65},
    {"symbol": "AMZN", "direction": "short", "score": 0.45}
  ],
  "do_not_trade": false,
  "reason": "Hormuz strait tensions confirmed by multiple sources, likely to push crude prices up benefiting integrated oil majors.",
  "generated_at": "2026-03-07T06:00:12.345678+00:00"
}
```

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SLACK_BOT_TOKEN` | yes | — | Slack bot OAuth token (`xoxb-...`) |
| `SLACK_APP_TOKEN` | yes | — | Slack app-level token (`xapp-...`) |
| `OLLAMA_URL` | no | `http://192.168.2.38:11434/api/chat` | Ollama chat API endpoint |
| `OLLAMA_MODEL` | no | `qwen2.5:14b-instruct` | Model name |
| `REQUEST_TIMEOUT_S` | no | `120` | Ollama request timeout in seconds |
| `MAX_SLACK_REPLY_LEN` | no | `3500` | Max reply length before truncation |
