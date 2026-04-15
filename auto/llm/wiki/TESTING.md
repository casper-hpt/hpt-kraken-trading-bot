# Testing the LLM Bot

## Prerequisites

1. **Ollama** must be running on the host machine (`192.168.2.38:11434`)
2. **Slack tokens** — you need both `SLACK_BOT_TOKEN` (`xoxb-...`) and `SLACK_APP_TOKEN` (`xapp-...`)
3. **Python 3.12** with a virtual environment set up

## Setup

```bash
cd bot/llm
source .venv/bin/activate
pip install -e .
```

Set your Slack tokens (or use the `.env` file):

```bash
export SLACK_BOT_TOKEN=xoxb-...
export SLACK_APP_TOKEN=xapp-...
```

## Running Locally

### On the home network

No extra setup needed — the default `OLLAMA_URL` points to `http://192.168.2.38:11434/api/chat`, which is reachable directly over LAN.

```bash
python -m hpt_llm
```

### Remote (outside the home network)

You need an SSH tunnel to reach Ollama through your NoIP domain. **kubectl port-forwarding is not needed** — Ollama runs directly on the host, not as a k8s service.

```bash
# 1. Open an SSH tunnel (runs in background)
ssh -f -N -L 11434:192.168.2.38:11434 <user>@<your-noip-domain>

# 2. Override the Ollama URL to use the tunnel
export OLLAMA_URL=http://localhost:11434/api/chat

# 3. Run the bot
python -m hpt_llm
```

To stop the tunnel when you're done:

```bash
kill $(lsof -ti:11434)
```

## Port Forwarding for QuestDB

QuestDB runs inside k3s, so it's not directly reachable from your Mac. You need to forward its PostgreSQL wire port (8812) to run tests that query the watchlist.

**On the machine running the k3s cluster (casper-os)**, run with `--address 0.0.0.0` so it's reachable over LAN:

```bash
kubectl port-forward svc/questdb 8812:8812 --address 0.0.0.0
```

Then on your Mac, the test will connect to `192.168.2.38:8812` using the default `DB_HOST`:

```bash
python tests/test_watchlist.py
```

> **Note:** Without `--address 0.0.0.0`, the port-forward only binds to `localhost` on casper-os, so your Mac can't reach it over the LAN.

### Alternative: SSH tunnel (remote or if you can't use --address 0.0.0.0)

```bash
# On your Mac — tunnel through to casper-os
ssh -f -N -L 8812:localhost:8812 casper@192.168.2.38

# Override DB_HOST to use the tunnel
DB_HOST=localhost python tests/test_watchlist.py
```

To stop the tunnel when done:

```bash
kill $(lsof -ti:8812)
```

## Testing the Supply Chain Theory Pipeline

The theory pipeline uses a 2-round tournament to score watchlist symbols. It requires both Ollama and QuestDB to be reachable.

### Quick test (mock news, small watchlist)

```bash
cd bot/llm
python tests/test_theory_loop.py --mock-news --max-symbols 30
```

This runs 3 R1 batches → ~3 longs, ~3 shorts → 1 R2 batch each. ~10 LLM calls total.

### Full test (real GDELT news, full watchlist)

```bash
python tests/test_theory_loop.py
```

### Options

| Flag | Description |
|---|---|
| `--loop 0` | Stop after news digest (1 LLM call) |
| `--loop 1` | Stop after tournament scoring (no final signal) |
| `--loop 2` | Full run — digest + tournament + signal (default) |
| `--mock-news` | Use fake headlines instead of calling GDELT |
| `--max-symbols N` | Limit watchlist to N symbols (0 = all) |

### Tournament config (env vars)

| Variable | Default | Description |
|---|---|---|
| `ROUND1_BATCH_SIZE` | `10` | Symbols per batch in Round 1 |
| `ROUND2_BATCH_SIZE` | `10` | Symbols per batch in Round 2 |
| `ROUND2_KEEP` | `20` | Longs/shorts to keep after Round 2 |
| `TOURNAMENT_SEED` | None | Optional seed for reproducible shuffle |

## Verifying It Works

1. The bot connects to Slack via **Socket Mode** (no ingress required — it dials out to Slack's API)
2. Once running, send a message to the bot in Slack and confirm it responds
3. Check the terminal logs for any connection errors to Ollama or Slack

## Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `OLLAMA_URL` | `http://192.168.2.38:11434/api/chat` | Ollama chat API endpoint |
| `OLLAMA_MODEL` | `qwen2.5:14b-instruct` | Model to use |
| `REQUEST_TIMEOUT_S` | `120` | Ollama request timeout (seconds) |
| `MAX_SLACK_REPLY_LEN` | `3500` | Max reply length before truncation |
