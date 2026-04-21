# llm-signal

LLM-powered crypto news signal classifier. On each cycle it fetches headlines from RSS feeds and GDELT, classifies each item via an LLM into a structured signal, deduplicates, and writes results to QuestDB. The trader reads these signals to optionally gate buy decisions.

## Architecture

```
RSS feeds (CoinDesk, CoinTelegraph, Decrypt, CryptoSlate)
GDELT (global event database)
        │
        ▼
  LLMClassifier (OpenAI-compatible API)
        │
        ▼
  CryptoSignal { direction, confidence, horizon, asset_scope, ... }
        │
        ▼
  QuestDB (crypto_signals table)
```

## Signal Schema

Each classified news item produces a signal with:

| Field | Values | Description |
|-------|--------|-------------|
| `direction` | `bullish` / `bearish` / `neutral` | Market direction assessment |
| `confidence` | 0–1 | How confident the LLM is |
| `novelty` | 0–1 | How surprising/new the news is |
| `tradability` | 0–1 | How likely to move the market |
| `catalyst_score` | 0–1 | `novelty × confidence × tradability` |
| `event_type` | `hack`, `regulatory`, `macro`, etc. | Category of event |
| `asset_scope` | `BTC`, `ETH`, `alt`, `market-wide` | Which assets are affected |
| `time_horizon` | `intraday`, `1-7d`, `1-4w`, `structural` | Expected impact duration |
| `affected_symbols` | list of tickers | Specific coins named in the article |

## Key Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_API_KEY` | — | Required. OpenAI-compatible API key |
| `LLM_API_BASE_URL` | `https://api.openai.com/v1` | LLM endpoint (swap for local model) |
| `LLM_MODEL` | `hpt-short` | Model name |
| `SIGNAL_POLL_INTERVAL_MINUTES` | `60` | Minutes between fetch cycles |
| `GDELT_ENABLED` | `true` | Enable GDELT news source |
| `GDELT_TIMESPAN` | `2h` | How far back GDELT looks |
| `RSS_FEED_URLS` | 4 default feeds | Semicolon-separated RSS URLs |
| `LLM_MAX_ITEMS_PER_CYCLE` | `20` | Max articles classified per cycle |
| `QUESTDB_HOST` | `localhost` | QuestDB hostname |
| `PROMETHEUS_PORT` | `9093` | Metrics endpoint |

## How the Trader Uses Signals

When `SIGNAL_GATE_ENABLED=true` in the trader, it queries QuestDB for recent bearish signals and skips buy orders for any symbol that appears in those signals with confidence above `SIGNAL_CONFIDENCE_THRESHOLD` (default 0.70) within `SIGNAL_LOOKBACK_HOURS` (default 24h).
