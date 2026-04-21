# trader

Momentum + EMA trend strategy engine. Reads 15-minute OHLCV bars from QuestDB every cycle, scores each coin using quantile-normalized momentum, applies a per-symbol EMA trend filter, then executes buys and sells on Kraken via private REST API.

## Architecture

```
QuestDB (crypto_bars_15m)
        │
        ▼
  momentum scorer  ──→  EMA trend filter  ──→  position evaluator
                                                      │
                              QuestDB (crypto_signals) ┤ (optional signal gate)
                                                      │
                                                      ▼
                                              Kraken REST API
```

## Strategy

1. **Momentum scoring** — rolling quantile score over the last 1000 bars per symbol, stride 50, window 5000
2. **EMA trend filter** — per-symbol optimized fast/slow EMA pair loaded from `src/data/ema_params.json`; `trend_ok` = fast EMA above slow EMA
3. **Buy** — momentum score > 3.0 and `trend_ok`, up to `MAX_POSITIONS` equal-weight slots
4. **Sell triggers:**
   - Stop-loss: price falls ≥ 8% below entry
   - EMA bearish crossover (after minimum 8 bars held)
   - Profit-take tiers at +5% / +10% / +15% / +20% (sell 25% per tier, full exit at final tier)
5. **LLM signal gate** (optional) — if `SIGNAL_GATE_ENABLED=true`, skips buys for symbols with recent bearish signals from the llm-signal service

## Key Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KRAKEN_API_KEY` | — | Required for live trading |
| `KRAKEN_API_SECRET` | — | Required for live trading |
| `DRY_RUN` | `false` | Log orders without executing |
| `CYCLE_INTERVAL` | `15` | Minutes between rebalance cycles |
| `MAX_POSITIONS` | `5` | Maximum concurrent positions |
| `STOP_LOSS_PCT` | `0.08` | Stop-loss threshold (8%) |
| `SIGNAL_GATE_ENABLED` | `false` | Enable LLM signal gating |
| `QUESTDB_HOST` | `localhost` | QuestDB hostname |
| `PROMETHEUS_PORT` | `9095` | Metrics endpoint |

## Positions File

Open positions are persisted to `POSITIONS_PATH` (default `/data/positions.json`) so the trader survives restarts without losing state.

## EMA Refit

EMA parameters are periodically re-optimized and written to `src/data/ema_params.json`:
```bash
docker compose run --rm ema-refit
```
