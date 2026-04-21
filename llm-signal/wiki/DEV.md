# llm-signal — Development

## Setup

```bash
cd llm-signal
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Running Locally

```bash
cd llm-signal
source .venv/bin/activate

LLM_API_KEY=your-key \
QUESTDB_HOST=localhost \
python -m crypto_signal_service
```

The service runs one classification cycle immediately, then sleeps until the next interval (`SIGNAL_POLL_INTERVAL_MINUTES`).

## Debug Scripts

One-off scripts for testing individual components without running the full service:

```bash
# Test RSS + GDELT fetching
python tests/debug/fetch_sources.py

# Test LLM classification on a sample item
LLM_API_KEY=your-key python tests/debug/classify.py
```

## Tests

```bash
pytest tests/
```

## Project Layout

```
llm-signal/
  src/crypto_signal_service/
    main.py              # Entry point and main loop
    config.py            # All env-var config
    llm_classifier.py    # LLM classification + CryptoSignal dataclass
    dedup_store.py       # Deduplication (skip already-seen headlines)
    metrics.py           # Prometheus metrics
    fetchers/
      rss_fetcher.py     # RSS feed polling → FeedItem list
      gdelt_fetcher.py   # GDELT GKG API → FeedItem list
    storage/
      signal_schema.py   # QuestDB table schema
      signal_writer.py   # ILP/HTTP write to QuestDB
```

## Using a Local LLM

Point `LLM_API_BASE_URL` at any OpenAI-compatible endpoint (e.g. Ollama):

```bash
LLM_API_BASE_URL=http://localhost:11434/v1 \
LLM_MODEL=llama3.2 \
LLM_API_KEY=ollama \
python -m crypto_signal_service
```

## Updating Dependencies

```bash
pip-compile requirements/requirements-ss.in -o requirements/requirements-ss.txt
pip install -r requirements/requirements-ss.txt
```
