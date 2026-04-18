"""Prometheus metrics for the Crypto Data Collector.

Exposes a /metrics HTTP endpoint for Prometheus to scrape.
"""

from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger(__name__)

# ── Ingestion metrics ────────────────────────────────────────────────────
BARS_INSERTED_TOTAL = Counter(
    "collector_bars_inserted_total",
    "Total bars written to QuestDB",
)
CYCLES_TOTAL = Counter(
    "collector_cycles_total",
    "Number of ingest cycles completed",
)
CYCLE_DURATION = Gauge(
    "collector_cycle_duration_seconds",
    "Wall-clock time of last ingest cycle",
)
API_ERRORS_TOTAL = Counter(
    "collector_api_errors_total",
    "API call failures (Kraken)",
)
WATCHLIST_SIZE = Gauge(
    "collector_watchlist_size",
    "Number of symbols in watchlist",
)


def start_metrics_server(port: int = 9092) -> None:
    """Start the Prometheus HTTP server on the given port."""
    start_http_server(port)
    log.info("Prometheus metrics server started on port %d", port)
