from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger(__name__)

CYCLES_TOTAL = Counter(
    "signal_service_cycles_total",
    "Number of signal poll cycles completed",
)
CLASSIFIED_TOTAL = Counter(
    "signal_service_classified_total",
    "Total signals successfully classified and written",
)
LLM_ERRORS_TOTAL = Counter(
    "signal_service_llm_errors_total",
    "LLM classification failures",
)
FEED_ERRORS_TOTAL = Counter(
    "signal_service_feed_errors_total",
    "RSS feed fetch failures",
)
CYCLE_DURATION = Gauge(
    "signal_service_cycle_duration_seconds",
    "Wall-clock time of last signal cycle",
)


def start_metrics_server(port: int = 9093) -> None:
    start_http_server(port)
    log.info("Prometheus metrics server started on port %d", port)
