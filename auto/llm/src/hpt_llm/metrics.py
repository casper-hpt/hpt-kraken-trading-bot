"""Prometheus metrics for the HPT LLM bot.

Exposes a /metrics HTTP endpoint for Prometheus to scrape.
"""

from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger(__name__)

# ── Slack metrics ────────────────────────────────────────────────────────
SLACK_MESSAGES_TOTAL = Counter(
    "llm_slack_messages_total",
    "Slack events received",
    ["type"],
)
SLACK_RESPONSE_ERRORS_TOTAL = Counter(
    "llm_slack_response_errors_total",
    "Errors sending Slack replies",
)

# ── Ollama / LLM metrics ────────────────────────────────────────────────
OLLAMA_CALLS_TOTAL = Counter(
    "llm_ollama_calls_total",
    "Total Ollama API calls",
)
OLLAMA_CALL_DURATION = Gauge(
    "llm_ollama_call_duration_seconds",
    "Duration of last Ollama call",
)
OLLAMA_ERRORS_TOTAL = Counter(
    "llm_ollama_errors_total",
    "Ollama API failures or timeouts",
)

# ── Tool execution metrics ───────────────────────────────────────────────
TOOL_CALLS_TOTAL = Counter(
    "llm_tool_calls_total",
    "Tool invocations by name",
    ["tool"],
)
TOOL_ERRORS_TOTAL = Counter(
    "llm_tool_errors_total",
    "Tool execution failures by name",
    ["tool"],
)
TOOL_DURATION = Gauge(
    "llm_tool_duration_seconds",
    "Duration of last tool call",
    ["tool"],
)

# ── Theory pipeline metrics ──────────────────────────────────────────────
THEORY_RUNS_TOTAL = Counter(
    "llm_theory_runs_total",
    "Nightly theory jobs started",
)
THEORY_RUN_DURATION = Gauge(
    "llm_theory_run_duration_seconds",
    "Duration of last full pipeline run",
)
THEORY_ARTICLES_SCANNED = Gauge(
    "llm_theory_articles_scanned",
    "GDELT articles in last run",
)
THEORY_SYMBOLS_SCORED = Gauge(
    "llm_theory_symbols_scored",
    "Watchlist symbols scored in last run",
)
THEORY_ERRORS_TOTAL = Counter(
    "llm_theory_errors_total",
    "Pipeline failures",
)
THEORY_SIGNAL_PUBLISHED_TOTAL = Counter(
    "llm_theory_signal_published_total",
    "Signals published to Kafka",
)
THEORY_DO_NOT_TRADE = Gauge(
    "llm_theory_do_not_trade",
    "1 if last signal was do_not_trade, 0 otherwise",
)

# ── Data source metrics ──────────────────────────────────────────────────
QUESTDB_QUERY_DURATION = Gauge(
    "llm_questdb_query_duration_seconds",
    "QuestDB query latency",
    ["query"],
)
QUESTDB_ERRORS_TOTAL = Counter(
    "llm_questdb_errors_total",
    "QuestDB query failures",
)
KAFKA_PUBLISH_ERRORS_TOTAL = Counter(
    "llm_kafka_publish_errors_total",
    "Kafka publish failures",
)


def start_metrics_server(port: int = 9096) -> None:
    """Start the Prometheus HTTP server on the given port."""
    start_http_server(port)
    log.info("Prometheus metrics server started on port %d", port)
