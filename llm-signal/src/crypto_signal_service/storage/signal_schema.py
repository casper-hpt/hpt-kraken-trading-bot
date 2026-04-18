from __future__ import annotations

import logging

import requests

log = logging.getLogger(__name__)

_DDL = """CREATE TABLE IF NOT EXISTS crypto_signals (
    ts                TIMESTAMP,
    signal_id         STRING,
    event_type        SYMBOL,
    asset_scope       SYMBOL,
    affected_symbols  STRING,
    time_horizon      SYMBOL,
    direction         SYMBOL,
    confidence        DOUBLE,
    novelty           DOUBLE,
    tradability       DOUBLE,
    catalyst_score    DOUBLE,
    key_reason        STRING,
    headline          STRING,
    source_url        STRING,
    ingested_at       TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
  DEDUP UPSERT KEYS(ts, signal_id);"""


class SignalSchemaManager:
    def __init__(self, exec_url: str):
        self._exec_url = exec_url

    def ensure_schema(self) -> None:
        log.info("Ensuring QuestDB schema (crypto_signals)...")
        try:
            r = requests.get(self._exec_url, params={"query": _DDL}, timeout=30)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(str(data["error"]))
        except Exception:
            log.exception("Failed to create crypto_signals table")
            raise
        log.info("Schema ensured.")
