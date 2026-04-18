from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd
from questdb.ingress import Sender, IngressError  # type: ignore

from .questdb_rest import QuestDBRest
from .watchlist import WatchItem


LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class QuestDBWriter:
    ilp_conf: str
    rest: QuestDBRest

    def get_last_ts(self, symbol: str) -> pd.Timestamp | None:
        """Get the most recent timestamp for a symbol from the database.

        Args:
            symbol: Crypto symbol (will be sanitized to alphanumeric + common chars)

        Returns:
            Most recent timestamp or None if no data exists
        """
        # Sanitize symbol to prevent SQL injection
        sanitized = "".join(c for c in symbol if c.isalnum() or c in ".-")
        if not sanitized or sanitized != symbol.strip():
            LOG.warning("Symbol sanitization changed value: %r -> %r", symbol, sanitized)
            if not sanitized:
                return None

        q = f"SELECT max(ts) last_ts FROM crypto_bars_15m WHERE symbol = '{sanitized}';"
        v = self.rest.scalar(q, "last_ts")
        if v is None:
            return None
        try:
            ts = pd.to_datetime(v)
            if ts.tzinfo is not None:
                ts = ts.tz_convert("UTC").tz_localize(None)
            return ts
        except Exception as e:
            LOG.warning("Failed to parse timestamp %r for symbol %s: %s", v, sanitized, e)
            return None

    def get_first_ts(self, symbol: str) -> pd.Timestamp | None:
        """Get the earliest timestamp for a symbol from the database."""
        sanitized = "".join(c for c in symbol if c.isalnum() or c in ".-")
        if not sanitized or sanitized != symbol.strip():
            LOG.warning("Symbol sanitization changed value: %r -> %r", symbol, sanitized)
            if not sanitized:
                return None

        q = f"SELECT min(ts) first_ts FROM crypto_bars_15m WHERE symbol = '{sanitized}';"
        v = self.rest.scalar(q, "first_ts")
        if v is None:
            return None
        try:
            ts = pd.to_datetime(v)
            if ts.tzinfo is not None:
                ts = ts.tz_convert("UTC").tz_localize(None)
            return ts
        except Exception as e:
            LOG.warning("Failed to parse timestamp %r for symbol %s: %s", v, sanitized, e)
            return None

    def write_bars(self, df: pd.DataFrame, table_name: str = "crypto_bars_15m") -> int:
        """Write a DataFrame of bars via ILP/HTTP.

        Expected columns:
          - ts (datetime64)
          - symbol (str or categorical)
          - open/high/low/close (float)
          - volume (int or float)
          - source (str)
          - ingested_at (datetime64)
        """
        if df.empty:
            return 0

        try:
            with Sender.from_conf(self.ilp_conf) as sender:
                sender.dataframe(
                    df,
                    table_name=table_name,
                    symbols=["symbol", "source"],
                    at="ts",
                )
                sender.flush()
            return int(len(df))
        except IngressError as e:
            LOG.error("QuestDB ILP ingest error: %s", e)
            raise

    def write_watchlist(self, items: list[WatchItem]) -> int:
        """Write watchlist items to the watchlist table via ILP/HTTP.

        Args:
            items: List of WatchItem objects from watchlist.json

        Returns:
            Number of rows inserted
        """
        if not items:
            return 0

        now = pd.Timestamp.utcnow().tz_localize(None)

        rows = [{"updated_at": now, "symbol": item.symbol} for item in items]
        df = pd.DataFrame(rows)

        try:
            with Sender.from_conf(self.ilp_conf) as sender:
                sender.dataframe(
                    df,
                    table_name="crypto_watchlist",
                    symbols=["symbol"],
                    at="updated_at",
                )
                sender.flush()
            LOG.info("Synced %d watchlist items to QuestDB", len(df))
            return int(len(df))
        except IngressError as e:
            LOG.error("QuestDB ILP ingest error for watchlist: %s", e)
            raise
