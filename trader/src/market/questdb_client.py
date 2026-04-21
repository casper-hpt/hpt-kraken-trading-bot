"""QuestDB client for fetching OHLCV bars via PGWire (Postgres protocol).

Connects to a local QuestDB instance that maintains up-to-date crypto bars.
Replaces the Kraken REST API client — all data comes from QuestDB instead.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import psycopg

from src.config import (
    QUESTDB_HOST,
    QUESTDB_PORT,
    QUESTDB_USER,
    QUESTDB_PASSWORD,
    QUESTDB_DBNAME,
)


class QuestDBClient:
    """Fetches OHLCV bars from a local QuestDB instance.

    Usage::

        client = QuestDBClient(log=log)
        client.update_cache(["BTC", "ETH"])   # fetch bars from QuestDB
        df = client.get_all_cached_bars()      # single DataFrame
    """

    COLUMNS = ["ts", "symbol", "open", "high", "low", "close", "volume"]

    def __init__(self, log: Optional[logging.Logger] = None):
        self.log = log or logging.getLogger(__name__)
        self._bar_cache: dict[str, pd.DataFrame] = {}

    def _connect(self) -> psycopg.Connection:
        """Open a new PGWire connection to QuestDB."""
        return psycopg.connect(
            host=QUESTDB_HOST,
            port=QUESTDB_PORT,
            user=QUESTDB_USER,
            password=QUESTDB_PASSWORD,
            dbname=QUESTDB_DBNAME,
        )

    # ── Public API ───────────────────────────────────────────────────────

    def fetch_bars(self, symbol: str, days: int = 90) -> pd.DataFrame:
        """Fetch OHLCV bars for *symbol* from the last *days* days."""
        query = """
            SELECT ts, symbol, open, high, low, close, volume
            FROM crypto_bars_15m
            WHERE symbol = %s
              AND ts >= dateadd('d', %s, now())
            ORDER BY ts
        """
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (symbol, -days))
                    rows = cur.fetchall()
        except Exception:
            self.log.exception("[%s] Failed to fetch bars from QuestDB", symbol)
            return pd.DataFrame(columns=self.COLUMNS)

        if not rows:
            self.log.warning("[%s] No bars returned from QuestDB", symbol)
            return pd.DataFrame(columns=self.COLUMNS)

        df = pd.DataFrame(rows, columns=self.COLUMNS)
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = df[col].astype(float)
        df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_localize(None)
        df["symbol"] = df["symbol"].astype(str)
        df = df.drop_duplicates(subset=["ts", "symbol"], keep="last")
        df = df.sort_values("ts").reset_index(drop=True)
        return df

    def fetch_all_symbols(
        self, symbols: list[str], days: int = 90
    ) -> pd.DataFrame:
        """Fetch bars for multiple symbols in a single query."""
        if not symbols:
            return pd.DataFrame(columns=self.COLUMNS)

        placeholders = ", ".join(["%s"] * len(symbols))
        query = f"""
            SELECT ts, symbol, open, high, low, close, volume
            FROM crypto_bars_15m
            WHERE symbol IN ({placeholders})
              AND ts >= dateadd('d', %s, now())
            ORDER BY symbol, ts
        """
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (*symbols, -days))
                    rows = cur.fetchall()
        except Exception:
            self.log.exception("Failed to fetch bars for %d symbols from QuestDB", len(symbols))
            return pd.DataFrame(columns=self.COLUMNS)

        if not rows:
            self.log.warning("No bars returned from QuestDB for %d symbols", len(symbols))
            return pd.DataFrame(columns=self.COLUMNS)

        df = pd.DataFrame(rows, columns=self.COLUMNS)
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = df[col].astype(float)
        df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_localize(None)
        df["symbol"] = df["symbol"].astype(str)
        df = df.drop_duplicates(subset=["ts", "symbol"], keep="last")
        df = df.sort_values(["symbol", "ts"]).reset_index(drop=True)
        return df

    # ── Cache management ─────────────────────────────────────────────────

    def update_cache(self, symbols: list[str], warmup_bars: int = 5200) -> None:
        """Refresh in-memory bar cache from QuestDB for all *symbols*.

        Fetches enough days to cover *warmup_bars* worth of 15-min bars.
        """
        # 5200 bars * 15 min / 60 / 24 ≈ 54 days; fetch 90 to be safe
        days = max(90, (warmup_bars * 15) // (60 * 24) + 10)

        self.log.info("Fetching bars for %d symbols from QuestDB (last %d days) ...", len(symbols), days)
        df = self.fetch_all_symbols(symbols, days=days)

        if df.empty:
            self.log.warning("No data returned from QuestDB")
            return

        for sym, group in df.groupby("symbol"):
            self._bar_cache[str(sym)] = group.reset_index(drop=True)
            self.log.debug("[%s] Cached %d bars from QuestDB", sym, len(group))

        self.log.info(
            "QuestDB cache updated: %d symbols, %d total bars",
            len(self._bar_cache),
            len(df),
        )

    def get_cached_bars(self, symbol: str) -> pd.DataFrame:
        """Return cached bars for a single symbol."""
        return self._bar_cache.get(
            symbol,
            pd.DataFrame(columns=self.COLUMNS),
        )

    def get_all_cached_bars(self) -> pd.DataFrame:
        """Return all cached bars as a single DataFrame."""
        if not self._bar_cache:
            return pd.DataFrame(columns=self.COLUMNS)
        return pd.concat(self._bar_cache.values(), ignore_index=True)

    # ── Watchlist ────────────────────────────────────────────────────────

    def fetch_watchlist(self) -> list[str]:
        """Fetch the current watchlist from QuestDB."""
        query = """
            SELECT symbol
            FROM crypto_watchlist
            LATEST ON updated_at PARTITION BY symbol
            ORDER BY symbol
        """
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
        except Exception:
            self.log.exception("Failed to fetch watchlist from QuestDB")
            return []

        return [str(row[0]) for row in rows]

    def fetch_market_sentiment(
        self,
        confidence_threshold: float = 0.65,
    ) -> tuple[float, float]:
        """Compute weighted bullish and bearish market sentiment scores.

        Each signal is weighted by catalyst_score (novelty×confidence×tradability)
        and a recency decay (24h half-life). Only signals whose fallout window
        still covers today are included. Returns (bullish_score, bearish_score).
        Degrades gracefully — returns (0.0, 0.0) on any error.
        """
        query = f"""
            SELECT direction, catalyst_score, ts, fallout_days
            FROM crypto_signals
            WHERE direction IN ('bullish', 'bearish')
              AND confidence >= {confidence_threshold}
              AND ts >= dateadd('d', -90, now())
            ORDER BY ts DESC
        """
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
        except Exception:
            self.log.warning("fetch_market_sentiment failed; proceeding without signal gate")
            return (0.0, 0.0)

        now = datetime.now(timezone.utc)
        bullish_score = 0.0
        bearish_score = 0.0

        for direction, catalyst_score, ts, fallout_days in rows:
            if fallout_days is None:
                continue
            if catalyst_score is None:
                continue
            # Make ts timezone-aware for comparison
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts + timedelta(days=int(fallout_days)) < now:
                continue  # fallout window has expired
            age_hours = (now - ts).total_seconds() / 3600
            recency = math.exp(-0.029 * age_hours)  # 24h half-life
            weighted = float(catalyst_score) * recency
            if direction == "bullish":
                bullish_score += weighted
            elif direction == "bearish":
                bearish_score += weighted

        return (bullish_score, bearish_score)

