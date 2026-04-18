"""Tests for data transformation functions in main module."""
from __future__ import annotations

import pandas as pd
import pytest

from crypto_data_collector.main import _build_bar_frame


class TestBuildBarFrame:
    """Tests for _build_bar_frame function."""

    def test_build_bar_frame_basic(self):
        """Test basic bar frame construction."""
        bars = pd.DataFrame([
            {"ts": "2026-01-30 10:15:00", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100}
        ])

        df = _build_bar_frame(bars, "BTC")

        assert list(df.columns) == ["ts", "symbol", "open", "high", "low", "close", "volume", "source", "ingested_at"]
        assert df.loc[0, "symbol"] == "BTC"

    def test_build_bar_frame_sets_source(self):
        """Test that source is set to kraken."""
        bars = pd.DataFrame([
            {"ts": "2026-01-30 10:15:00", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100}
        ])

        df = _build_bar_frame(bars, "ETH")

        assert df.loc[0, "source"] == "kraken"

    def test_build_bar_frame_sets_ingested_at(self):
        """Test that ingested_at is set to current UTC time (tz-naive)."""
        bars = pd.DataFrame([
            {"ts": "2026-01-30 10:15:00", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100}
        ])

        before = pd.Timestamp.utcnow().tz_localize(None)
        df = _build_bar_frame(bars, "SOL")
        after = pd.Timestamp.utcnow().tz_localize(None)

        ingested_at = df.loc[0, "ingested_at"]
        assert before <= ingested_at <= after
        assert ingested_at.tzinfo is None

    def test_build_bar_frame_empty_dataframe(self):
        """Test that empty DataFrame returns empty DataFrame."""
        bars = pd.DataFrame()

        df = _build_bar_frame(bars, "BTC")

        assert df.empty

    def test_build_bar_frame_preserves_values(self):
        """Test that OHLCV values are preserved."""
        bars = pd.DataFrame([
            {"ts": "2026-01-30 10:15:00", "open": 100.5, "high": 105.0, "low": 99.25, "close": 103.75, "volume": 12345}
        ])

        df = _build_bar_frame(bars, "BTC")

        assert df.loc[0, "open"] == 100.5
        assert df.loc[0, "high"] == 105.0
        assert df.loc[0, "low"] == 99.25
        assert df.loc[0, "close"] == 103.75
        assert df.loc[0, "volume"] == 12345

    def test_build_bar_frame_multiple_rows(self):
        """Test with multiple bar rows."""
        bars = pd.DataFrame([
            {"ts": "2026-01-30 10:00:00", "open": 100, "high": 101, "low": 99, "close": 100.5, "volume": 1000},
            {"ts": "2026-01-30 10:15:00", "open": 100.5, "high": 102, "low": 100, "close": 101.5, "volume": 1500},
            {"ts": "2026-01-30 10:30:00", "open": 101.5, "high": 103, "low": 101, "close": 102.5, "volume": 1200},
        ])

        df = _build_bar_frame(bars, "ETH")

        assert len(df) == 3
        assert all(df["symbol"] == "ETH")
        assert all(df["source"] == "kraken")

    def test_build_bar_frame_converts_ts_to_datetime(self):
        """Test that ts column is converted to datetime."""
        bars = pd.DataFrame([
            {"ts": "2026-01-30 10:15:00", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100}
        ])

        df = _build_bar_frame(bars, "BTC")

        assert pd.api.types.is_datetime64_any_dtype(df["ts"])

    def test_build_bar_frame_symbol_is_category(self):
        """Test that symbol column is category dtype."""
        bars = pd.DataFrame([
            {"ts": "2026-01-30 10:15:00", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100}
        ])

        df = _build_bar_frame(bars, "BTC")

        assert df["symbol"].dtype.name == "category"

    def test_build_bar_frame_source_is_category(self):
        """Test that source column is category dtype."""
        bars = pd.DataFrame([
            {"ts": "2026-01-30 10:15:00", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100}
        ])

        df = _build_bar_frame(bars, "BTC")

        assert df["source"].dtype.name == "category"
