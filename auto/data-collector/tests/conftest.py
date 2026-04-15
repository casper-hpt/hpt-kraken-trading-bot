"""Pytest configuration and shared fixtures."""
from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture
def sample_bar_data() -> pd.DataFrame:
    """Sample bar data as returned by Kraken client."""
    return pd.DataFrame([
        {"ts": "2026-01-30 10:00:00", "open": 100.0, "high": 105.0, "low": 99.0, "close": 104.0, "volume": 1000.0},
        {"ts": "2026-01-30 10:15:00", "open": 104.0, "high": 106.0, "low": 103.0, "close": 105.5, "volume": 1500.0},
        {"ts": "2026-01-30 10:30:00", "open": 105.5, "high": 107.0, "low": 105.0, "close": 106.0, "volume": 1200.0},
    ])


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables for Config."""
    monkeypatch.setenv("QUESTDB_HOST", "localhost")
    monkeypatch.setenv("QUESTDB_HTTP_PORT", "9000")


@pytest.fixture
def sample_watchlist_data() -> dict:
    """Sample watchlist data."""
    return {
        "BTC": {"symbol": "BTC"},
        "ETH": {"symbol": "ETH"},
        "SOL": {"symbol": "SOL"},
    }


@pytest.fixture
def kraken_ohlc_response() -> dict:
    """Sample Kraken OHLC API response."""
    return {
        "error": [],
        "result": {
            "XBTUSD": [
                [1706608800, "42000.0", "42500.0", "41900.0", "42300.0", "42200.0", "10.5", 150],
                [1706609700, "42300.0", "42600.0", "42200.0", "42550.0", "42400.0", "8.2", 120],
            ],
            "last": 1706609700,
        },
    }


@pytest.fixture
def questdb_exec_response() -> dict:
    """Sample QuestDB /exec response."""
    return {
        "columns": [
            {"name": "last_ts", "type": "TIMESTAMP"},
        ],
        "dataset": [
            ["2026-01-30T10:15:00.000000Z"],
        ],
    }
