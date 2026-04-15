"""Tests for watchlist module."""
from __future__ import annotations

import pytest

from crypto_data_collector.watchlist import load_watchlist, symbols_from_watchlist, WatchItem


class TestLoadWatchlist:
    """Tests for load_watchlist function."""

    def test_load_watchlist_basic(self, tmp_path):
        """Test loading a basic watchlist with symbol keys."""
        p = tmp_path / "watchlist.json"
        p.write_text('{"AAPL":{"symbol":"AAPL"}, "MSFT":{"symbol":"MSFT"}}', encoding="utf-8")

        items = load_watchlist(p)

        assert len(items) == 2
        assert all(isinstance(item, WatchItem) for item in items)
        assert items[0].symbol == "AAPL"
        assert items[1].symbol == "MSFT"

    def test_load_watchlist_normalizes_to_uppercase(self, tmp_path):
        """Test that symbols are normalized to uppercase."""
        p = tmp_path / "watchlist.json"
        p.write_text('{"aapl":{"symbol":"aapl"}, "Msft":{"symbol":"Msft"}}', encoding="utf-8")

        items = load_watchlist(p)

        assert items[0].symbol == "AAPL"
        assert items[1].symbol == "MSFT"

    def test_load_watchlist_uses_key_as_fallback(self, tmp_path):
        """Test that key is used when symbol field is missing."""
        p = tmp_path / "watchlist.json"
        p.write_text('{"GOOG":{}, "AMZN":{"other_field":"value"}}', encoding="utf-8")

        items = load_watchlist(p)

        assert items[0].symbol == "AMZN"
        assert items[1].symbol == "GOOG"

    def test_load_watchlist_preserves_raw_data(self, tmp_path):
        """Test that raw data is preserved in WatchItem."""
        p = tmp_path / "watchlist.json"
        p.write_text('{"AAPL":{"symbol":"AAPL","score":1.5,"weight":0.1}}', encoding="utf-8")

        items = load_watchlist(p)

        assert items[0].raw["score"] == 1.5
        assert items[0].raw["weight"] == 0.1

    def test_load_watchlist_sorted_alphabetically(self, tmp_path):
        """Test that watchlist items are sorted alphabetically."""
        p = tmp_path / "watchlist.json"
        p.write_text('{"ZZZZ":{"symbol":"ZZZZ"}, "AAAA":{"symbol":"AAAA"}, "MMMM":{"symbol":"MMMM"}}', encoding="utf-8")

        items = load_watchlist(p)

        symbols = [item.symbol for item in items]
        assert symbols == ["AAAA", "MMMM", "ZZZZ"]

    def test_load_watchlist_raises_on_non_dict(self, tmp_path):
        """Test that ValueError is raised for non-dict JSON."""
        p = tmp_path / "watchlist.json"
        p.write_text('["AAPL", "MSFT"]', encoding="utf-8")

        with pytest.raises(ValueError, match="must be an object/dict"):
            load_watchlist(p)

    def test_load_watchlist_raises_on_missing_file(self, tmp_path):
        """Test that FileNotFoundError is raised for missing file."""
        p = tmp_path / "nonexistent.json"

        with pytest.raises(FileNotFoundError):
            load_watchlist(p)

    def test_load_watchlist_skips_empty_symbols(self, tmp_path):
        """Test that empty symbol keys are skipped."""
        p = tmp_path / "watchlist.json"
        p.write_text('{"":{"value":1}, "AAPL":{"symbol":"AAPL"}}', encoding="utf-8")

        items = load_watchlist(p)

        assert len(items) == 1
        assert items[0].symbol == "AAPL"


class TestSymbolsFromWatchlist:
    """Tests for symbols_from_watchlist function."""

    def test_symbols_from_watchlist(self, tmp_path):
        """Test extracting symbols from watchlist."""
        p = tmp_path / "watchlist.json"
        p.write_text('{"AAPL":{"symbol":"AAPL"}, "msft":{"symbol":"msft"}, "X":{}}', encoding="utf-8")

        syms = symbols_from_watchlist(p)

        assert syms == ["AAPL", "MSFT", "X"]

    def test_symbols_from_watchlist_empty(self, tmp_path):
        """Test extracting symbols from empty watchlist."""
        p = tmp_path / "watchlist.json"
        p.write_text('{}', encoding="utf-8")

        syms = symbols_from_watchlist(p)

        assert syms == []


class TestWatchItem:
    """Tests for WatchItem dataclass."""

    def test_watchitem_creation(self):
        """Test WatchItem creation."""
        item = WatchItem(symbol="AAPL", raw={"score": 1.5})

        assert item.symbol == "AAPL"
        assert item.raw["score"] == 1.5

    def test_watchitem_frozen(self):
        """Test that WatchItem is frozen (immutable)."""
        item = WatchItem(symbol="AAPL", raw={})

        with pytest.raises(AttributeError):
            item.symbol = "MSFT"
