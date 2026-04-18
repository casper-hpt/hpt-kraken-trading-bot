"""Tests for watchlist module."""
from __future__ import annotations

import pytest

from crypto_data_collector.watchlist import load_watchlist, symbols_from_watchlist, WatchItem


class TestLoadWatchlist:
    def test_load_watchlist_basic(self, tmp_path):
        p = tmp_path / "watchlist.json"
        p.write_text('["AAPL", "MSFT"]', encoding="utf-8")

        items = load_watchlist(p)

        assert len(items) == 2
        assert all(isinstance(item, WatchItem) for item in items)
        assert items[0].symbol == "AAPL"
        assert items[1].symbol == "MSFT"

    def test_load_watchlist_normalizes_to_uppercase(self, tmp_path):
        p = tmp_path / "watchlist.json"
        p.write_text('["aapl", "Msft"]', encoding="utf-8")

        items = load_watchlist(p)

        assert items[0].symbol == "AAPL"
        assert items[1].symbol == "MSFT"

    def test_load_watchlist_sorted_alphabetically(self, tmp_path):
        p = tmp_path / "watchlist.json"
        p.write_text('["ZZZZ", "AAAA", "MMMM"]', encoding="utf-8")

        items = load_watchlist(p)

        assert [item.symbol for item in items] == ["AAAA", "MMMM", "ZZZZ"]

    def test_load_watchlist_raises_on_non_list(self, tmp_path):
        p = tmp_path / "watchlist.json"
        p.write_text('{"AAPL": "AAPL"}', encoding="utf-8")

        with pytest.raises(ValueError, match="must be a JSON array"):
            load_watchlist(p)

    def test_load_watchlist_raises_on_missing_file(self, tmp_path):
        p = tmp_path / "nonexistent.json"

        with pytest.raises(FileNotFoundError):
            load_watchlist(p)

    def test_load_watchlist_skips_empty_strings(self, tmp_path):
        p = tmp_path / "watchlist.json"
        p.write_text('["", "AAPL"]', encoding="utf-8")

        items = load_watchlist(p)

        assert len(items) == 1
        assert items[0].symbol == "AAPL"


class TestSymbolsFromWatchlist:
    def test_symbols_from_watchlist(self, tmp_path):
        p = tmp_path / "watchlist.json"
        p.write_text('["AAPL", "msft", "X"]', encoding="utf-8")

        syms = symbols_from_watchlist(p)

        assert syms == ["AAPL", "MSFT", "X"]

    def test_symbols_from_watchlist_empty(self, tmp_path):
        p = tmp_path / "watchlist.json"
        p.write_text('[]', encoding="utf-8")

        syms = symbols_from_watchlist(p)

        assert syms == []


class TestWatchItem:
    def test_watchitem_creation(self):
        item = WatchItem(symbol="AAPL")

        assert item.symbol == "AAPL"

    def test_watchitem_frozen(self):
        item = WatchItem(symbol="AAPL")

        with pytest.raises(AttributeError):
            item.symbol = "MSFT"
