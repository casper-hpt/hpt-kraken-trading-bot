"""Tests for Kraken client module."""
from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from crypto_data_collector.kraken_client import (
    KrakenClient,
    KrakenAPIError,
    symbol_to_pair,
    parse_bulk_csv,
    find_csv_in_zip,
    read_csv_from_zip,
)


class TestSymbolToPair:
    """Tests for symbol_to_pair function."""

    def test_btc_maps_to_xbtusd(self):
        assert symbol_to_pair("BTC") == "XBTUSD"

    def test_doge_maps_to_xdgusd(self):
        assert symbol_to_pair("DOGE") == "XDGUSD"

    def test_eth_maps_to_ethusd(self):
        assert symbol_to_pair("ETH") == "ETHUSD"

    def test_sol_maps_to_solusd(self):
        assert symbol_to_pair("SOL") == "SOLUSD"

    def test_case_insensitive(self):
        assert symbol_to_pair("btc") == "XBTUSD"
        assert symbol_to_pair("eth") == "ETHUSD"

    def test_unknown_symbol_defaults_to_symbol_usd(self):
        assert symbol_to_pair("XYZ") == "XYZUSD"


class TestKrakenClient:
    """Tests for KrakenClient class."""

    def test_client_creation_defaults(self):
        client = KrakenClient()

        assert client.base_url == "https://api.kraken.com/0/public"
        assert client.timeout_s == 30
        assert client.max_retries == 5
        assert client.backoff_s == 2.0

    def test_client_custom_values(self):
        client = KrakenClient(
            base_url="https://custom.api.com",
            timeout_s=60,
            max_retries=3,
            backoff_s=1.0,
        )

        assert client.base_url == "https://custom.api.com"
        assert client.timeout_s == 60
        assert client.max_retries == 3

    def test_client_frozen(self):
        client = KrakenClient()

        with pytest.raises(AttributeError):
            client.base_url = "other"

    @patch("crypto_data_collector.kraken_client.requests.get")
    def test_fetch_ohlc_success(self, mock_get, kraken_ohlc_response):
        mock_response = MagicMock()
        mock_response.json.return_value = kraken_ohlc_response
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = KrakenClient()
        df = client.fetch_ohlc("XBTUSD")

        assert not df.empty
        assert list(df.columns) == ["ts", "open", "high", "low", "close", "volume"]
        assert len(df) == 2

    @patch("crypto_data_collector.kraken_client.requests.get")
    def test_fetch_ohlc_parses_values(self, mock_get, kraken_ohlc_response):
        mock_response = MagicMock()
        mock_response.json.return_value = kraken_ohlc_response
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = KrakenClient()
        df = client.fetch_ohlc("XBTUSD")

        row = df.iloc[0]
        assert row["open"] == 42000.0
        assert row["high"] == 42500.0
        assert row["low"] == 41900.0
        assert row["close"] == 42300.0
        assert row["volume"] == 10.5

    @patch("crypto_data_collector.kraken_client.requests.get")
    def test_fetch_ohlc_sorted_by_timestamp(self, mock_get, kraken_ohlc_response):
        mock_response = MagicMock()
        mock_response.json.return_value = kraken_ohlc_response
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = KrakenClient()
        df = client.fetch_ohlc("XBTUSD")

        timestamps = df["ts"].tolist()
        assert timestamps == sorted(timestamps)

    @patch("crypto_data_collector.kraken_client.requests.get")
    def test_fetch_ohlc_timestamps_are_tz_naive(self, mock_get, kraken_ohlc_response):
        mock_response = MagicMock()
        mock_response.json.return_value = kraken_ohlc_response
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = KrakenClient()
        df = client.fetch_ohlc("XBTUSD")

        for ts in df["ts"]:
            assert ts.tzinfo is None

    @patch("crypto_data_collector.kraken_client.requests.get")
    def test_fetch_ohlc_empty_result(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"error": [], "result": {"last": 0}}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = KrakenClient()
        df = client.fetch_ohlc("XBTUSD")

        assert df.empty
        assert list(df.columns) == ["ts", "open", "high", "low", "close", "volume"]

    @patch("crypto_data_collector.kraken_client.requests.get")
    def test_fetch_ohlc_api_error(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"error": ["EQuery:Unknown asset pair"]}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = KrakenClient()

        with pytest.raises(KrakenAPIError, match="Unknown asset pair"):
            client.fetch_ohlc("INVALIDPAIR")

    @patch("crypto_data_collector.kraken_client.requests.get")
    def test_fetch_ohlc_passes_since_param(self, mock_get, kraken_ohlc_response):
        mock_response = MagicMock()
        mock_response.json.return_value = kraken_ohlc_response
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = KrakenClient()
        client.fetch_ohlc("XBTUSD", interval=15, since=1706600000)

        call_kwargs = mock_get.call_args[1]
        assert call_kwargs["params"]["since"] == 1706600000
        assert call_kwargs["params"]["interval"] == 15
        assert call_kwargs["params"]["pair"] == "XBTUSD"


class TestParseBulkCsv:
    """Tests for parse_bulk_csv function."""

    def test_parse_basic_csv(self):
        csv = "1706608800,42000.0,42500.0,41900.0,42300.0,10.5,150\n1706609700,42300.0,42600.0,42200.0,42550.0,8.2,120\n"

        df = parse_bulk_csv(csv)

        assert len(df) == 2
        assert list(df.columns) == ["ts", "open", "high", "low", "close", "volume"]

    def test_parse_csv_values(self):
        csv = "1706608800,42000.0,42500.0,41900.0,42300.0,10.5,150\n"

        df = parse_bulk_csv(csv)

        row = df.iloc[0]
        assert row["open"] == 42000.0
        assert row["high"] == 42500.0
        assert row["low"] == 41900.0
        assert row["close"] == 42300.0
        assert row["volume"] == 10.5

    def test_parse_csv_timestamps_are_tz_naive(self):
        csv = "1706608800,1,2,0.5,1.5,100,10\n"

        df = parse_bulk_csv(csv)

        assert df["ts"].iloc[0].tzinfo is None

    def test_parse_csv_sorted_by_timestamp(self):
        csv = "1706609700,2,3,1,2.5,200,20\n1706608800,1,2,0.5,1.5,100,10\n"

        df = parse_bulk_csv(csv)

        timestamps = df["ts"].tolist()
        assert timestamps == sorted(timestamps)

    def test_parse_csv_bytes(self):
        csv_bytes = b"1706608800,42000.0,42500.0,41900.0,42300.0,10.5,150\n"

        df = parse_bulk_csv(csv_bytes)

        assert len(df) == 1

    def test_parse_empty_csv(self):
        csv = ""

        df = parse_bulk_csv(csv)

        assert df.empty
        assert list(df.columns) == ["ts", "open", "high", "low", "close", "volume"]

    def test_parse_csv_drops_trades_column(self):
        csv = "1706608800,42000.0,42500.0,41900.0,42300.0,10.5,150\n"

        df = parse_bulk_csv(csv)

        assert "trades" not in df.columns


class TestFindCsvInZip:
    """Tests for find_csv_in_zip function."""

    def _make_zip(self, tmp_path: Path, filenames: list[str]) -> Path:
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            for name in filenames:
                zf.writestr(name, "1706608800,1,2,0.5,1.5,100,10\n")
        return zip_path

    def test_find_btc_csv(self, tmp_path):
        zp = self._make_zip(tmp_path, ["XBTUSD_15.csv"])

        result = find_csv_in_zip(zp, "BTC", interval=15)

        assert result == "XBTUSD_15.csv"

    def test_find_eth_csv(self, tmp_path):
        zp = self._make_zip(tmp_path, ["ETHUSD_15.csv"])

        result = find_csv_in_zip(zp, "ETH", interval=15)

        assert result == "ETHUSD_15.csv"

    def test_find_eth_csv_legacy_name(self, tmp_path):
        zp = self._make_zip(tmp_path, ["XETHZUSD_15.csv"])

        result = find_csv_in_zip(zp, "ETH", interval=15)

        assert result == "XETHZUSD_15.csv"

    def test_find_doge_csv(self, tmp_path):
        zp = self._make_zip(tmp_path, ["XDGUSD_15.csv"])

        result = find_csv_in_zip(zp, "DOGE", interval=15)

        assert result == "XDGUSD_15.csv"

    def test_find_sol_csv(self, tmp_path):
        zp = self._make_zip(tmp_path, ["SOLUSD_15.csv"])

        result = find_csv_in_zip(zp, "SOL", interval=15)

        assert result == "SOLUSD_15.csv"

    def test_returns_none_when_not_found(self, tmp_path):
        zp = self._make_zip(tmp_path, ["XBTUSD_15.csv"])

        result = find_csv_in_zip(zp, "DOESNOTEXIST", interval=15)

        assert result is None

    def test_case_insensitive_fallback(self, tmp_path):
        zp = self._make_zip(tmp_path, ["solusd_15.csv"])

        result = find_csv_in_zip(zp, "SOL", interval=15)

        assert result == "solusd_15.csv"


class TestReadCsvFromZip:
    """Tests for read_csv_from_zip function."""

    def test_reads_csv_content(self, tmp_path):
        zip_path = tmp_path / "test.zip"
        content = b"1706608800,1,2,0.5,1.5,100,10\n"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("XBTUSD_15.csv", content)

        result = read_csv_from_zip(zip_path, "XBTUSD_15.csv")

        assert result == content


class TestKrakenAPIError:
    """Tests for KrakenAPIError exception."""

    def test_error_inherits_from_runtime_error(self):
        error = KrakenAPIError("test error")

        assert isinstance(error, RuntimeError)
        assert str(error) == "test error"
