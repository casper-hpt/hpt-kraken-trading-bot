"""Tests for Kraken client module."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from crypto_data_collector.kraken_client import (
    KrakenClient,
    KrakenAPIError,
    symbol_to_pair,
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



class TestKrakenAPIError:
    """Tests for KrakenAPIError exception."""

    def test_error_inherits_from_runtime_error(self):
        error = KrakenAPIError("test error")

        assert isinstance(error, RuntimeError)
        assert str(error) == "test error"
