"""Tests for QuestDB writer module."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from crypto_data_collector.questdb_writer import QuestDBWriter
from crypto_data_collector.questdb_rest import QuestDBRest


class TestQuestDBWriter:
    """Tests for QuestDBWriter class."""

    def test_writer_creation(self):
        """Test QuestDBWriter creation."""
        rest = MagicMock(spec=QuestDBRest)
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)

        assert writer.ilp_conf == "http::addr=localhost:9000;"
        assert writer.rest is rest

    def test_writer_frozen(self):
        """Test that QuestDBWriter is frozen."""
        rest = MagicMock(spec=QuestDBRest)
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)

        with pytest.raises(AttributeError):
            writer.ilp_conf = "other"


class TestGetLastTs:
    """Tests for QuestDBWriter.get_last_ts method."""

    def test_get_last_ts_returns_timestamp(self):
        """Test that get_last_ts returns parsed timestamp."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = "2026-01-30T10:15:00.000000Z"
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)

        result = writer.get_last_ts("BTC")

        assert result == pd.Timestamp("2026-01-30 10:15:00")

    def test_get_last_ts_returns_none_for_no_data(self):
        """Test that get_last_ts returns None when no data exists."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = None
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)

        result = writer.get_last_ts("BTC")

        assert result is None

    def test_get_last_ts_sanitizes_symbol(self):
        """Test that symbol is sanitized in query."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = None
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)

        writer.get_last_ts("BTC")

        rest.scalar.assert_called_once()
        query = rest.scalar.call_args[0][0]
        assert "BTC" in query
        assert "'; DROP TABLE" not in query

    def test_get_last_ts_rejects_sql_injection(self):
        """Test that SQL injection attempts are sanitized."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = None
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)

        result = writer.get_last_ts("BTC'; DROP TABLE crypto_bars_15m; --")

        rest.scalar.assert_called_once()
        query = rest.scalar.call_args[0][0]
        assert "DROP TABLE" not in query

    def test_get_last_ts_allows_valid_symbols(self):
        """Test that valid symbols with dots and hyphens are allowed."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = None
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)

        writer.get_last_ts("BTC.X")

        query = rest.scalar.call_args[0][0]
        assert "BTC.X" in query

    def test_get_last_ts_returns_none_for_empty_symbol(self):
        """Test that empty symbol returns None."""
        rest = MagicMock(spec=QuestDBRest)
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)

        result = writer.get_last_ts("   ")

        assert result is None
        rest.scalar.assert_not_called()

    def test_get_last_ts_handles_parse_error(self):
        """Test that timestamp parse errors return None."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = "invalid-timestamp-format"
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)

        result = writer.get_last_ts("BTC")

        assert result is None


class TestWriteBars:
    """Tests for QuestDBWriter.write_bars method."""

    def test_write_bars_empty_dataframe(self):
        """Test that empty DataFrame returns 0."""
        rest = MagicMock(spec=QuestDBRest)
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)
        df = pd.DataFrame()

        result = writer.write_bars(df)

        assert result == 0

    @patch("crypto_data_collector.questdb_writer.Sender")
    def test_write_bars_success(self, mock_sender_class):
        """Test successful bar writing."""
        mock_sender = MagicMock()
        mock_sender_class.from_conf.return_value.__enter__ = MagicMock(return_value=mock_sender)
        mock_sender_class.from_conf.return_value.__exit__ = MagicMock(return_value=False)

        rest = MagicMock(spec=QuestDBRest)
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)

        df = pd.DataFrame([
            {
                "ts": pd.Timestamp("2026-01-30 10:00:00"),
                "symbol": "BTC",
                "open": 42000.0,
                "high": 42500.0,
                "low": 41900.0,
                "close": 42300.0,
                "volume": 10.5,
                "source": "kraken",
                "ingested_at": pd.Timestamp("2026-01-30 10:05:00"),
            }
        ])

        result = writer.write_bars(df)

        assert result == 1
        mock_sender.dataframe.assert_called_once()
        mock_sender.flush.assert_called_once()

    @patch("crypto_data_collector.questdb_writer.Sender")
    def test_write_bars_multiple_rows(self, mock_sender_class):
        """Test writing multiple bars."""
        mock_sender = MagicMock()
        mock_sender_class.from_conf.return_value.__enter__ = MagicMock(return_value=mock_sender)
        mock_sender_class.from_conf.return_value.__exit__ = MagicMock(return_value=False)

        rest = MagicMock(spec=QuestDBRest)
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)

        df = pd.DataFrame([
            {"ts": pd.Timestamp("2026-01-30 10:00:00"), "symbol": "BTC", "open": 42000.0, "high": 42500.0, "low": 41900.0, "close": 42300.0, "volume": 10.5, "source": "kraken", "ingested_at": pd.Timestamp.now()},
            {"ts": pd.Timestamp("2026-01-30 10:15:00"), "symbol": "BTC", "open": 42300.0, "high": 42600.0, "low": 42200.0, "close": 42550.0, "volume": 8.2, "source": "kraken", "ingested_at": pd.Timestamp.now()},
            {"ts": pd.Timestamp("2026-01-30 10:30:00"), "symbol": "BTC", "open": 42550.0, "high": 42700.0, "low": 42400.0, "close": 42600.0, "volume": 12.1, "source": "kraken", "ingested_at": pd.Timestamp.now()},
        ])

        result = writer.write_bars(df)

        assert result == 3

    @patch("crypto_data_collector.questdb_writer.Sender")
    def test_write_bars_passes_correct_params(self, mock_sender_class):
        """Test that correct parameters are passed to sender."""
        mock_sender = MagicMock()
        mock_sender_class.from_conf.return_value.__enter__ = MagicMock(return_value=mock_sender)
        mock_sender_class.from_conf.return_value.__exit__ = MagicMock(return_value=False)

        rest = MagicMock(spec=QuestDBRest)
        writer = QuestDBWriter(ilp_conf="http::addr=localhost:9000;", rest=rest)

        df = pd.DataFrame([
            {"ts": pd.Timestamp("2026-01-30 10:00:00"), "symbol": "ETH", "open": 3000.0, "high": 3050.0, "low": 2990.0, "close": 3030.0, "volume": 100.0, "source": "kraken", "ingested_at": pd.Timestamp.now()},
        ])

        writer.write_bars(df, table_name="custom_table")

        call_kwargs = mock_sender.dataframe.call_args[1]
        assert call_kwargs["table_name"] == "custom_table"
        assert call_kwargs["symbols"] == ["symbol", "source"]
        assert call_kwargs["at"] == "ts"
