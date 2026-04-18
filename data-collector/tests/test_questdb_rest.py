"""Tests for QuestDB REST client module."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from crypto_data_collector.questdb_rest import QuestDBRest, QuestDBError


class TestQuestDBRest:
    """Tests for QuestDBRest class."""

    def test_client_creation(self):
        """Test QuestDBRest creation."""
        client = QuestDBRest(exec_url="http://localhost:9000/exec")

        assert client.exec_url == "http://localhost:9000/exec"
        assert client.timeout_s == 30

    def test_client_custom_timeout(self):
        """Test QuestDBRest with custom timeout."""
        client = QuestDBRest(exec_url="http://localhost:9000/exec", timeout_s=60)

        assert client.timeout_s == 60

    def test_client_frozen(self):
        """Test that QuestDBRest is frozen."""
        client = QuestDBRest(exec_url="http://localhost:9000/exec")

        with pytest.raises(AttributeError):
            client.exec_url = "http://other:9000/exec"

    @patch("crypto_data_collector.questdb_rest.requests.get")
    def test_exec_success(self, mock_get):
        """Test successful query execution."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"dataset": [[1, 2, 3]], "count": 1}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = QuestDBRest(exec_url="http://localhost:9000/exec")
        result = client.exec("SELECT * FROM test")

        assert result == {"dataset": [[1, 2, 3]], "count": 1}
        mock_get.assert_called_once_with(
            "http://localhost:9000/exec",
            params={"query": "SELECT * FROM test"},
            timeout=30,
        )

    @patch("crypto_data_collector.questdb_rest.requests.get")
    def test_exec_handles_error_response(self, mock_get):
        """Test handling of error in response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"error": "Table does not exist"}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = QuestDBRest(exec_url="http://localhost:9000/exec")

        with pytest.raises(QuestDBError, match="Table does not exist"):
            client.exec("SELECT * FROM nonexistent")

    @patch("crypto_data_collector.questdb_rest.requests.get")
    def test_exec_handles_http_error(self, mock_get):
        """Test handling of HTTP error."""
        mock_get.side_effect = requests.RequestException("Connection refused")

        client = QuestDBRest(exec_url="http://localhost:9000/exec")

        with pytest.raises(QuestDBError, match="Connection refused"):
            client.exec("SELECT * FROM test")

    @patch("crypto_data_collector.questdb_rest.requests.get")
    def test_scalar_success(self, mock_get):
        """Test successful scalar query."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "columns": [{"name": "count", "type": "LONG"}],
            "dataset": [[42]],
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = QuestDBRest(exec_url="http://localhost:9000/exec")
        result = client.scalar("SELECT count(*) as count FROM test", "count")

        assert result == 42

    @patch("crypto_data_collector.questdb_rest.requests.get")
    def test_scalar_returns_none_for_empty_dataset(self, mock_get):
        """Test scalar returns None for empty dataset."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "columns": [{"name": "value", "type": "LONG"}],
            "dataset": [],
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = QuestDBRest(exec_url="http://localhost:9000/exec")
        result = client.scalar("SELECT value FROM empty_table", "value")

        assert result is None

    @patch("crypto_data_collector.questdb_rest.requests.get")
    def test_scalar_returns_none_for_missing_column(self, mock_get):
        """Test scalar returns None when column not found."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "columns": [{"name": "other", "type": "LONG"}],
            "dataset": [[1]],
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = QuestDBRest(exec_url="http://localhost:9000/exec")
        result = client.scalar("SELECT other FROM test", "missing_column")

        assert result is None

    @patch("crypto_data_collector.questdb_rest.requests.get")
    def test_scalar_returns_none_for_no_columns(self, mock_get):
        """Test scalar returns None when no columns in response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = QuestDBRest(exec_url="http://localhost:9000/exec")
        result = client.scalar("SELECT * FROM test", "col")

        assert result is None

    @patch("crypto_data_collector.questdb_rest.requests.get")
    def test_scalar_handles_null_value(self, mock_get):
        """Test scalar handles null values."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "columns": [{"name": "value", "type": "TIMESTAMP"}],
            "dataset": [[None]],
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = QuestDBRest(exec_url="http://localhost:9000/exec")
        result = client.scalar("SELECT max(ts) as value FROM empty", "value")

        assert result is None

    @patch("crypto_data_collector.questdb_rest.requests.get")
    def test_scalar_multiple_columns(self, mock_get):
        """Test scalar extracts correct column from multiple columns."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "columns": [
                {"name": "a", "type": "LONG"},
                {"name": "b", "type": "LONG"},
                {"name": "c", "type": "LONG"},
            ],
            "dataset": [[1, 2, 3]],
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = QuestDBRest(exec_url="http://localhost:9000/exec")

        assert client.scalar("SELECT a, b, c FROM test", "a") == 1
        assert client.scalar("SELECT a, b, c FROM test", "b") == 2
        assert client.scalar("SELECT a, b, c FROM test", "c") == 3


class TestQuestDBError:
    """Tests for QuestDBError exception."""

    def test_error_inherits_from_runtime_error(self):
        """Test that QuestDBError inherits from RuntimeError."""
        error = QuestDBError("test error")

        assert isinstance(error, RuntimeError)
        assert str(error) == "test error"
