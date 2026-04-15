"""Tests for config module."""
from __future__ import annotations

import pytest

from crypto_data_collector.config import Config


class TestConfig:
    """Tests for Config dataclass."""

    def test_config_from_env_defaults(self, monkeypatch):
        """Test Config.from_env with all defaults."""
        monkeypatch.delenv("QUESTDB_HOST", raising=False)
        monkeypatch.delenv("QUESTDB_HTTP_PORT", raising=False)
        monkeypatch.delenv("INGEST_INTERVAL_MINUTES", raising=False)
        monkeypatch.delenv("LOG_LEVEL", raising=False)

        cfg = Config.from_env()

        assert cfg.questdb_host == "localhost"
        assert cfg.questdb_http_port == 9000
        assert cfg.ingest_interval_minutes == 15
        assert cfg.log_level == "INFO"

    def test_config_from_env_custom_values(self, monkeypatch):
        """Test Config.from_env with custom values."""
        monkeypatch.setenv("QUESTDB_HOST", "questdb.example.com")
        monkeypatch.setenv("QUESTDB_HTTP_PORT", "8080")
        monkeypatch.setenv("INGEST_INTERVAL_MINUTES", "30")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        cfg = Config.from_env()

        assert cfg.questdb_host == "questdb.example.com"
        assert cfg.questdb_http_port == 8080
        assert cfg.ingest_interval_minutes == 30
        assert cfg.log_level == "DEBUG"

    def test_config_questdb_exec_url(self):
        """Test questdb_exec_url property."""
        cfg = Config(questdb_host="myhost", questdb_http_port=9000)

        assert cfg.questdb_exec_url == "http://myhost:9000/exec"

    def test_config_questdb_ilp_conf(self):
        """Test questdb_ilp_conf property."""
        cfg = Config(questdb_host="myhost", questdb_http_port=9000)

        assert cfg.questdb_ilp_conf == "http::addr=myhost:9000;"

    def test_config_frozen(self):
        """Test that Config is frozen (immutable)."""
        cfg = Config()

        with pytest.raises(AttributeError):
            cfg.questdb_host = "new_host"

    def test_config_defaults(self):
        """Test Config default values."""
        cfg = Config()

        assert cfg.questdb_host == "localhost"
        assert cfg.questdb_http_port == 9000
        assert cfg.ingest_interval_minutes == 15
        assert cfg.log_level == "INFO"
