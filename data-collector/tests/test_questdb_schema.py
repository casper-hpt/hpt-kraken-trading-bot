"""Tests for QuestDB schema module."""
from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from crypto_data_collector.questdb_schema import SchemaManager
from crypto_data_collector.questdb_rest import QuestDBRest


class TestSchemaManager:
    """Tests for SchemaManager class."""

    def test_schema_manager_creation(self):
        """Test SchemaManager creation."""
        rest = MagicMock(spec=QuestDBRest)
        manager = SchemaManager(rest=rest)

        assert manager.rest is rest

    def test_schema_manager_frozen(self):
        """Test that SchemaManager is frozen."""
        rest = MagicMock(spec=QuestDBRest)
        manager = SchemaManager(rest=rest)

        with pytest.raises(AttributeError):
            manager.rest = MagicMock()


class TestEnsureSchema:
    """Tests for SchemaManager.ensure_schema method."""

    def test_ensure_schema_creates_bars_table(self):
        """Test that ensure_schema creates crypto_bars_15m table."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = True  # tables already WAL
        manager = SchemaManager(rest=rest)

        manager.ensure_schema()

        calls = [c[0][0] for c in rest.exec.call_args_list]
        bars_ddl = [c for c in calls if "CREATE TABLE IF NOT EXISTS crypto_bars_15m" in c]
        assert len(bars_ddl) == 1

    def test_ensure_schema_creates_watchlist_table(self):
        """Test that ensure_schema creates watchlist table."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = True
        manager = SchemaManager(rest=rest)

        manager.ensure_schema()

        calls = [c[0][0] for c in rest.exec.call_args_list]
        watchlist_ddl = [c for c in calls if "CREATE TABLE IF NOT EXISTS watchlist" in c]
        assert len(watchlist_ddl) == 1

    def test_ensure_schema_bars_includes_all_columns(self):
        """Test that bars DDL includes all required columns."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = True
        manager = SchemaManager(rest=rest)

        manager.ensure_schema()

        calls = [c[0][0] for c in rest.exec.call_args_list]
        bars_ddl = [c for c in calls if "CREATE TABLE IF NOT EXISTS crypto_bars_15m" in c][0]
        assert "ts TIMESTAMP" in bars_ddl
        assert "symbol SYMBOL" in bars_ddl
        assert "open DOUBLE" in bars_ddl
        assert "high DOUBLE" in bars_ddl
        assert "low DOUBLE" in bars_ddl
        assert "close DOUBLE" in bars_ddl
        assert "volume LONG" in bars_ddl
        assert "source SYMBOL" in bars_ddl
        assert "ingested_at TIMESTAMP" in bars_ddl

    def test_ensure_schema_bars_includes_timestamp_designation(self):
        """Test that bars DDL includes designated timestamp."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = True
        manager = SchemaManager(rest=rest)

        manager.ensure_schema()

        calls = [c[0][0] for c in rest.exec.call_args_list]
        bars_ddl = [c for c in calls if "CREATE TABLE IF NOT EXISTS crypto_bars_15m" in c][0]
        assert "TIMESTAMP(ts)" in bars_ddl

    def test_ensure_schema_bars_includes_partition_by_day(self):
        """Test that bars DDL includes daily partitioning."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = True
        manager = SchemaManager(rest=rest)

        manager.ensure_schema()

        calls = [c[0][0] for c in rest.exec.call_args_list]
        bars_ddl = [c for c in calls if "CREATE TABLE IF NOT EXISTS crypto_bars_15m" in c][0]
        assert "PARTITION BY DAY" in bars_ddl

    def test_ensure_schema_bars_includes_wal(self):
        """Test that bars DDL includes WAL for durability."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = True
        manager = SchemaManager(rest=rest)

        manager.ensure_schema()

        calls = [c[0][0] for c in rest.exec.call_args_list]
        bars_ddl = [c for c in calls if "CREATE TABLE IF NOT EXISTS crypto_bars_15m" in c][0]
        assert "WAL" in bars_ddl

    def test_ensure_schema_bars_includes_dedup_keys(self):
        """Test that bars DDL includes deduplication keys."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = True
        manager = SchemaManager(rest=rest)

        manager.ensure_schema()

        calls = [c[0][0] for c in rest.exec.call_args_list]
        bars_ddl = [c for c in calls if "CREATE TABLE IF NOT EXISTS crypto_bars_15m" in c][0]
        assert "DEDUP UPSERT KEYS(ts, symbol)" in bars_ddl

    def test_ensure_schema_watchlist_is_simple(self):
        """Test that watchlist DDL has only symbol and updated_at."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = True
        manager = SchemaManager(rest=rest)

        manager.ensure_schema()

        calls = [c[0][0] for c in rest.exec.call_args_list]
        wl_ddl = [c for c in calls if "CREATE TABLE IF NOT EXISTS watchlist" in c][0]
        assert "updated_at TIMESTAMP" in wl_ddl
        assert "symbol SYMBOL" in wl_ddl
        # Should NOT contain old stock-specific columns
        assert "score" not in wl_ddl
        assert "sentiment" not in wl_ddl
        assert "earnings" not in wl_ddl

    def test_ensure_schema_is_idempotent(self):
        """Test that ensure_schema can be called multiple times."""
        rest = MagicMock(spec=QuestDBRest)
        rest.scalar.return_value = True
        manager = SchemaManager(rest=rest)

        manager.ensure_schema()
        manager.ensure_schema()

        calls = [c[0][0] for c in rest.exec.call_args_list]
        bars_ddls = [c for c in calls if "IF NOT EXISTS" in c and "crypto_bars_15m" in c]
        assert len(bars_ddls) == 2  # called twice
