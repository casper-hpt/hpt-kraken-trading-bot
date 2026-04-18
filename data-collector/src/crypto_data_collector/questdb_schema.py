from __future__ import annotations

import logging
from dataclasses import dataclass

from .questdb_rest import QuestDBRest


LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchemaManager:
    rest: QuestDBRest

    def _is_wal_table(self, table_name: str) -> bool | None:
        """Check if a table uses WAL. Returns None if the table doesn't exist."""
        try:
            v = self.rest.scalar(
                f"SELECT walEnabled FROM tables() WHERE name = '{table_name}';",
                "walEnabled",
            )
            if v is None:
                return None
            return bool(v)
        except Exception:
            return None

    def _convert_to_wal(self, table_name: str) -> None:
        """Convert an existing non-WAL table to WAL."""
        LOG.warning("Table %s exists but is not WAL-enabled, converting...", table_name)
        self.rest.exec(f"ALTER TABLE {table_name} SET TYPE WAL;")
        LOG.info("Table %s converted to WAL.", table_name)

    def ensure_schema(self) -> None:
        """Create the crypto_bars_15m and watchlist tables if they don't exist.

        Uses WAL for durability and DEDUP UPSERT KEYS for deduplication.
        Converts existing non-WAL tables to WAL if needed.
        """
        bars_ddl = """CREATE TABLE IF NOT EXISTS crypto_bars_15m (
    ts TIMESTAMP,
    symbol SYMBOL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    source SYMBOL,
    ingested_at TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
  DEDUP UPSERT KEYS(ts, symbol);"""

        watchlist_ddl = """CREATE TABLE IF NOT EXISTS crypto_watchlist (
    updated_at TIMESTAMP,
    symbol SYMBOL
) TIMESTAMP(updated_at) PARTITION BY MONTH WAL
  DEDUP UPSERT KEYS(updated_at, symbol);"""

        LOG.info("Ensuring QuestDB schema (crypto_bars_15m, crypto_watchlist)...")
        self.rest.exec(bars_ddl)
        self.rest.exec(watchlist_ddl)

        # Convert any existing non-WAL tables to WAL
        for table in ("crypto_bars_15m", "crypto_watchlist"):
            wal = self._is_wal_table(table)
            if wal is False:
                self._convert_to_wal(table)

        LOG.info("Schema ensured.")

    def purge_old_bars(self, retention_days: int = 90) -> None:
        """Drop daily partitions from crypto_bars_15m older than retention_days."""
        try:
            self.rest.exec(
                f"ALTER TABLE crypto_bars_15m DROP PARTITION "
                f"WHERE ts < dateadd('d', -{retention_days}, now());"
            )
            LOG.info("Purged crypto_bars_15m partitions older than %d days.", retention_days)
        except Exception as e:
            LOG.warning("Failed to purge old bar partitions: %s", e)
