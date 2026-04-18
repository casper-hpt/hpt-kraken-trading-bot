#!/usr/bin/env python3
"""Backfill OHLCV bars via Kraken Trades API.

Fetches raw trades from Kraken's /0/public/Trades endpoint (which serves full
history, unlike /0/public/OHLC which is limited to ~720 bars) and aggregates
them into 15-minute OHLCV candles.

This script:
- Starts from last_ts in QuestDB (or optional --from)
- Fetches all trades in the gap window using nanosecond-based pagination
- Aggregates trades into 15m candles and inserts into QuestDB

Usage:
    python scripts/backfill.py
    python scripts/backfill.py --coins BTC,ETH
    python scripts/backfill.py --from 2026-01-01T00:00:00Z --to 2026-02-01T00:00:00Z
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from crypto_data_collector.config import Config
from crypto_data_collector.kraken_client import KrakenClient, symbol_to_pair, trades_to_ohlcv_15m
from crypto_data_collector.questdb_rest import QuestDBRest
from crypto_data_collector.questdb_schema import SchemaManager
from crypto_data_collector.questdb_writer import QuestDBWriter
from crypto_data_collector.watchlist import load_watchlist
from crypto_data_collector.main import _build_bar_frame

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backfill")

INTERVAL_MINUTES = 15


def _to_utc(ts: pd.Timestamp | None) -> pd.Timestamp | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _ensure_ts_utc(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure df['ts'] exists and is tz-aware UTC timestamps."""
    if df.empty:
        return df
    if "ts" not in df.columns:
        raise KeyError("Expected bars DataFrame to contain a 'ts' column.")
    # If already datetime64[ns, UTC], this is basically a no-op.
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def backfill_symbol(
    sym: str,
    kraken: KrakenClient,
    writer: QuestDBWriter,
    start_override: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
    sleep_s: float = 0.3,
) -> int:
    """Backfill 15m bars by fetching trades and aggregating into candles.

    Uses Kraken's /0/public/Trades endpoint (which serves full history)
    instead of /0/public/OHLC (limited to ~720 bars / ~7.5 days at 15m).
    """
    pair = symbol_to_pair(sym)
    last_ts_db = _to_utc(writer.get_last_ts(sym))

    cursor = _to_utc(start_override) if start_override is not None else last_ts_db
    end_ts = _to_utc(end_ts) if end_ts is not None else pd.Timestamp.now(tz="UTC")

    if cursor is None:
        log.warning("  %s  no start time available (no data in DB and no --from). Skipping.", sym)
        return 0

    start_s = int(cursor.timestamp())
    end_s = int(end_ts.timestamp())

    if start_s >= end_s:
        log.info("  %s  already up to date.", sym)
        return 0

    gap_days = (end_s - start_s) / 86400.0
    log.info("  %s  fetching trades for %.1f day window: %s -> %s", sym, gap_days, cursor, end_ts)

    trade_rows = kraken.fetch_trades_window(pair, start_s, end_s, sleep_s=sleep_s)
    log.info("  %s  fetched %d trades", sym, len(trade_rows))

    if not trade_rows:
        log.info("  %s  no trades found in window.", sym)
        return 0

    bars = trades_to_ohlcv_15m(trade_rows)
    bars = _ensure_ts_utc(bars)

    if bars.empty:
        log.info("  %s  no complete bars from trades.", sym)
        return 0

    # Filter out bars already in DB
    if last_ts_db is not None:
        bars = bars[bars["ts"] > last_ts_db]

    if bars.empty:
        log.info("  %s  all bars already in DB.", sym)
        return 0

    # Write in chunks to manage memory
    chunk_size = 50_000
    total_inserted = 0
    for start in range(0, len(bars), chunk_size):
        chunk = bars.iloc[start:start + chunk_size]
        df = _build_bar_frame(chunk, sym)
        total_inserted += writer.write_bars(df)

    log.info("  %s  inserted %d bars (%s -> %s)",
             sym, total_inserted, bars["ts"].min(), bars["ts"].max())
    return total_inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill gap between seed data and now")
    parser.add_argument(
        "--coins", type=str, default=None,
        help="Comma-separated coin list (default: loads from watchlist)",
    )
    parser.add_argument(
        "--watchlist", type=str,
        default=str(PROJECT_ROOT / "crypto_watchlist.json"),
        help="Path to watchlist JSON",
    )
    parser.add_argument(
        "--from", dest="from_ts", type=str, default=None,
        help="Optional start timestamp (ISO8601), e.g. 2026-01-01T00:00:00Z. "
             "If omitted, starts from last_ts in DB per symbol.",
    )
    parser.add_argument(
        "--to", dest="to_ts", type=str, default=None,
        help="Optional end timestamp (ISO8601), e.g. 2026-02-01T00:00:00Z. "
             "If omitted, ends at now (UTC).",
    )
    args = parser.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    cfg = Config.from_env()

    if args.coins:
        symbols = [c.strip().upper() for c in args.coins.split(",") if c.strip()]
    else:
        watchlist_items = load_watchlist(args.watchlist)
        symbols = [item.symbol for item in watchlist_items]

    start_override = pd.Timestamp(args.from_ts, tz="UTC") if args.from_ts else None
    end_ts = pd.Timestamp(args.to_ts, tz="UTC") if args.to_ts else None

    log.info("Backfill starting for %d symbols: %s", len(symbols), symbols)
    if start_override:
        log.info("Using --from: %s", start_override)
    if end_ts:
        log.info("Using --to:   %s", end_ts)

    rest = QuestDBRest(cfg.questdb_exec_url)
    SchemaManager(rest).ensure_schema()
    writer = QuestDBWriter(cfg.questdb_ilp_conf, rest)

    kraken = KrakenClient()
    grand_total = 0

    for sym in symbols:
        log.info("Backfilling %s ...", sym)
        inserted = backfill_symbol(
            sym=sym,
            kraken=kraken,
            writer=writer,
            start_override=start_override,
            end_ts=end_ts,
        )
        grand_total += inserted
        if inserted > 0:
            log.info("  Waiting 5s for QuestDB to settle...")
            time.sleep(5)

    log.info("Backfill done. Total inserted across all symbols: %d", grand_total)


if __name__ == "__main__":
    main()
