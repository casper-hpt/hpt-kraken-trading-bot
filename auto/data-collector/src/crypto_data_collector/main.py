from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from pathlib import Path

import pandas as pd

from .config import Config
from .logging_setup import setup_logging
from .metrics import (
    BARS_INSERTED_TOTAL, CYCLES_TOTAL, CYCLE_DURATION,
    API_ERRORS_TOTAL, WATCHLIST_SIZE, start_metrics_server,
)
from .watchlist import load_watchlist
from .kraken_client import KrakenClient, symbol_to_pair, find_csv_in_zip, read_csv_from_zip, parse_bulk_csv, trades_to_ohlcv_15m
from .questdb_rest import QuestDBRest
from .questdb_schema import SchemaManager
from .questdb_writer import QuestDBWriter
from .scheduler import Scheduler

LOG = logging.getLogger(__name__)

_shutdown_requested = False


def _signal_handler(signum: int, frame: object) -> None:
    global _shutdown_requested
    sig_name = signal.Signals(signum).name
    LOG.info("Received %s, initiating graceful shutdown...", sig_name)
    _shutdown_requested = True


def _maybe_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore[import-untyped]
        load_dotenv()
    except ImportError:
        pass


def _build_bar_frame(bars: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if bars.empty:
        return bars

    now = pd.Timestamp.utcnow().tz_localize(None)

    df = bars.copy()
    df["symbol"] = symbol
    df["source"] = "kraken"
    df["ingested_at"] = now

    df["symbol"] = pd.Categorical(df["symbol"].astype(object))
    df["source"] = pd.Categorical(df["source"].astype(object))
    df["ts"] = pd.to_datetime(df["ts"]).dt.tz_localize(None)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"]).dt.tz_localize(None)

    return df[["ts", "symbol", "open", "high", "low", "close", "volume", "source", "ingested_at"]]


def run_cycle(cfg: Config, watchlist_path: str) -> int:
    """Fetch latest 15-min bars from Kraken for all watchlist symbols."""
    cycle_start = time.monotonic()

    watchlist_items = load_watchlist(watchlist_path)
    if not watchlist_items:
        LOG.warning("No symbols found in watchlist: %s", watchlist_path)
        return 0

    symbols = [item.symbol for item in watchlist_items]
    WATCHLIST_SIZE.set(len(symbols))

    rest = QuestDBRest(cfg.questdb_exec_url)
    SchemaManager(rest).ensure_schema()
    writer = QuestDBWriter(cfg.questdb_ilp_conf, rest)

    writer.write_watchlist(watchlist_items)

    kraken = KrakenClient()

    total_inserted = 0
    for sym in symbols:
        if _shutdown_requested:
            LOG.info("Shutdown requested, stopping symbol iteration early")
            break
        try:
            last_ts = writer.get_last_ts(sym)
            since: int | None = None
            if last_ts is not None:
                since = int(last_ts.timestamp())

            pair = symbol_to_pair(sym)
            bars = kraken.fetch_ohlc(pair, interval=15, since=since)
            if last_ts is not None and not bars.empty:
                bars = bars[bars["ts"] > last_ts]

            df = _build_bar_frame(bars, sym)
            inserted = writer.write_bars(df)
            total_inserted += inserted
            LOG.info("Symbol=%s pair=%s inserted=%s last_ts=%s", sym, pair, inserted, last_ts)
        except Exception as e:
            API_ERRORS_TOTAL.inc()
            LOG.exception("Failed to ingest %s: %s", sym, e)
            continue

    BARS_INSERTED_TOTAL.inc(total_inserted)
    CYCLES_TOTAL.inc()
    CYCLE_DURATION.set(time.monotonic() - cycle_start)
    return total_inserted


def cmd_bootstrap(cfg: Config) -> int:
    rest = QuestDBRest(cfg.questdb_exec_url)
    SchemaManager(rest).ensure_schema()
    return 0


def cmd_seed(cfg: Config, watchlist_path: str, zip_path: str) -> int:
    """Load bulk Kraken OHLCVT CSV data from a ZIP into QuestDB."""
    global _shutdown_requested

    zp = Path(zip_path)
    if not zp.exists():
        LOG.error("ZIP file not found: %s", zp)
        return 1

    watchlist_items = load_watchlist(watchlist_path)
    if not watchlist_items:
        LOG.warning("No symbols found in watchlist: %s", watchlist_path)
        return 0

    symbols = [item.symbol for item in watchlist_items]

    rest = QuestDBRest(cfg.questdb_exec_url)
    SchemaManager(rest).ensure_schema()
    writer = QuestDBWriter(cfg.questdb_ilp_conf, rest)

    writer.write_watchlist(watchlist_items)

    total_inserted = 0
    for sym in symbols:
        if _shutdown_requested:
            LOG.info("Shutdown requested, stopping seed early")
            break

        csv_name = find_csv_in_zip(zp, sym, interval=15)
        if csv_name is None:
            LOG.warning("No CSV found in ZIP for symbol %s, skipping", sym)
            continue

        LOG.info("Seeding %s from %s ...", sym, csv_name)
        try:
            csv_bytes = read_csv_from_zip(zp, csv_name)
            bars = parse_bulk_csv(csv_bytes)
            LOG.info("  Parsed %d bars for %s (%s -> %s)",
                     len(bars), sym,
                     bars["ts"].min() if not bars.empty else "N/A",
                     bars["ts"].max() if not bars.empty else "N/A")

            # Write in chunks to avoid memory issues with large CSVs
            chunk_size = 50_000
            sym_inserted = 0
            for start in range(0, len(bars), chunk_size):
                if _shutdown_requested:
                    break
                chunk = bars.iloc[start:start + chunk_size]
                df = _build_bar_frame(chunk, sym)
                inserted = writer.write_bars(df)
                sym_inserted += inserted

            total_inserted += sym_inserted
            LOG.info("  Symbol=%s total_inserted=%d", sym, sym_inserted)
        except Exception as e:
            LOG.exception("Failed to seed %s: %s", sym, e)
            continue

    LOG.info("Seed complete. total_inserted=%d", total_inserted)
    return 0


def _backfill_window(
    sym: str,
    pair: str,
    kraken: KrakenClient,
    writer: QuestDBWriter,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    first_ts_db: pd.Timestamp | None,
    last_ts_db: pd.Timestamp | None,
    sleep_s: float = 0.3,
    chunk_days: int = 7,
) -> int:
    """Fetch trades for a time window in chunks and write bars incrementally.

    Splits the window into sub-windows of ``chunk_days`` so that progress
    is persisted to QuestDB after each chunk.  If a chunk fails (e.g. rate
    limit), the bars written so far are kept and the next chunk is attempted
    after a cooldown.
    """
    start_s = int(start_ts.timestamp())
    end_s = int(end_ts.timestamp())

    if start_s >= end_s:
        return 0

    gap_days = (end_s - start_s) / 86400.0
    LOG.info("  %s  fetching trades for %.1f day window in %d-day chunks: %s -> %s",
             sym, gap_days, chunk_days, start_ts, end_ts)

    chunk_seconds = chunk_days * 86400
    total_inserted = 0
    chunk_start = start_s

    while chunk_start < end_s:
        if _shutdown_requested:
            LOG.info("  %s  shutdown requested, stopping backfill window early", sym)
            break

        chunk_end = min(chunk_start + chunk_seconds, end_s)
        chunk_start_ts = pd.Timestamp(chunk_start, unit="s")
        chunk_end_ts = pd.Timestamp(chunk_end, unit="s")
        chunk_span = (chunk_end - chunk_start) / 86400.0
        LOG.info("  %s  chunk: %.1f days (%s -> %s)", sym, chunk_span, chunk_start_ts, chunk_end_ts)

        try:
            trade_rows = kraken.fetch_trades_window(pair, chunk_start, chunk_end, sleep_s=sleep_s)
            LOG.info("  %s  fetched %d trades for chunk", sym, len(trade_rows))

            if trade_rows:
                bars = trades_to_ohlcv_15m(trade_rows)
                if not bars.empty:
                    if first_ts_db is not None and last_ts_db is not None:
                        bars = bars[(bars["ts"] < first_ts_db) | (bars["ts"] > last_ts_db)]
                    elif last_ts_db is not None:
                        bars = bars[bars["ts"] > last_ts_db]

                    if not bars.empty:
                        bar_chunk_size = 50_000
                        chunk_inserted = 0
                        for i in range(0, len(bars), bar_chunk_size):
                            df = _build_bar_frame(bars.iloc[i:i + bar_chunk_size], sym)
                            chunk_inserted += writer.write_bars(df)
                        total_inserted += chunk_inserted
                        LOG.info("  %s  inserted %d bars for chunk (%s -> %s)",
                                 sym, chunk_inserted, bars["ts"].min(), bars["ts"].max())
        except Exception as e:
            LOG.warning("  %s  chunk failed (%s -> %s): %s. Keeping %d bars written so far.",
                        sym, chunk_start_ts, chunk_end_ts, e, total_inserted)

        chunk_start = chunk_end

        if chunk_start < end_s:
            time.sleep(5)

    LOG.info("  %s  window complete: %d bars inserted total", sym, total_inserted)
    return total_inserted


def _backfill_symbol(
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

    Fills both directions:
      - Backwards from earliest DB bar to 90-days-ago target
      - Forwards from latest DB bar to now
    """
    pair = symbol_to_pair(sym)
    now = pd.Timestamp.utcnow().tz_localize(None)
    target_start = start_override if start_override is not None else (now - pd.Timedelta(days=90))
    if end_ts is None:
        end_ts = now

    last_ts_db = writer.get_last_ts(sym)
    first_ts_db = writer.get_first_ts(sym) if last_ts_db is not None else None

    total_inserted = 0

    if first_ts_db is None:
        # Empty DB — backfill the entire window
        LOG.info("  %s  no data in DB, backfilling from %s", sym, target_start)
        total_inserted += _backfill_window(
            sym, pair, kraken, writer, target_start, end_ts, None, None, sleep_s,
        )
    else:
        # Fill backwards if existing data doesn't reach target_start
        if first_ts_db > target_start:
            LOG.info("  %s  filling backwards: %s -> %s", sym, target_start, first_ts_db)
            total_inserted += _backfill_window(
                sym, pair, kraken, writer, target_start, first_ts_db, first_ts_db, last_ts_db, sleep_s,
            )
        else:
            LOG.info("  %s  historical data already reaches %s (target: %s)", sym, first_ts_db, target_start)

        # Fill forwards from latest bar to now
        if last_ts_db < end_ts:
            LOG.info("  %s  filling forwards: %s -> %s", sym, last_ts_db, end_ts)
            total_inserted += _backfill_window(
                sym, pair, kraken, writer, last_ts_db, end_ts, first_ts_db, last_ts_db, sleep_s,
            )
        else:
            LOG.info("  %s  already up to date.", sym)

    return total_inserted


def cmd_backfill(cfg: Config, watchlist_path: str, from_ts: str | None = None) -> int:
    """Backfill gap between last seeded bar and now via Kraken API."""
    watchlist_items = load_watchlist(watchlist_path)
    if not watchlist_items:
        LOG.warning("No symbols found in watchlist: %s", watchlist_path)
        return 0

    symbols = [item.symbol for item in watchlist_items]

    start_override: pd.Timestamp | None = None
    if from_ts is not None:
        start_override = pd.Timestamp(from_ts).tz_localize(None)
        LOG.info("Using explicit start override: %s", start_override)

    rest = QuestDBRest(cfg.questdb_exec_url)
    SchemaManager(rest).ensure_schema()
    writer = QuestDBWriter(cfg.questdb_ilp_conf, rest)

    kraken = KrakenClient()
    grand_total = 0

    LOG.info("Backfill starting for %d symbols: %s", len(symbols), symbols)
    for sym in symbols:
        if _shutdown_requested:
            LOG.info("Shutdown requested, stopping backfill early")
            break
        LOG.info("Backfilling %s ...", sym)
        try:
            inserted = _backfill_symbol(sym, kraken, writer, start_override=start_override)
            grand_total += inserted
            if inserted > 0:
                LOG.info("  Waiting 5s for QuestDB to settle...")
                time.sleep(5)
        except Exception as e:
            LOG.exception("Failed to backfill %s: %s", sym, e)
            continue

    LOG.info("Backfill done. Total inserted across all symbols: %d", grand_total)
    return 0


def cmd_run_once(cfg: Config, watchlist: str) -> int:
    inserted = run_cycle(cfg, watchlist)
    LOG.info("Cycle complete. total_inserted=%s", inserted)
    return 0


def cmd_serve(cfg: Config, watchlist: str) -> int:
    """Run ingestion loop until shutdown signal received."""
    global _shutdown_requested

    start_metrics_server(cfg.prometheus_port)
    LOG.info("Prometheus metrics available on :%d/metrics", cfg.prometheus_port)

    sched = Scheduler(interval_minutes=cfg.ingest_interval_minutes, align_to_boundary=True)
    LOG.info("Starting serve loop. interval_minutes=%s watchlist=%s", cfg.ingest_interval_minutes, watchlist)

    while not _shutdown_requested:
        try:
            inserted = run_cycle(cfg, watchlist)
            LOG.info("Cycle complete. total_inserted=%s. Sleeping until next tick...", inserted)
        except Exception as e:
            LOG.exception("Cycle failed, will retry next tick: %s", e)
        if not _shutdown_requested:
            sched.sleep_until_next_tick()

    LOG.info("Serve loop terminated gracefully")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="crypto-data-collector", description="Kraken OHLCV -> QuestDB ingestor")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("bootstrap", help="Create/ensure QuestDB schema exists")

    p_seed = sub.add_parser("seed", help="Load bulk Kraken OHLCVT CSV data from ZIP into QuestDB")
    p_seed.add_argument("--watchlist", default="crypto_watchlist.json", help="Path to watchlist JSON")
    p_seed.add_argument("--zip", required=True, help="Path to Kraken OHLCVT ZIP file")

    p_bf = sub.add_parser("backfill", help="Backfill gap between seed data and now via Kraken API")
    p_bf.add_argument("--watchlist", default="crypto_watchlist.json", help="Path to watchlist JSON")
    p_bf.add_argument("--from", dest="from_ts", default=None, help="ISO-8601 start timestamp (default: 90 days ago if DB is empty)")

    p_once = sub.add_parser("run-once", help="Run one ingestion cycle from Kraken API")
    p_once.add_argument("--watchlist", default="crypto_watchlist.json", help="Path to watchlist JSON")

    p_srv = sub.add_parser("serve", help="Run ingestion loop (15-min cadence)")
    p_srv.add_argument("--watchlist", default="crypto_watchlist.json", help="Path to watchlist JSON")

    return p


def main(argv: list[str] | None = None) -> None:
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    _maybe_load_dotenv()
    cfg = Config.from_env()
    setup_logging(cfg.log_level)

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "bootstrap":
        rc = cmd_bootstrap(cfg)
    elif args.cmd == "seed":
        rc = cmd_seed(cfg, args.watchlist, args.zip)
    elif args.cmd == "backfill":
        rc = cmd_backfill(cfg, args.watchlist, from_ts=args.from_ts)
    elif args.cmd == "run-once":
        rc = cmd_run_once(cfg, args.watchlist)
    elif args.cmd == "serve":
        rc = cmd_serve(cfg, args.watchlist)
    else:
        raise SystemExit(2)

    raise SystemExit(rc)


if __name__ == "__main__":
    main()
