#!/usr/bin/env python3
"""Download Kraken bulk OHLCVT data and seed QuestDB.

Usage:
    # Auto-download from Kraken's Google Drive and seed:
    python scripts/seed_cache.py

    # Use an already-downloaded ZIP:
    python scripts/seed_cache.py --zip /path/to/Kraken_OHLCVT.zip

    # Specify which coins (default: loads from crypto_watchlist.json):
    python scripts/seed_cache.py --coins BTC,ETH,SOL

    # Skip download, just verify existing data:
    python scripts/seed_cache.py --verify-only
"""

import argparse
import logging
import shutil
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from crypto_data_collector.config import Config
from crypto_data_collector.kraken_client import find_csv_in_zip, read_csv_from_zip, parse_bulk_csv
from crypto_data_collector.questdb_rest import QuestDBRest
from crypto_data_collector.questdb_schema import SchemaManager
from crypto_data_collector.questdb_writer import QuestDBWriter
from crypto_data_collector.watchlist import load_watchlist
from crypto_data_collector.main import _build_bar_frame

# Kraken publishes OHLCVT CSVs here (folder with quarterly ZIPs):
KRAKEN_GDRIVE_FOLDER = (
    "https://drive.google.com/drive/folders/1aoA6SKgPbS_p3pYStXUXFvmjqShJ2jv9"
)

DEFAULT_ZIP_PATH = PROJECT_ROOT / "data" / "Kraken_OHLCVT.zip"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("seed_cache")


def download_from_gdrive(dest: Path) -> bool:
    """Try to download the Kraken OHLCVT ZIP via gdown."""
    try:
        import gdown
    except ImportError:
        log.error(
            "gdown not installed. Install with: pip install gdown\n"
            "Or download manually from:\n  %s\n"
            "Then re-run with: python scripts/seed_cache.py --zip /path/to/file.zip",
            KRAKEN_GDRIVE_FOLDER,
        )
        return False

    log.info("Listing files in Kraken OHLCVT Google Drive folder ...")
    try:
        files = gdown.download_folder(
            url=KRAKEN_GDRIVE_FOLDER,
            quiet=True,
            skip_download=True,
        )
    except Exception:
        log.exception("Failed to list Google Drive folder")
        return False

    if not files:
        log.error("No files found in Google Drive folder")
        return False

    ohlcvt_files = [
        f for f in files
        if f.path.lower().endswith(".zip") and "ohlcvt" in f.path.lower()
    ]

    if not ohlcvt_files:
        ohlcvt_files = [f for f in files if f.path.lower().endswith(".zip")]

    if not ohlcvt_files:
        log.error(
            "No OHLCVT ZIP files found in folder. Download manually from:\n  %s",
            KRAKEN_GDRIVE_FOLDER,
        )
        return False

    target = ohlcvt_files[-1]
    log.info("Downloading: %s", target.path)

    dest.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            output = gdown.download(id=target.id, output=str(Path(tmpdir) / "download.zip"))
            if output and Path(output).exists():
                shutil.move(output, str(dest))
                log.info("Downloaded to %s", dest)
                return True
        except Exception:
            log.exception("Download failed")

    return False


def parse_coins(raw: str) -> list[str]:
    return [c.strip().upper() for c in raw.split(",") if c.strip()]


def _print_db_stats(writer: QuestDBWriter, symbols: list[str]) -> None:
    """Print per-symbol bar counts from QuestDB."""
    log.info("")
    log.info("=== QUESTDB STATS ===")
    total = 0
    for sym in sorted(symbols):
        sanitized = "".join(c for c in sym if c.isalnum() or c in ".-")
        try:
            data = writer.rest.exec(
                f"SELECT count() cnt, min(ts) min_ts, max(ts) max_ts "
                f"FROM crypto_bars_15m WHERE symbol = '{sanitized}';"
            )
            ds = data.get("dataset", [[]])
            row = ds[0] if ds else [0, None, None]
            cnt = row[0] or 0
            total += cnt
            if cnt > 0:
                log.info("  %-10s  %7d bars   %s -> %s", sym, cnt, row[1], row[2])
            else:
                log.info("  %-10s  NO DATA", sym)
        except Exception as e:
            log.warning("  %-10s  ERROR: %s", sym, e)
    log.info("  %-10s  %7d bars total", "TOTAL", total)
    log.info("=====================")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed QuestDB from Kraken bulk OHLCVT data")
    parser.add_argument(
        "--zip", type=str, default=None,
        help="Path to an already-downloaded Kraken OHLCVT ZIP file",
    )
    parser.add_argument(
        "--coins", type=str, default=None,
        help="Comma-separated coin list (default: loads from watchlist)",
    )
    parser.add_argument(
        "--watchlist", type=str, default=str(PROJECT_ROOT / "crypto_watchlist.json"),
        help="Path to watchlist JSON",
    )
    parser.add_argument(
        "--verify-only", action="store_true",
        help="Just print DB stats, don't download or seed",
    )
    parser.add_argument(
        "--dest", type=str, default=None,
        help=f"Where to save the ZIP (default: {DEFAULT_ZIP_PATH})",
    )
    args = parser.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    cfg = Config.from_env()

    # Resolve symbol list
    if args.coins:
        symbols = parse_coins(args.coins)
    else:
        watchlist_items = load_watchlist(args.watchlist)
        symbols = [item.symbol for item in watchlist_items]
    log.info("Symbol list: %d symbols - %s", len(symbols), symbols)

    rest = QuestDBRest(cfg.questdb_exec_url)
    SchemaManager(rest).ensure_schema()
    writer = QuestDBWriter(cfg.questdb_ilp_conf, rest)

    if args.verify_only:
        _print_db_stats(writer, symbols)
        return

    # Resolve ZIP path
    zip_path = Path(args.zip) if args.zip else DEFAULT_ZIP_PATH

    if args.zip:
        src_zip = Path(args.zip)
        if not src_zip.exists():
            log.error("ZIP file not found: %s", src_zip)
            sys.exit(1)
        dest = Path(args.dest) if args.dest else DEFAULT_ZIP_PATH
        if src_zip.resolve() != dest.resolve():
            log.info("Copying %s -> %s", src_zip, dest)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_zip, dest)
            zip_path = dest

    if not zip_path.exists():
        log.info("No ZIP found at %s - attempting download ...", zip_path)
        dest = Path(args.dest) if args.dest else DEFAULT_ZIP_PATH
        if not download_from_gdrive(dest):
            log.error(
                "Could not auto-download. Please download manually:\n"
                "  1. Go to: %s\n"
                "  2. Download the OHLCVT ZIP file\n"
                "  3. Re-run: python scripts/seed_cache.py --zip /path/to/downloaded.zip",
                KRAKEN_GDRIVE_FOLDER,
            )
            sys.exit(1)
        zip_path = dest

    # Seed QuestDB from the ZIP
    log.info("Seeding QuestDB from %s ...", zip_path)
    total_inserted = 0

    for sym in symbols:
        csv_name = find_csv_in_zip(zip_path, sym, interval=15)
        if csv_name is None:
            log.warning("No CSV found in ZIP for symbol %s, skipping", sym)
            continue

        log.info("Loading %s from %s ...", sym, csv_name)
        try:
            csv_bytes = read_csv_from_zip(zip_path, csv_name)
            bars = parse_bulk_csv(csv_bytes)
            log.info("  Parsed %d bars for %s (%s -> %s)",
                     len(bars), sym,
                     bars["ts"].min() if not bars.empty else "N/A",
                     bars["ts"].max() if not bars.empty else "N/A")

            # Write in chunks
            chunk_size = 50_000
            sym_inserted = 0
            for start in range(0, len(bars), chunk_size):
                chunk = bars.iloc[start:start + chunk_size]
                df = _build_bar_frame(chunk, sym)
                inserted = writer.write_bars(df)
                sym_inserted += inserted

            total_inserted += sym_inserted
            log.info("  Symbol=%s inserted=%d", sym, sym_inserted)
        except Exception as e:
            log.exception("Failed to seed %s: %s", sym, e)
            continue

    log.info("Seed complete. total_inserted=%d", total_inserted)

    # Print stats
    _print_db_stats(writer, symbols)
    log.info("Done!")


if __name__ == "__main__":
    main()
