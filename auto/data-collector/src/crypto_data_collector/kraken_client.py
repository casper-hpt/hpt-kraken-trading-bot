from __future__ import annotations

import io
import logging
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import pandas as pd
import requests


LOG = logging.getLogger(__name__)


class KrakenAPIError(RuntimeError):
    pass


# Symbols where Kraken's pair name differs from {SYMBOL}USD.
_SYMBOL_TO_PAIR: dict[str, str] = {
    "BTC": "XBTUSD",
    "DOGE": "XDGUSD",
}

# Possible filename prefixes in Kraken bulk OHLCVT ZIPs for special symbols.
_SYMBOL_TO_CSV_PREFIXES: dict[str, list[str]] = {
    "BTC": ["XBTUSD", "XXBTZUSD"],
    "DOGE": ["XDGUSD", "XXDGZUSD"],
    "ETH": ["ETHUSD", "XETHZUSD"],
    "XRP": ["XRPUSD", "XXRPZUSD"],
    "LTC": ["LTCUSD", "XLTCZUSD"],
    "XLM": ["XLMUSD", "XXLMZUSD"],
    "ETC": ["ETCUSD", "XETCZUSD"],
    "XMR": ["XMRUSD", "XXMRZUSD"],
    "ZEC": ["ZECUSD", "XZECZUSD"],
}

_BULK_CSV_COLUMNS = ["ts", "open", "high", "low", "close", "volume", "trades"]


def symbol_to_pair(symbol: str) -> str:
    """Map a watchlist symbol (e.g. 'BTC') to its Kraken pair name (e.g. 'XBTUSD')."""
    return _SYMBOL_TO_PAIR.get(symbol.upper(), f"{symbol.upper()}USD")


@dataclass(frozen=True)
class KrakenClient:
    base_url: str = "https://api.kraken.com/0/public"
    timeout_s: int = 30
    max_retries: int = 5
    backoff_s: float = 2.0

    # Class-level rate limiting (~1 req/sec for Kraken public API)
    _last_request_time: ClassVar[float] = 0.0

    def _wait_for_rate_limit(self) -> None:
        elapsed = time.time() - KrakenClient._last_request_time
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        KrakenClient._last_request_time = time.time()

    def fetch_ohlc(
        self,
        pair: str,
        interval: int = 15,
        since: int | None = None,
    ) -> pd.DataFrame:
        """Fetch OHLC bars from Kraken REST API.

        Args:
            pair: Kraken pair name (e.g. 'XBTUSD', 'ETHUSD').
            interval: Bar interval in minutes (default 15).
            since: Unix timestamp; return bars after this time.

        Returns:
            DataFrame with columns: ts, open, high, low, close, volume
        """
        params: dict[str, Any] = {"pair": pair, "interval": interval}
        if since is not None:
            params["since"] = since

        url = f"{self.base_url}/OHLC"
        empty = pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

        last_err: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                self._wait_for_rate_limit()
                r = requests.get(url, params=params, timeout=self.timeout_s)
                r.raise_for_status()
                payload: dict[str, Any] = r.json()

                errors = payload.get("error", [])
                if errors:
                    raise KrakenAPIError("; ".join(str(e) for e in errors))

                result = payload.get("result", {})
                # The result dict has the pair key (which may differ from input)
                # and a 'last' key.  Find the pair data.
                pair_data: list | None = None
                for k, v in result.items():
                    if k != "last" and isinstance(v, list):
                        pair_data = v
                        break

                if not pair_data:
                    return empty

                rows = []
                for entry in pair_data:
                    # [time, open, high, low, close, vwap, volume, count]
                    ts = pd.to_datetime(int(entry[0]), unit="s", utc=True).tz_localize(None)
                    rows.append({
                        "ts": ts,
                        "open": float(entry[1]),
                        "high": float(entry[2]),
                        "low": float(entry[3]),
                        "close": float(entry[4]),
                        "volume": float(entry[6]),
                    })

                df = pd.DataFrame(rows)
                if not df.empty:
                    df.sort_values("ts", inplace=True)
                    df.reset_index(drop=True, inplace=True)
                return df

            except KrakenAPIError:
                raise
            except Exception as e:
                last_err = e
                sleep_s = self.backoff_s * attempt
                LOG.warning(
                    "Kraken OHLC fetch failed for %s (attempt %s/%s): %s. Sleeping %.1fs",
                    pair, attempt, self.max_retries, e, sleep_s,
                )
                time.sleep(sleep_s)

        raise KrakenAPIError(f"Failed to fetch OHLC for {pair} after {self.max_retries} retries: {last_err}")

    def fetch_trades_window(
        self,
        pair: str,
        start_ts_s: int,
        end_ts_s: int,
        sleep_s: float = 0.3,
        max_requests: int = 10_000,
    ) -> list[tuple[float, float, float]]:
        """Fetch trades in [start_ts_s, end_ts_s) via Kraken Trades endpoint.

        Uses nanosecond ``since`` and paginates using the returned ``last`` token.

        Args:
            pair: Kraken pair name (e.g. 'XBTUSD').
            start_ts_s: Window start as Unix seconds.
            end_ts_s: Window end as Unix seconds.
            sleep_s: Sleep between requests to respect rate limits.
            max_requests: Safety cap on pagination loops.

        Returns:
            List of (timestamp_s, price, volume) tuples.
        """
        since_ns = int(start_ts_s) * 1_000_000_000
        end_s = float(end_ts_s)
        url = f"{self.base_url}/Trades"

        rows: list[tuple[float, float, float]] = []
        requests_made = 0

        while requests_made < max_requests:
            # --- retry loop for a single page ---
            last_err: Exception | None = None
            resp_data = None

            for attempt in range(1, self.max_retries + 1):
                try:
                    self._wait_for_rate_limit()
                    r = requests.get(
                        url,
                        params={"pair": pair, "since": str(since_ns)},
                        timeout=self.timeout_s,
                    )
                    r.raise_for_status()
                    resp_data = r.json()

                    errors = resp_data.get("error", [])
                    if errors:
                        raise KrakenAPIError("; ".join(str(e) for e in errors))
                    break
                except KrakenAPIError as e:
                    if "Too many requests" in str(e) and attempt < self.max_retries:
                        sleep_time = self.backoff_s * attempt * 5
                        LOG.warning(
                            "Kraken rate limited for %s (attempt %s/%s). Sleeping %.1fs",
                            pair, attempt, self.max_retries, sleep_time,
                        )
                        time.sleep(sleep_time)
                        last_err = e
                    else:
                        raise
                except Exception as e:
                    last_err = e
                    sleep_time = self.backoff_s * attempt
                    LOG.warning(
                        "Kraken Trades fetch failed for %s (attempt %s/%s): %s. Sleeping %.1fs",
                        pair, attempt, self.max_retries, e, sleep_time,
                    )
                    time.sleep(sleep_time)
            else:
                raise KrakenAPIError(
                    f"Failed to fetch Trades for {pair} after {self.max_retries} retries: {last_err}"
                )

            requests_made += 1
            result = resp_data["result"]

            # Find trades list (key is the full pair name, not "last")
            trades = None
            for k, v in result.items():
                if k != "last" and isinstance(v, list):
                    trades = v
                    break

            if not trades:
                break

            last = int(result["last"])  # next since (nanoseconds)

            # trades: [price, volume, time, side, ordertype, misc, ...]
            reached_end = False
            for entry in trades:
                t = float(entry[2])
                if t >= end_s:
                    reached_end = True
                    break
                rows.append((t, float(entry[0]), float(entry[1])))

            if reached_end:
                break

            # No progress — safety break
            if last == since_ns:
                break
            since_ns = last

            if requests_made % 100 == 0:
                LOG.info(
                    "  %s  trades progress: %d requests, %d trades so far",
                    pair, requests_made, len(rows),
                )

            if sleep_s > 0:
                time.sleep(sleep_s)

        LOG.info(
            "  %s  trades fetch complete: %d requests, %d trades total",
            pair, requests_made, len(rows),
        )
        return rows


def parse_bulk_csv(csv_data: str | bytes | io.IOBase) -> pd.DataFrame:
    """Parse a Kraken bulk OHLCVT CSV (no header).

    Columns: timestamp, open, high, low, close, volume, trades

    Returns:
        DataFrame with columns: ts, open, high, low, close, volume
    """
    df = pd.read_csv(
        io.StringIO(csv_data) if isinstance(csv_data, str) else io.BytesIO(csv_data) if isinstance(csv_data, bytes) else csv_data,
        header=None,
        names=_BULK_CSV_COLUMNS,
    )
    if df.empty:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

    df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="s", utc=True).dt.tz_localize(None)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    df.sort_values("ts", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df[["ts", "open", "high", "low", "close", "volume"]]


def find_csv_in_zip(zip_path: Path, symbol: str, interval: int = 15) -> str | None:
    """Find the CSV filename inside a Kraken bulk ZIP for a given symbol.

    Tries multiple naming conventions (e.g. XBTUSD_15.csv, XXBTZUSD_15.csv).

    Returns:
        The matching filename inside the ZIP, or None.
    """
    sym = symbol.upper()
    prefixes = _SYMBOL_TO_CSV_PREFIXES.get(sym, [f"{sym}USD"])
    candidates = [f"{p}_{interval}.csv" for p in prefixes]

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        # Build a lookup from basename -> full path inside the ZIP
        basename_map: dict[str, str] = {}
        for n in names:
            base = n.rsplit("/", 1)[-1] if "/" in n else n
            basename_map[base] = n
            basename_map[base.lower()] = n
        for candidate in candidates:
            # Try exact match on full path first
            if candidate in basename_map:
                return basename_map[candidate]
            # Case-insensitive fallback
            match = basename_map.get(candidate.lower())
            if match:
                return match
    return None


def trades_to_ohlcv_15m(trade_rows: list[tuple[float, float, float]]) -> pd.DataFrame:
    """Aggregate raw trades into 15-minute OHLCV bars.

    Args:
        trade_rows: List of (timestamp_s, price, volume) tuples.

    Returns:
        DataFrame with columns: ts, open, high, low, close, volume
    """
    empty = pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    if not trade_rows:
        return empty

    df = pd.DataFrame(trade_rows, columns=["ts_s", "price", "volume"])
    df["ts"] = pd.to_datetime(df["ts_s"], unit="s", utc=True)
    df = df.set_index("ts").sort_index()

    ohlc = df["price"].resample("15min").ohlc()
    vol = df["volume"].resample("15min").sum().rename("volume")

    out = pd.concat([ohlc, vol], axis=1)
    out = out.dropna(subset=["open"])  # Remove intervals with no trades
    out = out.reset_index()
    out["ts"] = out["ts"].dt.tz_localize(None)  # Match existing convention

    return out[["ts", "open", "high", "low", "close", "volume"]]


def read_csv_from_zip(zip_path: Path, csv_name: str) -> bytes:
    """Read a single CSV file from a ZIP archive."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        return zf.read(csv_name)
