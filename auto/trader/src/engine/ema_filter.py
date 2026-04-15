# src/engine/ema_filter.py
"""
EMA trend filter for the momentum strategy.

Loads per-symbol EMA parameters from a JSON file and computes
the trend_ok flag (fast_ema > slow_ema) for filtering buy candidates.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class EMAPair:
    """Per-symbol EMA parameters."""
    fast: int
    slow: int


# Default path for EMA parameters JSON
DEFAULT_EMA_PARAMS_PATH = Path(__file__).parent.parent / "data" / "ema_params.json"


def load_ema_params(
    path: Optional[str | Path] = None,
    log: Optional[logging.Logger] = None,
) -> dict[str, EMAPair]:
    """
    Load per-symbol EMA parameters from JSON file.

    Args:
        path: Path to ema_params.json. Defaults to src/data/ema_params.json
        log: Optional logger

    Returns:
        Dict mapping symbol -> EMAPair

    JSON format:
    {
        "AAPL": {"fast_ema": 50, "slow_ema": 200},
        "MSFT": {"fast_ema": 100, "slow_ema": 400},
        ...
    }
    """
    if path is None:
        path = os.getenv("EMA_PARAMS_PATH", str(DEFAULT_EMA_PARAMS_PATH))

    path = Path(path)

    if not path.exists():
        if log:
            log.warning("EMA params file not found: %s", path)
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        ema_map: dict[str, EMAPair] = {}
        for symbol, params in data.items():
            fast = params.get("fast_ema")
            slow = params.get("slow_ema")
            if fast is not None and slow is not None:
                ema_map[symbol] = EMAPair(fast=int(fast), slow=int(slow))

        if log:
            log.info("Loaded EMA params for %d symbols from %s", len(ema_map), path)

        return ema_map

    except Exception as e:
        if log:
            log.warning("Failed to load EMA params from %s: %s", path, e)
        return {}


def save_ema_params(
    ema_map: dict[str, EMAPair],
    path: Optional[str | Path] = None,
    log: Optional[logging.Logger] = None,
) -> None:
    """
    Save per-symbol EMA parameters to JSON file.

    Args:
        ema_map: Dict mapping symbol -> EMAPair
        path: Path to save JSON. Defaults to src/data/ema_params.json
        log: Optional logger
    """
    if path is None:
        path = os.getenv("EMA_PARAMS_PATH", str(DEFAULT_EMA_PARAMS_PATH))

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    data = {
        symbol: {"fast_ema": pair.fast, "slow_ema": pair.slow}
        for symbol, pair in ema_map.items()
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)

    if log:
        log.info("Saved EMA params for %d symbols to %s", len(ema_map), path)


def apply_ema_trend_filter(
    df: pd.DataFrame,
    ema_map: dict[str, EMAPair],
) -> pd.DataFrame:
    """
    Apply per-symbol EMAs and compute trend_ok + bearish crossover sell flag.

    Adds:
      - fast_ema
      - slow_ema
      - trend_ok: fast_ema > slow_ema
      - sell_on_ema_cross: True when fast crosses BELOW slow on this bar
        (prev_fast >= prev_slow) AND (fast < slow)
    """
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values(["symbol", "ts"])

    df["fast_ema"] = np.nan
    df["slow_ema"] = np.nan

    for sym, group_idx in df.groupby("symbol", sort=False).groups.items():
        if sym not in ema_map:
            continue

        pair = ema_map[sym]
        close = df.loc[group_idx, "close"].astype(float)

        df.loc[group_idx, "fast_ema"] = close.ewm(span=pair.fast, adjust=False).mean().to_numpy()
        df.loc[group_idx, "slow_ema"] = close.ewm(span=pair.slow, adjust=False).mean().to_numpy()

    df["trend_ok"] = (df["fast_ema"] > df["slow_ema"]).fillna(False)

    # --- NEW: bearish crossover sell signal (fast crosses BELOW slow) ---
    prev_fast = df.groupby("symbol", sort=False)["fast_ema"].shift(1)
    prev_slow = df.groupby("symbol", sort=False)["slow_ema"].shift(1)
    df["sell_on_ema_cross"] = ((prev_fast >= prev_slow) & (df["fast_ema"] < df["slow_ema"])).fillna(False)

    return df


def get_latest_trend_status(
    df: pd.DataFrame,
    ema_map: dict[str, EMAPair],
) -> dict[str, bool]:
    """
    Compute EMA trend filter and return only the latest trend_ok per symbol.

    Args:
        df: DataFrame with columns [ts, symbol, close]
        ema_map: Dict mapping symbol -> EMAPair

    Returns:
        Dict mapping symbol -> trend_ok at latest timestamp
    """
    filtered = apply_ema_trend_filter(df, ema_map)

    if filtered.empty:
        return {}

    latest_ts = filtered["ts"].max()
    latest = filtered[filtered["ts"] == latest_ts]

    return dict(zip(latest["symbol"], latest["trend_ok"]))
