"""EMA parameter refit via grid search.

For each symbol, tries every valid (fast, slow) pair from FAST_LIST × SLOW_LIST
and picks the pair that maximises Sharpe of 1-bar forward returns when
trend_ok=True.  This directly optimises the metric the strategy cares about:
are we in an uptrend when we're allowed to buy?
"""

from __future__ import annotations

import logging
import math
from typing import Optional

import pandas as pd

from src.config import FAST_LIST, SLOW_LIST
from src.engine.ema_filter import EMAPair, load_ema_params, save_ema_params
from src.market.questdb_client import QuestDBClient


def _sharpe(returns: pd.Series) -> float:
    n = len(returns.dropna())
    if n < 50:
        return -math.inf
    sigma = returns.std()
    if sigma < 1e-10:
        return -math.inf
    return float(returns.mean() / sigma)


def _best_pair(
    close: pd.Series,
    fast_list: tuple[int, ...],
    slow_list: tuple[int, ...],
    min_ratio: float,
    min_trend_samples: int,
) -> Optional[EMAPair]:
    fwd = close.pct_change().shift(-1)
    best_score = -math.inf
    best: Optional[EMAPair] = None

    for fast in fast_list:
        fast_ema = close.ewm(span=fast, adjust=False).mean()
        for slow in slow_list:
            if slow < fast * min_ratio:
                continue
            slow_ema = close.ewm(span=slow, adjust=False).mean()
            trend_ok = fast_ema > slow_ema
            masked = fwd[trend_ok].dropna()
            if len(masked) < min_trend_samples:
                continue
            score = _sharpe(masked)
            if score > best_score:
                best_score = score
                best = EMAPair(fast=fast, slow=slow)

    return best


def refit_ema_params(
    db_client: QuestDBClient,
    symbols: list[str],
    fast_list: tuple[int, ...] = FAST_LIST,
    slow_list: tuple[int, ...] = SLOW_LIST,
    min_ratio: float = 1.5,
    min_trend_samples: int = 100,
    days: int = 90,
    output_path: Optional[str] = None,
    log: Optional[logging.Logger] = None,
) -> dict[str, EMAPair]:
    """Grid-search best (fast, slow) EMA pair per symbol.

    Falls back to existing params for any symbol with insufficient data.
    Persists results via save_ema_params.
    """
    if log is None:
        log = logging.getLogger(__name__)

    log.info("EMA refit: fetching %d days of bars for %d symbols", days, len(symbols))
    df = db_client.fetch_all_symbols(symbols, days=days)

    if df.empty:
        log.warning("No bars returned from QuestDB — refit aborted")
        return {}

    existing = load_ema_params(path=output_path, log=log)
    result: dict[str, EMAPair] = {}

    for sym in symbols:
        sym_bars = df[df["symbol"] == sym].sort_values("ts")
        if sym_bars.empty:
            log.warning("[%s] No data — keeping existing params", sym)
            if sym in existing:
                result[sym] = existing[sym]
            continue

        close = sym_bars["close"].astype(float).reset_index(drop=True)
        log.info("[%s] Grid search over %d bars ...", sym, len(close))

        best = _best_pair(close, fast_list, slow_list, min_ratio, min_trend_samples)

        if best is None:
            log.warning("[%s] No valid pair found — keeping existing params", sym)
            if sym in existing:
                result[sym] = existing[sym]
        else:
            prev = existing.get(sym)
            log.info("[%s] %s -> fast=%d slow=%d", sym, f"fast={prev.fast} slow={prev.slow}" if prev else "new", best.fast, best.slow)
            result[sym] = best

    save_ema_params(result, path=output_path, log=log)
    log.info("Refit complete: updated %d/%d symbols", len(result), len(symbols))
    return result
