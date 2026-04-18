# src/engine/momentum.py
"""
Rolling quantile momentum score calculation.

Computes momentum as:
1. Raw momentum = (close - close_n_bars_ago) / close_n_bars_ago
2. Rolling quantile transform per symbol using historical window
3. Inverse normal transform -> ~N(0,1)

This is a causal implementation that only uses past data within each
symbol's history, avoiding look-ahead bias from cross-sectional ranking.
"""

from __future__ import annotations

import logging
import warnings

import numpy as np
import pandas as pd
from scipy.stats import norm
from sklearn.preprocessing import QuantileTransformer


def compute_momentum_scores(
    df: pd.DataFrame,
    lookback_bars: int = 1000,
    window_size: int = 5000,
    stride_length: int = 100,
    log: logging.Logger | None = None,
) -> pd.DataFrame:
    """
    Compute momentum scores using rolling quantile transform per symbol.

    This is a causal implementation that only uses historical data within
    each symbol's history (no cross-sectional look-ahead).

    Args:
        df: DataFrame with columns [ts, symbol, close]
            - ts: timestamp
            - symbol: stock ticker
            - close: closing price
        lookback_bars: Number of bars for raw momentum calculation
        window_size: Number of samples for rolling QuantileTransformer fitting
        stride_length: Step size between recomputations (for efficiency)
        log: Optional logger

    Returns:
        DataFrame with original columns plus:
        - mom_raw: raw momentum (pct change from lookback)
        - momentum_score: inverse-normal transformed rolling quantile score
    """
    if log is None:
        log = logging.getLogger(__name__)

    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values(["symbol", "ts"])

    # Group by symbol for per-symbol momentum calculation
    g = df.groupby("symbol", sort=False)

    close = df["close"].astype(float)
    first_close = g["close"].transform("first")
    lag_n = g["close"].shift(lookback_bars)
    idx = g.cumcount()

    # Use first_close as denominator until we have enough history
    denom = np.where(idx < lookback_bars, first_close.to_numpy(), lag_n.to_numpy())
    df["mom_raw"] = (close.to_numpy() - denom) / denom

    # Apply rolling quantile transform per symbol
    df = _rolling_quantile_transform(
        df,
        window_size=window_size,
        stride_length=stride_length,
        log=log,
    )

    return df


def _rolling_quantile_transform(
    df_input: pd.DataFrame,
    window_size: int = 5000,
    stride_length: int = 100,
    eps: float = 1e-6,
    log: logging.Logger | None = None,
) -> pd.DataFrame:
    """
    Apply rolling QuantileTransformer and inverse-normal transform to 'mom_raw'.

    Args:
        df_input: DataFrame with 'ts', 'symbol', and 'mom_raw' columns.
        window_size: Number of samples in rolling window for QuantileTransformer.
        stride_length: Step size between recomputations (> 1 for efficiency).
        eps: Small epsilon to clip probabilities to avoid -inf/+inf.
        log: Optional logger

    Returns:
        DataFrame with 'momentum_score' calculated using rolling transformation.
    """
    if log is None:
        log = logging.getLogger(__name__)

    if stride_length < 1:
        raise ValueError("stride_length must be >= 1")

    df = df_input.copy()
    df["momentum_score"] = np.nan

    symbols = df["symbol"].unique()
    log.info("Computing rolling quantile transform for %d symbols", len(symbols))

    for symbol in symbols:
        group = df[df["symbol"] == symbol]

        # Skip if not enough data for window or quantile bins
        n_samples = group["mom_raw"].dropna().shape[0]
        if n_samples < window_size:
            log.debug("[%s] Skipping - insufficient data (%d < %d)",
                      symbol, n_samples, window_size)
            continue
        if n_samples < 1000:
            log.debug("[%s] Skipping - insufficient data for quantile transform (%d < 1000)",
                      symbol, n_samples)
            continue

        qt = QuantileTransformer(output_distribution="uniform", random_state=42)

        transformed_scores = []
        original_indices = []
        last_score = np.nan

        for i in range(len(group)):
            original_indices.append(group.index[i])

            # Not enough history yet for first window
            if i < window_size - 1:
                transformed_scores.append(np.nan)
                continue

            # Only recompute on stride steps; otherwise reuse last computed score
            if stride_length > 1 and (i % stride_length) != 0:
                transformed_scores.append(last_score)
                continue

            window_data = (
                group["mom_raw"]
                .iloc[i - window_size + 1 : i + 1]
                .dropna()
                .to_numpy()
                .reshape(-1, 1)
            )

            if len(window_data) == 0:
                last_score = np.nan
                transformed_scores.append(np.nan)
                continue

            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message="n_quantiles.*is greater than.*n_samples",
                        category=UserWarning,
                    )
                    qt.fit(window_data)

                current_val = group["mom_raw"].iloc[i]
                if pd.isna(current_val):
                    last_score = np.nan
                    transformed_scores.append(np.nan)
                    continue

                u_transformed = qt.transform(np.array([[current_val]]))
                score = norm.ppf(u_transformed.flatten().clip(eps, 1 - eps))[0]

                last_score = score
                transformed_scores.append(score)
            except ValueError:
                # e.g., all window values identical -> QT fit can fail
                last_score = np.nan
                transformed_scores.append(np.nan)

        df.loc[original_indices, "momentum_score"] = transformed_scores

    return df


def get_latest_momentum_scores(
    df: pd.DataFrame,
    lookback_bars: int = 1000,
    window_size: int = 5000,
    stride_length: int = 100,
    log: logging.Logger | None = None,
) -> dict[str, float]:
    """
    Compute momentum scores and return only the latest value per symbol.

    Args:
        df: DataFrame with columns [ts, symbol, close]
        lookback_bars: Number of bars for raw momentum calculation
        window_size: Number of samples for rolling QuantileTransformer fitting
        stride_length: Step size between recomputations (for efficiency)
        log: Optional logger

    Returns:
        Dict mapping symbol -> momentum_score at latest timestamp
    """
    scored = compute_momentum_scores(
        df,
        lookback_bars=lookback_bars,
        window_size=window_size,
        stride_length=stride_length,
        log=log,
    )

    if scored.empty:
        return {}

    # Get latest timestamp
    latest_ts = scored["ts"].max()
    latest = scored[scored["ts"] == latest_ts]

    return dict(zip(latest["symbol"], latest["momentum_score"]))
