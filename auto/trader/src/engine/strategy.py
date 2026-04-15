"""Momentum + EMA trend strategy evaluation.

Called by the Engine every rebalance cycle. This module:
  1. Takes cached OHLCV bars from the QuestDBClient.
  2. Computes rolling momentum scores (quantile-transformed).
  3. Applies the per-symbol EMA trend filter.
  4. Determines which coins to buy/sell based on momentum rank
     and EMA crossover signals.
  5. Returns Position updates and signal dicts for Prometheus.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd

from src.config import (
    BUY_THRESH,
    MIN_HOLD_BARS,
    MOMENTUM_LOOKBACK,
    QUANTILE_STRIDE,
    QUANTILE_WINDOW,
    STOP_LOSS_PCT,
    PROFIT_TAKE_TIERS,
    PROFIT_TAKE_FRACTION,
)
from src.engine.ema_filter import EMAPair, apply_ema_trend_filter
from src.engine.momentum import compute_momentum_scores
from src.market.questdb_client import QuestDBClient
from src.positions.positions_cache import CASH_SYMBOL, Position


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def evaluate_positions(
    current_list: list[str],
    positions: list[Position],
    ema_map: dict[str, EMAPair],
    db_client: QuestDBClient,
    max_positions: int,
    log: logging.Logger,
) -> tuple[list[Position], list[str], dict, list[dict]]:
    """Evaluate all coins and return updated positions + signals.

    Flow:
      1. Get all cached bars from QuestDBClient
      2. Compute momentum scores
      3. Apply EMA trend filter
      4. Extract latest-bar signals per symbol
      5. Sell on EMA bearish crossover (if held long enough)
      5b. Incremental profit taking at 5% intervals (25% each tier)
      6. Buy top-ranked momentum candidates with trend_ok
      7. Return ``(new_positions, new_symbols, signals, partial_sells)``

    Args:
        current_list: Symbol per slot (``$CASH$`` for empty slots).
        positions: Existing Position objects (parallel to *current_list*).
        ema_map: Per-symbol EMA parameters from ``ema_params.json``.
        db_client: QuestDBClient with updated bar cache.
        max_positions: Number of portfolio slots.
        log: Logger instance.

    Returns:
        ``(new_positions, new_symbols, signals, partial_sells)``
        where partial_sells is a list of ``{symbol, slot_id, quantity}`` dicts.
    """
    # 1. Get all cached bars
    all_bars = db_client.get_all_cached_bars()
    if all_bars.empty:
        log.warning("No bar data available — returning positions unchanged")
        return positions, current_list, {}, []

    # 2. Compute momentum scores
    try:
        scored = compute_momentum_scores(
            all_bars,
            lookback_bars=MOMENTUM_LOOKBACK,
            window_size=QUANTILE_WINDOW,
            stride_length=QUANTILE_STRIDE,
            log=log,
        )
    except Exception:
        log.exception("Momentum scoring failed")
        return positions, current_list, {}, []

    # 3. Apply EMA trend filter
    try:
        filtered = apply_ema_trend_filter(scored, ema_map)
    except Exception:
        log.exception("EMA filter failed")
        return positions, current_list, {}, []

    if filtered.empty:
        log.warning("Filtered dataframe is empty")
        return positions, current_list, {}, []

    # 4. Extract latest-bar signals per symbol
    latest_ts = filtered["ts"].max()
    latest_rows = filtered[filtered["ts"] == latest_ts]

    coin_signals: dict[str, dict] = {}
    for _, row in latest_rows.iterrows():
        sym = row["symbol"]
        coin_signals[sym] = {
            "close": float(row["close"]),
            "momentum_score": (
                float(row["momentum_score"])
                if pd.notna(row.get("momentum_score"))
                else None
            ),
            "trend_ok": bool(row.get("trend_ok", False)),
            "sell_on_ema_cross": bool(row.get("sell_on_ema_cross", False)),
            "fast_ema": (
                float(row["fast_ema"]) if pd.notna(row.get("fast_ema")) else None
            ),
            "slow_ema": (
                float(row["slow_ema"]) if pd.notna(row.get("slow_ema")) else None
            ),
        }

    # [TEMP DEBUG] Print momentum scores
    log.info("=== MOMENTUM SCORES ===")
    sorted_signals = sorted(
        coin_signals.items(),
        key=lambda x: x[1]["momentum_score"] if x[1]["momentum_score"] is not None else -999,
        reverse=True,
    )
    for sym, sig in sorted_signals:
        score = sig["momentum_score"]
        trend = sig["trend_ok"]
        log.info(
            "  %-10s  momentum=%-8s  trend_ok=%-5s  close=%.4f",
            sym,
            f"{score:.4f}" if score is not None else "N/A",
            str(trend),
            sig["close"],
        )
    log.info("=======================")

    # 5. Determine sells and buys
    held_coins: dict[str, int] = {}
    cash_slots: list[int] = []
    for i, sym in enumerate(current_list):
        if sym != CASH_SYMBOL:
            held_coins[sym] = i
        else:
            cash_slots.append(i)

    now = _utc_now_iso()
    new_positions = list(positions)
    new_symbols = list(current_list)
    partial_sells: list[dict] = []  # {symbol, slot_id, quantity} for profit-tier sells

    # 5a. Update or sell existing positions
    for coin, slot_idx in list(held_coins.items()):
        sig = coin_signals.get(coin)
        pos = positions[slot_idx]

        if sig is None:
            # No data for this coin — keep position, increment bars_held
            new_positions[slot_idx] = Position(
                id=pos.id,
                symbol=coin,
                quantity=pos.quantity,
                weight=pos.weight,
                entry_price=pos.entry_price,
                entry_ts=pos.entry_ts,
                current_price=pos.current_price,
                updated_at=now,
                bars_held=pos.bars_held + 1,
                initial_quantity=pos.initial_quantity,
                profit_tiers_taken=pos.profit_tiers_taken,
            )
            continue

        # Stop-loss: sell immediately if price drops >= STOP_LOSS_PCT below entry
        stop_loss_sell = False
        if pos.entry_price is not None and pos.entry_price > 0:
            loss_pct = (sig["close"] - pos.entry_price) / pos.entry_price
            if loss_pct <= -STOP_LOSS_PCT:
                stop_loss_sell = True

        ema_sell = sig["sell_on_ema_cross"] and pos.bars_held >= MIN_HOLD_BARS

        if stop_loss_sell or ema_sell:
            # Full liquidation — sell everything remaining
            new_positions[slot_idx] = Position(
                id=pos.id,
                symbol=CASH_SYMBOL,
                quantity=0.0,
                weight=pos.weight,
                entry_price=None,
                entry_ts=None,
                current_price=None,
                updated_at=now,
                bars_held=0,
            )
            new_symbols[slot_idx] = CASH_SYMBOL
            cash_slots.append(slot_idx)
            if stop_loss_sell:
                reason = f"STOP_LOSS({loss_pct:+.1%})"
            else:
                reason = "EMA bearish crossover"
            log.info(
                "SELL %s (slot %d) — %s after %d bars",
                coin,
                slot_idx,
                reason,
                pos.bars_held,
            )
        else:
            # Position held — check for incremental profit taking
            profit_tier_sell = False
            new_tiers_taken = pos.profit_tiers_taken

            if pos.entry_price is not None and pos.entry_price > 0:
                gain_pct = (sig["close"] - pos.entry_price) / pos.entry_price

                # How many tiers should we have taken at this gain level?
                tiers_due = sum(1 for tier in PROFIT_TAKE_TIERS if gain_pct >= tier)

                if tiers_due > pos.profit_tiers_taken:
                    new_tier_count = tiers_due - pos.profit_tiers_taken
                    is_final = tiers_due >= len(PROFIT_TAKE_TIERS)

                    if is_final:
                        # Full exit at final tier
                        new_positions[slot_idx] = Position(
                            id=pos.id,
                            symbol=CASH_SYMBOL,
                            quantity=0.0,
                            weight=pos.weight,
                            entry_price=None,
                            entry_ts=None,
                            current_price=None,
                            updated_at=now,
                            bars_held=0,
                        )
                        new_symbols[slot_idx] = CASH_SYMBOL
                        cash_slots.append(slot_idx)
                        log.info(
                            "PROFIT_TAKE %s (slot %d) — final tier at +%.1f%%, selling ALL remaining after %d bars",
                            coin, slot_idx, gain_pct * 100, pos.bars_held,
                        )
                        profit_tier_sell = True
                    else:
                        # Partial sell — emit fraction for the trader to compute actual shares
                        sell_fraction = PROFIT_TAKE_FRACTION * new_tier_count
                        partial_sells.append({
                            "symbol": coin,
                            "slot_id": pos.id,
                            "fraction": sell_fraction,
                        })
                        new_tiers_taken = tiers_due
                        log.info(
                            "PROFIT_TAKE %s (slot %d) — tier %d at +%.1f%%, selling %.0f%% after %d bars",
                            coin, slot_idx, tiers_due, gain_pct * 100,
                            sell_fraction * 100, pos.bars_held,
                        )

            if not profit_tier_sell:
                new_positions[slot_idx] = Position(
                    id=pos.id,
                    symbol=coin,
                    quantity=pos.quantity,
                    weight=pos.weight,
                    entry_price=pos.entry_price,
                    entry_ts=pos.entry_ts,
                    current_price=sig["close"],
                    updated_at=now,
                    bars_held=pos.bars_held + 1,
                    profit_tiers_taken=new_tiers_taken,
                )

    # 5b. Buy candidates: momentum > BUY_THRESH and trend_ok
    buy_candidates: list[tuple[str, dict]] = []
    held_set = set(held_coins.keys())
    for sym, sig in coin_signals.items():
        if sym in held_set:
            continue
        if sig["momentum_score"] is None:
            continue
        if sig["momentum_score"] > BUY_THRESH and sig["trend_ok"]:
            buy_candidates.append((sym, sig))

    buy_candidates.sort(key=lambda x: x[1]["momentum_score"], reverse=True)

    for sym, sig in buy_candidates:
        if not cash_slots:
            break
        slot_idx = cash_slots.pop(0)
        new_positions[slot_idx] = Position(
            id=positions[slot_idx].id,
            symbol=sym,
            quantity=0.0,
            weight=1.0 / max_positions,
            entry_price=sig["close"],
            entry_ts=now,
            current_price=sig["close"],
            updated_at=now,
            bars_held=0,
        )
        new_symbols[slot_idx] = sym
        log.info(
            "BUY %s (slot %d) — momentum=%.2f",
            sym,
            slot_idx,
            sig["momentum_score"],
        )

    # 6. Build signals dict for Prometheus
    held_after = {p.symbol for p in new_positions if p.symbol != CASH_SYMBOL}
    signals: dict[str, dict] = {}
    for sym, sig in coin_signals.items():
        signals[sym] = {
            "close": sig["close"],
            "ts": now,
            "last_bar_ts": float(latest_ts.value // 10**9),
            "fast_ema": sig["fast_ema"],
            "slow_ema": sig["slow_ema"],
            "momentum_score": sig["momentum_score"],
            "trend_ok": sig["trend_ok"],
            "sell_on_ema_cross": sig["sell_on_ema_cross"],
            "held": sym in held_after,
        }

    return new_positions, new_symbols, signals, partial_sells
