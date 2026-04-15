"""Prometheus metrics for crypto-trader.

Combines strategy metrics (analyst_*) and trade-execution metrics (trader_*).
"""

from __future__ import annotations

import logging
from typing import Optional

from prometheus_client import Counter, Gauge, start_http_server

from src.positions.positions_cache import CASH_SYMBOL, Position

log = logging.getLogger(__name__)

# ── Per-symbol signal gauges ──────────────────────────────────────────────────
SYMBOL_PRICE = Gauge("analyst_symbol_price", "Latest close price", ["symbol"])
SYMBOL_FAST_EMA = Gauge("analyst_symbol_fast_ema", "Current fast EMA value", ["symbol"])
SYMBOL_SLOW_EMA = Gauge("analyst_symbol_slow_ema", "Current slow EMA value", ["symbol"])
SYMBOL_MOMENTUM = Gauge("analyst_symbol_momentum_score", "Quantile-transformed momentum score", ["symbol"])
SYMBOL_TREND_OK = Gauge("analyst_symbol_trend_ok", "1 if fast_ema > slow_ema else 0", ["symbol"])
SYMBOL_HELD = Gauge("analyst_symbol_held", "1 if position is currently held else 0", ["symbol"])
SYMBOL_LAST_BAR_TS = Gauge("analyst_symbol_last_bar_ts", "Unix timestamp of latest bar", ["symbol"])

# ── Portfolio-level metrics ───────────────────────────────────────────────────
ACTIVE_POSITIONS = Gauge("analyst_active_positions", "Number of non-cash positions currently held")
CYCLE_BUYS_TOTAL = Counter("analyst_cycle_buys_total", "Cumulative buy events since process start")
CYCLE_SELLS_TOTAL = Counter("analyst_cycle_sells_total", "Cumulative sell events since process start")
REBALANCE_CYCLES_TOTAL = Counter("analyst_rebalance_cycles_total", "Cumulative rebalance cycles completed")
REBALANCE_DURATION = Gauge("analyst_rebalance_cycle_duration_seconds", "Wall-clock seconds of the last rebalance cycle")

# ── Per-position metrics ──────────────────────────────────────────────────────
POSITION_ENTRY_PRICE = Gauge("analyst_position_entry_price", "Entry price when position was opened", ["symbol", "slot_id"])
POSITION_CURRENT_PRICE = Gauge("analyst_position_current_price", "Latest close price for this position", ["symbol", "slot_id"])
POSITION_UNREALIZED_PNL = Gauge("analyst_position_unrealized_pnl", "Unrealized P&L: (current - entry) / entry", ["symbol", "slot_id"])
POSITION_BARS_HELD = Gauge("analyst_position_bars_held", "Number of 15-min bars since entry", ["symbol", "slot_id"])

# ── Trade execution metrics ───────────────────────────────────────────────────
BUYS_ATTEMPTED_TOTAL = Counter("trader_buys_attempted_total", "Buy orders attempted")
BUYS_SUCCEEDED_TOTAL = Counter("trader_buys_succeeded_total", "Buy orders succeeded")
SELLS_ATTEMPTED_TOTAL = Counter("trader_sells_attempted_total", "Sell orders attempted")
SELLS_SUCCEEDED_TOTAL = Counter("trader_sells_succeeded_total", "Sell orders succeeded")
ERRORS_TOTAL = Counter("trader_errors_total", "Errors during trade execution")
ORDER_SLIPPAGE = Gauge(
    "trader_order_slippage",
    "Fill price minus strategy entry price (positive = paid more than expected)",
    ["symbol"],
)

# Track which slot_ids had active positions last cycle (for stale label cleanup)
_prev_active_slots: set[tuple[str, str]] = set()


def start_metrics_server(port: int = 9095) -> None:
    """Start the Prometheus HTTP server on the given port."""
    start_http_server(port)
    log.info("Prometheus metrics server started on port %d", port)


def update_metrics(
    signals: dict[str, dict],
    new_positions: list[Position],
    new_buys: list[dict],
    new_sells: list[dict],
    cycle_duration_seconds: Optional[float] = None,
) -> None:
    """Update all Prometheus metrics after a rebalance cycle."""
    global _prev_active_slots

    # 1. Per-symbol signal gauges
    for sym, sig in signals.items():
        SYMBOL_PRICE.labels(symbol=sym).set(sig["close"])
        if sig.get("fast_ema") is not None:
            SYMBOL_FAST_EMA.labels(symbol=sym).set(sig["fast_ema"])
        if sig.get("slow_ema") is not None:
            SYMBOL_SLOW_EMA.labels(symbol=sym).set(sig["slow_ema"])
        if sig.get("momentum_score") is not None:
            SYMBOL_MOMENTUM.labels(symbol=sym).set(sig["momentum_score"])
        SYMBOL_TREND_OK.labels(symbol=sym).set(1.0 if sig.get("trend_ok") else 0.0)
        SYMBOL_HELD.labels(symbol=sym).set(1.0 if sig.get("held") else 0.0)
        if sig.get("last_bar_ts") is not None:
            SYMBOL_LAST_BAR_TS.labels(symbol=sym).set(sig["last_bar_ts"])

    # 2. Portfolio-level metrics
    active_count = sum(1 for p in new_positions if p.symbol != CASH_SYMBOL)
    ACTIVE_POSITIONS.set(active_count)
    CYCLE_BUYS_TOTAL.inc(len(new_buys))
    CYCLE_SELLS_TOTAL.inc(len(new_sells))
    REBALANCE_CYCLES_TOTAL.inc()
    if cycle_duration_seconds is not None:
        REBALANCE_DURATION.set(cycle_duration_seconds)

    # 3. Per-position metrics (with stale label cleanup)
    current_active_slots: set[tuple[str, str]] = set()
    for pos in new_positions:
        if pos.symbol == CASH_SYMBOL:
            continue
        labels = {"symbol": pos.symbol, "slot_id": pos.id}
        current_active_slots.add((pos.symbol, pos.id))

        if pos.entry_price is not None:
            POSITION_ENTRY_PRICE.labels(**labels).set(pos.entry_price)
        if pos.current_price is not None:
            POSITION_CURRENT_PRICE.labels(**labels).set(pos.current_price)
        if pos.entry_price and pos.current_price:
            pnl = (pos.current_price - pos.entry_price) / pos.entry_price
            POSITION_UNREALIZED_PNL.labels(**labels).set(pnl)
        POSITION_BARS_HELD.labels(**labels).set(pos.bars_held)

    # Remove stale position labels (slots sold this cycle)
    stale_slots = _prev_active_slots - current_active_slots
    for sym, slot_id in stale_slots:
        for gauge in (POSITION_ENTRY_PRICE, POSITION_CURRENT_PRICE,
                      POSITION_UNREALIZED_PNL, POSITION_BARS_HELD):
            try:
                gauge.remove(sym, slot_id)
            except KeyError:
                pass

    _prev_active_slots = current_active_slots
