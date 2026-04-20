"""Crypto trader engine: strategy evaluation + direct trade execution.

Runs a continuous rebalance loop at a configurable interval. Each cycle:
  1. Fetch / refresh OHLCV bars from QuestDB
  2. Evaluate momentum + EMA trend strategy
  3. Execute partial profit-take sells
  4. Execute full sells
  5. Wait for settlement
  6. Execute buys
  7. Update Prometheus metrics

No Kafka — strategy and trading run in the same process.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone

from src.config import (
    SIGNAL_GATE_ENABLED,
    SIGNAL_LOOKBACK_HOURS,
    SIGNAL_CONFIDENCE_THRESHOLD,
    SIGNAL_BLOCK_HORIZONS,
)
from src.engine.ema_filter import EMAPair
from src.engine.strategy import evaluate_positions
from src.market.questdb_client import QuestDBClient
from src.metrics import (
    update_metrics,
    BUYS_ATTEMPTED_TOTAL,
    BUYS_SUCCEEDED_TOTAL,
    SELLS_ATTEMPTED_TOTAL,
    SELLS_SUCCEEDED_TOTAL,
    ERRORS_TOTAL,
)
from src.positions.positions_cache import CASH_SYMBOL, load_positions, save_positions
from src.trader.client import KrakenTrader


def _next_boundary(now: datetime, minutes: int) -> datetime:
    """Return next datetime aligned to *minutes* boundary."""
    now0 = now.replace(second=0, microsecond=0)
    m = now0.minute
    add = (minutes - (m % minutes)) % minutes
    if add == 0:
        add = minutes
    return now0 + timedelta(minutes=add)


class Engine:
    """Crypto trader engine: runs the strategy and executes trades directly.

    Market data comes from QuestDB. Strategy: momentum ranking + EMA trend filter.
    Trades are executed directly on Kraken via the API key in the environment.
    """

    def __init__(
        self,
        trader: KrakenTrader,
        db_client: QuestDBClient,
        ema_map: dict[str, EMAPair],
        coin_list: list[str],
        positions_path: str,
        cycle_interval: int,
        max_positions: int,
        settlement_delay: float,
        log: logging.Logger,
    ):
        self.trader = trader
        self.db_client = db_client
        self.ema_map = ema_map
        self.coin_list = coin_list
        self.positions_path = positions_path
        self.cycle_interval = cycle_interval
        self.max_positions = max_positions
        self.settlement_delay = settlement_delay
        self.log = log

        self._stop_event = threading.Event()
        self._rebalance_thread = threading.Thread(
            target=self._rebalance_loop,
            name="trader-rebalance",
            daemon=True,
        )

    # ── Lifecycle ────────────────────────────────────────────────────────

    def start(self) -> None:
        self.log.info("Starting crypto-trader engine ...")
        self._rebalance_thread.start()
        self.log.info(
            "Rebalance thread started (interval=%d min, %d coins)",
            self.cycle_interval,
            len(self.coin_list),
        )

    def stop(self) -> None:
        self.log.info("Stopping crypto-trader engine ...")
        self._stop_event.set()
        self._rebalance_thread.join(timeout=60)
        self.trader.close()
        self.log.info("Engine stopped.")

    def run(self) -> None:
        """Start and block until interrupted."""
        self.start()
        try:
            while not self._stop_event.is_set():
                self._stop_event.wait(timeout=1.0)
        except KeyboardInterrupt:
            self.log.info("Interrupted by user")
        finally:
            self.stop()

    # ── Main loop ────────────────────────────────────────────────────────

    def _rebalance_loop(self) -> None:
        """Continuous rebalance loop — runs 24/7."""
        self.log.info("Warming up bar cache for %d coins from QuestDB ...", len(self.coin_list))
        self.db_client.update_cache(self.coin_list)
        self.log.info("Warmup complete. Running first rebalance cycle.")

        self._do_rebalance_cycle()

        while not self._stop_event.is_set():
            now = datetime.now(timezone.utc)
            nxt = _next_boundary(now, self.cycle_interval)
            sleep_sec = max(1.0, (nxt - now).total_seconds())
            self._stop_event.wait(timeout=sleep_sec)

            if self._stop_event.is_set():
                break

            try:
                self.db_client.update_cache(self.coin_list)
                self._do_rebalance_cycle()
            except Exception:
                self.log.exception("Rebalance cycle failed")

    # ── Single cycle ─────────────────────────────────────────────────────

    def _do_rebalance_cycle(self) -> None:
        self.log.info("Running rebalance cycle ...")
        cycle_start = time.monotonic()

        positions = load_positions(self.positions_path)
        prev_symbols = [p.symbol for p in positions]

        blocked_symbols: set[str] = set()
        if SIGNAL_GATE_ENABLED:
            try:
                blocked_symbols = self.db_client.fetch_bearish_blocked_symbols(
                    lookback_hours=SIGNAL_LOOKBACK_HOURS,
                    confidence_threshold=SIGNAL_CONFIDENCE_THRESHOLD,
                    block_horizons=SIGNAL_BLOCK_HORIZONS,
                )
                if blocked_symbols:
                    self.log.info(
                        "Signal gate: blocking buys for %s (bearish, conf>=%.2f, within %dh)",
                        sorted(blocked_symbols),
                        SIGNAL_CONFIDENCE_THRESHOLD,
                        SIGNAL_LOOKBACK_HOURS,
                    )
                else:
                    self.log.info("Signal gate: active, no bearish symbols blocked")
            except Exception:
                self.log.warning("Signal gate fetch failed; proceeding without it")

        new_positions, new_symbols, signals, partial_sells = evaluate_positions(
            current_list=prev_symbols,
            positions=positions,
            ema_map=self.ema_map,
            db_client=self.db_client,
            max_positions=self.max_positions,
            log=self.log,
            blocked_symbols=blocked_symbols,
        )

        # Compute full buys / sells from slot changes
        new_buys: list[dict] = []
        new_sells: list[dict] = []
        for i, (prev, new) in enumerate(zip(prev_symbols, new_symbols)):
            if prev == CASH_SYMBOL and new != CASH_SYMBOL:
                pos = new_positions[i]
                new_buys.append({
                    "symbol": new,
                    "slot_id": pos.id,
                    "entry_price": pos.entry_price,
                    "weight": pos.weight,
                })
            elif prev != CASH_SYMBOL and new == CASH_SYMBOL:
                new_sells.append({"symbol": prev, "slot_id": positions[i].id})

        self.log.info(
            "Rebalance: BUYS=%s  SELLS=%s  PARTIAL_SELLS=%s",
            [b["symbol"] for b in new_buys],
            [s["symbol"] for s in new_sells],
            [s["symbol"] for s in partial_sells],
        )

        # Log positions
        self.log.info("=== POSITIONS AFTER REBALANCE ===")
        for pos in new_positions:
            if pos.symbol == CASH_SYMBOL:
                self.log.info("  slot %-3s  %-10s  weight=%.4f", pos.id, pos.symbol, pos.weight)
            else:
                self.log.info(
                    "  slot %-3s  %-10s  weight=%.4f  entry=%-10s  current=%-10s  bars_held=%d",
                    pos.id, pos.symbol, pos.weight,
                    f"{pos.entry_price:.4f}" if pos.entry_price else "N/A",
                    f"{pos.current_price:.4f}" if pos.current_price else "N/A",
                    pos.bars_held,
                )
        self.log.info("=================================")

        # Persist positions before executing trades
        save_positions(new_positions, self.positions_path)

        if new_buys or new_sells or partial_sells:
            self._execute_trades(new_buys, new_sells, partial_sells)
        else:
            self.log.info("No trades this cycle.")

        # Update Prometheus metrics
        cycle_duration = time.monotonic() - cycle_start
        if signals:
            update_metrics(
                signals=signals,
                new_positions=new_positions,
                new_buys=new_buys,
                new_sells=new_sells,
                cycle_duration_seconds=cycle_duration,
            )

    def _execute_trades(
        self,
        new_buys: list[dict],
        new_sells: list[dict],
        partial_sells: list[dict],
    ) -> None:
        """Execute partial sells, full sells, then buys."""

        # 1. Partial profit-take sells
        for sell in partial_sells:
            SELLS_ATTEMPTED_TOTAL.inc()
            try:
                ok = self.trader.execute_fractional_sell(sell["symbol"], sell["fraction"])
                if ok:
                    SELLS_SUCCEEDED_TOTAL.inc()
                else:
                    ERRORS_TOTAL.inc()
            except Exception:
                self.log.exception("Partial sell failed for %s", sell["symbol"])
                ERRORS_TOTAL.inc()

        # 2. Full sells
        for sell in new_sells:
            SELLS_ATTEMPTED_TOTAL.inc()
            try:
                ok = self.trader.execute_sell_symbol(sell["symbol"])
                if ok:
                    SELLS_SUCCEEDED_TOTAL.inc()
                else:
                    ERRORS_TOTAL.inc()
            except Exception:
                self.log.exception("Sell failed for %s", sell["symbol"])
                ERRORS_TOTAL.inc()

        # 3. Wait for settlement before buying
        if (new_sells or partial_sells) and new_buys:
            self.log.info("Waiting %.1fs for sells to settle ...", self.settlement_delay)
            time.sleep(self.settlement_delay)

        # 4. Convert any non-USD fiat to USD
        if new_buys:
            try:
                if self.trader.convert_fiat_to_usd():
                    self.log.info("Fiat converted, waiting %.1fs ...", self.settlement_delay)
                    time.sleep(self.settlement_delay)
            except Exception:
                self.log.exception("Fiat conversion failed")

        # 5. Buys
        for buy in new_buys:
            BUYS_ATTEMPTED_TOTAL.inc()
            try:
                ok = self.trader.execute_weighted_buy(
                    symbol=buy["symbol"],
                    weight=buy["weight"],
                    entry_price=buy["entry_price"] or 0.0,
                )
                if ok:
                    BUYS_SUCCEEDED_TOTAL.inc()
                else:
                    ERRORS_TOTAL.inc()
            except Exception:
                self.log.exception("Buy failed for %s", buy["symbol"])
                ERRORS_TOTAL.inc()
