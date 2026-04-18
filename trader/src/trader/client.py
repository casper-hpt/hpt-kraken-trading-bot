# src/trader/client.py

import logging
import time
from typing import Optional

from trader.src.kraken_api import (
    KrakenApiClient,
    symbol_to_pair,
    kraken_asset_to_symbol,
    symbol_to_kraken_asset,
    FIAT_ASSETS,
)
from trader.src.kraken_api.exceptions import KrakenInsufficientFundsError
from trader.src.metrics import ORDER_SLIPPAGE


class KrakenTrader:
    """Client for executing trades on Kraken using limit orders only."""

    # Minimum order value in dollars
    MIN_ORDER_VALUE_USD = 1.0
    # Volume precision (decimal places) for order quantities
    VOLUME_PRECISION = 8
    # Fee buffer: reserve a small percentage for fees on limit orders
    FEE_BUFFER = 0.998
    # Seconds between each fill-status poll
    LIMIT_ORDER_POLL_INTERVAL = 5.0
    # Seconds to wait per attempt before checking price drift
    LIMIT_ORDER_FILL_TIMEOUT = 60.0
    # Fraction of price movement (e.g. 0.005 = 0.5%) that triggers cancel+repost
    LIMIT_ORDER_DRIFT_PCT = 0.005
    # Maximum cancel+repost cycles before giving up on an order
    LIMIT_ORDER_MAX_RETRIES = 5

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        dry_run: bool = False,
        logger: Optional[logging.Logger] = None,
        limit_order_poll_interval: float = LIMIT_ORDER_POLL_INTERVAL,
        limit_order_fill_timeout: float = LIMIT_ORDER_FILL_TIMEOUT,
        limit_order_drift_pct: float = LIMIT_ORDER_DRIFT_PCT,
        limit_order_max_retries: int = LIMIT_ORDER_MAX_RETRIES,
    ):
        self.dry_run = dry_run
        self.log = logger or logging.getLogger(__name__)
        self.limit_order_poll_interval = limit_order_poll_interval
        self.limit_order_fill_timeout = limit_order_fill_timeout
        self.limit_order_drift_pct = limit_order_drift_pct
        self.limit_order_max_retries = limit_order_max_retries

        if not dry_run:
            self.client = KrakenApiClient(api_key=api_key, api_secret=api_secret)
            self.log.info("KrakenTrader initialized (limit orders only)")
        else:
            self.client = None
            self.log.info("KrakenTrader initialized in DRY RUN mode")

    def close(self) -> None:
        """Close the trader and release resources."""
        if self.client:
            self.client.close()
            self.log.debug("KrakenTrader client closed")

    def get_portfolio(self) -> dict:
        """
        Get current portfolio information including positions.

        Returns dict with:
            equity: total account equity in USD
            buying_power: available USD cash for trading
            positions: list of {"symbol": str, "quantity": float} (dust filtered)
        """
        if self.dry_run:
            self.log.info("[DRY RUN] Would fetch portfolio")
            return {"equity": 0, "buying_power": 0, "positions": []}

        # Get trade balance for total equity
        trade_balance = self.client.get_trade_balance()
        equity = self._safe_float(trade_balance.equivalent_balance, "equity")

        # Get asset balances for positions and USD cash
        balance = self.client.get_balance()

        # Extract actual USD cash as buying power
        usd_cash = 0.0
        for fiat_key in ("ZUSD", "USD"):
            if fiat_key in balance.assets:
                usd_cash += self._safe_float(balance.assets[fiat_key], f"balance({fiat_key})")

        # Build positions list, filtering out dust
        positions = []
        for asset_key, amount in balance.assets.items():
            # Skip fiat currencies
            if asset_key in FIAT_ASSETS:
                continue
            # Skip yield-bearing/reward/tokenized variants
            if "." in asset_key:
                continue

            symbol = kraken_asset_to_symbol(asset_key)
            qty = self._safe_float(amount, f"balance({asset_key})")
            if qty <= 0:
                continue

            # Check position value against minimum to filter dust
            try:
                pair = symbol_to_pair(symbol)
                ticker = self.client.get_ticker(pair)
                price = self._safe_float(ticker.last, f"price({symbol})") if ticker.last else 0
                position_value = qty * price
                if position_value < self.MIN_ORDER_VALUE_USD:
                    self.log.debug(
                        f"Filtering dust position {symbol}: {qty} units worth ${position_value:.4f}"
                    )
                    continue
            except Exception as e:
                self.log.debug(f"Could not price {symbol} for dust filter, including it: {e}")

            positions.append({"symbol": symbol, "quantity": qty})

        return {
            "equity": equity,
            "buying_power": usd_cash,
            "positions": positions,
        }

    @staticmethod
    def _safe_float(value, label: str = "value") -> float:
        """Safely convert a value to float, returning 0.0 on failure."""
        if value is None:
            return 0.0
        try:
            result = float(value)
            if result != result or result == float("inf") or result == float("-inf"):
                logging.getLogger(__name__).warning(f"_safe_float: {label} is {result}, returning 0.0")
                return 0.0
            return result
        except (ValueError, TypeError, ArithmeticError):
            logging.getLogger(__name__).warning(f"_safe_float: could not convert {label}='{value}', returning 0.0")
            return 0.0

    def get_quote(self, symbol: str) -> Optional[float]:
        """Get current price for a symbol."""
        if self.dry_run:
            self.log.info(f"[DRY RUN] Would fetch quote for {symbol}")
            return None

        try:
            pair = symbol_to_pair(symbol)
            ticker = self.client.get_ticker(pair)
            if ticker and ticker.last:
                return float(ticker.last)
        except Exception as e:
            self.log.error(f"Failed to get quote for {symbol}: {e}")
        return None

    # Non-USD fiat → USD conversion details: (pair, side, fiat_is_quote)
    # fiat_is_quote=True  → our fiat is the quote currency, need price to calc volume
    # fiat_is_quote=False → our fiat is the base currency, volume = our balance
    FIAT_TO_USD = {
        "ZCAD": ("USDCAD", "buy", True),
        "CAD":  ("USDCAD", "buy", True),
        "ZEUR": ("EURUSD", "sell", False),
        "EUR":  ("EURUSD", "sell", False),
        "ZGBP": ("GBPUSD", "sell", False),
        "GBP":  ("GBPUSD", "sell", False),
    }

    def convert_fiat_to_usd(self) -> bool:
        """Convert any non-USD fiat balances to USD via market orders.

        Fiat-to-fiat conversions remain as market orders — the cost difference
        is negligible and the pairs don't always support resting limit orders.

        Returns True if any conversions were placed.
        """
        if self.dry_run:
            self.log.info("[DRY RUN] Would convert non-USD fiat to USD")
            return False

        balance = self.client.get_balance()
        converted_any = False
        seen_pairs = set()

        for fiat_key, (pair, side, fiat_is_quote) in self.FIAT_TO_USD.items():
            if fiat_key not in balance.assets:
                continue
            # Avoid double-converting (e.g. ZCAD and CAD both present)
            if pair in seen_pairs:
                continue

            amount = self._safe_float(balance.assets[fiat_key], f"balance({fiat_key})")
            if amount < self.MIN_ORDER_VALUE_USD:
                continue

            try:
                if fiat_is_quote:
                    # Our fiat is the quote currency (e.g. CAD in USDCAD).
                    # Volume is in base currency (USD), so divide by price.
                    ticker = self.client.get_ticker(pair)
                    price = self._safe_float(ticker.last, f"price({pair})")
                    if not price or price <= 0:
                        self.log.error(f"Could not get price for {pair}, skipping conversion")
                        continue
                    volume = (amount * self.FEE_BUFFER) / price
                else:
                    # Our fiat is the base currency (e.g. EUR in EURUSD).
                    volume = amount * self.FEE_BUFFER

                volume = round(volume, 2)  # fiat pairs use 2 decimal places
                if volume <= 0:
                    continue

                self.log.info(
                    f"Converting {fiat_key} ({amount:.2f}) to USD: "
                    f"{side} {volume} on {pair}"
                )
                order_response = self.client.add_order(
                    pair=pair,
                    side=side,
                    ordertype="market",
                    volume=str(volume),
                )

                txids = ", ".join(order_response.txids) if order_response.txids else "none"
                self.log.info(
                    f"Fiat conversion order placed: txid={txids}, "
                    f"desc='{order_response.description}'"
                )
                converted_any = True
                seen_pairs.add(pair)

            except Exception as e:
                self.log.error(f"Failed to convert {fiat_key} to USD: {e}", exc_info=True)

        return converted_any

    def _get_asset_balance(self, symbol: str) -> float:
        """Get the balance of a specific asset by standard symbol."""
        balance = self.client.get_balance()
        possible_keys = symbol_to_kraken_asset(symbol)

        for key in possible_keys:
            if key in balance.assets:
                return self._safe_float(balance.assets[key], f"balance({key})")

        # Also try exact match on the symbol itself
        if symbol.upper() in balance.assets:
            return self._safe_float(balance.assets[symbol.upper()], f"balance({symbol})")

        return 0.0

    def _execute_limit_order(
        self,
        pair: str,
        side: str,
        volume: str,
        symbol: str,
        target_value: Optional[float] = None,
    ) -> Optional[float]:
        """
        Place a limit order and manage it until fully filled.

        Buy orders are placed at the current best bid; sell orders at the
        current best ask. This ensures the order rests on the book as a
        maker order, avoiding taker fees.

        After each LIMIT_ORDER_FILL_TIMEOUT window without a fill, the price
        is re-checked. If it has drifted more than LIMIT_ORDER_DRIFT_PCT the
        open order is cancelled and a fresh one is posted at the new price.
        This repeats up to LIMIT_ORDER_MAX_RETRIES times before giving up.

        Args:
            pair:         Kraken trading pair (e.g. "XBTUSD")
            side:         "buy" or "sell"
            volume:       Initial order volume in base currency (recalculated
                          on retry for buy orders when target_value is set)
            symbol:       Human-readable symbol for logging (e.g. "BTC")
            target_value: For buy orders only — the target USD spend. When set,
                          volume is recalculated from the new bid on each retry
                          so the order always reflects current pricing.

        Returns:
            Average fill price on success, or None if all retries exhausted.
        """
        current_volume = volume

        for attempt in range(1, self.limit_order_max_retries + 1):
            # Fetch ticker to get best bid/ask for the limit price
            try:
                ticker = self.client.get_ticker(pair)
            except Exception as e:
                self.log.error(f"Could not get ticker for {pair} on attempt {attempt}: {e}")
                return None

            if side == "buy":
                limit_price_dec = ticker.bid
            else:
                limit_price_dec = ticker.ask

            if not limit_price_dec or limit_price_dec <= 0:
                self.log.error(
                    f"No valid {'bid' if side == 'buy' else 'ask'} price for {pair} "
                    f"on attempt {attempt}"
                )
                return None

            limit_price = float(limit_price_dec)

            # For buy orders, recalculate volume from the fresh limit price
            if side == "buy" and target_value is not None:
                new_vol = round((target_value * self.FEE_BUFFER) / limit_price, self.VOLUME_PRECISION)
                if new_vol <= 0:
                    self.log.warning(f"Recalculated volume for {symbol} is {new_vol}, aborting")
                    return None
                current_volume = str(new_vol)

            price_str = f"{limit_price:.10g}"

            self.log.info(
                f"LIMIT {side.upper()} {symbol}: vol={current_volume} price={price_str} "
                f"(attempt {attempt}/{self.limit_order_max_retries})"
            )

            try:
                order_response = self.client.add_order(
                    pair=pair,
                    side=side,
                    ordertype="limit",
                    volume=current_volume,
                    price=price_str,
                )
            except Exception as e:
                self.log.error(f"Failed to place limit order for {symbol} on attempt {attempt}: {e}")
                return None

            if not order_response.txids:
                self.log.error(f"No txids returned for {side} limit order on {symbol}")
                return None

            txids = order_response.txids
            self.log.info(
                f"Limit order placed for {symbol}: txid={', '.join(txids)}, "
                f"desc='{order_response.description}'"
            )

            # Poll for fill within this attempt's timeout window
            deadline = time.monotonic() + self.limit_order_fill_timeout
            fill_price = None

            while time.monotonic() < deadline:
                time.sleep(self.limit_order_poll_interval)
                try:
                    orders = self.client.query_orders(txids)
                    for txid, info in orders.items():
                        if info.status == "closed":
                            fill_price = float(info.price) if info.price else limit_price
                            self.log.info(
                                f"Limit order filled for {side.upper()} {symbol}: "
                                f"txid={txid}, fill_price={fill_price:.6g}, "
                                f"vol_exec={info.vol_exec}, cost={info.cost}"
                            )
                            return fill_price

                        if info.status == "canceled":
                            self.log.warning(
                                f"Limit order was cancelled externally for {symbol} (txid={txid})"
                            )
                            return None

                except Exception as e:
                    self.log.warning(f"Poll error for {symbol} {side} order: {e}")

            # Timeout — check whether price has drifted enough to warrant a repost
            if attempt == self.limit_order_max_retries:
                # Last attempt: cancel and give up
                self.log.error(
                    f"Limit {side.upper()} order for {symbol} unfilled after "
                    f"{self.limit_order_max_retries} attempts — cancelling"
                )
                self._cancel_orders(txids, symbol)
                return None

            try:
                current_ticker = self.client.get_ticker(pair)
                ref_price = float(current_ticker.bid if side == "buy" else current_ticker.ask)
            except Exception as e:
                self.log.warning(f"Could not get current price for drift check on {symbol}: {e}")
                self._cancel_orders(txids, symbol)
                return None

            drift = abs(ref_price - limit_price) / limit_price

            self.log.warning(
                f"Limit {side.upper()} order for {symbol} unfilled after "
                f"{self.limit_order_fill_timeout:.0f}s — price drift {drift:.2%} "
                f"(placed={limit_price:.6g}, now={ref_price:.6g})"
            )

            # Always cancel and repost — whether drift crossed the threshold or not —
            # so we stay at the current best price
            self._cancel_orders(txids, symbol)

            if drift <= self.limit_order_drift_pct:
                self.log.info(
                    f"Drift {drift:.2%} within threshold ({self.limit_order_drift_pct:.2%}) "
                    f"for {symbol} — reposting at refreshed price"
                )
            else:
                self.log.info(
                    f"Drift {drift:.2%} exceeded threshold ({self.limit_order_drift_pct:.2%}) "
                    f"for {symbol} — reposting at new price"
                )

        # Should not reach here, but be safe
        return None

    def _cancel_orders(self, txids: list[str], symbol: str) -> None:
        """Best-effort cancel of a list of order txids."""
        for txid in txids:
            try:
                cancelled = self.client.cancel_order(txid)
                if cancelled:
                    self.log.info(f"Cancelled order {txid} for {symbol}")
                else:
                    self.log.warning(f"Cancel returned count=0 for {txid} ({symbol}) — may already be filled/cancelled")
            except Exception as e:
                self.log.warning(f"Failed to cancel order {txid} for {symbol}: {e}")

    def execute_weighted_buy(self, symbol: str, weight: float, entry_price: float = 0.0) -> bool:
        """
        Execute a limit buy order using portfolio weight to calculate dollar amount.

        The limit price is set to the current best bid so the order rests on
        the book (maker fee). If the order doesn't fill within the timeout
        and price has drifted, it is cancelled and reposted at the new bid.

        Args:
            symbol: Ticker symbol to buy
            weight: Fraction of total portfolio equity to allocate
            entry_price: Strategy entry price for slippage tracking
        """
        self.log.info(f"Executing BUY for {symbol} (weight={weight:.4f})")

        if self.dry_run:
            self.log.info(f"[DRY RUN] Would buy {symbol} with weight {weight:.4f}")
            return True

        try:
            portfolio = self.get_portfolio()
            buying_power = portfolio.get("buying_power", 0)
            equity = portfolio.get("equity", 0)

            target_value = equity * weight
            if target_value <= 0:
                self.log.warning(f"Calculated target value for {symbol} is {target_value}, skipping")
                return False

            if target_value > buying_power:
                self.log.error(
                    f"Insufficient buying power for {symbol}: need ${target_value:.2f}, "
                    f"have ${buying_power:.2f}"
                )
                raise KrakenInsufficientFundsError(
                    message=f"Insufficient buying power for {symbol}",
                    error_code="insufficient_buying_power",
                )

            # Estimate initial volume from current quote (will be recalculated at bid on first attempt)
            current_price = self.get_quote(symbol)
            if not current_price or current_price <= 0:
                self.log.error(f"Could not get valid quote for {symbol}")
                return False

            initial_volume = round((target_value * self.FEE_BUFFER) / current_price, self.VOLUME_PRECISION)

            estimated_order_value = initial_volume * current_price
            if estimated_order_value < self.MIN_ORDER_VALUE_USD:
                self.log.warning(
                    f"Order value ${estimated_order_value:.2f} for {symbol} is below "
                    f"minimum ${self.MIN_ORDER_VALUE_USD:.2f}, skipping"
                )
                return False

            pair = symbol_to_pair(symbol)
            fill_price = self._execute_limit_order(
                pair=pair,
                side="buy",
                volume=str(initial_volume),
                symbol=symbol,
                target_value=target_value,
            )

            if fill_price is None:
                self.log.error(f"BUY limit order for {symbol} did not fill")
                return False

            if entry_price > 0:
                slippage = fill_price - entry_price
                slippage_pct = slippage / entry_price * 100
                ORDER_SLIPPAGE.labels(symbol=symbol).set(slippage)
                self.log.info(
                    f"[SLIPPAGE] {symbol}: strategy=${entry_price:.4f} "
                    f"fill=${fill_price:.4f} diff=${slippage:+.4f} ({slippage_pct:+.2f}%)"
                )

            return True

        except KrakenInsufficientFundsError:
            raise
        except Exception as e:
            self.log.error(f"Failed to execute BUY for {symbol}: {e}", exc_info=True)
            return False

    def execute_fractional_sell(self, symbol: str, fraction: float) -> bool:
        """Sell a fraction (e.g., 0.25 = 25%) of the current position via limit order."""
        self.log.info(f"Executing FRACTIONAL SELL for {symbol}: {fraction:.0%}")

        if self.dry_run:
            self.log.info(f"[DRY RUN] Would sell {fraction:.0%} of {symbol}")
            return True

        try:
            balance = self._get_asset_balance(symbol)
            if not balance or balance <= 0:
                self.log.warning(f"No balance found for {symbol}, skipping fractional sell")
                return False

            volume = round(balance * fraction, self.VOLUME_PRECISION)
            if volume <= 0:
                self.log.warning(f"Computed volume {volume} too small for {symbol}")
                return False

            pair = symbol_to_pair(symbol)
            fill_price = self._execute_limit_order(
                pair=pair,
                side="sell",
                volume=str(volume),
                symbol=symbol,
            )

            if fill_price is None:
                self.log.error(f"FRACTIONAL SELL limit order for {symbol} did not fill")
                return False

            self.log.info(
                f"FRACTIONAL SELL filled for {symbol}: {fraction:.0%} of {balance:.6f} "
                f"= {volume:.6f} @ {fill_price:.6g}"
            )
            return True

        except Exception as e:
            self.log.error(f"Failed to execute FRACTIONAL SELL for {symbol}: {e}", exc_info=True)
            return False

    def execute_sell_symbol(self, symbol: str) -> bool:
        """Execute a limit sell order for a symbol (close entire position)."""
        self.log.info(f"Executing SELL for {symbol}")

        if self.dry_run:
            self.log.info(f"[DRY RUN] Would sell all of {symbol}")
            return True

        try:
            balance = self._get_asset_balance(symbol)
            if not balance or balance <= 0:
                self.log.warning(f"No balance found for {symbol}, skipping sell")
                return False

            volume = round(balance, self.VOLUME_PRECISION)

            pair = symbol_to_pair(symbol)
            fill_price = self._execute_limit_order(
                pair=pair,
                side="sell",
                volume=str(volume),
                symbol=symbol,
            )

            if fill_price is None:
                self.log.error(f"SELL limit order for {symbol} did not fill")
                return False

            self.log.info(f"SELL filled for {symbol}: {volume:.6f} @ {fill_price:.6g}")
            return True

        except Exception as e:
            self.log.error(f"Failed to execute SELL for {symbol}: {e}", exc_info=True)
            return False
