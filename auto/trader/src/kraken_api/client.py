# src/kraken_api/client.py

import logging
import time
import urllib.parse
from typing import Optional

import httpx

from .auth import get_kraken_signature
from .exceptions import (
    KrakenApiError,
    KrakenAuthError,
    KrakenRateLimitError,
    KrakenInsufficientFundsError,
    KrakenOrderError,
    KrakenInvalidPairError,
)
from .models import (
    KrakenBalance,
    KrakenTradeBalance,
    KrakenTicker,
    KrakenOrderResponse,
    KrakenOrderInfo,
)

logger = logging.getLogger(__name__)


# ── Symbol mapping ────────────────────────────────────────────────────────────
# Standard tickers (BTC, ETH, SOL). Kraken uses its own naming.

# Standard symbol → Kraken trading pair (against USD)
SYMBOL_TO_PAIR: dict[str, str] = {
    "BTC": "XBTUSD",
    "DOGE": "XDGUSD",
}

# Kraken balance asset key → standard symbol
KRAKEN_ASSET_TO_SYMBOL: dict[str, str] = {
    "XXBT": "BTC",
    "XBT": "BTC",
    "XETH": "ETH",
    "XXDG": "DOGE",
    "ZUSD": "USD",
    "USD": "USD",
}

# Fiat / stablecoin asset keys to skip when listing crypto positions
FIAT_ASSETS = {"ZUSD", "USD", "ZEUR", "EUR", "ZGBP", "GBP", "ZCAD", "CAD", "ZJPY", "JPY", "USDT", "USDC"}


def symbol_to_pair(symbol: str) -> str:
    """Convert a standard ticker symbol to a Kraken trading pair."""
    return SYMBOL_TO_PAIR.get(symbol.upper(), f"{symbol.upper()}USD")


def kraken_asset_to_symbol(asset_key: str) -> str:
    """Convert a Kraken balance asset key to a standard ticker symbol."""
    # Check explicit mapping first
    if asset_key in KRAKEN_ASSET_TO_SYMBOL:
        return KRAKEN_ASSET_TO_SYMBOL[asset_key]
    # Strip leading X/Z prefix for 4-char keys (legacy naming: XSOL → SOL)
    if len(asset_key) == 4 and asset_key[0] in ("X", "Z") and asset_key not in FIAT_ASSETS:
        return asset_key[1:]
    # Passthrough for modern keys (SOL, DOT, ADA, etc.)
    return asset_key


def symbol_to_kraken_asset(symbol: str) -> list[str]:
    """Return possible Kraken balance keys for a standard symbol."""
    reverse: dict[str, list[str]] = {}
    for k, v in KRAKEN_ASSET_TO_SYMBOL.items():
        reverse.setdefault(v, []).append(k)
    if symbol.upper() in reverse:
        return reverse[symbol.upper()]
    # Fallback: try the symbol itself and X-prefixed version
    s = symbol.upper()
    return [s, f"X{s}"]


class KrakenApiClient:
    """Client for interacting with the Kraken REST API.

    Uses a pre-provisioned API Key (api_key + api_secret) for
    HMAC-SHA512 signed private requests.
    """

    BASE_URL = "https://api.kraken.com"

    def __init__(self, api_key: str, api_secret: str):
        self._http_client = httpx.Client(timeout=30.0)
        self._api_key = api_key
        self._api_secret = api_secret
        # Monotonic nonce counter (microsecond timestamp, always increasing)
        self._last_nonce = 0

    def close(self) -> None:
        """Close the HTTP client and release resources."""
        if self._http_client:
            self._http_client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ── Public endpoints ──────────────────────────────────────────────────

    def get_ticker(self, pair: str) -> KrakenTicker:
        """
        Get ticker information for a trading pair.

        Args:
            pair: Kraken trading pair (e.g. "XBTUSD", "SOLUSD")

        Returns:
            KrakenTicker with ask, bid, last, volume
        """
        url = f"{self.BASE_URL}/0/public/Ticker"
        params = {"pair": pair}

        try:
            response = self._http_client.get(url, params=params)
            data = response.json()
        except httpx.RequestError as e:
            raise KrakenApiError(
                message=f"Network error fetching ticker: {e}",
                error_code="network_error",
            )

        self._check_errors(data)

        result = data.get("result", {})
        if not result:
            raise KrakenInvalidPairError(
                message=f"No ticker data returned for pair {pair}",
                error_code="no_ticker_data",
            )

        # Kraken may return the pair under a different key than requested
        pair_key = next(iter(result))
        return KrakenTicker.from_api_response(pair_key, result[pair_key])

    # ── Private endpoints ─────────────────────────────────────────────────

    def _get_nonce(self) -> int:
        """Generate a strictly increasing nonce (microsecond timestamp)."""
        nonce = int(time.time() * 1_000_000)
        if nonce <= self._last_nonce:
            nonce = self._last_nonce + 1
        self._last_nonce = nonce
        return nonce

    def _private_request(self, endpoint: str, data: Optional[dict] = None) -> dict:
        """
        Make an authenticated request to a Kraken private endpoint.

        Uses API Key + HMAC-SHA512 signing (API-Key + API-Sign headers).

        Args:
            endpoint: API path (e.g. "/0/private/Balance")
            data: Additional POST parameters

        Returns:
            The 'result' dict from the Kraken response

        Raises:
            KrakenApiError: On API errors
        """
        if data is None:
            data = {}

        # Add nonce to request data
        data["nonce"] = self._get_nonce()

        # Compute HMAC-SHA512 signature
        signature = get_kraken_signature(endpoint, data, self._api_secret)

        headers = {
            "API-Key": self._api_key,
            "API-Sign": signature,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        url = f"{self.BASE_URL}{endpoint}"

        try:
            response = self._http_client.post(
                url,
                headers=headers,
                content=urllib.parse.urlencode(data),
            )
            resp_data = response.json()
        except httpx.RequestError as e:
            raise KrakenApiError(
                message=f"Network error: {e}",
                error_code="network_error",
            )

        self._check_errors(resp_data)
        return resp_data.get("result", {})

    def get_balance(self) -> KrakenBalance:
        """Get account balances for all assets."""
        result = self._private_request("/0/private/Balance")
        return KrakenBalance.from_api_response(result)

    def get_trade_balance(self, asset: str = "ZUSD") -> KrakenTradeBalance:
        """
        Get trade balance summary (equity, free margin, etc.).

        Args:
            asset: Base asset for balance calculation (default USD)
        """
        result = self._private_request("/0/private/TradeBalance", {"asset": asset})
        return KrakenTradeBalance.from_api_response(result)

    def add_order(
        self,
        pair: str,
        side: str,
        ordertype: str,
        volume: str,
        price: Optional[str] = None,
        validate: bool = False,
    ) -> KrakenOrderResponse:
        """
        Place an order on Kraken.

        Args:
            pair: Trading pair (e.g. "XBTUSD")
            side: "buy" or "sell"
            ordertype: "market", "limit", "stop-loss", etc.
            volume: Order volume in base currency
            price: Limit price (required for limit orders)
            validate: If True, validate only without placing

        Returns:
            KrakenOrderResponse with txids and description
        """
        data = {
            "pair": pair,
            "type": side,
            "ordertype": ordertype,
            "volume": volume,
        }
        if price is not None:
            data["price"] = price
        if validate:
            data["validate"] = "true"

        result = self._private_request("/0/private/AddOrder", data)
        return KrakenOrderResponse.from_api_response(result)

    def cancel_order(self, txid: str) -> bool:
        """
        Cancel an open order by transaction ID.

        Args:
            txid: Transaction ID returned by add_order

        Returns:
            True if one or more orders were successfully cancelled
        """
        result = self._private_request("/0/private/CancelOrder", {"txid": txid})
        return int(result.get("count", 0)) > 0

    def query_orders(self, txids: list[str]) -> dict[str, KrakenOrderInfo]:
        """
        Query order info by transaction IDs.

        Args:
            txids: List of transaction IDs from AddOrder

        Returns:
            Dict mapping txid → KrakenOrderInfo
        """
        result = self._private_request(
            "/0/private/QueryOrders",
            {"txid": ",".join(txids)},
        )
        orders = {}
        for txid, order_data in result.items():
            if isinstance(order_data, dict):
                orders[txid] = KrakenOrderInfo.from_api_response(order_data)
        return orders

    # ── Error handling ────────────────────────────────────────────────────

    @staticmethod
    def _check_errors(response_data: dict) -> None:
        """
        Check the Kraken response for errors and raise appropriate exceptions.

        Kraken returns errors as: {"error": ["ECategory:Message"], "result": {...}}
        """
        errors = response_data.get("error", [])
        if not errors:
            return

        error_str = "; ".join(errors)

        # Classify the error
        for err in errors:
            err_upper = err.upper()

            if "EAPI:INVALID KEY" in err_upper or "EAPI:INVALID SIGNATURE" in err_upper:
                raise KrakenAuthError(message=error_str, error_code="auth_error")

            if "EAPI:INVALID TOKEN" in err_upper or "EGENERAL:PERMISSION DENIED" in err_upper:
                raise KrakenAuthError(message=error_str, error_code="oauth_token_invalid")

            if "EAPI:INVALID NONCE" in err_upper:
                raise KrakenAuthError(message=error_str, error_code="invalid_nonce")

            if "EAPI:RATE LIMIT" in err_upper or "EGeneral:Too many requests" in err_upper:
                raise KrakenRateLimitError(message=error_str, error_code="rate_limit")

            if "EORDER:INSUFFICIENT FUNDS" in err_upper or "EOrder:Insufficient funds" in err:
                raise KrakenInsufficientFundsError(message=error_str, error_code="insufficient_funds")

            if "EORDER:" in err_upper:
                raise KrakenOrderError(message=error_str, error_code="order_error")

            if "EGeneral:Unknown asset pair" in err or "EQuery:Unknown asset pair" in err:
                raise KrakenInvalidPairError(message=error_str, error_code="invalid_pair")

        # Generic fallback
        raise KrakenApiError(message=error_str, error_code="api_error")
