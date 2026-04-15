# src/kraken_api/__init__.py

"""
Kraken REST API client module.

Provides a Python interface to the Kraken cryptocurrency exchange API
for balance queries, ticker data, and order placement.
"""

from .client import (
    KrakenApiClient,
    symbol_to_pair,
    kraken_asset_to_symbol,
    symbol_to_kraken_asset,
    FIAT_ASSETS,
)

from .models import (
    KrakenBalance,
    KrakenTradeBalance,
    KrakenTicker,
    KrakenOrderResponse,
    KrakenOrderInfo,
    safe_decimal,
)

from .exceptions import (
    KrakenApiError,
    KrakenAuthError,
    KrakenRateLimitError,
    KrakenInsufficientFundsError,
    KrakenOrderError,
    KrakenInvalidPairError,
)

__all__ = [
    "KrakenApiClient",
    "symbol_to_pair",
    "kraken_asset_to_symbol",
    "symbol_to_kraken_asset",
    "FIAT_ASSETS",
    "KrakenBalance",
    "KrakenTradeBalance",
    "KrakenTicker",
    "KrakenOrderResponse",
    "KrakenOrderInfo",
    "safe_decimal",
    "KrakenApiError",
    "KrakenAuthError",
    "KrakenRateLimitError",
    "KrakenInsufficientFundsError",
    "KrakenOrderError",
    "KrakenInvalidPairError",
]
