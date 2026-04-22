# src/kraken_api/models.py

import logging
import re
from decimal import Decimal, InvalidOperation
from typing import Optional

from pydantic import BaseModel, Field

_log = logging.getLogger(__name__)


def safe_decimal(value, default: Decimal = Decimal("0")) -> Decimal:
    """
    Safely convert a value to Decimal, returning default on failure.

    Handles: None, NaN, Infinity, currency symbols, commas, empty strings,
    and any other value that Decimal() can't parse.
    """
    if value is None:
        return default

    raw = str(value).strip()

    if not raw or raw.lower() in ("nan", "inf", "-inf", "infinity", "-infinity", "n/a", "null", "none", "--"):
        _log.warning(f"safe_decimal: unparseable value '{raw}', using default {default}")
        return default

    cleaned = re.sub(r"[^\d.\-+eE]", "", raw)

    if not cleaned:
        _log.warning(f"safe_decimal: no numeric content in '{raw}', using default {default}")
        return default

    try:
        result = Decimal(cleaned)
        if result.is_nan() or result.is_infinite():
            _log.warning(f"safe_decimal: parsed to {result} from '{raw}', using default {default}")
            return default
        return result
    except (InvalidOperation, ValueError, ArithmeticError):
        _log.warning(f"safe_decimal: failed to parse '{raw}' (cleaned: '{cleaned}'), using default {default}")
        return default


class KrakenBalance(BaseModel):
    """Parsed account balance from Kraken /0/private/Balance."""
    assets: dict[str, Decimal] = Field(default_factory=dict)

    @classmethod
    def from_api_response(cls, result: dict) -> "KrakenBalance":
        """Parse the 'result' dict from Kraken Balance endpoint."""
        assets = {}
        for asset_key, amount_str in result.items():
            amount = safe_decimal(amount_str)
            if amount > 0:
                assets[asset_key] = amount
        return cls(assets=assets)


class KrakenTradeBalance(BaseModel):
    """Parsed trade balance from Kraken /0/private/TradeBalance."""
    equivalent_balance: Decimal = Field(Decimal("0"), alias="eb")
    trade_balance: Decimal = Field(Decimal("0"), alias="tb")
    margin: Decimal = Field(Decimal("0"), alias="m")
    unrealized_pnl: Decimal = Field(Decimal("0"), alias="n")
    cost_basis: Decimal = Field(Decimal("0"), alias="c")
    current_value: Decimal = Field(Decimal("0"), alias="v")
    equity: Decimal = Field(Decimal("0"), alias="e")
    free_margin: Decimal = Field(Decimal("0"), alias="mf")

    class Config:
        populate_by_name = True

    @classmethod
    def from_api_response(cls, result: dict) -> "KrakenTradeBalance":
        """Parse the 'result' dict from Kraken TradeBalance endpoint."""
        return cls(
            eb=safe_decimal(result.get("eb")),
            tb=safe_decimal(result.get("tb")),
            m=safe_decimal(result.get("m")),
            n=safe_decimal(result.get("n")),
            c=safe_decimal(result.get("c")),
            v=safe_decimal(result.get("v")),
            e=safe_decimal(result.get("e")),
            mf=safe_decimal(result.get("mf")),
        )


class KrakenTicker(BaseModel):
    """Parsed ticker data from Kraken /0/public/Ticker."""
    pair: str
    ask: Optional[Decimal] = None
    bid: Optional[Decimal] = None
    last: Optional[Decimal] = None
    volume_today: Optional[Decimal] = None

    @classmethod
    def from_api_response(cls, pair_key: str, data: dict) -> "KrakenTicker":
        """
        Parse ticker data for a single pair.

        Kraken ticker fields are arrays:
          a = [price, whole_lot_volume, lot_volume]  (ask)
          b = [price, whole_lot_volume, lot_volume]  (bid)
          c = [price, lot_volume]                     (last trade closed)
          v = [today, last_24h]                       (volume)
        """
        ask = safe_decimal(data.get("a", [None])[0]) if data.get("a") else None
        bid = safe_decimal(data.get("b", [None])[0]) if data.get("b") else None
        last = safe_decimal(data.get("c", [None])[0]) if data.get("c") else None
        volume = safe_decimal(data.get("v", [None])[0]) if data.get("v") else None

        return cls(
            pair=pair_key,
            ask=ask if ask and ask > 0 else None,
            bid=bid if bid and bid > 0 else None,
            last=last if last and last > 0 else None,
            volume_today=volume if volume and volume > 0 else None,
        )


class KrakenOrderResponse(BaseModel):
    """Parsed response from Kraken /0/private/AddOrder."""
    txids: list[str] = Field(default_factory=list)
    description: str = ""

    @classmethod
    def from_api_response(cls, result: dict) -> "KrakenOrderResponse":
        """Parse the 'result' dict from Kraken AddOrder endpoint."""
        txids = result.get("txid", [])
        descr = result.get("descr", {})
        order_desc = descr.get("order", "") if isinstance(descr, dict) else str(descr)
        return cls(txids=txids, description=order_desc)


class KrakenOrderInfo(BaseModel):
    """Parsed order info from Kraken /0/private/QueryOrders."""
    status: str = ""
    price: Optional[Decimal] = None
    vol_exec: Optional[Decimal] = None
    cost: Optional[Decimal] = None
    fee: Optional[Decimal] = None

    @classmethod
    def from_api_response(cls, data: dict) -> "KrakenOrderInfo":
        """Parse a single order entry from QueryOrders result."""
        return cls(
            status=data.get("status", ""),
            price=safe_decimal(data.get("price")) or None,
            vol_exec=safe_decimal(data.get("vol_exec")) or None,
            cost=safe_decimal(data.get("cost")) or None,
            fee=safe_decimal(data.get("fee")) or None,
        )
