# src/positions/positions_cache.py

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Any


@dataclass
class Position:
    id: str
    symbol: str
    quantity: float
    weight: float
    entry_price: Optional[float]
    entry_ts: Optional[str]
    current_price: Optional[float]
    updated_at: str
    bars_held: int = 0  # Number of 15-min bars since entry (for min_hold logic)
    initial_quantity: Optional[float] = None  # Original quantity at entry (for profit tiers)
    profit_tiers_taken: int = 0  # Number of profit-take tiers already executed
    peak_price: Optional[float] = None  # Highest close seen since entry (for trailing stop)


# ---- helpers ----

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _coerce_float(v: Any, field: str) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid float for {field}: {v!r}") from e

def _position_from_dict(d: dict[str, Any]) -> Position:
    # required
    for k in ("id", "symbol", "quantity", "weight"):
        if k not in d:
            raise ValueError(f"Position missing required key: {k}")

    return Position(
        id=str(d["id"]),
        symbol=str(d["symbol"]),
        quantity=_coerce_float(d["quantity"], "quantity") or 0.0,
        weight=_coerce_float(d["weight"], "weight") or 0.0,
        entry_price=_coerce_float(d.get("entry_price"), "entry_price"),
        entry_ts=(None if d.get("entry_ts") in ("", None) else str(d["entry_ts"])),
        current_price=_coerce_float(d.get("current_price"), "current_price"),
        updated_at=str(d.get("updated_at") or utc_now_iso()),
        bars_held=int(d.get("bars_held", 0)),
        initial_quantity=_coerce_float(d.get("initial_quantity"), "initial_quantity"),
        profit_tiers_taken=int(d.get("profit_tiers_taken", 0)),
        peak_price=_coerce_float(d.get("peak_price"), "peak_price"),
    )


# ---- public API ----

def load_positions(positions_path: str) -> list[Position]:
    """Load all current positions from a JSON file.
    positions.json: a list of positions data
    """
    if not os.path.exists(positions_path):
        return []

    with open(positions_path, "r", encoding="utf-8") as f:
        raw = f.read().strip()
        if not raw:
            return []
        data = json.loads(raw)

    if not isinstance(data, list):
        raise ValueError("positions JSON must be a list, or a dict with key 'positions' containing a list")

    return [_position_from_dict(item) for item in data]


def save_positions(positions: list[Position], positions_path: str) -> None:
    """Save positions to a JSON file (atomic)."""
    os.makedirs(os.path.dirname(os.path.abspath(positions_path)), exist_ok=True)

    payload = [asdict(p) for p in positions]
    dirpath = os.path.dirname(os.path.abspath(positions_path))

    with tempfile.NamedTemporaryFile("w", delete=False, dir=dirpath, encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
        tmp_path = f.name

    os.replace(tmp_path, positions_path)


CASH_SYMBOL = "$CASH$"


def init_positions(positions_path: str, max_positions: int) -> list[Position]:
    """
    Initialize positions file with cash slots if empty or missing.

    If the file exists and has positions, returns them unchanged.
    Otherwise, creates max_positions cash slots with equal weights.

    Args:
        positions_path: Path to positions JSON file
        max_positions: Number of position slots to create

    Returns:
        List of Position objects (either existing or newly created)
    """
    existing = load_positions(positions_path)
    if existing:
        return existing

    # Create cash positions with equal weights
    weight = 1.0 / max_positions
    now = utc_now_iso()

    positions = [
        Position(
            id=str(i + 1),
            symbol=CASH_SYMBOL,
            quantity=0.0,
            weight=weight,
            entry_price=None,
            entry_ts=None,
            current_price=None,
            updated_at=now,
        )
        for i in range(max_positions)
    ]

    save_positions(positions, positions_path)
    return positions
