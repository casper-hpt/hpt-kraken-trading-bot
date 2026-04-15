from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WatchItem:
    symbol: str
    raw: dict[str, Any]


def load_watchlist(path: str | Path) -> list[WatchItem]:
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))

    if not isinstance(data, dict):
        raise ValueError("watchlist.json must be an object/dict keyed by symbol")

    items: list[WatchItem] = []
    for k, v in data.items():
        if isinstance(v, dict) and "symbol" in v and isinstance(v["symbol"], str):
            sym = v["symbol"].strip().upper()
        else:
            # fallback: use key
            sym = str(k).strip().upper()
            if not sym:
                continue
            v = v if isinstance(v, dict) else {"symbol": sym}

        items.append(WatchItem(symbol=sym, raw=v))
    # stable order
    items.sort(key=lambda x: x.symbol)
    return items


def symbols_from_watchlist(path: str | Path) -> list[str]:
    return [wi.symbol for wi in load_watchlist(path)]
