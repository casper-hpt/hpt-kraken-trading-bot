from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WatchItem:
    symbol: str


def load_watchlist(path: str | Path) -> list[WatchItem]:
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))

    if not isinstance(data, list):
        raise ValueError("watchlist.json must be a JSON array of symbol strings")

    items: list[WatchItem] = []
    for entry in data:
        sym = str(entry).strip().upper()
        if sym:
            items.append(WatchItem(symbol=sym))

    items.sort(key=lambda x: x.symbol)
    return items


def symbols_from_watchlist(path: str | Path) -> list[str]:
    return [wi.symbol for wi in load_watchlist(path)]
