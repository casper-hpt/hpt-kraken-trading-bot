import json

import requests

from .base import register_tool


@register_tool(
    name="get_bitcoin_price",
    description="Get the current price of Bitcoin in USD",
    parameters={"type": "object", "properties": {}, "required": []},
)
def get_bitcoin_price() -> str:
    """Fetch the current Bitcoin price in USD from CoinGecko."""
    resp = requests.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={"ids": "bitcoin", "vs_currencies": "usd"},
        timeout=10,
    )
    resp.raise_for_status()
    price = resp.json()["bitcoin"]["usd"]
    return json.dumps({"price_usd": price})
