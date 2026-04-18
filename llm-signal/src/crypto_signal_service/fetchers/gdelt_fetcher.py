from __future__ import annotations

import hashlib
import logging
import time

import requests

from .rss_fetcher import FeedItem

log = logging.getLogger(__name__)

_GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; crypto-signal-service/1.0)"}

# Single broad query — one API call, then classify locally.
# Quoted phrases are not allowed inside OR lists by GDELT; use bare terms only.
# Keep term count low — GDELT rejects queries it deems too long.
_DEFAULT_QUERY = (
    "(bitcoin OR ethereum OR crypto OR stablecoin OR defi "
    "OR inflation OR recession OR tariff OR sanctions "
    "OR regulation OR hack OR bankruptcy) "
    "sourcelang:english"
)

_TOPIC_KEYWORDS: dict[str, list[str]] = {
    "bitcoin_ethereum": [
        "bitcoin", "btc", "ethereum", "eth", "crypto price", "altcoin",
        "digital asset", "crypto market", "bull run", "bear market",
    ],
    "regulation": [
        "sec", "cftc", "regulation", "ban", "illegal", "legislation",
        "compliance", "government", "lawsuit", "enforcement", "policy",
    ],
    "macro_economics": [
        "federal reserve", "fed rate", "interest rate", "inflation", "cpi",
        "recession", "dollar", "monetary", "treasury", "yield curve",
        "gdp", "unemployment", "central bank", "quantitative easing",
    ],
    "exchange_custody": [
        "exchange", "hack", "bankruptcy", "custody", "coinbase", "binance",
        "kraken", "ftx", "collapse", "insolvency", "withdrawal", "exploit",
    ],
    "stablecoins": [
        "stablecoin", "usdt", "usdc", "tether", "peg", "depegged",
        "algorithmic stablecoin", "dai", "reserve",
    ],
    "defi_mining": [
        "defi", "nft", "web3", "staking", "mining", "hash rate", "miner",
        "yield farming", "liquidity pool", "smart contract", "protocol",
    ],
}

_PER_TOPIC_CAP = 15


def _classify_article(title: str) -> list[str]:
    title_lower = title.lower()
    return [
        topic
        for topic, keywords in _TOPIC_KEYWORDS.items()
        if any(kw in title_lower for kw in keywords)
    ]


class GDELTFetcher:
    """Fetches recent crypto/macro articles from GDELT DOC API.

    Uses a single broad query then classifies articles locally by topic,
    returning the same FeedItem type as RSSFetcher.
    """

    def __init__(self, timeout_s: float = 30.0):
        self.timeout_s = timeout_s

    def _get_with_retry(self, params: dict, retries: int = 3) -> dict | None:
        for attempt in range(retries):
            try:
                r = requests.get(
                    _GDELT_DOC_URL, params=params, headers=_HEADERS, timeout=self.timeout_s
                )
                if r.status_code == 429:
                    wait = 60
                    log.warning(
                        "GDELT rate limited (429), waiting %ds (attempt %d/%d)",
                        wait, attempt + 1, retries,
                    )
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                text = r.text.strip()
                if not text:
                    log.warning("GDELT returned empty body (attempt %d/%d), retrying", attempt + 1, retries)
                    time.sleep(15)
                    continue
                try:
                    return r.json()
                except (ValueError, requests.exceptions.JSONDecodeError):
                    log.warning(
                        "GDELT returned non-JSON (attempt %d/%d): %s",
                        attempt + 1, retries, text[:200],
                    )
                    time.sleep(15)
                    continue
            except requests.exceptions.HTTPError as exc:
                log.warning("GDELT HTTP error %s (attempt %d/%d)", exc, attempt + 1, retries)
                if attempt < retries - 1:
                    time.sleep(15)
                else:
                    return None
            except Exception:
                log.warning("GDELT fetch failed (attempt %d/%d)", attempt + 1, retries, exc_info=True)
                if attempt < retries - 1:
                    time.sleep(10)
        log.warning("GDELT exhausted %d retries, skipping", retries)
        return None

    def _make_signal_id(self, url: str, title: str) -> str:
        return hashlib.sha256(f"{url}{title}".encode()).hexdigest()[:16]

    def fetch(
        self,
        query: str = _DEFAULT_QUERY,
        max_records: int = 250,
        timespan: str = "2h",
    ) -> list[FeedItem]:
        """Fetch articles from GDELT with a single query, classify by topic locally.

        Args:
            query: GDELT full-text search query (defaults to broad crypto/macro query).
            max_records: Maximum articles to fetch from GDELT (cap 250).
            timespan: Lookback window, e.g. "1h", "2h", "72h".
        """
        params = {
            "query": query,
            "mode": "ArtList",
            "maxrecords": str(max_records),
            "timespan": timespan,
            "format": "json",
            "sort": "DateDesc",
        }
        data = self._get_with_retry(params)
        if data is None:
            return []

        articles = data.get("articles") or []

        # Classify into per-topic buckets, cap each, then flatten (dedup by signal_id)
        buckets: dict[str, list[FeedItem]] = {topic: [] for topic in _TOPIC_KEYWORDS}
        seen_ids: set[str] = set()
        items: list[FeedItem] = []

        for article in articles:
            url = article.get("url") or ""
            title = article.get("title") or ""
            if not title:
                continue

            signal_id = self._make_signal_id(url, title)
            topics = _classify_article(title)

            for topic in topics:
                if len(buckets[topic]) < _PER_TOPIC_CAP:
                    buckets[topic].append(
                        FeedItem(
                            signal_id=signal_id,
                            title=title,
                            summary=topic,
                            pub_ts=article.get("seendate"),
                            source_url=url,
                        )
                    )

            if topics and signal_id not in seen_ids:
                seen_ids.add(signal_id)
                items.append(FeedItem(
                    signal_id=signal_id,
                    title=title,
                    summary=topics[0],
                    pub_ts=article.get("seendate"),
                    source_url=url,
                ))

        log.debug(
            "GDELT returned %d articles, %d matched topics (timespan=%s)",
            len(articles), len(items), timespan,
        )
        for topic, bucket in buckets.items():
            log.debug("  %s: %d articles", topic, len(bucket))

        return items
