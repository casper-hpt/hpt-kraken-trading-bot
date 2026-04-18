from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass

import feedparser

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FeedItem:
    signal_id: str    # sha256(url + title)[:16]
    title: str
    summary: str
    pub_ts: str | None
    source_url: str


class RSSFetcher:
    def __init__(self, timeout_s: float = 15.0):
        self.timeout_s = timeout_s

    def _make_signal_id(self, url: str, title: str) -> str:
        return hashlib.sha256(f"{url}{title}".encode()).hexdigest()[:16]

    def fetch_feed(self, url: str) -> list[FeedItem]:
        try:
            feed = feedparser.parse(
                url,
                request_headers={"User-Agent": "crypto-signal-service/1.0"},
            )
        except Exception:
            log.warning("Failed to fetch feed: %s", url, exc_info=True)
            return []

        items: list[FeedItem] = []
        for entry in feed.entries:
            link = getattr(entry, "link", "") or ""
            title = getattr(entry, "title", "") or ""
            summary = (
                getattr(entry, "summary", "")
                or getattr(entry, "description", "")
                or ""
            )
            pub_ts = getattr(entry, "published", None)

            if not title:
                continue

            items.append(FeedItem(
                signal_id=self._make_signal_id(link, title),
                title=title,
                summary=summary,
                pub_ts=pub_ts,
                source_url=link,
            ))

        return items

    def fetch_all(self, feed_urls: list[str]) -> list[FeedItem]:
        seen: set[str] = set()
        result: list[FeedItem] = []
        for url in feed_urls:
            for item in self.fetch_feed(url):
                if item.signal_id not in seen:
                    seen.add(item.signal_id)
                    result.append(item)
        return result
