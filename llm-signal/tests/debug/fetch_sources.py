"""Debug script: fetch live items from RSS feeds and GDELT and print results."""
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from crypto_signal_service.fetchers.rss_fetcher import RSSFetcher
from crypto_signal_service.fetchers.gdelt_fetcher import GDELTFetcher
from crypto_signal_service.config import Config

cfg = Config.from_env()

print("=" * 60)
print("RSS FEEDS")
print("=" * 60)
rss = RSSFetcher(timeout_s=cfg.rss_timeout_s)
rss_items = rss.fetch_all(list(cfg.rss_feed_urls))
print(f"Total: {len(rss_items)} items\n")
for item in rss_items[:5]:
    print(f"  [{item.signal_id}] {item.title[:80]}")
    if item.pub_ts:
        print(f"           published: {item.pub_ts}")
    print(f"           url: {item.source_url[:80]}")
    print()

print("=" * 60)
print("GDELT")
print("=" * 60)
gdelt = GDELTFetcher(timeout_s=cfg.rss_timeout_s)
gdelt_items = gdelt.fetch(
    query=cfg.gdelt_query,
    max_records=cfg.gdelt_max_records,
    timespan=cfg.gdelt_timespan,
)
print(f"Total: {len(gdelt_items)} items\n")
for item in gdelt_items[:5]:
    print(f"  [{item.signal_id}] {item.title[:80]}")
    if item.pub_ts:
        print(f"           seen: {item.pub_ts}")
    print(f"           url: {item.source_url[:80]}")
    print()
