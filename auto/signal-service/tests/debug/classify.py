"""Debug script: fetch live GDELT items and run them through the LLM classifier."""
import json
import time
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from crypto_signal_service.config import Config
from crypto_signal_service.fetchers.gdelt_fetcher import GDELTFetcher
from crypto_signal_service.llm_classifier import LLMClassifier
from crypto_signal_service.fetchers.rss_fetcher import FeedItem

_CACHE = Path(__file__).parent / ".gdelt_cache.json"
_CACHE_TTL = 1800  # 30 minutes


def _load_cached() -> list[FeedItem] | None:
    if not _CACHE.exists():
        return None
    if time.time() - _CACHE.stat().st_mtime > _CACHE_TTL:
        return None
    data = json.loads(_CACHE.read_text())
    return [FeedItem(**d) for d in data]


def _save_cache(items: list[FeedItem]) -> None:
    _CACHE.write_text(json.dumps([i.__dict__ for i in items], indent=2))


cfg = Config.from_env()

cached = _load_cached()
if cached is not None:
    print(f"[cache hit] Using {len(cached)} cached GDELT items (delete {_CACHE.name} to refresh)\n")
    items = cached
else:
    gdelt = GDELTFetcher(timeout_s=cfg.rss_timeout_s)
    items = gdelt.fetch(
        query=cfg.gdelt_query,
        max_records=5,
        timespan=cfg.gdelt_timespan,
    )
    _save_cache(items)
    print(f"Fetched {len(items)} items from GDELT\n")

classifier = LLMClassifier(
    api_key=cfg.llm_api_key,
    base_url=cfg.llm_api_base_url,
    model=cfg.llm_model,
    timeout_s=cfg.llm_timeout_s,
)

print("=" * 60)
for item in items:
    print(f"HEADLINE: {item.title}")
    signal = classifier.classify(item)
    if signal:
        print(f"  direction:    {signal.direction}")
        print(f"  event_type:   {signal.event_type}")
        print(f"  asset_scope:  {signal.asset_scope}")
        print(f"  symbols:      {signal.affected_symbols}")
        print(f"  time_horizon: {signal.time_horizon}")
        print(f"  confidence:   {signal.confidence:.2f}")
        print(f"  novelty:      {signal.novelty:.2f}")
        print(f"  tradability:  {signal.tradability:.2f}")
        print(f"  catalyst:     {signal.catalyst_score:.4f}")
        print(f"  reason:       {signal.key_reason}")
    else:
        print("  [classification failed]")
    print()
