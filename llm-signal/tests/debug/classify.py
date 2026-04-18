"""Debug script: fetch live GDELT items and run them through the LLM classifier."""
import json
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from crypto_signal_service.config import Config
from crypto_signal_service.fetchers.gdelt_fetcher import GDELTFetcher

from crypto_signal_service.llm_classifier import LLMClassifier, _SYSTEM_PROMPT

cfg = Config.from_env()

gdelt = GDELTFetcher(timeout_s=cfg.rss_timeout_s)
items = gdelt.fetch(
    query=cfg.gdelt_query,
    max_records=5,
    timespan=cfg.gdelt_timespan,
)
print(f"Fetched {len(items)} items from GDELT\n")

classifier = LLMClassifier(
    api_key=cfg.llm_api_key,
    base_url=cfg.llm_api_base_url,
    model=cfg.llm_model,
    timeout_s=cfg.llm_timeout_s,
)

for item in items:
    print("=" * 60)
    print(f"URL: {item.source_url}")
    print()

    article_text = classifier._fetch_article(item.source_url)
    body = article_text if article_text else item.summary[:500]
    user_content = f"Headline: {item.title}\nArticle: {body}"

    print("--- SYSTEM PROMPT ---")
    print(_SYSTEM_PROMPT)
    print()
    print("--- USER MESSAGE ---")
    print(user_content)
    print()

    signal = classifier.classify(item)

    print("--- MODEL OUTPUT ---")
    if signal:
        print(json.dumps({
            "direction": signal.direction,
            "event_type": signal.event_type,
            "asset_scope": signal.asset_scope,
            "affected_symbols": signal.affected_symbols,
            "time_horizon": signal.time_horizon,
            "confidence": round(signal.confidence, 2),
            "novelty": round(signal.novelty, 2),
            "tradability": round(signal.tradability, 2),
            "catalyst_score": round(signal.catalyst_score, 4),
            "key_reason": signal.key_reason,
        }, indent=2))
    else:
        print("[classification failed — check logs above]")
    print()
