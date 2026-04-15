"""Crypto event classifier.

Fetches crypto-relevant news from GDELT, classifies each headline into a
structured event object using the local Ollama model, persists events to
QuestDB, and publishes them to a Kafka topic for consumption by the trader.

Event types tracked:
  regulation          ETF approvals, stablecoin laws, exchange enforcement
  institutional_flow  ETF flows, treasury buys/sells, custody announcements
  stablecoin_liquidity Stablecoin supply expansion/contraction
  token_supply        Unlocks, vesting cliffs, emissions changes, burns
  protocol_upgrade    Chain upgrades, hard forks, validator issues
  hack_exploit        Hacks, bridge failures, exchange freezes
  narrative_rotation  AI coins, memes, L2s, DePIN, restaking narratives
  macro               Macro events with broad crypto market impact
  other               Anything that doesn't fit the above
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

import requests

from ..config import OLLAMA_MODEL, OLLAMA_URL, REQUEST_TIMEOUT_S
from ..kafka_producer import publish_signal
from ..metrics import KAFKA_PUBLISH_ERRORS_TOTAL

logger = logging.getLogger(__name__)

# ── GDELT ────────────────────────────────────────────────────────────────────

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; hpt-llm/1.0)"}

# Single broad crypto query — one API call
CRYPTO_QUERY = (
    "(bitcoin OR ethereum OR cryptocurrency OR crypto OR stablecoin OR defi "
    "OR NFT OR blockchain OR SEC crypto OR CFTC crypto OR ETF approval OR "
    "crypto ETF OR token unlock OR crypto hack OR crypto regulation OR "
    "crypto law OR staking rules OR crypto exchange OR DePIN OR restaking OR "
    "layer2 OR rollup OR crypto treasury OR Coinbase OR Binance OR Kraken "
    "OR crypto ban OR crypto arrest OR crypto sanction) "
    "sourcelang:english"
)

# Keyword-based pre-classification into event buckets for context
CRYPTO_TOPIC_KEYWORDS: dict[str, list[str]] = {
    "regulation": [
        "sec", "cftc", "etf", "regulation", "law", "bill", "ban", "approve",
        "approval", "enforcement", "lawsuit", "compliance", "stablecoin law",
        "exchange license", "delisting", "sanctions", "legal", "court",
    ],
    "institutional_flow": [
        "etf inflow", "etf outflow", "treasury", "institutional", "custody",
        "exchange listing", "integration", "grayscale", "blackrock", "fidelity",
        "fund", "hedge fund", "asset manager", "company buy", "company buys",
    ],
    "stablecoin_liquidity": [
        "stablecoin", "usdt", "usdc", "dai", "liquidity", "supply expansion",
        "depeg", "mint", "burn", "tether", "circle",
    ],
    "token_supply": [
        "unlock", "vesting", "emission", "treasury sale", "airdrop", "token burn",
        "incentive", "distribution", "cliff", "release schedule",
    ],
    "protocol_upgrade": [
        "upgrade", "hard fork", "soft fork", "validator", "bridge", "layer2",
        "l2", "rollup", "fees", "revenue", "eip", "mainnet", "testnet",
        "migration", "merge", "consensus",
    ],
    "hack_exploit": [
        "hack", "exploit", "breach", "stolen", "attack", "rug pull", "scam",
        "freeze", "laundering", "bridge attack", "drained", "vulnerability",
        "security incident",
    ],
    "narrative_rotation": [
        "ai coin", "ai token", "memecoin", "meme coin", "depin", "restaking",
        "narrative", "trend", "season", "hype", "altcoin season", "meta",
    ],
    "macro": [
        "federal reserve", "interest rate", "inflation", "cpi", "recession",
        "dollar", "risk-off", "risk-on", "geopolitical", "war", "tariff",
        "macro",
    ],
}

MAX_ARTICLES = 150
CLASSIFIER_BATCH_SIZE = 20  # headlines per LLM call


def _fetch_crypto_news(timespan: str = "4h") -> list[dict]:
    """Fetch crypto headlines from GDELT with retry on rate-limit."""
    params = {
        "query": CRYPTO_QUERY,
        "mode": "ArtList",
        "maxrecords": str(MAX_ARTICLES),
        "timespan": timespan,
        "format": "json",
        "sort": "DateDesc",
    }
    for attempt in range(3):
        try:
            resp = requests.get(
                GDELT_DOC_API, params=params, headers=GDELT_HEADERS, timeout=30
            )
            if resp.status_code == 429:
                wait = 60
                logger.warning("GDELT rate-limited; waiting %ds (attempt %d/3)", wait, attempt + 1)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            if not resp.text.strip():
                return []
            data = resp.json()
            articles = data.get("articles", [])
            logger.info("GDELT returned %d crypto articles (timespan=%s)", len(articles), timespan)
            return articles
        except requests.exceptions.JSONDecodeError:
            logger.warning("GDELT returned non-JSON for crypto query")
            return []
        except Exception:
            logger.exception("GDELT crypto query failed (attempt %d/3)", attempt + 1)
            if attempt < 2:
                time.sleep(10)
    return []


def _pre_classify(title: str) -> str:
    """Quick keyword-based topic hint — used to give the LLM context."""
    title_lower = title.lower()
    for topic, keywords in CRYPTO_TOPIC_KEYWORDS.items():
        if any(kw in title_lower for kw in keywords):
            return topic
    return "other"


def _build_classifier_prompt(headlines: list[dict]) -> str:
    """Build the Ollama classification prompt for a batch of headlines."""
    headlines_text = "\n".join(
        f"{i+1}. [{item['topic_hint']}] {item['title']}"
        for i, item in enumerate(headlines)
    )

    return f"""\
You are a crypto market event classifier. Classify each headline below into a structured JSON event.

## Event types:
- regulation: ETF approvals/rejections, stablecoin laws, exchange enforcement, token listings/delistings
- institutional_flow: ETF inflows/outflows, treasury buys/sells, custody, fund integrations
- stablecoin_liquidity: Stablecoin supply changes, depegs, USDC/USDT/DAI events
- token_supply: Unlocks, vesting, emissions changes, burns, airdrops
- protocol_upgrade: Chain upgrades, forks, validator issues, L2 launches
- hack_exploit: Hacks, exploits, bridge attacks, exchange freezes, sanctions
- narrative_rotation: New sector trends (AI coins, memes, DePIN, restaking)
- macro: Broad macro events affecting crypto (Fed, inflation, geopolitical)
- other: Does not fit above categories

## Output:
Return ONLY a valid JSON array — no markdown, no explanation.
One object per headline, in the same order as the input.

Each object must have exactly these fields:
{{
  "event_type": "<one of the event types above>",
  "asset_scope": "<specific ticker like BTC or ETH, or sector name, or market-wide>",
  "affected_symbols": ["<ticker>"],
  "time_horizon": "<intraday | 1-7d | 1-4w | structural>",
  "direction": "<bullish | bearish | mixed | neutral>",
  "confidence": <0.0–1.0>,
  "novelty": <0.0–1.0>,
  "tradability": <0.0–1.0>,
  "persistence": <0.0–1.0>,
  "key_reason": "<one sentence why this is bullish/bearish/mixed/neutral>",
  "headline": "<original headline text>"
}}

## Scoring rules:
- confidence: 0.8+ only for clear, unambiguous events (confirmed hack, ETF approval, major unlock)
- novelty: 0.8+ for genuinely new information; 0.2 if it looks like old news recycled
- tradability: 0.8+ for concrete, actionable events; <0.4 for vague sentiment/speculation
- persistence: 0.8+ for structural changes (regulation, major upgrade); 0.2 for noise
- affected_symbols: only symbols you are confident about — empty array if unclear
- asset_scope: use a specific ticker if the event clearly targets one asset, otherwise "market-wide" or "sector"

## Headlines to classify:
{headlines_text}
"""


def _call_ollama(prompt: str) -> list[dict]:
    """Call Ollama to classify headlines. Returns list of event dicts."""
    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
                "format": "json",
                "options": {"temperature": 0.1},
            },
            timeout=REQUEST_TIMEOUT_S,
        )
        response.raise_for_status()
        content = response.json()["message"]["content"].strip()

        # Ollama with format=json always returns a JSON string; parse it
        parsed = json.loads(content)

        # The model may return the array directly or wrap it in a key
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            # Try common wrapper keys
            for key in ("events", "results", "classifications", "articles"):
                if isinstance(parsed.get(key), list):
                    return parsed[key]
            # Single event returned as object — wrap it
            if "event_type" in parsed:
                return [parsed]

        logger.warning("Unexpected Ollama response shape: %s", type(parsed).__name__)
        return []

    except json.JSONDecodeError as e:
        logger.warning("Ollama returned non-JSON: %s", e)
        return []
    except Exception:
        logger.exception("Ollama classification call failed")
        return []


def _validate_event(raw: dict, now_iso: str) -> dict | None:
    """Validate and normalise a raw LLM event dict. Returns None if invalid."""
    VALID_EVENT_TYPES = {
        "regulation", "institutional_flow", "stablecoin_liquidity", "token_supply",
        "protocol_upgrade", "hack_exploit", "narrative_rotation", "macro", "other",
    }
    VALID_DIRECTIONS = {"bullish", "bearish", "mixed", "neutral"}
    VALID_HORIZONS = {"intraday", "1-7d", "1-4w", "structural"}

    if not isinstance(raw, dict):
        return None

    event_type = str(raw.get("event_type", "other")).lower()
    if event_type not in VALID_EVENT_TYPES:
        event_type = "other"

    direction = str(raw.get("direction", "neutral")).lower()
    if direction not in VALID_DIRECTIONS:
        direction = "neutral"

    time_horizon = str(raw.get("time_horizon", "intraday")).lower()
    if time_horizon not in VALID_HORIZONS:
        time_horizon = "intraday"

    def _clamp(val, default=0.5) -> float:
        try:
            return max(0.0, min(1.0, float(val)))
        except (TypeError, ValueError):
            return default

    confidence = _clamp(raw.get("confidence", 0.5))
    novelty = _clamp(raw.get("novelty", 0.5))
    tradability = _clamp(raw.get("tradability", 0.5))
    persistence = _clamp(raw.get("persistence", 0.5))
    catalyst_score = round(novelty * tradability * persistence, 4)

    asset_scope = str(raw.get("asset_scope", "market-wide")).strip().upper()
    if not asset_scope or asset_scope in ("", "UNKNOWN", "N/A"):
        asset_scope = "MARKET-WIDE"

    affected_raw = raw.get("affected_symbols", [])
    if isinstance(affected_raw, list):
        affected_symbols = [str(s).upper().strip() for s in affected_raw if s]
    else:
        affected_symbols = []

    headline = str(raw.get("headline", "")).strip()
    if not headline:
        return None  # Drop events with no headline

    key_reason = str(raw.get("key_reason", "")).strip()

    return {
        "ts": now_iso,
        "event_type": event_type,
        "asset_scope": asset_scope,
        "affected_symbols": affected_symbols,
        "time_horizon": time_horizon,
        "direction": direction,
        "confidence": confidence,
        "novelty": novelty,
        "tradability": tradability,
        "persistence": persistence,
        "catalyst_score": catalyst_score,
        "key_reason": key_reason,
        "headline": headline,
    }


def classify_crypto_events(timespan: str = "4h") -> list[dict]:
    """
    Fetch crypto news from GDELT and classify each headline into a structured
    event using the local Ollama model.

    Returns a list of validated event dicts. High-confidence, high-tradability
    bearish events are the most actionable for the trading engine.
    """
    articles = _fetch_crypto_news(timespan=timespan)
    if not articles:
        logger.info("No crypto articles returned from GDELT")
        return []

    # Deduplicate by title
    seen_titles: set[str] = set()
    headline_items = []
    for a in articles:
        title = (a.get("title") or "").strip()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        headline_items.append({
            "title": title,
            "url": a.get("url", ""),
            "topic_hint": _pre_classify(title),
        })

    logger.info("Classifying %d unique crypto headlines via Ollama ...", len(headline_items))

    now_iso = datetime.now(timezone.utc).isoformat()
    all_events: list[dict] = []

    # Process in batches to stay within context limits
    for batch_start in range(0, len(headline_items), CLASSIFIER_BATCH_SIZE):
        batch = headline_items[batch_start: batch_start + CLASSIFIER_BATCH_SIZE]
        prompt = _build_classifier_prompt(batch)

        raw_events = _call_ollama(prompt)

        for raw in raw_events:
            event = _validate_event(raw, now_iso)
            if event is not None:
                # Attach source URL from the original article if we can match by headline
                for item in batch:
                    if item["title"] == event["headline"]:
                        event["source_url"] = item.get("url", "")
                        break
                else:
                    event["source_url"] = ""
                all_events.append(event)

        # Brief pause between batches to avoid overloading Ollama
        if batch_start + CLASSIFIER_BATCH_SIZE < len(headline_items):
            time.sleep(2)

    logger.info(
        "Classified %d events from %d headlines (timespan=%s)",
        len(all_events),
        len(headline_items),
        timespan,
    )

    # Log a summary of actionable events
    actionable = [
        e for e in all_events
        if e["confidence"] >= 0.7 and e["tradability"] >= 0.6 and e["direction"] != "neutral"
    ]
    if actionable:
        logger.info("Actionable events (%d):", len(actionable))
        for e in actionable:
            logger.info(
                "  [%s] %s | scope=%s | dir=%s | conf=%.2f | trad=%.2f | %s",
                e["event_type"], e["direction"], e["asset_scope"],
                e["direction"], e["confidence"], e["tradability"],
                e["headline"][:80],
            )

    return all_events


def run_crypto_event_pipeline(timespan: str = "4h", kafka_topic: str = "crypto-event-signals") -> int:
    """
    Full pipeline: fetch → classify → persist to QuestDB → publish to Kafka.

    Returns the number of events published.
    """
    from ..questdb import write_crypto_events

    events = classify_crypto_events(timespan=timespan)
    if not events:
        return 0

    # Write to QuestDB (best-effort)
    try:
        written = write_crypto_events(events)
        logger.info("Wrote %d crypto events to QuestDB", written)
    except Exception:
        logger.exception("Failed to write crypto events to QuestDB")

    # Publish each event to Kafka
    published = 0
    for event in events:
        try:
            if publish_signal(event, topic=kafka_topic):
                published += 1
        except Exception:
            KAFKA_PUBLISH_ERRORS_TOTAL.inc()
            logger.warning("Failed to publish event to Kafka: %s", event.get("headline", "?")[:60])

    logger.info("Published %d/%d crypto events to Kafka topic '%s'", published, len(events), kafka_topic)
    return published
