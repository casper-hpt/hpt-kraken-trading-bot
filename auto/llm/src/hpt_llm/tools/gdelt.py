import json
import logging
import time

import requests

from .base import register_tool

logger = logging.getLogger(__name__)

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; hpt-llm/1.0)"}

# Single broad query - one API call instead of six
QUERY = (
    "(OPEC OR crude oil OR pipeline OR refinery OR sanctions OR embargo "
    "OR shipping OR Suez OR Hormuz OR lithium OR semiconductor OR rare earth "
    "OR war OR conflict OR military) "
    "sourcelang:english"
)

# Classify articles locally by matching title keywords
TOPIC_KEYWORDS = {
    "oil_supply": ["opec", "oil price", "oil production", "oil output", "crude oil", "refinery", "petroleum", "gasoline", "gas price", "barrel"],
    "sanctions": ["sanction", "embargo", "export ban", "trade restriction", "tariff", "trade war"],
    "war_impacts": ["war", "conflict", "military", "armed", "troops", "invasion", "airstrike", "missile", "bombing"],
    "shipping_routes": ["shipping", "suez", "hormuz", "port", "freight", "vessel", "tanker", "maritime", "cargo", "strait"],
    "critical_minerals": ["lithium", "rare earth", "semiconductor", "chip shortage", "cobalt", "nickel", "mineral", "mining"],
}


def _query_gdelt(query: str, max_records: int = 250, timespan: str = "72h", retries: int = 3) -> list[dict]:
    """Query GDELT DOC 2.0 API with retry on rate-limit."""
    params = {
        "query": query,
        "mode": "ArtList",
        "maxrecords": str(max_records),
        "timespan": timespan,
        "format": "json",
        "sort": "DateDesc",
    }
    for attempt in range(retries):
        try:
            resp = requests.get(GDELT_DOC_API, params=params, headers=HEADERS, timeout=30)
            if resp.status_code == 429:
                wait = 60
                logger.warning("GDELT rate limited, waiting %ds (attempt %d/%d)", wait, attempt + 1, retries)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            text = resp.text.strip()
            if not text:
                return []
            data = resp.json()
            return data.get("articles", [])
        except requests.exceptions.JSONDecodeError:
            logger.warning("GDELT returned non-JSON response for: %s", query)
            return []
        except Exception:
            logger.exception("GDELT query failed for: %s (attempt %d/%d)", query, attempt + 1, retries)
            if attempt < retries - 1:
                time.sleep(10)
    return []


def _classify_article(title: str) -> list[str]:
    """Classify an article into topic buckets based on title keywords."""
    title_lower = title.lower()
    return [
        topic
        for topic, keywords in TOPIC_KEYWORDS.items()
        if any(kw in title_lower for kw in keywords)
    ]


def fetch_all_topics(timespan: str = "72h") -> dict[str, list[dict]]:
    """Fetch GDELT articles with a single query, then classify by topic."""
    articles = _query_gdelt(QUERY, max_records=250, timespan=timespan)

    results: dict[str, list[dict]] = {topic: [] for topic in TOPIC_KEYWORDS}
    for a in articles:
        item = {
            "title": a.get("title", ""),
            "url": a.get("url", ""),
            "source": a.get("domain", ""),
            "date": a.get("seendate", ""),
            "language": a.get("language", ""),
        }
        topics = _classify_article(item["title"])
        for topic in topics:
            if len(results[topic]) < 15:
                results[topic].append(item)
    return results


@register_tool(
    name="scan_gdelt_news",
    description=(
        "Scan GDELT news feeds for supply chain disruption signals across 5 categories: "
        "oil supply disruptions, sanctions, war impacts, shipping routes, and critical minerals. "
        "Returns recent headlines grouped by topic."
    ),
    parameters={
        "type": "object",
        "properties": {
            "timespan": {
                "type": "string",
                "description": "How far back to look (e.g. '24h', '72h', '7d'). Default '72h'.",
            },
        },
        "required": [],
    },
)
def scan_gdelt_news(timespan: str = "72h") -> str:
    """Scan GDELT for supply chain disruption news and return grouped results."""
    results = fetch_all_topics(timespan=timespan)

    summary = {}
    for topic, articles in results.items():
        summary[topic] = {
            "count": len(articles),
            "headlines": [
                {"title": a["title"], "source": a["source"], "date": a["date"]}
                for a in articles[:10]
            ],
        }

    return json.dumps(summary, indent=2)
