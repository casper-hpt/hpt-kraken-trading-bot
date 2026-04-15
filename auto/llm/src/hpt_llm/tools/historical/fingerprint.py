"""Build a structured search fingerprint from a Storyline.

The fingerprint captures the key terms needed to search for
similar historical events via GDELT's DOC API.
"""

import logging
import re
from dataclasses import dataclass, field

from .storyline_clusterer import Storyline

logger = logging.getLogger(__name__)

# Map common terms to CAMEO-style event categories
EVENT_TYPE_KEYWORDS = {
    "military_action": [
        "strike", "airstrike", "bomb", "bombing", "missile", "attack",
        "invasion", "offensive", "troops", "deploy", "military",
        "artillery", "drone", "raid",
    ],
    "retaliation": [
        "retaliation", "retaliatory", "revenge", "response", "counter",
        "counterattack", "escalation", "escalate",
    ],
    "diplomacy": [
        "negotiate", "negotiation", "talks", "summit", "deal", "agreement",
        "treaty", "ceasefire", "truce", "peace", "diplomatic",
    ],
    "sanctions": [
        "sanction", "embargo", "ban", "restriction", "tariff", "penalty",
        "freeze", "blacklist",
    ],
    "economic": [
        "rate", "interest", "inflation", "gdp", "recession", "employment",
        "fed", "federal reserve", "central bank", "monetary", "fiscal",
        "debt", "deficit", "yield", "bond",
    ],
    "energy": [
        "oil", "crude", "opec", "pipeline", "refinery", "petroleum",
        "gas", "lng", "energy", "barrel", "drilling",
    ],
    "maritime": [
        "shipping", "tanker", "vessel", "port", "strait", "hormuz",
        "suez", "canal", "maritime", "naval", "blockade", "seizure",
    ],
    "cyber": [
        "cyber", "hack", "breach", "ransomware", "malware",
    ],
    "political": [
        "election", "coup", "protest", "unrest", "assassination",
        "regime", "government",
    ],
    "supply_chain": [
        "semiconductor", "chip", "lithium", "rare earth", "supply chain",
        "shortage", "mineral", "export control",
    ],
}

# Countries and their common references
COUNTRY_MAP = {
    "iran": ["iran", "iranian", "tehran", "persian"],
    "israel": ["israel", "israeli", "jerusalem", "tel aviv"],
    "united_states": ["us", "usa", "united states", "american", "washington"],
    "russia": ["russia", "russian", "moscow", "kremlin"],
    "china": ["china", "chinese", "beijing"],
    "ukraine": ["ukraine", "ukrainian", "kyiv"],
    "saudi_arabia": ["saudi", "riyadh"],
    "iraq": ["iraq", "iraqi", "baghdad"],
    "syria": ["syria", "syrian", "damascus"],
    "yemen": ["yemen", "yemeni", "houthi"],
    "lebanon": ["lebanon", "lebanese", "hezbollah"],
    "turkey": ["turkey", "turkish", "ankara", "erdogan"],
    "north_korea": ["north korea", "pyongyang", "dprk"],
    "taiwan": ["taiwan", "taiwanese", "taipei"],
    "india": ["india", "indian", "delhi", "modi"],
    "pakistan": ["pakistan", "pakistani", "islamabad"],
    "egypt": ["egypt", "egyptian", "cairo"],
    "libya": ["libya", "libyan", "tripoli"],
    "venezuela": ["venezuela", "venezuelan", "caracas", "maduro"],
    "palestine": ["palestine", "palestinian", "gaza", "west bank", "hamas"],
}


@dataclass
class StoryFingerprint:
    """Structured representation of a storyline for historical search."""

    label: str
    actors: list[str]
    countries: list[str]
    event_types: list[str]
    keywords: list[str]
    search_queries: list[str] = field(default_factory=list)
    source_article_count: int = 0

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "actors": self.actors,
            "countries": self.countries,
            "event_types": self.event_types,
            "keywords": self.keywords,
            "search_queries": self.search_queries,
            "source_article_count": self.source_article_count,
        }


def _detect_event_types(text: str) -> list[str]:
    """Detect event type categories from text."""
    text_lower = text.lower()
    found = []
    for event_type, keywords in EVENT_TYPE_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            found.append(event_type)
    return found


def _detect_countries(text: str) -> list[str]:
    """Detect countries mentioned in text."""
    text_lower = text.lower()
    found = []
    for country, aliases in COUNTRY_MAP.items():
        if any(alias in text_lower for alias in aliases):
            found.append(country)
    return found


def _build_search_queries(fingerprint: "StoryFingerprint") -> list[str]:
    """Build GDELT-compatible search queries from a fingerprint.

    Generates 2-3 targeted queries that capture the storyline from
    different angles. GDELT DOC API uses boolean operators.
    """
    queries = []

    # Query 1: actors + event type (most specific)
    if fingerprint.actors and fingerprint.event_types:
        actor_part = " OR ".join(fingerprint.actors[:3])
        # Pick the most specific event keywords
        event_keywords = []
        for et in fingerprint.event_types[:2]:
            event_keywords.extend(EVENT_TYPE_KEYWORDS.get(et, [])[:3])
        if event_keywords:
            event_part = " OR ".join(event_keywords[:4])
            queries.append(f"({actor_part}) ({event_part})")

    # Query 2: countries + top keywords (broader)
    if fingerprint.countries and fingerprint.keywords:
        country_names = []
        for c in fingerprint.countries[:3]:
            # Use the first (canonical) alias
            aliases = COUNTRY_MAP.get(c, [c])
            country_names.append(aliases[0])
        country_part = " OR ".join(country_names)
        kw_part = " OR ".join(fingerprint.keywords[:4])
        queries.append(f"({country_part}) ({kw_part})")

    # Query 3: just the top keywords if we have enough (fallback)
    if fingerprint.keywords and len(queries) < 2:
        kw_part = " OR ".join(fingerprint.keywords[:5])
        queries.append(kw_part)

    return queries


def build_fingerprint(storyline: Storyline) -> StoryFingerprint:
    """Build a structured search fingerprint from a storyline cluster.

    Analyzes all article titles in the storyline to extract:
    - actors (people, organizations)
    - countries
    - event types
    - top keywords
    - pre-built GDELT search queries
    """
    # Combine all titles for analysis
    all_text = " ".join(storyline.titles)

    # Extract structured fields
    countries = _detect_countries(all_text)
    event_types = _detect_event_types(all_text)

    # Get top keywords by frequency across titles (excluding stop words and actor names)
    word_counts: dict[str, int] = {}
    for title in storyline.titles:
        words = re.findall(r"[a-z]{3,}", title.lower())
        for w in words:
            if w not in _all_country_aliases() and len(w) > 3:
                word_counts[w] = word_counts.get(w, 0) + 1

    top_keywords = sorted(word_counts, key=word_counts.get, reverse=True)[:10]

    fp = StoryFingerprint(
        label=storyline.label,
        actors=sorted(storyline.actors),
        countries=countries,
        event_types=event_types,
        keywords=top_keywords,
        source_article_count=storyline.article_count,
    )

    fp.search_queries = _build_search_queries(fp)

    logger.info(
        "Fingerprint: label=%s, actors=%s, countries=%s, event_types=%s, queries=%d",
        fp.label, fp.actors, fp.countries, fp.event_types, len(fp.search_queries),
    )
    return fp


_COUNTRY_ALIAS_CACHE: set[str] | None = None


def _all_country_aliases() -> set[str]:
    global _COUNTRY_ALIAS_CACHE
    if _COUNTRY_ALIAS_CACHE is None:
        _COUNTRY_ALIAS_CACHE = set()
        for aliases in COUNTRY_MAP.values():
            _COUNTRY_ALIAS_CACHE.update(aliases)
    return _COUNTRY_ALIAS_CACHE
