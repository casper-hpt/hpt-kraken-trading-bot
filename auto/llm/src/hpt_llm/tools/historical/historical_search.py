"""Search GDELT for historical analogs to a current storyline.

Given a StoryFingerprint, queries GDELT DOC API across historical
date ranges, scores returned articles for similarity, and clusters
results into distinct historical episodes.
"""

import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import requests

from .fingerprint import StoryFingerprint, _detect_countries, _detect_event_types
from ..base import register_tool

logger = logging.getLogger(__name__)

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; hpt-llm/1.0)"}

# How far back to search
DEFAULT_LOOKBACK_YEARS = 10
WINDOW_SIZE_YEARS = 2  # Search in 2-year chunks to minimize API calls
MAX_RECORDS_PER_QUERY = 75
RATE_LIMIT_WAIT_S = 65  # GDELT rate limit window
BETWEEN_CALLS_WAIT_S = 5  # Polite delay between calls
SIMILARITY_THRESHOLD = 0.20


@dataclass
class HistoricalArticle:
    """A single article returned from a historical GDELT search."""

    title: str
    url: str
    source: str
    date: str
    tone: float = 0.0
    countries: list[str] = field(default_factory=list)
    event_types: list[str] = field(default_factory=list)
    keywords: set[str] = field(default_factory=set)
    similarity_score: float = 0.0


@dataclass
class HistoricalEpisode:
    """A cluster of historical articles about the same event."""

    title: str
    date_range: str
    articles: list[HistoricalArticle] = field(default_factory=list)
    similarity_score: float = 0.0
    countries: list[str] = field(default_factory=list)
    event_types: list[str] = field(default_factory=list)
    why_matched: dict = field(default_factory=dict)

    @property
    def article_count(self) -> int:
        return len(self.articles)

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "date_range": self.date_range,
            "article_count": self.article_count,
            "similarity_score": round(self.similarity_score, 3),
            "countries": self.countries,
            "event_types": self.event_types,
            "why_matched": self.why_matched,
            "sample_headlines": [a.title for a in self.articles[:5]],
        }


# ---------------------------------------------------------------------------
# GDELT querying
# ---------------------------------------------------------------------------


def _query_gdelt_historical(
    query: str,
    start_date: datetime,
    end_date: datetime,
    max_records: int = MAX_RECORDS_PER_QUERY,
    retries: int = 3,
) -> tuple[list[dict], dict]:
    """Query GDELT DOC API for a specific date range.

    Returns (articles, request_info) where request_info contains
    the actual URL/params for debugging.
    """
    params = {
        "query": f"{query} sourcelang:english",
        "mode": "ArtList",
        "maxrecords": str(max_records),
        "startdatetime": start_date.strftime("%Y%m%d%H%M%S"),
        "enddatetime": end_date.strftime("%Y%m%d%H%M%S"),
        "format": "json",
        "sort": "HybridRel",  # Relevance-weighted sort
    }

    request_info = {
        "url": GDELT_DOC_API,
        "params": {k: v for k, v in params.items()},
        "query": query,
        "date_range": f"{start_date.date()} to {end_date.date()}",
    }

    for attempt in range(retries):
        try:
            resp = requests.get(
                GDELT_DOC_API, params=params, headers=HEADERS, timeout=30
            )

            if resp.status_code == 429:
                logger.warning(
                    "GDELT rate limited, waiting %ds (attempt %d/%d)",
                    RATE_LIMIT_WAIT_S, attempt + 1, retries,
                )
                time.sleep(RATE_LIMIT_WAIT_S)
                continue

            resp.raise_for_status()
            text = resp.text.strip()
            if not text:
                return [], request_info

            data = resp.json()
            articles = data.get("articles", [])
            request_info["articles_returned"] = len(articles)
            return articles, request_info

        except requests.exceptions.JSONDecodeError:
            logger.warning("GDELT returned non-JSON for query: %s", query)
            request_info["error"] = "non-JSON response"
            return [], request_info
        except Exception as e:
            logger.exception(
                "GDELT query failed (attempt %d/%d): %s", attempt + 1, retries, e
            )
            if attempt < retries - 1:
                time.sleep(5)

    request_info["error"] = "max retries exceeded"
    return [], request_info


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def _extract_keywords(title: str) -> set[str]:
    """Extract meaningful keywords from a title."""
    stop = {
        "the", "and", "for", "that", "this", "with", "from", "are", "was",
        "were", "has", "have", "had", "will", "would", "could", "should",
        "not", "but", "also", "into", "over", "after", "before", "about",
        "says", "said", "report", "reports", "news", "new", "amid",
    }
    words = set(re.findall(r"[a-z]{3,}", title.lower()))
    return words - stop


def _jaccard(a: set | list, b: set | list) -> float:
    """Jaccard similarity."""
    a_set, b_set = set(a), set(b)
    if not a_set or not b_set:
        return 0.0
    return len(a_set & b_set) / len(a_set | b_set)


def _title_has_relevance(title: str, fingerprint: StoryFingerprint) -> bool:
    """Quick check: does the title mention ANY of the fingerprint's key terms?

    GDELT matches on full article text, so many results have irrelevant titles.
    We filter to articles whose titles actually mention the topic.
    """
    title_lower = title.lower()
    # Check actors
    for actor in fingerprint.actors:
        if actor.lower() in title_lower:
            return True
    # Check top keywords
    for kw in fingerprint.keywords[:5]:
        if kw.lower() in title_lower:
            return True
    # Check country names
    from .fingerprint import COUNTRY_MAP
    for country in fingerprint.countries:
        for alias in COUNTRY_MAP.get(country, [country]):
            if alias in title_lower:
                return True
    return False


def _score_article(
    article: dict, fingerprint: StoryFingerprint
) -> HistoricalArticle | None:
    """Score a single historical article against the current fingerprint.

    Returns None if the article title has no relevance to the fingerprint
    (GDELT matches on body text, so many results are noise).
    """
    title = article.get("title", "")

    # Gate: title must mention at least one relevant term
    if not _title_has_relevance(title, fingerprint):
        return None

    keywords = _extract_keywords(title)
    countries = _detect_countries(title)
    event_types = _detect_event_types(title)
    tone = float(article.get("tone", 0.0) or 0.0)

    # Weighted similarity
    kw_sim = _jaccard(keywords, fingerprint.keywords)
    country_sim = _jaccard(countries, fingerprint.countries)
    event_sim = _jaccard(event_types, fingerprint.event_types)
    actor_sim = _jaccard(
        _extract_keywords(title) & set(fingerprint.actors),
        set(fingerprint.actors),
    )

    score = (
        0.25 * kw_sim
        + 0.30 * country_sim
        + 0.25 * event_sim
        + 0.20 * actor_sim
    )

    return HistoricalArticle(
        title=title,
        url=article.get("url", ""),
        source=article.get("domain", ""),
        date=article.get("seendate", ""),
        tone=tone,
        countries=countries,
        event_types=event_types,
        keywords=keywords,
        similarity_score=score,
    )


# ---------------------------------------------------------------------------
# Episode clustering
# ---------------------------------------------------------------------------


def _cluster_into_episodes(
    articles: list[HistoricalArticle],
    time_window_days: int = 7,
) -> list[HistoricalEpisode]:
    """Cluster scored historical articles into distinct episodes.

    Articles within `time_window_days` of each other with similar
    countries/event_types get grouped together.
    """
    if not articles:
        return []

    # Sort by date
    def parse_date(a: HistoricalArticle) -> datetime:
        try:
            # GDELT date format: YYYYMMDDTHHmmSS or similar
            clean = a.date.replace("T", "").replace("Z", "")[:14]
            return datetime.strptime(clean, "%Y%m%d%H%M%S")
        except (ValueError, IndexError):
            return datetime(2000, 1, 1)

    articles.sort(key=parse_date)

    episodes: list[HistoricalEpisode] = []

    for article in articles:
        art_date = parse_date(article)
        placed = False

        for episode in episodes:
            # Check if article fits in this episode's time window
            ep_dates = [parse_date(a) for a in episode.articles]
            ep_start = min(ep_dates)
            ep_end = max(ep_dates)

            if abs((art_date - ep_end).days) <= time_window_days:
                # Check topical overlap
                country_overlap = bool(
                    set(article.countries) & set(episode.countries)
                )
                event_overlap = bool(
                    set(article.event_types) & set(episode.event_types)
                )

                if country_overlap or event_overlap:
                    episode.articles.append(article)
                    episode.countries = list(
                        set(episode.countries) | set(article.countries)
                    )
                    episode.event_types = list(
                        set(episode.event_types) | set(article.event_types)
                    )
                    placed = True
                    break

        if not placed:
            ep = HistoricalEpisode(
                title=article.title,
                date_range=article.date[:8] if article.date else "unknown",
                articles=[article],
                countries=list(article.countries),
                event_types=list(article.event_types),
            )
            episodes.append(ep)

    # Compute episode-level scores and metadata
    for ep in episodes:
        scores = [a.similarity_score for a in ep.articles]
        ep.similarity_score = max(scores) if scores else 0.0

        # Fix date range
        dates = [parse_date(a) for a in ep.articles]
        start = min(dates).strftime("%Y-%m-%d")
        end = max(dates).strftime("%Y-%m-%d")
        ep.date_range = f"{start} to {end}" if start != end else start

        # Pick best title (highest scoring article)
        best = max(ep.articles, key=lambda a: a.similarity_score)
        ep.title = best.title

    return episodes


# ---------------------------------------------------------------------------
# Main search function
# ---------------------------------------------------------------------------


def search_similar_historical_incidents(
    fingerprint: StoryFingerprint,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    top_k: int = 8,
    similarity_threshold: float = SIMILARITY_THRESHOLD,
) -> dict:
    """Search GDELT for historical events similar to the given fingerprint.

    For each search query in the fingerprint, queries GDELT across
    yearly time windows going back `lookback_years`. Scores and clusters
    results into distinct historical episodes.

    Returns a dict with:
        - fingerprint: the input fingerprint
        - episodes: ranked list of historical episodes
        - api_calls: details of every GDELT request made (for debugging)
        - stats: summary statistics
    """
    all_articles: list[HistoricalArticle] = []
    api_calls: list[dict] = []
    now = datetime.utcnow()

    logger.info(
        "Searching historical analogs for: %s (queries=%d, years=%d)",
        fingerprint.label, len(fingerprint.search_queries), lookback_years,
    )

    # Build time windows (2-year chunks to minimize API calls)
    windows = []
    for chunk_start in range(0, lookback_years, WINDOW_SIZE_YEARS):
        end_years = chunk_start
        start_years = min(chunk_start + WINDOW_SIZE_YEARS, lookback_years)
        end_dt = now - timedelta(days=365 * end_years)
        start_dt = now - timedelta(days=365 * start_years)
        # Skip the most recent 30 days (covered by live GDELT)
        if end_years == 0:
            end_dt = now - timedelta(days=30)
        windows.append((start_dt, end_dt))

    total_calls = len(fingerprint.search_queries) * len(windows)
    logger.info(
        "Will make %d API calls (%d queries x %d windows)",
        total_calls, len(fingerprint.search_queries), len(windows),
    )

    for query in fingerprint.search_queries:
        for start_dt, end_dt in windows:
            raw_articles, request_info = _query_gdelt_historical(
                query=query,
                start_date=start_dt,
                end_date=end_dt,
                max_records=MAX_RECORDS_PER_QUERY,
            )
            api_calls.append(request_info)

            # Score each article (None = title not relevant)
            matched = 0
            for raw in raw_articles:
                scored = _score_article(raw, fingerprint)
                if scored is not None and scored.similarity_score >= similarity_threshold:
                    all_articles.append(scored)
                    matched += 1

            logger.info(
                "Query '%s' [%s to %s]: %d returned, %d relevant",
                query[:50],
                start_dt.date(),
                end_dt.date(),
                len(raw_articles),
                matched,
            )

            # Polite delay to avoid rate limiting
            time.sleep(BETWEEN_CALLS_WAIT_S)

    # Cluster into episodes
    episodes = _cluster_into_episodes(all_articles)

    # Sort by similarity score and take top_k
    episodes.sort(key=lambda e: e.similarity_score, reverse=True)
    episodes = episodes[:top_k]

    # Add why_matched explanations
    for ep in episodes:
        ep.why_matched = {
            "country_overlap": sorted(
                set(ep.countries) & set(fingerprint.countries)
            ),
            "event_type_overlap": sorted(
                set(ep.event_types) & set(fingerprint.event_types)
            ),
            "article_count": ep.article_count,
            "top_score": round(ep.similarity_score, 3),
        }

    result = {
        "fingerprint": fingerprint.to_dict(),
        "episodes": [ep.to_dict() for ep in episodes],
        "api_calls": api_calls,
        "stats": {
            "total_api_calls": len(api_calls),
            "total_articles_scored": len(all_articles),
            "episodes_found": len(episodes),
            "queries_used": fingerprint.search_queries,
        },
    }

    logger.info(
        "Search complete: %d API calls, %d articles scored, %d episodes returned",
        len(api_calls), len(all_articles), len(episodes),
    )

    return result


# ---------------------------------------------------------------------------
# Registered tool (callable by LLM)
# ---------------------------------------------------------------------------


@register_tool(
    name="search_historical_analogs",
    description=(
        "Search for historical events similar to a current news storyline. "
        "Takes actors, countries, event types, and keywords, then queries "
        "GDELT for similar historical incidents going back up to 10 years. "
        "Returns ranked episodes with similarity scores and explanations."
    ),
    parameters={
        "type": "object",
        "properties": {
            "actors": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Key actors involved (e.g. ['Iran', 'Israel', 'US'])",
            },
            "countries": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Countries involved (e.g. ['iran', 'israel'])",
            },
            "event_types": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Event categories (e.g. ['military_action', 'retaliation'])",
            },
            "keywords": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Key terms (e.g. ['missile', 'airstrike', 'nuclear'])",
            },
            "lookback_years": {
                "type": "integer",
                "description": "How many years back to search. Default 10.",
            },
            "top_k": {
                "type": "integer",
                "description": "Number of top episodes to return. Default 8.",
            },
        },
        "required": ["actors", "countries", "keywords"],
    },
)
def search_historical_analogs(
    actors: list[str],
    countries: list[str],
    keywords: list[str],
    event_types: list[str] | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    top_k: int = 8,
) -> str:
    """LLM-callable wrapper around search_similar_historical_incidents."""
    from .fingerprint import StoryFingerprint, _build_search_queries

    fp = StoryFingerprint(
        label=" | ".join(actors[:3] + keywords[:2]),
        actors=actors,
        countries=countries,
        event_types=event_types or [],
        keywords=keywords,
        source_article_count=0,
    )
    fp.search_queries = _build_search_queries(fp)

    result = search_similar_historical_incidents(
        fingerprint=fp,
        lookback_years=lookback_years,
        top_k=top_k,
    )

    # Return without api_calls detail for LLM (too verbose)
    return json.dumps(
        {
            "episodes": result["episodes"],
            "stats": result["stats"],
        },
        indent=2,
    )
