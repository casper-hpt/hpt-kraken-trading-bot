"""Cluster today's GDELT articles into distinct storylines.

Each storyline is a group of articles about the same event/topic.
Clustering uses title keyword overlap — no embeddings, no external deps.
"""

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Common words to ignore when comparing article titles
STOP_WORDS = frozenset({
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "has", "have", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "can", "shall", "not", "no", "nor", "so",
    "yet", "both", "each", "all", "any", "few", "more", "most", "other",
    "some", "such", "than", "too", "very", "just", "also", "into", "over",
    "after", "before", "between", "under", "about", "up", "out", "off",
    "then", "once", "here", "there", "when", "where", "why", "how", "what",
    "which", "who", "whom", "this", "that", "these", "those", "its", "it",
    "as", "new", "says", "said", "report", "reports", "news", "update",
    "amid", "per", "via", "near",
})

# Known geopolitical actors and entities — helps with clustering
KNOWN_ACTORS = {
    "us", "usa", "united states", "america", "biden", "trump",
    "china", "beijing", "xi jinping",
    "russia", "moscow", "putin",
    "iran", "tehran", "irgc",
    "israel", "idf", "netanyahu",
    "ukraine", "kyiv", "zelensky",
    "saudi", "saudi arabia", "mbs",
    "north korea", "pyongyang", "kim jong un",
    "nato", "eu", "european union",
    "opec", "fed", "federal reserve",
    "houthi", "hezbollah", "hamas",
    "taiwan", "tsmc",
}


@dataclass
class Storyline:
    """A cluster of articles about the same event."""

    articles: list[dict] = field(default_factory=list)
    keywords: set[str] = field(default_factory=set)
    actors: set[str] = field(default_factory=set)
    label: str = ""

    @property
    def titles(self) -> list[str]:
        return [a.get("title", "") for a in self.articles]

    @property
    def article_count(self) -> int:
        return len(self.articles)


def _extract_keywords(title: str) -> set[str]:
    """Extract meaningful keywords from an article title."""
    words = set(re.findall(r"[a-z]{2,}", title.lower()))
    return words - STOP_WORDS


def _extract_actors(title: str) -> set[str]:
    """Extract known actors mentioned in a title."""
    title_lower = title.lower()
    found = set()
    for actor in KNOWN_ACTORS:
        if actor in title_lower:
            found.add(actor)
    return found


def _jaccard(a: set, b: set) -> float:
    """Jaccard similarity between two sets."""
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _is_same_storyline(
    article_keywords: set[str],
    article_actors: set[str],
    storyline: Storyline,
    keyword_threshold: float = 0.25,
    actor_threshold: float = 0.5,
) -> bool:
    """Decide if an article belongs to an existing storyline.

    Match if actors overlap significantly, OR if keyword overlap is high enough.
    Actor overlap is weighted more heavily — "Iran" + "strike" in two articles
    is a stronger signal than shared generic terms.
    """
    actor_sim = _jaccard(article_actors, storyline.actors)
    keyword_sim = _jaccard(article_keywords, storyline.keywords)

    # Strong actor match is sufficient (e.g., both mention Iran + Israel)
    if article_actors and storyline.actors and actor_sim >= actor_threshold:
        return True

    # Otherwise need decent keyword overlap
    if keyword_sim >= keyword_threshold:
        return True

    # Weighted combo for borderline cases
    combined = 0.6 * actor_sim + 0.4 * keyword_sim
    return combined >= 0.3


def cluster_articles(articles: list[dict], min_articles: int = 2) -> list[Storyline]:
    """Cluster articles into distinct storylines.

    Args:
        articles: List of article dicts with at least a "title" key.
        min_articles: Minimum articles to form a storyline (singles are noise).

    Returns:
        List of Storyline objects, sorted by article count descending.
    """
    storylines: list[Storyline] = []

    for article in articles:
        title = article.get("title", "")
        if not title:
            continue

        keywords = _extract_keywords(title)
        actors = _extract_actors(title)

        placed = False
        for storyline in storylines:
            if _is_same_storyline(keywords, actors, storyline):
                storyline.articles.append(article)
                storyline.keywords |= keywords
                storyline.actors |= actors
                placed = True
                break

        if not placed:
            sl = Storyline(
                articles=[article],
                keywords=keywords,
                actors=actors,
            )
            storylines.append(sl)

    # Filter out noise (single articles) and sort by size
    storylines = [s for s in storylines if s.article_count >= min_articles]
    storylines.sort(key=lambda s: s.article_count, reverse=True)

    # Generate labels from most common keywords
    for sl in storylines:
        sl.label = _generate_label(sl)

    logger.info(
        "Clustered %d articles into %d storylines",
        len(articles),
        len(storylines),
    )
    return storylines


def _generate_label(storyline: Storyline) -> str:
    """Generate a short label for a storyline from its top keywords/actors."""
    # Use actors first, then top keywords
    parts = []
    if storyline.actors:
        parts.extend(sorted(storyline.actors)[:3])

    # Count keyword frequency across titles
    word_counts: dict[str, int] = {}
    for title in storyline.titles:
        for word in _extract_keywords(title):
            if word not in storyline.actors:
                word_counts[word] = word_counts.get(word, 0) + 1

    top_words = sorted(word_counts, key=word_counts.get, reverse=True)[:3]
    parts.extend(top_words)

    return " | ".join(parts[:5]) if parts else "unknown"
