"""Historical analog search sub-package.

Clusters current news into storylines, builds search fingerprints,
and queries GDELT for similar past events.
"""

from .storyline_clusterer import Storyline, cluster_articles
from .fingerprint import StoryFingerprint, build_fingerprint
from .historical_search import search_similar_historical_incidents

__all__ = [
    "Storyline",
    "cluster_articles",
    "StoryFingerprint",
    "build_fingerprint",
    "search_similar_historical_incidents",
]
