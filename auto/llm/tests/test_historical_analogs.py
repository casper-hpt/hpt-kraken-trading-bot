"""Integration tests for the historical analog search pipeline.

These tests hit the real GDELT API so you can see exactly what
is being requested and returned. Run with:

    pytest tests/test_historical_analogs.py -v -s

The -s flag is important — it shows the actual API requests and responses.
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Direct imports to avoid pulling in the full tools package (kafka, prometheus, etc.)
_historical_dir = Path(__file__).parent.parent / "src" / "hpt_llm" / "tools" / "historical"
_tools_dir = _historical_dir.parent

import importlib.util

def _import_from(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

_base = _import_from(_tools_dir / "base.py", "hpt_llm.tools.base")
_clusterer = _import_from(_historical_dir / "storyline_clusterer.py", "hpt_llm.tools.historical.storyline_clusterer")
_fingerprint = _import_from(_historical_dir / "fingerprint.py", "hpt_llm.tools.historical.fingerprint")
_hsearch = _import_from(_historical_dir / "historical_search.py", "hpt_llm.tools.historical.historical_search")
_gdelt = _import_from(_tools_dir / "gdelt.py", "hpt_llm.tools.gdelt")

Storyline = _clusterer.Storyline
cluster_articles = _clusterer.cluster_articles
StoryFingerprint = _fingerprint.StoryFingerprint
build_fingerprint = _fingerprint.build_fingerprint
search_similar_historical_incidents = _hsearch.search_similar_historical_incidents
_query_gdelt_historical = _hsearch._query_gdelt_historical
_score_article = _hsearch._score_article
fetch_all_topics = _gdelt.fetch_all_topics


# ---------------------------------------------------------------------------
# Test 1: Storyline clustering with real GDELT data
# ---------------------------------------------------------------------------


def test_cluster_live_gdelt_articles():
    """Pull real articles from GDELT, cluster them, print storylines."""
    print("\n" + "=" * 70)
    print("TEST 1: Clustering live GDELT articles into storylines")
    print("=" * 70)

    news_data = fetch_all_topics(timespan="72h")
    total = sum(len(v) for v in news_data.values())
    print(f"\nFetched {total} articles across {len(news_data)} topics")

    # Flatten all articles
    all_articles = []
    for topic, articles in news_data.items():
        for a in articles:
            a["topic"] = topic
            all_articles.append(a)

    print(f"Total articles to cluster: {len(all_articles)}")

    # Cluster
    storylines = cluster_articles(all_articles, min_articles=2)

    print(f"\nFound {len(storylines)} distinct storylines:\n")
    for i, sl in enumerate(storylines):
        print(f"  Storyline {i + 1}: {sl.label}")
        print(f"    Articles: {sl.article_count}")
        print(f"    Actors: {sorted(sl.actors)}")
        print(f"    Sample titles:")
        for title in sl.titles[:3]:
            print(f"      - {title}")
        print()

    assert len(storylines) > 0, "Should find at least one storyline"


# ---------------------------------------------------------------------------
# Test 2: Fingerprint generation
# ---------------------------------------------------------------------------


def test_fingerprint_from_storyline():
    """Build fingerprints from live storylines, show search queries."""
    print("\n" + "=" * 70)
    print("TEST 2: Building fingerprints from live storylines")
    print("=" * 70)

    news_data = fetch_all_topics(timespan="72h")
    all_articles = []
    for topic, articles in news_data.items():
        for a in articles:
            a["topic"] = topic
            all_articles.append(a)

    storylines = cluster_articles(all_articles, min_articles=2)

    if not storylines:
        print("No storylines found, skipping")
        return

    for i, sl in enumerate(storylines[:3]):
        fp = build_fingerprint(sl)
        print(f"\n  Storyline {i + 1}: {sl.label}")
        print(f"    Fingerprint:")
        print(f"      Actors:      {fp.actors}")
        print(f"      Countries:   {fp.countries}")
        print(f"      Event types: {fp.event_types}")
        print(f"      Keywords:    {fp.keywords}")
        print(f"      Search queries:")
        for q in fp.search_queries:
            print(f"        -> {q}")
        print()


# ---------------------------------------------------------------------------
# Test 3: Raw GDELT historical query (see exactly what comes back)
# ---------------------------------------------------------------------------


def test_raw_gdelt_historical_query():
    """Make a single historical GDELT query and show raw results."""
    print("\n" + "=" * 70)
    print("TEST 3: Raw GDELT historical query")
    print("=" * 70)

    # Search for Iran-related military events in 2020
    query = "(Iran OR Israel) (strike OR missile OR attack)"
    start = datetime(2020, 1, 1)
    end = datetime(2020, 3, 31)

    print(f"\n  Query: {query}")
    print(f"  Date range: {start.date()} to {end.date()}")

    articles, request_info = _query_gdelt_historical(
        query=query, start_date=start, end_date=end, max_records=10
    )

    print(f"\n  Request info:")
    print(f"    URL: {request_info['url']}")
    print(f"    Params: {json.dumps(request_info['params'], indent=6)}")
    print(f"\n  Articles returned: {len(articles)}")

    for a in articles[:5]:
        print(f"\n    Title:  {a.get('title', 'N/A')}")
        print(f"    Source: {a.get('domain', 'N/A')}")
        print(f"    Date:   {a.get('seendate', 'N/A')}")
        print(f"    Tone:   {a.get('tone', 'N/A')}")
        print(f"    URL:    {a.get('url', 'N/A')[:80]}")

    assert isinstance(articles, list), "Should return a list"


# ---------------------------------------------------------------------------
# Test 4: Full pipeline — live articles → storylines → historical search
# ---------------------------------------------------------------------------


def test_full_pipeline():
    """End-to-end: get today's news, cluster, search for historical analogs.

    This is the main integration test. Shows every API call made.
    """
    print("\n" + "=" * 70)
    print("TEST 4: Full pipeline — live articles to historical analogs")
    print("=" * 70)

    # Step 1: Get today's articles
    print("\n--- Step 1: Fetching current GDELT articles ---")
    news_data = fetch_all_topics(timespan="72h")
    total = sum(len(v) for v in news_data.values())
    print(f"  Fetched {total} articles")

    all_articles = []
    for topic, articles in news_data.items():
        for a in articles:
            a["topic"] = topic
            all_articles.append(a)

    # Step 2: Cluster into storylines
    print("\n--- Step 2: Clustering into storylines ---")
    storylines = cluster_articles(all_articles, min_articles=2)
    print(f"  Found {len(storylines)} storylines")

    if not storylines:
        print("  No storylines found, skipping historical search")
        return

    # Step 3: Search historical analogs for the top storyline
    # (limit to 3 years and top storyline to keep test fast)
    top_storyline = storylines[0]
    print(f"\n--- Step 3: Searching history for top storyline ---")
    print(f"  Storyline: {top_storyline.label}")
    print(f"  Articles: {top_storyline.article_count}")

    fp = build_fingerprint(top_storyline)
    print(f"  Fingerprint queries: {fp.search_queries}")

    result = search_similar_historical_incidents(
        fingerprint=fp,
        lookback_years=3,  # Keep test fast
        top_k=5,
    )

    # Print API calls made
    print(f"\n--- API calls made ({len(result['api_calls'])}) ---")
    for call in result["api_calls"]:
        print(f"\n  Query: {call.get('query', 'N/A')}")
        print(f"  Date range: {call.get('date_range', 'N/A')}")
        print(f"  Articles returned: {call.get('articles_returned', 'N/A')}")
        if "error" in call:
            print(f"  ERROR: {call['error']}")

    # Print episodes found
    print(f"\n--- Historical episodes found ({len(result['episodes'])}) ---")
    for i, ep in enumerate(result["episodes"]):
        print(f"\n  Episode {i + 1}:")
        print(f"    Title: {ep['title']}")
        print(f"    Date range: {ep['date_range']}")
        print(f"    Similarity: {ep['similarity_score']}")
        print(f"    Articles: {ep['article_count']}")
        print(f"    Countries: {ep['countries']}")
        print(f"    Event types: {ep['event_types']}")
        print(f"    Why matched: {ep['why_matched']}")
        print(f"    Sample headlines:")
        for h in ep["sample_headlines"][:3]:
            print(f"      - {h}")

    # Print stats
    print(f"\n--- Stats ---")
    print(f"  {json.dumps(result['stats'], indent=4)}")


# ---------------------------------------------------------------------------
# Test 5: Synthetic fingerprint search (known scenario)
# ---------------------------------------------------------------------------


def test_known_scenario_search():
    """Search for a known historical event to verify scoring works.

    Uses the Soleimani killing (Jan 2020) as a known test case.
    """
    print("\n" + "=" * 70)
    print("TEST 5: Known scenario — Soleimani killing (Jan 2020)")
    print("=" * 70)

    fp = StoryFingerprint(
        label="US-Iran military escalation",
        actors=["us", "iran", "iraq"],
        countries=["iran", "iraq", "united_states"],
        event_types=["military_action", "retaliation"],
        keywords=["strike", "missile", "military", "attack", "retaliation"],
        search_queries=[
            "(Iran OR Iraq) (strike OR missile OR attack)",
            "(Iran OR US) (military OR retaliation OR escalation)",
        ],
        source_article_count=0,
    )

    print(f"\n  Fingerprint: {fp.label}")
    print(f"  Queries: {fp.search_queries}")

    result = search_similar_historical_incidents(
        fingerprint=fp,
        lookback_years=6,
        top_k=5,
    )

    print(f"\n  API calls: {len(result['api_calls'])}")
    print(f"  Articles scored: {result['stats']['total_articles_scored']}")
    print(f"  Episodes found: {len(result['episodes'])}")

    for i, ep in enumerate(result["episodes"]):
        print(f"\n  Episode {i + 1}:")
        print(f"    {ep['title']}")
        print(f"    {ep['date_range']} (score: {ep['similarity_score']})")
        print(f"    Countries: {ep['countries']}")
        print(f"    Why: {ep['why_matched']}")
        for h in ep["sample_headlines"][:2]:
            print(f"      - {h}")


# ---------------------------------------------------------------------------
# Allow running directly: python tests/test_historical_analogs.py
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run historical analog tests")
    parser.add_argument(
        "test",
        nargs="?",
        default="all",
        choices=["all", "cluster", "fingerprint", "raw", "pipeline", "known"],
        help="Which test to run (default: all)",
    )
    args = parser.parse_args()

    tests = {
        "cluster": test_cluster_live_gdelt_articles,
        "fingerprint": test_fingerprint_from_storyline,
        "raw": test_raw_gdelt_historical_query,
        "pipeline": test_full_pipeline,
        "known": test_known_scenario_search,
    }

    if args.test == "all":
        for name, fn in tests.items():
            try:
                fn()
            except Exception as e:
                print(f"\n  FAILED: {name}: {e}")
    else:
        tests[args.test]()
