"""Integration test for the tournament-based supply chain theory pipeline.

Requires:
    1. Ollama running and reachable (home network or SSH tunnel)
    2. QuestDB port-forwarded:
       kubectl port-forward svc/questdb 8812:8812 --address 0.0.0.0

Usage:
    cd bot/llm
    python tests/test_theory_loop.py              # full run (all loops)
    python tests/test_theory_loop.py --loop 0     # news digest only
    python tests/test_theory_loop.py --loop 1     # news digest + tournament
    python tests/test_theory_loop.py --loop 2     # all loops (same as full)
    python tests/test_theory_loop.py --mock-news  # skip GDELT, use fake headlines
    python tests/test_theory_loop.py --mock-news --max-symbols 30  # quick test
"""

import argparse
import json
import logging
import os
import sys

# Env defaults for local testing
os.environ.setdefault("DB_HOST", "192.168.2.38")
os.environ.setdefault("DB_PORT", "8812")
os.environ.setdefault("DB_USER", "admin")
os.environ.setdefault("DB_PASSWORD", "quest")
os.environ.setdefault("DB_NAME", "qdb")
os.environ.setdefault("SLACK_BOT_TOKEN", "xoxb-test")
os.environ.setdefault("SLACK_APP_TOKEN", "xapp-test")
os.environ.setdefault("KAFKA_PRODUCER_BOOTSTRAP_SERVERS", "localhost:9092")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from hpt_llm.questdb import get_watchlist_details
from hpt_llm.tools.supply_chain_theory import (
    _generate_news_digest,
    _run_tournament,
    _generate_final_signal,
    _format_news,
    ROUND1_BATCH_SIZE,
    ROUND2_BATCH_SIZE,
    ROUND2_KEEP,
)
from hpt_llm.tools.gdelt import fetch_all_topics


MOCK_NEWS = {
    "oil_supply": [
        {"title": "OPEC+ agrees to deeper production cuts amid weak demand", "source": "reuters.com", "date": "2026-03-07"},
        {"title": "US crude oil inventories fall sharply, prices surge", "source": "bloomberg.com", "date": "2026-03-06"},
    ],
    "sanctions": [
        {"title": "EU expands sanctions on Russian oil exports", "source": "ft.com", "date": "2026-03-07"},
        {"title": "New tariffs on Chinese semiconductors take effect", "source": "wsj.com", "date": "2026-03-06"},
    ],
    "war_impacts": [
        {"title": "Red Sea shipping attacks intensify, Houthi missiles hit cargo vessel", "source": "bbc.com", "date": "2026-03-07"},
    ],
    "shipping_routes": [
        {"title": "Suez Canal traffic drops 40% as shippers reroute around Africa", "source": "cnbc.com", "date": "2026-03-06"},
        {"title": "Container freight rates spike on Asia-Europe routes", "source": "freightwaves.com", "date": "2026-03-07"},
    ],
    "critical_minerals": [
        {"title": "Lithium prices rebound on EV battery demand recovery", "source": "mining.com", "date": "2026-03-06"},
    ],
}


def test_watchlist():
    """Step 0: Verify QuestDB connectivity and watchlist data."""
    print("\n" + "=" * 60)
    print("STEP 0: Checking QuestDB watchlist")
    print("=" * 60)

    details = get_watchlist_details()
    if not details:
        print("\nFAILED: No watchlist data. Is QuestDB port-forwarded?")
        sys.exit(1)

    print(f"\nLoaded {len(details)} symbols with details.")
    print("\nSample (first 3):")
    for d in details[:3]:
        print(f"  {d['symbol']:6s} | sector={d.get('sector', 'N/A'):20s} | "
              f"industry={d.get('industry', 'N/A'):25s} | "
              f"desc={str(d.get('description', ''))[:60]}...")
    print()

    # Check data quality
    has_desc = sum(1 for d in details if d.get("description"))
    has_sector = sum(1 for d in details if d.get("sector"))
    has_transcript = sum(1 for d in details if d.get("earnings_transcript"))
    print(f"Data quality: {has_desc}/{len(details)} have descriptions, "
          f"{has_sector}/{len(details)} have sectors, "
          f"{has_transcript}/{len(details)} have earnings transcripts")

    return details


def test_loop0(news_data: dict) -> str:
    """Loop 0: Generate news digest."""
    print("\n" + "=" * 60)
    print("LOOP 0: Generating news digest")
    print("=" * 60)

    total = sum(len(v) for v in news_data.values())
    print(f"\nInput: {total} articles across {len(news_data)} topics")

    themes = _generate_news_digest(news_data)

    if not themes:
        print("\nFAILED: Empty news digest from LLM")
        sys.exit(1)

    print(f"\nNews digest ({len(themes)} chars):")
    print("-" * 40)
    print(themes[:1000])
    if len(themes) > 1000:
        print(f"\n... ({len(themes) - 1000} more chars)")
    print("-" * 40)
    return themes


def test_tournament(themes: str, watchlist: list[dict]) -> tuple[list[dict], list[dict]]:
    """Loop 1: Tournament scoring."""
    print("\n" + "=" * 60)
    print(f"LOOP 1: Tournament scoring {len(watchlist)} symbols")
    print(f"  Round 1: batches of {ROUND1_BATCH_SIZE}")
    print(f"  Round 2: batches of {ROUND2_BATCH_SIZE}, keep top {ROUND2_KEEP}")
    print("=" * 60)

    round1_scores, longs, shorts = _run_tournament(themes, watchlist)

    if not round1_scores:
        print("\nFAILED: No scores returned")
        sys.exit(1)

    print(f"\nRound 1: Scored {len(round1_scores)}/{len(watchlist)} symbols")

    # Show distribution (score range is now -1.0 to +1.0)
    strong_long = [s for s in round1_scores if s["score"] > 0.30]
    strong_short = [s for s in round1_scores if s["score"] < -0.30]
    neutral = [s for s in round1_scores if -0.30 <= s["score"] <= 0.30]
    print(f"Round 1 distribution: {len(strong_long)} long-leaning (>+0.30), "
          f"{len(strong_short)} short-leaning (<-0.30), {len(neutral)} neutral")

    print(f"\nRound 2 results: {len(longs)} final longs, {len(shorts)} final shorts")

    # Show top 5 longs
    print("\nTop 5 LONG candidates (Round 2):")
    for s in longs[:5]:
        print(f"  {s['symbol']:6s} score={s['score']:+.2f}  {s['reasoning'][:80]}")

    # Show top 5 shorts
    print("\nTop 5 SHORT candidates (Round 2):")
    for s in shorts[:5]:
        print(f"  {s['symbol']:6s} score={s['score']:+.2f}  {s['reasoning'][:80]}")

    return longs, shorts


def test_loop2(themes: str, longs: list[dict], shorts: list[dict]) -> dict | None:
    """Loop 2: Final signal generation."""
    print("\n" + "=" * 60)
    print("LOOP 2: Generating final signal")
    print("=" * 60)

    print(f"\nInput: {len(longs)} longs and {len(shorts)} shorts from tournament")

    signal = _generate_final_signal(themes, longs, shorts)

    if not signal:
        print("\nFAILED: Could not generate final signal")
        sys.exit(1)

    print(f"\nFinal signal:")
    print(json.dumps(signal, indent=2))

    # Validate signal structure
    required = ["theme", "confidence", "tickers", "do_not_trade", "reason"]
    missing = [k for k in required if k not in signal]
    if missing:
        print(f"\nWARNING: Signal missing keys: {missing}")

    ticker_count = len(signal.get("tickers", []))
    print(f"\nSignal has {ticker_count} tickers, do_not_trade={signal.get('do_not_trade')}")

    return signal


def main():
    parser = argparse.ArgumentParser(description="Test the tournament-based theory pipeline")
    parser.add_argument("--loop", type=int, choices=[0, 1, 2], default=2,
                        help="Stop after this loop (0=digest, 1=tournament, 2=signal)")
    parser.add_argument("--mock-news", action="store_true",
                        help="Use mock headlines instead of calling GDELT")
    parser.add_argument("--max-symbols", type=int, default=0,
                        help="Limit watchlist to N symbols (0=all, useful for quick tests)")
    args = parser.parse_args()

    # Step 0: Watchlist
    watchlist = test_watchlist()
    if args.max_symbols > 0:
        watchlist = watchlist[:args.max_symbols]
        print(f"\n(Limited to {args.max_symbols} symbols for testing)")

    # Fetch or mock news
    if args.mock_news:
        print("\nUsing mock news headlines")
        news_data = MOCK_NEWS
    else:
        print("\nFetching GDELT news...")
        news_data = fetch_all_topics(timespan="72h")
        total = sum(len(v) for v in news_data.values())
        if total == 0:
            print("WARNING: No GDELT articles found, falling back to mock news")
            news_data = MOCK_NEWS

    # Loop 0
    themes = test_loop0(news_data)
    if args.loop == 0:
        print("\n\nStopped after Loop 0 (--loop 0)")
        return

    # Loop 1: Tournament
    longs, shorts = test_tournament(themes, watchlist)
    if args.loop == 1:
        print("\n\nStopped after Loop 1 (--loop 1)")
        return

    # Loop 2
    signal = test_loop2(themes, longs, shorts)

    print("\n" + "=" * 60)
    print("ALL LOOPS PASSED")
    print("=" * 60)
    print(f"\nNote: Kafka publish was NOT attempted (test mode).")
    print(f"To run the full pipeline with Kafka, use the Slack bot or scheduled job.")


if __name__ == "__main__":
    main()
