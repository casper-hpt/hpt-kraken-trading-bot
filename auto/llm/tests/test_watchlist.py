"""Quick integration test for QuestDB watchlist access.

Usage:
    1. Port-forward QuestDB from your k3s machine:
       kubectl port-forward svc/questdb 8812:8812 --address 0.0.0.0

    2. Run this script:
       cd bot/llm
       DB_HOST=192.168.2.38 python -m tests.test_watchlist
"""

import os
import sys

# Point at the k3s host (override for local testing)
os.environ.setdefault("DB_HOST", "192.168.2.38")
os.environ.setdefault("DB_PORT", "8812")
os.environ.setdefault("DB_USER", "admin")
os.environ.setdefault("DB_PASSWORD", "quest")
os.environ.setdefault("DB_NAME", "qdb")

# Need dummy slack tokens so config.py doesn't blow up on import
os.environ.setdefault("SLACK_BOT_TOKEN", "xoxb-test")
os.environ.setdefault("SLACK_APP_TOKEN", "xapp-test")

from hpt_llm.questdb import get_watchlist_symbols


def main():
    print("Testing QuestDB watchlist connection...")
    print(f"  Host: {os.environ['DB_HOST']}:{os.environ['DB_PORT']}")

    symbols = get_watchlist_symbols()

    if not symbols:
        print("\nNo symbols returned. Either:")
        print("  - QuestDB is not reachable (port-forward not running?)")
        print("  - The watchlist table is empty")
        sys.exit(1)

    print(f"\nLoaded {len(symbols)} watchlist symbols:")
    for i, sym in enumerate(symbols):
        print(f"  {sym}", end="")
        if (i + 1) % 10 == 0:
            print()
    print()
    print("\nWatchlist query works!")


if __name__ == "__main__":
    main()
