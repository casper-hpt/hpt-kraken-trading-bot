import json
import logging
import time
from datetime import datetime, timezone

import psycopg

from .config import QUESTDB_HOST, QUESTDB_PORT, QUESTDB_USER, QUESTDB_PASSWORD, QUESTDB_DATABASE
from .metrics import QUESTDB_ERRORS_TOTAL, QUESTDB_QUERY_DURATION

logger = logging.getLogger(__name__)


def _get_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        dbname=QUESTDB_DATABASE,
        autocommit=True,
        connect_timeout=10,
    )


def get_watchlist_symbols() -> list[str]:
    """Fetch current watchlist symbols from QuestDB."""
    query = """
        SELECT symbol
        FROM stocks_watchlist
        LATEST ON updated_at PARTITION BY symbol
        ORDER BY symbol
    """
    t0 = time.monotonic()
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                symbols = [row[0] for row in rows]
                logger.info("Loaded %d watchlist symbols from QuestDB", len(symbols))
                return symbols
    except Exception as e:
        QUESTDB_ERRORS_TOTAL.inc()
        logger.error("Failed to query QuestDB watchlist: %s", e)
        return []
    finally:
        QUESTDB_QUERY_DURATION.labels(query="get_watchlist_symbols").set(time.monotonic() - t0)


def get_watchlist_details() -> list[dict]:
    """Fetch full watchlist details from QuestDB for LLM scoring.

    Returns a list of dicts with keys: symbol, description, sector, industry,
    country, next_earnings_date, earnings_transcript.
    """
    query = """
        SELECT symbol, description, sector, industry, country,
               next_earnings_date, earnings_transcript
        FROM stocks_watchlist
        LATEST ON updated_at PARTITION BY symbol
        ORDER BY symbol
    """
    t0 = time.monotonic()
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                results = [dict(zip(columns, row)) for row in rows]
                logger.info("Loaded %d watchlist details from QuestDB", len(results))
                return results
    except Exception as e:
        QUESTDB_ERRORS_TOTAL.inc()
        logger.error("Failed to query QuestDB watchlist details: %s", e)
        return []
    finally:
        QUESTDB_QUERY_DURATION.labels(query="get_watchlist_details").set(time.monotonic() - t0)


_CRYPTO_EVENTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS crypto_event_signals (
    ts TIMESTAMP,
    event_type SYMBOL,
    asset_scope SYMBOL,
    affected_symbols STRING,
    time_horizon SYMBOL,
    direction SYMBOL,
    confidence DOUBLE,
    novelty DOUBLE,
    tradability DOUBLE,
    persistence DOUBLE,
    catalyst_score DOUBLE,
    key_reason STRING,
    headline STRING
) TIMESTAMP(ts) PARTITION BY DAY;
"""

_CRYPTO_EVENTS_INSERT = """
INSERT INTO crypto_event_signals
    (ts, event_type, asset_scope, affected_symbols, time_horizon, direction,
     confidence, novelty, tradability, persistence, catalyst_score, key_reason, headline)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_table_ensured = False


def write_crypto_events(events: list[dict]) -> int:
    """
    Persist classified crypto events to QuestDB.

    Creates the table on first call if it doesn't exist. Returns the number
    of rows successfully inserted.
    """
    global _table_ensured
    if not events:
        return 0

    t0 = time.monotonic()
    inserted = 0
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                if not _table_ensured:
                    cur.execute(_CRYPTO_EVENTS_TABLE_DDL)
                    _table_ensured = True

                for event in events:
                    ts_str = event.get("ts") or datetime.now(timezone.utc).isoformat()
                    # QuestDB timestamps via PGWire accept ISO strings
                    affected_json = json.dumps(event.get("affected_symbols", []))
                    cur.execute(
                        _CRYPTO_EVENTS_INSERT,
                        (
                            ts_str,
                            event.get("event_type", "other"),
                            event.get("asset_scope", "MARKET-WIDE"),
                            affected_json,
                            event.get("time_horizon", "intraday"),
                            event.get("direction", "neutral"),
                            float(event.get("confidence", 0.0)),
                            float(event.get("novelty", 0.0)),
                            float(event.get("tradability", 0.0)),
                            float(event.get("persistence", 0.0)),
                            float(event.get("catalyst_score", 0.0)),
                            str(event.get("key_reason", ""))[:500],
                            str(event.get("headline", ""))[:500],
                        ),
                    )
                    inserted += 1

        logger.info("Wrote %d crypto events to QuestDB", inserted)
        return inserted
    except Exception as e:
        QUESTDB_ERRORS_TOTAL.inc()
        logger.error("Failed to write crypto events to QuestDB: %s", e, exc_info=True)
        return inserted
    finally:
        QUESTDB_QUERY_DURATION.labels(query="write_crypto_events").set(time.monotonic() - t0)
